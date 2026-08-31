"""Task tool for delegating work to subagents."""

import asyncio
import logging
import uuid
from dataclasses import replace
from typing import TYPE_CHECKING, Annotated, Any, cast

from deerflow_extension_api import ConstraintProjectionV1, ConstraintProjectionV2, InvocationIdentityV1, SealedOriginV1, TrustedRunContextV1
from langchain.tools import InjectedToolCallId, tool
from langchain_core.callbacks import BaseCallbackManager
from langchain_core.messages import ToolMessage
from langgraph.config import get_stream_writer
from langgraph.types import Command

from deerflow.authz.principal import normalize_authz_attributes
from deerflow.authz.runtime import authorization_provider_from_context
from deerflow.config import get_app_config
from deerflow.extensions import resolve_run_extensions
from deerflow.runtime.accepted_invocation import (
    INVOCATION_IDENTITY_CONTEXT_KEY,
    INVOCATION_ORIGIN_CONTEXT_KEY,
    TRUSTED_RUN_CONTEXT_KEY,
    ResolvedAgentMaterialV1,
    canonical_digest,
)
from deerflow.runtime.agent_revision import RESOLVED_AGENT_MATERIAL_CONTEXT_KEY
from deerflow.runtime.constraints import (
    INVOCATION_CONSTRAINTS_CONTEXT_KEY,
    SUBAGENT_RESERVATION_CONTEXT_KEY,
    InvocationSubagentDispatchLedger,
    InvocationSubagentReservation,
    SubagentDispatchOutcome,
)
from deerflow.runtime.skill_projection import (
    SKILL_PROJECTION_TOKEN_CONTEXT_KEY,
    SkillProjectionConsumerToken,
)
from deerflow.runtime.tool_evidence import (
    TOOL_EVIDENCE_CONTEXT_KEY,
    TOOL_EVIDENCE_SINK_KEY,
    DurableToolReceiptSink,
    ToolEvidenceRuntimeBinding,
    cross_loop_receipt_sink,
    stable_subagent_task_id,
)
from deerflow.runtime.user_context import resolve_runtime_user_id
from deerflow.sandbox.security import LOCAL_BASH_SUBAGENT_DISABLED_MESSAGE, is_host_bash_allowed
from deerflow.subagents import SubagentExecutor, get_available_subagent_names, get_subagent_config
from deerflow.subagents.config import resolve_subagent_model_name
from deerflow.subagents.executor import (
    SubagentStatus,
    cleanup_background_task,
    get_background_task_result,
    request_cancel_background_task,
)
from deerflow.subagents.status_contract import (
    SubagentStatusValue,
    SubagentStopReasonValue,
    format_subagent_result_message,
    make_subagent_additional_kwargs,
)
from deerflow.tools.types import Runtime
from deerflow.trace_context import DEERFLOW_TRACE_METADATA_KEY, get_current_trace_id, normalize_trace_id
from deerflow.utils.custom_events import aemit_custom_event

if TYPE_CHECKING:
    from deerflow.config.app_config import AppConfig

logger = logging.getLogger(__name__)


def _is_subagent_terminal(result: Any) -> bool:
    """Return whether a background subagent result is safe to clean up."""
    return result.status in {SubagentStatus.COMPLETED, SubagentStatus.FAILED, SubagentStatus.CANCELLED, SubagentStatus.TIMED_OUT} or getattr(result, "completed_at", None) is not None


async def _await_subagent_terminal(execution_id: str, max_polls: int) -> Any | None:
    """Poll until the background subagent reaches a terminal status or we run out of polls."""
    for _ in range(max_polls):
        result = get_background_task_result(execution_id)
        if result is None:
            return None
        if _is_subagent_terminal(result):
            return result
        await asyncio.sleep(5)
    return None


async def _deferred_cleanup_subagent_task(execution_id: str, trace_id: str, max_polls: int) -> None:
    """Keep polling a cancelled subagent until it can be safely removed."""
    cleanup_poll_count = 0
    while True:
        result = get_background_task_result(execution_id)
        if result is None:
            return
        if _is_subagent_terminal(result):
            cleanup_background_task(execution_id)
            return
        if cleanup_poll_count >= max_polls:
            logger.warning(f"[trace={trace_id}] Deferred cleanup for execution {execution_id} timed out after {cleanup_poll_count} polls")
            return
        await asyncio.sleep(5)
        cleanup_poll_count += 1


def _log_cleanup_failure(cleanup_task: asyncio.Task[None], *, trace_id: str, execution_id: str) -> None:
    if cleanup_task.cancelled():
        return

    exc = cleanup_task.exception()
    if exc is not None:
        logger.error(f"[trace={trace_id}] Deferred cleanup failed for execution {execution_id}: {exc}")


_deferred_cleanup_tasks: set[asyncio.Task[None]] = set()


def _schedule_deferred_subagent_cleanup(execution_id: str, trace_id: str, max_polls: int) -> asyncio.Task[None]:
    logger.debug(f"[trace={trace_id}] Scheduling deferred cleanup for cancelled execution {execution_id}")
    cleanup_task = asyncio.create_task(_deferred_cleanup_subagent_task(execution_id, trace_id, max_polls))
    _deferred_cleanup_tasks.add(cleanup_task)
    cleanup_task.add_done_callback(_deferred_cleanup_tasks.discard)
    cleanup_task.add_done_callback(lambda task: _log_cleanup_failure(task, trace_id=trace_id, execution_id=execution_id))
    return cleanup_task


def _find_usage_recorder(runtime: Any) -> Any | None:
    """Find a callback handler with ``record_external_llm_usage_records`` in the runtime config.

    LangChain may pass ``config["callbacks"]`` in three different shapes:

    - ``None`` (no callbacks registered): no recorder.
    - A plain ``list[BaseCallbackHandler]``: iterate it directly.
    - A ``BaseCallbackManager`` instance (e.g. ``AsyncCallbackManager`` on async
      tool runs): managers are not iterable, so we unwrap ``.handlers`` first.

    Any other shape (e.g. a single handler object accidentally passed without a
    list wrapper) cannot be iterated safely; treat it as "no recorder" rather
    than raise.
    """
    if runtime is None:
        return None
    config = getattr(runtime, "config", None)
    if not isinstance(config, dict):
        return None
    callbacks = config.get("callbacks")
    if isinstance(callbacks, BaseCallbackManager):
        callbacks = callbacks.handlers
    if not callbacks:
        return None
    if not isinstance(callbacks, list):
        return None
    for cb in callbacks:
        if hasattr(cb, "record_external_llm_usage_records"):
            return cb
    return None


def _summarize_usage(records: list[dict] | None) -> dict | None:
    """Summarize token usage records into a compact dict for SSE events."""
    if not records:
        return None
    return {
        "input_tokens": sum(r.get("input_tokens", 0) or 0 for r in records),
        "output_tokens": sum(r.get("output_tokens", 0) or 0 for r in records),
        "total_tokens": sum(r.get("total_tokens", 0) or 0 for r in records),
    }


def _report_subagent_usage(runtime: Any, result: Any) -> None:
    """Report subagent token usage to the parent RunJournal, if available.

    Each subagent task must be reported only once (guarded by usage_reported).
    """
    if getattr(result, "usage_reported", True):
        return
    records = getattr(result, "token_usage_records", None) or []
    if not records:
        return
    journal = _find_usage_recorder(runtime)
    if journal is None:
        logger.debug("No usage recorder found in runtime callbacks — subagent token usage not recorded")
        return
    try:
        journal.record_external_llm_usage_records(records)
        result.usage_reported = True
    except Exception:
        logger.warning("Failed to report subagent token usage", exc_info=True)


def _get_runtime_app_config(runtime: Any) -> "AppConfig | None":
    context = getattr(runtime, "context", None)
    if isinstance(context, dict):
        app_config = context.get("app_config")
        if app_config is not None:
            return cast("AppConfig", app_config)
    return None


def _merge_skill_allowlists(parent: list[str] | None, child: list[str] | None) -> list[str] | None:
    """Return the effective subagent skill allowlist under the parent policy."""
    if parent is None:
        return child
    if child is None:
        return list(parent)

    parent_set = set(parent)
    return [skill for skill in child if skill in parent_set]


def _task_result_command(
    *,
    tool_call_id: str,
    status: SubagentStatusValue,
    result: str | None = None,
    error: str | None = None,
    stop_reason: SubagentStopReasonValue | None = None,
    model_name: str | None = None,
    usage: dict[str, int] | None = None,
) -> Command:
    content, metadata_error = format_subagent_result_message(status, result=result, error=error, stop_reason=stop_reason)
    return Command(
        update={
            "messages": [
                ToolMessage(
                    content=content,
                    tool_call_id=tool_call_id,
                    name="task",
                    additional_kwargs=make_subagent_additional_kwargs(
                        status,
                        result=result,
                        error=metadata_error,
                        stop_reason=stop_reason,
                        model_name=model_name,
                        token_usage=usage,
                    ),
                )
            ]
        }
    )


@tool("task", parse_docstring=True)
async def task_tool(
    runtime: Runtime,
    description: str,
    prompt: str,
    subagent_type: str,
    tool_call_id: Annotated[str, InjectedToolCallId],
) -> str | Command:
    """Delegate a bounded task to a specialized subagent in its own context.

    Delegate only when expected benefit clearly exceeds delegation overhead.
    Useful benefits are:
    - Material wall-clock savings from independent parallel work
    - Specialist tools, skills, models, or domain instructions
    - Context isolation for a bounded, unusually context-heavy investigation

    Built-in subagent types:
    - **general-purpose**: A capable agent for bounded exploration and action. Use
      when the assignment has clear specialist or context-isolation benefit, or is
      one of several independent, non-overlapping tasks that can actually run in
      parallel.
    - **bash**: Command execution specialist for running bash commands. This is only
      available when host bash is explicitly allowed or when using an isolated shell
      sandbox such as `AioSandboxProvider`. Use it only for a bounded shell workflow
      with clear context-isolation or independent-parallel benefit.
      Routine git, build, test, or deploy operations are not sufficient reason to delegate.

    Additional custom subagent types may be defined in config.yaml under
    `subagents.custom_agents`. Each custom type can have its own system prompt,
    tools, skills, model, and timeout configuration. If an unknown subagent_type
    is provided, the error message will list all available types.

    When to use this tool:
    - Independent tasks that materially reduce wall-clock time when run in parallel
    - A specialist subagent provides capability unavailable on the direct path
    - Bounded exploration that would otherwise displace important parent context

    When NOT to use this tool:
    - Merely because a task is complex, multi-step, verbose, or touches a large repo
    - Splitting dependent steps across parallel subagents; keep the chain together
      and delegate it as one bounded task only when specialist or context-isolation
      benefit clearly wins
    - Parallel work with overlapping files, shared mutable state, or external side effects
    - Tasks requiring user interaction or clarification

    Costs to include in the delegation decision:
    - Repeating the same repository discovery in multiple contexts
    - Coordination, verification, and synthesis of returned results
    - Any task the parent can complete more cheaply with direct tools

    Args:
        description: A short (3-5 word) description of the task for logging/display. ALWAYS PROVIDE THIS PARAMETER FIRST.
        prompt: The task description for the subagent. Be specific and clear about what needs to be done. ALWAYS PROVIDE THIS PARAMETER SECOND.
        subagent_type: The type of subagent to use. ALWAYS PROVIDE THIS PARAMETER THIRD.
    """
    runtime_app_config = _get_runtime_app_config(runtime)
    metadata: dict = runtime.config.get("metadata", {}) if runtime is not None else {}
    parent_context = runtime.context if runtime is not None else None
    parent_context = parent_context if isinstance(parent_context, dict) else {}
    resolved_agent_material = parent_context.get(RESOLVED_AGENT_MATERIAL_CONTEXT_KEY)
    if not isinstance(resolved_agent_material, ResolvedAgentMaterialV1):
        resolved_agent_material = None

    resolved_subagent_definition = None
    allowed_subagents = metadata.get("allowed_subagents")
    if resolved_agent_material is not None:
        catalog = resolved_agent_material.subagent_catalog
        available_subagent_names = list(catalog.allowed_names)
        resolved_subagent_definition = catalog.get(subagent_type)
        if resolved_subagent_definition is None:
            return _task_result_command(
                tool_call_id=tool_call_id,
                status="failed",
                error="subagent_not_accepted",
            )
        config = resolved_subagent_definition.to_subagent_config()
    else:
        if allowed_subagents is None:
            available_subagent_names = get_available_subagent_names(app_config=runtime_app_config) if runtime_app_config is not None else get_available_subagent_names()
        else:
            available_subagent_names = get_available_subagent_names(app_config=runtime_app_config, allowed_subagents=allowed_subagents) if runtime_app_config is not None else get_available_subagent_names(allowed_subagents=allowed_subagents)

        # Preserve the dedicated sandbox-policy guidance before the generic
        # registry/policy membership gate filters bash from the visible catalog.
        if subagent_type == "bash":
            host_bash_allowed = is_host_bash_allowed(runtime_app_config) if runtime_app_config is not None else is_host_bash_allowed()
            if not host_bash_allowed:
                return _task_result_command(
                    tool_call_id=tool_call_id,
                    status="failed",
                    error=LOCAL_BASH_SUBAGENT_DISABLED_MESSAGE,
                )

        # Get subagent configuration
        config = get_subagent_config(subagent_type, app_config=runtime_app_config) if runtime_app_config is not None else get_subagent_config(subagent_type)
        if config is None or subagent_type not in available_subagent_names:
            if available_subagent_names:
                available = ", ".join(available_subagent_names)
            elif allowed_subagents is not None:
                available = "none permitted by caller policy"
            else:
                available = "none"
            error = f"Unknown subagent type '{subagent_type}'. Available: {available}"
            return _task_result_command(
                tool_call_id=tool_call_id,
                status="failed",
                error=error,
            )
    # Build config overrides
    overrides: dict = {}

    # Skills are loaded by SubagentExecutor per-session (aligned with Codex's pattern:
    # each subagent loads its own skills based on config, injected as conversation items).
    # No longer appended to system_prompt here.

    # Extract parent context from runtime
    sandbox_state = None
    thread_data = None
    thread_id = None
    parent_model = None
    trace_id = None
    user_id = None
    deerflow_trace_id = None
    if runtime is not None:
        sandbox_state = runtime.state.get("sandbox")
        thread_data = runtime.state.get("thread_data")
        thread_id = runtime.context.get("thread_id") if runtime.context else None
        if thread_id is None:
            thread_id = runtime.config.get("configurable", {}).get("thread_id")

        # Try to get parent model from configurable
        parent_model = metadata.get("model_name")

        # Get or generate trace_id for distributed tracing
        trace_id = metadata.get("trace_id") or str(uuid.uuid4())[:8]

    # Get user_id for tracing (uses standard resolution order)
    user_id = resolve_runtime_user_id(runtime)

    # Propagate the authenticated runtime context so delegated tool calls are
    # evaluated by GuardrailMiddleware with the same identity/attribution as
    # the lead agent. Sourced from the server-side context written by
    # inject_authenticated_user_context (and run_id by the run worker); stays
    # None when absent (e.g. internal-auth runs) so guardrail behavior is
    # unchanged. Without this, role-aware policy silently mis-attributes any
    # tool call delegated to a subagent (user_role=None).
    user_role = parent_context.get("user_role")
    oauth_provider = parent_context.get("oauth_provider")
    oauth_id = parent_context.get("oauth_id")
    run_id = parent_context.get("run_id")
    # IM-channel sender identity: group chats share one thread across senders,
    # so delegated bash commands need the dispatching turn's channel_user_id.
    channel_user_id = parent_context.get("channel_user_id")
    # The accepted records are installed by the worker after it scrubs caller
    # context.  They remain distinct: identity describes subject authority and
    # delegation, while Origin describes the trusted source/transport.
    trusted_run_context = parent_context.get(TRUSTED_RUN_CONTEXT_KEY)
    if not isinstance(trusted_run_context, TrustedRunContextV1):
        trusted_run_context = None
    if trusted_run_context is None:
        invocation_identity = parent_context.get(INVOCATION_IDENTITY_CONTEXT_KEY)
        if not isinstance(invocation_identity, InvocationIdentityV1):
            invocation_identity = None
        invocation_origin = parent_context.get(INVOCATION_ORIGIN_CONTEXT_KEY)
        if not isinstance(invocation_origin, SealedOriginV1):
            invocation_origin = None
        # Legacy consumers still receive is_internal, but an accepted identity is
        # authoritative and a human represented by an internal service stays human.
        is_internal = invocation_identity.effective_subject.kind == "service" if invocation_identity is not None else parent_context.get("is_internal") is True
        authz_attributes = normalize_authz_attributes(parent_context.get("authz_attributes"))
    # The run's immutable extension snapshot, published by the run worker. Stays
    # None outside that path (embedded client, standalone LangGraph Server), where
    # the executor keeps its process-singleton fallback.
    run_extensions = resolve_run_extensions(parent_context)
    authorization_provider = authorization_provider_from_context(parent_context)
    invocation_constraints = parent_context.get(INVOCATION_CONSTRAINTS_CONTEXT_KEY)
    subagent_reservation = parent_context.get(SUBAGENT_RESERVATION_CONTEXT_KEY)
    accepted_extension_generation = parent_context.get("accepted_extension_generation")
    accepted_extension_manifest_digest = parent_context.get("accepted_extension_manifest_digest")
    skill_projection_token = parent_context.get(SKILL_PROJECTION_TOKEN_CONTEXT_KEY)
    if not isinstance(skill_projection_token, SkillProjectionConsumerToken):
        skill_projection_token = None
    tool_evidence_binding = parent_context.get(TOOL_EVIDENCE_CONTEXT_KEY)
    tool_evidence_sink = parent_context.get(TOOL_EVIDENCE_SINK_KEY)
    if not isinstance(tool_evidence_binding, ToolEvidenceRuntimeBinding) or not isinstance(tool_evidence_sink, DurableToolReceiptSink):
        tool_evidence_binding = None
        tool_evidence_sink = None
    from deerflow.extensions.mcp import (
        build_mcp_preparation_audit_sink,
        mcp_invocation_facts_from_context,
    )

    mcp_invocation_facts = mcp_invocation_facts_from_context(parent_context)
    mcp_preparation_audit_sink = build_mcp_preparation_audit_sink(parent_context)
    deerflow_trace_id = normalize_trace_id(parent_context.get(DEERFLOW_TRACE_METADATA_KEY)) or normalize_trace_id(metadata.get(DEERFLOW_TRACE_METADATA_KEY)) or get_current_trace_id()

    parent_available_skills = metadata.get("available_skills")
    if parent_available_skills is not None and resolved_agent_material is None:
        overrides["skills"] = _merge_skill_allowlists(list(parent_available_skills), config.skills)

    if overrides:
        config = replace(config, **overrides)

    # Get available tools (excluding task tool to prevent nesting)
    # Lazy import to avoid circular dependency
    from deerflow.tools import get_available_tools

    # Inherit parent agent's tool_groups so subagents respect the same restrictions
    parent_tool_groups = metadata.get("tool_groups")
    resolved_app_config = runtime_app_config
    if config.model == "inherit" and parent_model is None and resolved_app_config is None:
        resolved_app_config = get_app_config()
    effective_model = resolve_subagent_model_name(config, parent_model, app_config=resolved_app_config)

    # Subagents should not have subagent tools enabled (prevent recursive nesting).
    # Subagents also must not get list_uploaded_files — they have an independent
    # ThreadState where runtime.state["uploaded_files"] is absent, so the
    # current-run file exclusion would not work.
    available_tools_kwargs = {
        "model_name": effective_model,
        "groups": parent_tool_groups,
        "subagent_enabled": False,
        "include_upload_tool": False,
    }
    if resolved_app_config is not None:
        available_tools_kwargs["app_config"] = resolved_app_config
    tools = get_available_tools(**available_tools_kwargs)

    dispatch_ledger = subagent_reservation if isinstance(subagent_reservation, InvocationSubagentDispatchLedger) else None
    dispatch_ticket = None
    if dispatch_ledger is not None:
        snapshot = resolved_agent_material.skill_snapshot if resolved_agent_material is not None else None
        dispatch_intent_digest = canonical_digest(
            {
                "version": 1,
                "prompt": prompt,
                "subagent_type": subagent_type,
                "subagent_config": {
                    "name": config.name,
                    "system_prompt": config.system_prompt,
                    "tools": config.tools,
                    "disallowed_tools": config.disallowed_tools,
                    "skills": config.skills,
                    "effective_model": effective_model,
                    "max_turns": config.max_turns,
                    "timeout_seconds": config.timeout_seconds,
                },
                "effective_tools": sorted(str(getattr(item, "name", type(item).__name__)) for item in tools),
                "accepted_agent_revision_digest": parent_context.get("accepted_agent_revision_digest"),
                "accepted_extension_generation": accepted_extension_generation,
                "accepted_extension_manifest_digest": accepted_extension_manifest_digest,
                "constraint_evidence_digest": getattr(
                    invocation_constraints,
                    "evidence_digest",
                    None,
                ),
                "skill_snapshot_id": getattr(snapshot, "snapshot_id", None),
                "skill_snapshot_digest": getattr(snapshot, "content_digest", None),
                "subagent_definition_digest": (resolved_subagent_definition.definition_digest if resolved_subagent_definition is not None else None),
                "parent_subagent_catalog_digest": (resolved_agent_material.subagent_catalog.digest if resolved_agent_material is not None else None),
            }
        )
        dispatch_ticket = dispatch_ledger.acquire(
            tool_call_id,
            dispatch_intent_digest,
        )
        if dispatch_ticket.outcome is SubagentDispatchOutcome.exhausted:
            return _task_result_command(
                tool_call_id=tool_call_id,
                status="failed",
                error=f"Invocation subagent limit ({dispatch_ledger.limit}) reached",
            )
        if dispatch_ticket.outcome is SubagentDispatchOutcome.conflict:
            return _task_result_command(
                tool_call_id=tool_call_id,
                status="failed",
                error="Subagent dispatch ID was reused with different intent",
            )
        if dispatch_ticket.outcome is SubagentDispatchOutcome.replay:
            return await dispatch_ledger.replay_result(dispatch_ticket)

    def complete_dispatch(result: str | Command) -> str | Command:
        if dispatch_ledger is not None and dispatch_ticket is not None:
            dispatch_ledger.complete(dispatch_ticket, result)
        return result

    # Create executor
    executor_kwargs = {
        "config": config,
        "tools": tools,
        "parent_model": parent_model,
        "sandbox_state": sandbox_state,
        "thread_data": thread_data,
        "trace_id": trace_id,
        "channel_user_id": channel_user_id,
        "deerflow_trace_id": deerflow_trace_id,
    }
    if trusted_run_context is not None:
        executor_kwargs["trusted_run_context"] = trusted_run_context
    else:
        executor_kwargs.update(
            thread_id=thread_id,
            user_id=user_id,
            user_role=user_role,
            oauth_provider=oauth_provider,
            oauth_id=oauth_id,
            run_id=run_id,
            is_internal=is_internal,
            authz_attributes=authz_attributes,
            invocation_identity=invocation_identity,
            invocation_origin=invocation_origin,
        )
    if resolved_app_config is not None:
        executor_kwargs["app_config"] = resolved_app_config
    if run_extensions is not None:
        executor_kwargs["extensions"] = run_extensions
    if authorization_provider is not None:
        executor_kwargs["authorization_provider"] = authorization_provider
    if isinstance(invocation_constraints, (ConstraintProjectionV1, ConstraintProjectionV2)):
        executor_kwargs["invocation_constraints"] = invocation_constraints
    if isinstance(subagent_reservation, InvocationSubagentReservation):
        executor_kwargs["subagent_reservation"] = subagent_reservation
    if type(accepted_extension_generation) is int and accepted_extension_generation >= 0:
        executor_kwargs["accepted_extension_generation"] = accepted_extension_generation
    if isinstance(accepted_extension_manifest_digest, str) and len(accepted_extension_manifest_digest) == 64 and all(character in "0123456789abcdef" for character in accepted_extension_manifest_digest):
        executor_kwargs["accepted_extension_manifest_digest"] = accepted_extension_manifest_digest
    if mcp_invocation_facts is not None:
        executor_kwargs["mcp_invocation_facts"] = mcp_invocation_facts
    if mcp_preparation_audit_sink is not None:
        executor_kwargs["mcp_preparation_audit_sink"] = mcp_preparation_audit_sink
    if resolved_agent_material is not None:
        executor_kwargs["resolved_agent_material"] = resolved_agent_material
    if skill_projection_token is not None:
        executor_kwargs["skill_projection_token"] = skill_projection_token
    if tool_evidence_binding is not None and tool_evidence_sink is not None:
        executor_kwargs["tool_evidence_parent_binding"] = tool_evidence_binding
        executor_kwargs["tool_evidence_sink"] = cross_loop_receipt_sink(tool_evidence_sink)
        executor_kwargs["tool_evidence_execution_task_id"] = stable_subagent_task_id(
            tool_evidence_binding,
            parent_tool_call_id=tool_call_id,
            subagent_name=config.name,
        )
    try:
        executor = SubagentExecutor(**executor_kwargs)
    except BaseException as exc:
        if dispatch_ledger is not None and dispatch_ticket is not None:
            dispatch_ledger.fail(dispatch_ticket, exc)
        raise

    # Keep the provider tool-call ID for stream/message correlation, but use a
    # server-generated execution ID for process-wide background task control.
    try:
        if resolved_agent_material is not None:
            executor.retain_resolved_agent_material(tool_call_id)
        execution_id = executor.execute_async(prompt, task_id=tool_call_id)
    except BaseException as exc:
        if dispatch_ledger is not None and dispatch_ticket is not None:
            dispatch_ledger.fail(dispatch_ticket, exc)
        raise

    # Poll for task completion in backend (removes need for LLM to poll)
    poll_count = 0
    last_status = None
    last_message_count = 0  # Track how many AI messages we've already sent
    # Polling timeout: execution timeout + 60s buffer, checked every 5s
    max_poll_count = (config.timeout_seconds + 60) // 5

    logger.info(f"[trace={trace_id}] Started background task {tool_call_id} (execution_id={execution_id}, subagent={subagent_type}, timeout={config.timeout_seconds}s, polling_limit={max_poll_count} polls)")

    try:
        writer = get_stream_writer()
        await aemit_custom_event(
            {
                "type": "task_started",
                "task_id": tool_call_id,
                "description": description,
                "model_name": effective_model,
            },
            writer=writer,
        )
    except BaseException as exc:
        request_cancel_background_task(execution_id)
        _schedule_deferred_subagent_cleanup(execution_id, trace_id, max_poll_count)
        if dispatch_ledger is not None and dispatch_ticket is not None:
            dispatch_ledger.fail(dispatch_ticket, exc)
        raise

    try:
        while True:
            result = get_background_task_result(execution_id)

            if result is None:
                logger.error(f"[trace={trace_id}] Task {tool_call_id} execution {execution_id} not found in background tasks")
                await aemit_custom_event(
                    {"type": "task_failed", "task_id": tool_call_id, "error": "Task disappeared from background tasks"},
                    writer=writer,
                )
                cleanup_background_task(execution_id)
                error = f"Task {tool_call_id} disappeared from background tasks"
                return complete_dispatch(
                    _task_result_command(
                        tool_call_id=tool_call_id,
                        status="failed",
                        error=error,
                    )
                )

            # Log status changes for debugging
            if result.status != last_status:
                logger.info(f"[trace={trace_id}] Task {tool_call_id} execution {execution_id} status: {result.status.value}")
                last_status = result.status

            # The collector publishes cumulative records. Reuse one snapshot for
            # both live progress and the terminal event so the frontend can
            # replace, rather than add, its per-task total.
            usage = _summarize_usage(getattr(result, "token_usage_records", None))

            # Check for new AI messages and send task_running events
            ai_messages = result.ai_messages or []
            current_message_count = len(ai_messages)
            if current_message_count > last_message_count:
                # Send task_running event for each new message
                for i in range(last_message_count, current_message_count):
                    message = ai_messages[i]
                    await aemit_custom_event(
                        {
                            "type": "task_running",
                            "task_id": tool_call_id,
                            "message": message,
                            "message_index": i + 1,  # 1-based index for display
                            "total_messages": current_message_count,
                            "usage": usage,
                            "model_name": effective_model,
                        },
                        writer=writer,
                    )
                    logger.info(f"[trace={trace_id}] Task {tool_call_id} sent message #{i + 1}/{current_message_count}")
                last_message_count = current_message_count

            # Check if task completed, failed, or timed out
            if result.status == SubagentStatus.COMPLETED:
                _report_subagent_usage(runtime, result)
                await aemit_custom_event(
                    {
                        "type": "task_completed",
                        "task_id": tool_call_id,
                        "result": result.result,
                        "usage": usage,
                        "model_name": effective_model,
                    },
                    writer=writer,
                )
                logger.info(f"[trace={trace_id}] Task {tool_call_id} completed after {poll_count} polls")
                cleanup_background_task(execution_id)
                # stop_reason carries a guardrail cap (token_capped / turn_capped)
                # when the run was ended early but still produced a final answer
                # — the work survives on result_brief like a clean success.
                return complete_dispatch(
                    _task_result_command(
                        tool_call_id=tool_call_id,
                        status="completed",
                        result=result.result,
                        stop_reason=result.stop_reason,
                        model_name=effective_model,
                        usage=usage,
                    )
                )
            elif result.status == SubagentStatus.FAILED:
                _report_subagent_usage(runtime, result)
                await aemit_custom_event(
                    {
                        "type": "task_failed",
                        "task_id": tool_call_id,
                        "error": result.error,
                        "usage": usage,
                        "model_name": effective_model,
                    },
                    writer=writer,
                )
                logger.error(f"[trace={trace_id}] Task {tool_call_id} failed: {result.error}")
                cleanup_background_task(execution_id)
                # A turn-capped run with no usable output surfaces as failed +
                # stop_reason=turn_capped; the cap note lets the lead tell "out
                # of budget" from "broken subagent".
                return complete_dispatch(
                    _task_result_command(
                        tool_call_id=tool_call_id,
                        status="failed",
                        error=result.error,
                        stop_reason=result.stop_reason,
                        model_name=effective_model,
                        usage=usage,
                    )
                )
            elif result.status == SubagentStatus.CANCELLED:
                _report_subagent_usage(runtime, result)
                await aemit_custom_event(
                    {
                        "type": "task_cancelled",
                        "task_id": tool_call_id,
                        "error": result.error,
                        "usage": usage,
                        "model_name": effective_model,
                    },
                    writer=writer,
                )
                logger.info(f"[trace={trace_id}] Task {tool_call_id} cancelled: {result.error}")
                cleanup_background_task(execution_id)
                return complete_dispatch(
                    _task_result_command(
                        tool_call_id=tool_call_id,
                        status="cancelled",
                        error=result.error,
                        model_name=effective_model,
                        usage=usage,
                    )
                )
            elif result.status == SubagentStatus.TIMED_OUT:
                _report_subagent_usage(runtime, result)
                await aemit_custom_event(
                    {
                        "type": "task_timed_out",
                        "task_id": tool_call_id,
                        "error": result.error,
                        "usage": usage,
                        "model_name": effective_model,
                    },
                    writer=writer,
                )
                logger.warning(f"[trace={trace_id}] Task {tool_call_id} timed out: {result.error}")
                cleanup_background_task(execution_id)
                return complete_dispatch(
                    _task_result_command(
                        tool_call_id=tool_call_id,
                        status="timed_out",
                        error=result.error,
                        model_name=effective_model,
                        usage=usage,
                    )
                )

            # Still running, wait before next poll
            await asyncio.sleep(5)
            poll_count += 1

            # Polling timeout as a safety net (in case thread pool timeout doesn't work)
            # Set to execution timeout + 60s buffer, in 5s poll intervals
            # This catches edge cases where the background task gets stuck
            if poll_count > max_poll_count:
                timeout_minutes = config.timeout_seconds // 60
                logger.error(f"[trace={trace_id}] Task {tool_call_id} polling timed out after {poll_count} polls (should have been caught by thread pool timeout)")
                _report_subagent_usage(runtime, result)
                usage = _summarize_usage(getattr(result, "token_usage_records", None))
                await aemit_custom_event(
                    {
                        "type": "task_timed_out",
                        "task_id": tool_call_id,
                        "usage": usage,
                        "model_name": effective_model,
                    },
                    writer=writer,
                )
                # The task may still be running in the background. Signal cooperative
                # cancellation and schedule deferred cleanup to remove the entry from
                # _background_tasks once the background thread reaches a terminal state.
                request_cancel_background_task(execution_id)
                _schedule_deferred_subagent_cleanup(execution_id, trace_id, max_poll_count)
                message = f"Task polling timed out after {timeout_minutes} minutes. This may indicate the background task is stuck. Status: {result.status.value}"
                return complete_dispatch(
                    _task_result_command(
                        tool_call_id=tool_call_id,
                        status="polling_timed_out",
                        error=message,
                        model_name=effective_model,
                        usage=usage,
                    )
                )
    except asyncio.CancelledError:
        if dispatch_ledger is not None and dispatch_ticket is not None:
            dispatch_ledger.cancel(dispatch_ticket)
        # Signal the background subagent thread to stop cooperatively.
        request_cancel_background_task(execution_id)

        # Wait (shielded) for the subagent to reach a terminal state so the
        # final token usage snapshot is reported to the parent RunJournal
        # before the parent worker persists get_completion_data().
        terminal_result = None
        try:
            terminal_result = await asyncio.shield(_await_subagent_terminal(execution_id, max_poll_count))
        except asyncio.CancelledError:
            pass

        # Report whatever the subagent collected (even if we timed out).
        final_result = terminal_result or get_background_task_result(execution_id)
        if final_result is not None:
            _report_subagent_usage(runtime, final_result)
        if final_result is not None and _is_subagent_terminal(final_result):
            cleanup_background_task(execution_id)
        else:
            _schedule_deferred_subagent_cleanup(execution_id, trace_id, max_poll_count)
        raise
    except Exception as exc:
        if dispatch_ledger is not None and dispatch_ticket is not None:
            dispatch_ledger.fail(dispatch_ticket, exc)
        raise
