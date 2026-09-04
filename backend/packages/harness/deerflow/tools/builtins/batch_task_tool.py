"""Explicit durable batch mode for many independent native-subagent items."""

from __future__ import annotations

import json
from collections.abc import Callable
from contextvars import ContextVar
from typing import Annotated, Any, cast

from deerflow_extension_api import TenantReferenceV1
from langchain.tools import InjectedToolCallId, tool
from langchain_core.messages import ToolMessage
from langgraph.types import Command
from pydantic import BaseModel, Field

from deerflow.authz.runtime import authorization_provider_from_context
from deerflow.config.subagent_batches_config import SubagentBatchesConfig
from deerflow.extensions import resolve_run_extensions
from deerflow.runtime.accepted_invocation import (
    AcceptedInvocation,
    ResolvedAgentMaterialV1,
)
from deerflow.runtime.agent_revision import RESOLVED_AGENT_MATERIAL_CONTEXT_KEY
from deerflow.runtime.constraints import INVOCATION_CONSTRAINTS_CONTEXT_KEY
from deerflow.runtime.execution_policy import (
    EXECUTION_POLICY_OBSERVER_CONTEXT_KEY,
    ExecutionBudgetV1,
    ExecutionPolicyObservationV1,
)
from deerflow.runtime.skill_projection import (
    SKILL_PROJECTION_TOKEN_CONTEXT_KEY,
    SkillProjectionConsumerToken,
)
from deerflow.runtime.tenant_identity import TENANT_REFERENCE_CONTEXT_KEY
from deerflow.runtime.tool_evidence import (
    get_active_tool_receipt,
    resolve_tool_evidence_context,
)
from deerflow.runtime.user_context import resolve_runtime_user_id
from deerflow.subagents.batch_acceptance import (
    PARENT_BATCH_ACCEPTANCE_CONTEXT_KEY,
    BatchAdmissionError,
    BatchItemRequestV1,
    BatchLimitsV1,
    ParentBoundBatchRequest,
)
from deerflow.subagents.batch_runtime import (
    SubagentBatchSubmitter,
    get_subagent_batch_submitter,
)
from deerflow.tools.types import Runtime


class BatchTaskItem(BaseModel):
    key: str = Field(min_length=1, max_length=128)
    prompt: str = Field(min_length=1, max_length=100_000)


_NO_EXPLICIT_BATCH_SUBMITTER = object()
_explicit_batch_submitter: ContextVar[SubagentBatchSubmitter | None | object] = ContextVar(
    "deerflow_explicit_subagent_batch_submitter",
    default=_NO_EXPLICIT_BATCH_SUBMITTER,
)
_explicit_batch_app_config: ContextVar[Any | None] = ContextVar(
    "deerflow_explicit_subagent_batch_app_config",
    default=None,
)


def _batch_submitter() -> SubagentBatchSubmitter | None:
    explicit = _explicit_batch_submitter.get()
    if explicit is not _NO_EXPLICIT_BATCH_SUBMITTER:
        return cast(SubagentBatchSubmitter | None, explicit)
    return get_subagent_batch_submitter()


def _batch_app_config(runtime: Runtime) -> Any | None:
    explicit = _explicit_batch_app_config.get()
    if explicit is not None:
        return explicit
    context = runtime.context if runtime is not None and isinstance(runtime.context, dict) else {}
    return context.get("app_config")


def _batch_thread_id(runtime: Runtime) -> str | None:
    context = runtime.context if runtime is not None and isinstance(runtime.context, dict) else {}
    accepted = context.get(PARENT_BATCH_ACCEPTANCE_CONTEXT_KEY)
    if isinstance(accepted, AcceptedInvocation):
        return accepted.thread_id
    value = context.get("thread_id")
    return value if isinstance(value, str) and value else None


def _bind_batch_tool(
    tool,
    submitter_provider: Callable[[], SubagentBatchSubmitter | None],
    app_config: Any | None,
):
    original_coroutine = tool.coroutine
    if original_coroutine is None:  # pragma: no cover - all batch tools are async
        raise RuntimeError(f"{tool.name} has no async implementation")

    async def bound_coroutine(**kwargs):
        submitter_token = _explicit_batch_submitter.set(submitter_provider())
        config_token = _explicit_batch_app_config.set(app_config)
        try:
            return await original_coroutine(**kwargs)
        finally:
            _explicit_batch_app_config.reset(config_token)
            _explicit_batch_submitter.reset(submitter_token)

    return tool.model_copy(update={"coroutine": bound_coroutine})


def bind_batch_tools(
    submitter: SubagentBatchSubmitter | None = None,
    *,
    submitter_provider: Callable[[], SubagentBatchSubmitter | None] | None = None,
    app_config: Any | None = None,
):
    """Return batch tools bound to an explicit SDK runtime submitter.

    A provider preserves runtime lifecycle semantics for already-compiled
    graphs: after their owned worker stops, the tools report unavailable and
    never fall through to another application's process-global submitter.
    """

    if (submitter is None) == (submitter_provider is None):
        raise ValueError("Provide exactly one of submitter or submitter_provider")
    provider = submitter_provider if submitter_provider is not None else lambda: submitter

    return tuple(_bind_batch_tool(tool, provider, app_config) for tool in (batch_task, batch_status, cancel_batch))


def _result(tool_call_id: str, *, content: str, batch: dict[str, Any] | None = None, error: bool = False) -> Command:
    metadata: dict[str, Any] = {"subagent_batch_error": error}
    if batch is not None:
        metadata.update(
            {
                "subagent_batch_id": batch["id"],
                "subagent_batch_status": batch["status"],
                "subagent_batch_total_items": batch["total_items"],
            }
        )
    return Command(
        update={
            "messages": [
                ToolMessage(
                    content=content,
                    tool_call_id=tool_call_id,
                    name="batch_task",
                    status="error" if error else "success",
                    additional_kwargs=metadata,
                )
            ]
        }
    )


@tool("batch_task", parse_docstring=True)
async def batch_task(
    runtime: Runtime,
    title: str,
    items: list[BatchTaskItem],
    subagent_type: str,
    tool_call_id: Annotated[str, InjectedToolCallId],
    max_live_items: int | None = None,
    max_running_items: int | None = None,
) -> Command:
    """Submit many independent items to DeerFlow's explicit durable batch mode.

    Use this only when every item is independent, idempotent or read-only, and
    can be completed without another item's output. This tool returns a batch
    identifier immediately; it never inserts thousands of results into the lead
    agent context. Use ``batch_status`` for a compact progress snapshot.

    Args:
        title: Short batch name shown to the user.
        items: Stable item keys and self-contained prompts.
        subagent_type: Native subagent definition used for every item.
        max_live_items: Optional queued-plus-running item window.
        max_running_items: Optional per-batch real execution concurrency.
    """
    submitter = _batch_submitter()
    if submitter is None:
        return _result(
            tool_call_id,
            content="Durable subagent batches are unavailable. Enable subagent_batches with a SQL database and restart Gateway.",
            error=True,
        )
    if not items:
        return _result(tool_call_id, content="A batch must contain at least one item.", error=True)
    keys = [item.key for item in items]
    if len(set(keys)) != len(keys):
        return _result(tool_call_id, content="Batch item keys must be unique.", error=True)

    context = runtime.context if runtime is not None and isinstance(runtime.context, dict) else {}
    app_config = _batch_app_config(runtime)
    accepted_parent = context.get(PARENT_BATCH_ACCEPTANCE_CONTEXT_KEY)
    material = context.get(RESOLVED_AGENT_MATERIAL_CONTEXT_KEY)
    tenant = context.get(TENANT_REFERENCE_CONTEXT_KEY)
    binding, receipt_sink = resolve_tool_evidence_context(context)
    receipt = get_active_tool_receipt()
    if not isinstance(accepted_parent, AcceptedInvocation) or not isinstance(material, ResolvedAgentMaterialV1) or not isinstance(tenant, TenantReferenceV1) or binding is None:
        return _result(
            tool_call_id,
            content="Batch submission rejected: parent_not_accepted.",
            error=True,
        )
    if receipt is None:
        return _result(
            tool_call_id,
            content="Batch submission rejected: tool_attempt_not_active.",
            error=True,
        )
    configured = getattr(app_config, "subagent_batches", None)
    batch_config = configured if isinstance(configured, SubagentBatchesConfig) else SubagentBatchesConfig()
    max_live = batch_config.default_max_live_items if max_live_items is None else max_live_items
    max_running = batch_config.default_max_running_items if max_running_items is None else max_running_items
    execution_budget = context.get("accepted_execution_budget")
    if isinstance(execution_budget, ExecutionBudgetV1):
        if len(items) > execution_budget.max_batch_items or len(items) > execution_budget.max_batch_attempts:
            return _result(
                tool_call_id,
                content="Batch submission rejected: policy_budget_exhausted.",
                error=True,
            )
        max_live = min(max_live, execution_budget.max_batch_items)
        max_running = min(max_running, execution_budget.max_batch_concurrency)
        max_attempts = min(
            batch_config.max_attempts,
            max(1, execution_budget.max_batch_attempts // len(items)),
        )
        max_total_runtime_seconds = min(
            batch_config.max_total_runtime_seconds,
            execution_budget.max_batch_runtime_seconds,
        )
    else:
        max_attempts = batch_config.max_attempts
        max_total_runtime_seconds = batch_config.max_total_runtime_seconds
    skill_token = context.get(SKILL_PROJECTION_TOKEN_CONTEXT_KEY)
    try:
        request = ParentBoundBatchRequest(
            tenant=tenant,
            accepted_parent=accepted_parent,
            resolved_parent_material=material,
            parent_tool_binding=binding,
            parent_tool_receipt=receipt,
            parent_tool_receipt_sink=receipt_sink,
            user_id=accepted_parent.principal.user_id,
            thread_id=accepted_parent.thread_id,
            run_id=binding.run_id,
            submission_key=f"{receipt.receipt_id}:{tool_call_id}",
            title=title.strip()[:256] or "Subagent batch",
            subagent_name=subagent_type,
            items=tuple(BatchItemRequestV1(key=item.key, prompt=item.prompt) for item in items),
            limits=BatchLimitsV1(
                max_live_items=max_live,
                max_running_items=max_running,
                max_attempts=max_attempts,
                max_attempt_records_per_item=(batch_config.max_attempt_records_per_item),
                max_result_chars=batch_config.max_result_chars,
                max_total_runtime_seconds=max_total_runtime_seconds,
            ),
            # Initial policy is deliberately non-cascading: parent-run
            # cancellation never mutates an independently accepted batch.
            parent_cancellable=False,
            app_config=app_config,
            extensions=resolve_run_extensions(context),
            authorization_provider=authorization_provider_from_context(context),
            invocation_constraints=context.get(INVOCATION_CONSTRAINTS_CONTEXT_KEY),
            skill_projection_token=(skill_token if isinstance(skill_token, SkillProjectionConsumerToken) else None),
        )
        batch = await submitter.accept(request)
    except BatchAdmissionError as exc:
        return _result(
            tool_call_id,
            content=f"Batch submission rejected: {exc.code}.",
            error=True,
        )
    except Exception:
        return _result(
            tool_call_id,
            content="Batch submission failed: batch_internal_error.",
            error=True,
        )
    policy_observer = context.get(EXECUTION_POLICY_OBSERVER_CONTEXT_KEY)
    if callable(policy_observer):
        await policy_observer(
            ExecutionPolicyObservationV1(
                kind="batch",
                count=len(items),
                attempt_count=len(items) * max_attempts,
                runtime_seconds=max_total_runtime_seconds,
                observation_id=f"batch_{receipt.receipt_id}",
            )
        )
    return _result(
        tool_call_id,
        batch=batch,
        content=(f"Batch {batch['id']} accepted with {batch['total_items']} items. It is running independently and survives Gateway restarts. Use batch_status for progress; do not launch ordinary task calls for these items."),
    )


@tool("batch_status", parse_docstring=True)
async def batch_status(runtime: Runtime, batch_id: str) -> str:
    """Return a compact durable batch progress snapshot.

    Args:
        batch_id: Server batch identifier returned by ``batch_task``.
    """
    submitter = _batch_submitter()
    if submitter is None:
        return "Durable subagent batches are unavailable."
    batch = await submitter.get_batch(batch_id=batch_id, user_id=resolve_runtime_user_id(runtime))
    if batch is None or batch.get("thread_id") != _batch_thread_id(runtime):
        return "Batch not found."
    return json.dumps(
        {
            "batch_id": batch["id"],
            "status": batch["status"],
            "total_items": batch["total_items"],
            "counts": batch["counts"],
            "acceptance_digest": batch.get("acceptance_digest"),
            "terminal_code": batch.get("terminal_code"),
        },
        ensure_ascii=False,
    )


@tool("cancel_batch", parse_docstring=True)
async def cancel_batch(runtime: Runtime, batch_id: str) -> str:
    """Cancel pending and running work in one durable subagent batch.

    Args:
        batch_id: Server batch identifier returned by ``batch_task``.
    """
    submitter = _batch_submitter()
    if submitter is None:
        return "Durable subagent batches are unavailable."
    user_id = resolve_runtime_user_id(runtime)
    visible = await submitter.get_batch(batch_id=batch_id, user_id=user_id)
    if visible is None or visible.get("thread_id") != _batch_thread_id(runtime):
        return "Batch not found."
    batch = await submitter.cancel_batch(batch_id=batch_id, user_id=user_id)
    if batch is None:
        return "Batch not found."
    return f"Batch {batch_id} cancellation requested."
