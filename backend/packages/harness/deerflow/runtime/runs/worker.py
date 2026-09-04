"""Background agent execution.

Runs an agent graph inside an ``asyncio.Task``, publishing events to
a :class:`StreamBridge` as they are produced.

Uses ``graph.astream(stream_mode=[...])`` which gives correct full-state
snapshots for ``values`` mode, proper ``{node: writes}`` for ``updates``,
and ``(chunk, metadata)`` tuples for ``messages`` mode.

Note: ``events`` mode is rejected by the gateway — it requires
``graph.astream_events()`` which cannot simultaneously produce ``values``
snapshots.  The JS open-source LangGraph API server works around this via
internal checkpoint callbacks that are not exposed in the Python public API.
"""

from __future__ import annotations

import asyncio
import copy
import gc
import inspect
import logging
import os
import sys
import threading
import time
import weakref
from collections.abc import AsyncIterator, Awaitable, Callable, Coroutine, Mapping
from contextlib import asynccontextmanager, nullcontext
from contextvars import Context
from dataclasses import dataclass, field, replace
from datetime import UTC, datetime
from functools import lru_cache
from typing import TYPE_CHECKING, Any, Final, Literal, cast

from deerflow_extension_api import TenantReferenceV1, VerifiedActorContextV1
from langgraph.checkpoint.base import empty_checkpoint
from langgraph.types import Overwrite

from deerflow.agents.goal_state import GoalEvaluation, GoalState
from deerflow.agents.middlewares.input_sanitization_middleware import (
    neutralize_untrusted_tags,
)
from deerflow.authz.provider import AuthorizationProvider
from deerflow.config.app_config import AppConfig
from deerflow.config.database_config import CheckpointChannelMode
from deerflow.constants import TOOL_RESULTS_DIRNAME
from deerflow.runtime.assembly_evidence import AssemblyEvidenceError
from deerflow.runtime.checkpoint_mode import (
    aensure_checkpoint_mode_compatible,
    inject_checkpoint_mode,
)
from deerflow.runtime.checkpoint_state import (
    CheckpointStateAccessor,
    build_state_mutation_graph,
    graph_reducer_channels,
    graph_state_schema,
    graph_writable_channels,
)
from deerflow.runtime.constraints import ConstraintFenceError
from deerflow.runtime.context_keys import CURRENT_RUN_PRE_EXISTING_MESSAGE_IDS_KEY
from deerflow.runtime.events.appender import (
    FencedRunEventAppender,
    RuntimeEventAuthority,
    RuntimeEventOwnershipLost,
)
from deerflow.runtime.events.catalog import (
    RUN_EXECUTION_STARTED_EVENT,
    SANDBOX_LIFECYCLE_EVENT,
)
from deerflow.runtime.events.message_identity import attach_message_seq, message_identity
from deerflow.runtime.execution_policy import ExecutionPolicyError
from deerflow.runtime.failure_evidence import RuntimeFailureV1, map_runtime_failure
from deerflow.runtime.goal import (
    DEFAULT_MAX_GOAL_CONTINUATIONS,
    DEFAULT_MAX_NO_PROGRESS_CONTINUATIONS,
    GoalWriteConflict,
    _call_checkpointer_method,
    _is_visible_message,
    _message_type,
    attach_goal_evaluation,
    compute_no_progress_count,
    create_goal_evaluator_model,
    evaluate_goal_completion,
    goal_thread_lock,
    latest_visible_assistant_signature,
    make_goal_continuation_message,
    read_thread_goal,
    should_continue_goal,
    visible_conversation_signature,
    write_thread_goal,
)
from deerflow.runtime.serialization import serialize
from deerflow.runtime.stream_bridge import StreamBridge
from deerflow.runtime.stream_modes import (
    normalize_stream_modes,
    to_langgraph_stream_modes,
)
from deerflow.runtime.tenant_identity import TENANT_REFERENCE_CONTEXT_KEY
from deerflow.runtime.user_context import get_current_user, get_effective_user_id, resolve_runtime_user_id
from deerflow.trace_context import DEERFLOW_TRACE_METADATA_KEY, ensure_trace_id
from deerflow.tracing import inject_langfuse_metadata
from deerflow.utils.messages import message_to_text
from deerflow.workspace_changes import (
    capture_workspace_snapshot,
    get_changed_output_paths,
    record_workspace_changes,
)
from deerflow.workspace_changes.types import WorkspaceSnapshot

from .manager import RunManager, RunRecord, RunStartOutcome
from .naming import resolve_root_run_name
from .recovery import (
    RECOVERY_CHECKPOINT_UNAVAILABLE_STOP_REASON,
    RECOVERY_TOOL_ATTEMPT_INDETERMINATE_STOP_REASON,
    ExecutionRecoveryDecision,
    ExecutionRecoveryDisposition,
)
from .schemas import RunStatus
from .store.base import BindAssemblyEvidenceOutcome, LifecycleType, RecoveryPolicy

if TYPE_CHECKING:
    from deerflow.sandbox.accepted_material import (
        AcceptedExecutionEvidence,
        AcceptedMaterializer,
        AcceptedMaterialLeaseV1,
        AcceptedMaterialRequest,
        AcceptedSandboxSessionBridge,
        AcceptedSkillExecutionEvidence,
    )
    from deerflow.sandbox.sandbox import Sandbox
    from deerflow.sandbox.sandbox_provider import SandboxProvider

logger = logging.getLogger(__name__)


class _ExecutionRecoveryTerminalized(RuntimeError):
    """The manager durably closed a takeover after worker preflight."""


_checkpoint_locks_guard = threading.Lock()
_checkpoint_locks_by_loop: weakref.WeakKeyDictionary[asyncio.AbstractEventLoop, dict[str, asyncio.Lock]] = weakref.WeakKeyDictionary()

# Completed LangGraph runs can leave callback Contexts and AsyncPregelLoop
# instances in unreachable reference cycles. They are collectable, but a busy
# Gateway may promote those cycles into older GC generations faster than the
# automatic collector revisits them, producing a rising post-GC heap floor.
# Coalesce terminal full collections so the cycles have a bounded lifetime
# without paying for a stop-the-world collection on every run.
_TERMINAL_CYCLE_COLLECTION_INTERVAL_SECONDS = 10.0
_TERMINAL_CYCLE_COLLECTION_INFO_THRESHOLD_SECONDS = 0.1
_terminal_cycle_collection_guard = threading.Lock()
_terminal_cycle_collection_last_at = time.monotonic()
_terminal_cycle_collection_scheduled_loops: weakref.WeakSet[asyncio.AbstractEventLoop] = weakref.WeakSet()


def _create_contextless_task(coro: Coroutine[Any, Any, Any]) -> asyncio.Task[Any]:
    """Schedule terminal housekeeping without retaining the run's ContextVars."""
    return asyncio.create_task(coro, context=Context())


def _schedule_terminal_cycle_collection() -> None:
    """Coalesce full cyclic-GC passes after completed LangGraph runs."""
    loop = asyncio.get_running_loop()
    with _terminal_cycle_collection_guard:
        if loop in _terminal_cycle_collection_scheduled_loops:
            return
        elapsed = time.monotonic() - _terminal_cycle_collection_last_at
        delay = max(0.0, _TERMINAL_CYCLE_COLLECTION_INTERVAL_SECONDS - elapsed)
        _terminal_cycle_collection_scheduled_loops.add(loop)

    async def _collect() -> None:
        global _terminal_cycle_collection_last_at

        try:
            with _terminal_cycle_collection_guard:
                now = time.monotonic()
                if now - _terminal_cycle_collection_last_at < _TERMINAL_CYCLE_COLLECTION_INTERVAL_SECONDS:
                    return
                _terminal_cycle_collection_last_at = now

            started_at = time.perf_counter()
            # Do not run a heap walk synchronously from the event-loop timer.
            # CPython's collector can still contend for the GIL, so surface slow
            # passes below rather than claiming this removes every pause.
            collected = await loop.run_in_executor(None, gc.collect)
            duration = time.perf_counter() - started_at
            if duration >= _TERMINAL_CYCLE_COLLECTION_INFO_THRESHOLD_SECONDS:
                logger.info(
                    "Terminal cyclic GC collected %d object(s) in %.3f seconds",
                    collected,
                    duration,
                )
            else:
                logger.debug(
                    "Terminal cyclic GC collected %d object(s) in %.3f seconds",
                    collected,
                    duration,
                )
        finally:
            with _terminal_cycle_collection_guard:
                _terminal_cycle_collection_scheduled_loops.discard(loop)

    def _start_collection() -> None:
        _create_contextless_task(_collect())

    # A blank Context prevents the timer itself from retaining the completed
    # run. The loop owns the TimerHandle; the WeakSet never keeps a test or
    # short-lived embedded-client event loop alive.
    loop.call_later(delay, _start_collection, context=Context())


async def _close_agent_stream(stream: Any) -> None:
    """Close a LangGraph stream deterministically after completion or early exit."""
    close = getattr(stream, "aclose", None)
    if close is None:
        return
    result = close()
    if inspect.isawaitable(result):
        await result


def _remove_callback(config: dict[str, Any], handler: Any) -> None:
    callbacks = config.get("callbacks")
    if isinstance(callbacks, list):
        callbacks[:] = [callback for callback in callbacks if callback is not handler]
        return
    remove_handler = getattr(callbacks, "remove_handler", None)
    if callable(remove_handler):
        try:
            remove_handler(handler)
        except Exception:
            logger.debug("could not detach terminal callback", exc_info=True)


def _release_run_scoped_references(
    configs: list[dict[str, Any]],
    runtime_context: dict[str, Any] | None,
    journal: Any | None,
) -> None:
    """Remove worker-owned graph references once durable finalization is done."""
    internal_context_keys = {
        "__run_journal",
        CURRENT_RUN_PRE_EXISTING_MESSAGE_IDS_KEY,
    }
    try:
        from deerflow.extensions import EXTENSION_SNAPSHOT_CONTEXT_KEY

        internal_context_keys.add(EXTENSION_SNAPSHOT_CONTEXT_KEY)
    except Exception:
        pass
    try:
        from deerflow_extension_api import EXTENSION_TASK_STORE_KEY

        internal_context_keys.add(EXTENSION_TASK_STORE_KEY)
    except Exception:
        pass

    seen_configs: set[int] = set()
    handlers = [journal] if journal is not None else []
    for runnable_config in configs:
        if not isinstance(runnable_config, dict) or id(runnable_config) in seen_configs:
            continue
        seen_configs.add(id(runnable_config))
        configurable = runnable_config.get("configurable")
        if isinstance(configurable, dict):
            configurable.pop("__pregel_runtime", None)
        context = runnable_config.get("context")
        if isinstance(context, dict):
            for key in internal_context_keys:
                context.pop(key, None)
        for handler in handlers:
            _remove_callback(runnable_config, handler)

    if isinstance(runtime_context, dict):
        for key in internal_context_keys:
            runtime_context.pop(key, None)


@asynccontextmanager
async def _checkpoint_thread_lock(thread_id: str) -> AsyncIterator[None]:
    """Serialize checkpoint mutations for one thread without blocking goal commands."""
    loop = asyncio.get_running_loop()
    with _checkpoint_locks_guard:
        locks = _checkpoint_locks_by_loop.get(loop)
        if locks is None:
            locks = {}
            _checkpoint_locks_by_loop[loop] = locks
        lock = locks.get(thread_id)
        if lock is None:
            lock = asyncio.Lock()
            locks[thread_id] = lock

    async with lock:
        yield


_DELIVERY_RECEIPT_RETRY_DELAYS_SECONDS = (0.1, 0.5)
_SKILL_PROJECTION_CLAIM_POLL_SECONDS = 0.05
_SKILL_PROJECTION_CLAIM_TIMEOUT_SECONDS = 5.0
_EXTENSION_TASK_NOTIFY_TIMEOUT_SECONDS = 3.0


class AcceptedSkillExecutionFenceError(RuntimeError):
    """The exact accepted sandbox attempt is no longer authoritative."""


@dataclass(slots=True)
class _AcceptedMaterializationResult:
    """Process-local adapter state paired with persisted execution evidence."""

    sandbox_id: str
    evidence: AcceptedExecutionEvidence | AcceptedSkillExecutionEvidence | None
    provider: SandboxProvider | None
    materializer: AcceptedMaterializer | None = None
    lease: AcceptedMaterialLeaseV1 | None = None
    sandbox: Sandbox | None = None
    request: AcceptedMaterialRequest | None = None

    async def validate(self) -> bool:
        if self.evidence is None:
            return True
        if self.materializer is not None:
            from deerflow.sandbox.accepted_material import (
                AcceptedExecutionEvidenceV1,
                AcceptedExecutionEvidenceV2,
                AcceptedMaterialLeaseV1,
            )

            if not isinstance(
                self.evidence,
                (AcceptedExecutionEvidenceV1, AcceptedExecutionEvidenceV2),
            ) or not isinstance(self.lease, AcceptedMaterialLeaseV1):
                return False
            return await self.materializer.validate(self.lease, self.evidence)
        if self.provider is None:
            return False
        return await self.provider.validate_accepted_skill_execution_async(
            self.sandbox_id,
            self.evidence,
        )

    async def renew(self) -> bool:
        if self.evidence is None:
            return True
        if self.materializer is not None:
            from deerflow.sandbox.accepted_material import AcceptedMaterialLeaseV1

            if not isinstance(self.lease, AcceptedMaterialLeaseV1):
                return False
            self.lease = await self.materializer.renew(self.lease)
            return True
        if self.provider is None:
            return False
        return await self.provider.renew_accepted_skill_execution_async(
            self.sandbox_id,
            self.evidence,
        )

    async def release(self) -> None:
        if self.materializer is not None and self.lease is not None:
            await self.materializer.release(self.lease)


async def _await_accepted_skill_projection_claim(
    *,
    user_id: str,
    thread_id: str,
    run_id: str,
    material: Any,
    abort_event: asyncio.Event,
) -> bool:
    """Wait boundedly for an interrupted predecessor's exact projection release."""
    from deerflow.runtime.skill_projection import (
        SkillProjectionBusyError,
        SkillProjectionEvidence,
        get_skill_projection_coordinator,
    )

    snapshot = material.skill_snapshot
    snapshot_id = None if snapshot is None else snapshot.snapshot_id
    evidence = SkillProjectionEvidence.from_snapshot(snapshot)
    coordinator = get_skill_projection_coordinator()
    loop = asyncio.get_running_loop()
    deadline = loop.time() + _SKILL_PROJECTION_CLAIM_TIMEOUT_SECONDS
    while not abort_event.is_set():
        if coordinator.try_claim_committed_run(
            user_id=user_id,
            thread_id=thread_id,
            run_id=run_id,
            snapshot_id=snapshot_id,
            evidence=evidence,
        ):
            return True
        remaining = deadline - loop.time()
        if remaining <= 0:
            raise SkillProjectionBusyError()
        try:
            await asyncio.wait_for(
                abort_event.wait(),
                timeout=min(_SKILL_PROJECTION_CLAIM_POLL_SECONDS, remaining),
            )
        except TimeoutError:
            pass
    return False


async def _materialize_accepted_skill_projection(
    runtime: object,
    *,
    user_id: str,
    record: RunRecord | None = None,
    claim_validator: Callable[[object], Awaitable[bool]] | None = None,
) -> _AcceptedMaterializationResult:
    """Prove accepted material before the authoritative running transition."""

    from deerflow.sandbox import get_sandbox_provider
    from deerflow.sandbox.sandbox_provider import (
        AcceptedSkillSandboxBindingError,
        accepted_skill_material_binding_from_runtime,
        ensure_accepted_skill_binding,
        invalidate_runtime_skill_projection_token,
        release_accepted_skill_consumer,
        require_runtime_accepted_skill_isolation,
    )

    context = getattr(runtime, "context", None)
    if not isinstance(context, dict):
        raise RuntimeError("accepted_skill_snapshot_runtime_identity_missing")
    thread_id = context.get("thread_id")
    if not isinstance(thread_id, str):
        raise RuntimeError("accepted_skill_snapshot_runtime_identity_missing")
    binding = accepted_skill_material_binding_from_runtime(
        runtime,
        user_id=user_id,
    )
    if binding is None:
        raise RuntimeError("accepted_skill_snapshot_runtime_identity_missing")
    provider = None
    sandbox_id: str | None = None
    sandbox = None
    request = None
    token = None
    materializer = None
    materialization_lease = None
    try:
        from deerflow.authz.sandbox_authz import (
            authorize_sandbox_execution_async,
            safe_app_config_async,
        )

        configured_app = context.get("app_config")
        await authorize_sandbox_execution_async(
            context=context,
            app_config=(configured_app if isinstance(configured_app, AppConfig) else await safe_app_config_async()),
        )
        provider = get_sandbox_provider()
        from deerflow.runtime.kubernetes_qualification import (
            accepted_sandbox_qualification_candidate_enabled,
        )
        from deerflow.sandbox.accepted_material import (
            AcceptedMaterialError,
            AcceptedMaterialExecutionClaimV1,
            AcceptedMaterialRequestV1,
            AcceptedMaterialRequestV2,
            accepted_scope_reference,
            capture_accepted_file_manifest,
            resolve_accepted_materializer,
            validate_accepted_materialization,
        )

        selection = await resolve_accepted_materializer(
            provider,
            binding=binding,
            thread_id=thread_id,
            user_id=user_id,
            require_durable_one_replica=record is not None,
            require_exact_two=(record is not None and record.recovery_policy is RecoveryPolicy.exact_two_takeover_v1),
            allow_qualification_candidate=(record is not None and accepted_sandbox_qualification_candidate_enabled()),
        )
        if selection is not None:
            from deerflow.runtime.accepted_invocation import (
                AcceptedInvocation,
                ResolvedAgentMaterialV1,
            )
            from deerflow.runtime.agent_revision import (
                RESOLVED_AGENT_MATERIAL_CONTEXT_KEY,
            )
            from deerflow.subagents.batch_acceptance import (
                PARENT_BATCH_ACCEPTANCE_CONTEXT_KEY,
            )

            material = context.get(RESOLVED_AGENT_MATERIAL_CONTEXT_KEY)
            tenant = context.get(TENANT_REFERENCE_CONTEXT_KEY)
            revision_digest = context.get("accepted_agent_revision_digest")
            snapshot = getattr(material, "skill_snapshot", None)
            skill_scopes = getattr(material, "skill_scopes", None)
            if not isinstance(material, ResolvedAgentMaterialV1) or snapshot is None or not isinstance(tenant, TenantReferenceV1) or not isinstance(revision_digest, str) or skill_scopes is None:
                raise RuntimeError("accepted_skill_snapshot_runtime_identity_missing")
            await asyncio.to_thread(material.verify_process_material)
            file_manifest = await asyncio.to_thread(
                capture_accepted_file_manifest,
                snapshot.root,
            )
            await asyncio.to_thread(material.verify_process_material)
            request_arguments = dict(
                run_id=binding.run_id,
                attempt_id=accepted_scope_reference(
                    tenant,
                    kind="attempt",
                    value=f"{binding.run_id}:{binding.generation}",
                ),
                tenant=tenant,
                user_ref=accepted_scope_reference(
                    tenant,
                    kind="user",
                    value=user_id,
                ),
                thread_ref=accepted_scope_reference(
                    tenant,
                    kind="thread",
                    value=thread_id,
                ),
                agent_revision_digest=revision_digest,
                skill_snapshot_digest=snapshot.snapshot_id,
                skill_scope_digest=skill_scopes.digest,
                file_manifest=file_manifest,
                runtime_image_digest=selection.runtime_image_digest,
                lease_expires_at=datetime.now(UTC) + selection.lease_duration,
            )
            accepted_invocation = context.get(
                PARENT_BATCH_ACCEPTANCE_CONTEXT_KEY,
            )
            tool_plane = accepted_invocation.tool_plane_revision if isinstance(accepted_invocation, AcceptedInvocation) else None
            if isinstance(accepted_invocation, AcceptedInvocation) and accepted_invocation.tenant == tenant and accepted_invocation.thread_id == thread_id and tool_plane is not None:
                request = AcceptedMaterialRequestV2.build(
                    **request_arguments,
                    accepted_invocation_ref=accepted_scope_reference(
                        tenant,
                        kind="invocation",
                        value=(f"{binding.run_id}:{accepted_invocation.runtime_identity_digest}"),
                    ),
                    accepted_invocation_digest=(accepted_invocation.runtime_identity_digest),
                    tool_plane_base_revision_digest=tool_plane["base_revision_digest"],
                    tool_plane_user_overlay_digest=tool_plane["user_overlay_digest"],
                    tool_plane_projection_digest=tool_plane["projection_digest"],
                    tool_plane_effective_digest=tool_plane["effective_digest"],
                    batch_child_attempt_ref=None,
                    capability_profile_digest=selection.capability_profile.digest,
                )
            else:
                request = AcceptedMaterialRequestV1.build(**request_arguments)
            materializer = selection.materializer
            execution_claim = None
            if record is not None:
                owner_worker_id = record.owner_worker_id
                if not isinstance(owner_worker_id, str) or not owner_worker_id or type(record.state_version) is not int:
                    raise RuntimeError(
                        "accepted_material_execution_owner_unavailable",
                    )
                expected_materialization_digest = None
                if record.execution_takeover:
                    persisted_evidence = record.execution_evidence_json
                    if isinstance(persisted_evidence, dict):
                        expected_materialization_digest = persisted_evidence.get(
                            "materialization_digest",
                        )
                    if not isinstance(expected_materialization_digest, str):
                        raise RuntimeError(
                            "accepted_material_recovery_evidence_unavailable",
                        )
                execution_claim = AcceptedMaterialExecutionClaimV1(
                    version=1,
                    tenant_digest=tenant.digest,
                    run_id=binding.run_id,
                    owner_worker_id=owner_worker_id,
                    state_version=record.state_version,
                    execution_takeover=record.execution_takeover,
                    expected_materialization_digest=(expected_materialization_digest),
                )
            if execution_claim is None:
                (
                    sandbox,
                    materialization_lease,
                    evidence,
                ) = await materializer.acquire_and_materialize(request)
            else:
                if claim_validator is None:
                    raise AcceptedMaterialError(
                        "accepted_material_claim_lost",
                    )
                try:
                    claim_current = await claim_validator(execution_claim)
                except asyncio.CancelledError:
                    raise
                except Exception:
                    raise AcceptedMaterialError(
                        "accepted_material_claim_lost",
                    ) from None
                if claim_current is not True:
                    raise AcceptedMaterialError(
                        "accepted_material_claim_lost",
                    )
                (
                    sandbox,
                    materialization_lease,
                    evidence,
                ) = await materializer.acquire_and_materialize(
                    request,
                    execution_claim=execution_claim,
                )
                try:
                    claim_current = await claim_validator(execution_claim)
                except asyncio.CancelledError:
                    raise
                except Exception:
                    raise AcceptedMaterialError(
                        "accepted_material_claim_lost",
                    ) from None
                if claim_current is not True:
                    raise AcceptedMaterialError(
                        "accepted_material_claim_lost",
                    )
            validate_accepted_materialization(
                selection=selection,
                request=request,
                lease=materialization_lease,
                evidence=evidence,
            )
            sandbox_id = sandbox.id
        elif record is None:
            sandbox_id = await provider.acquire_bound_accepted_skills_async(
                thread_id,
                user_id=user_id,
                binding=binding,
            )
            evidence = provider.accepted_skill_execution_evidence(sandbox_id)
        else:
            raise AcceptedMaterialError("sandbox_provider_unqualified")
        require_runtime_accepted_skill_isolation(
            provider,
            runtime,
            sandbox_id=sandbox_id,
        )
        bound, token, _created = ensure_accepted_skill_binding(
            runtime,
            sandbox_id=sandbox_id,
            user_id=user_id,
        )
        if bound is None:
            raise RuntimeError("accepted_skill_snapshot_binding_missing")
        await provider.bind_accepted_skill_snapshot_async(
            sandbox_id,
            thread_id=thread_id,
            user_id=user_id,
            binding=bound,
        )
        context["sandbox_id"] = sandbox_id
        return _AcceptedMaterializationResult(
            sandbox_id=sandbox_id,
            evidence=evidence,
            provider=provider,
            materializer=materializer,
            lease=materialization_lease,
            sandbox=sandbox,
            request=request,
        )
    except BaseException as exc:
        deferred_interrupt = exc if not isinstance(exc, Exception) else None
        invalidate_runtime_skill_projection_token(runtime, token)
        if token is not None:
            try:
                await asyncio.to_thread(release_accepted_skill_consumer, token)
            except Exception:
                logger.warning(
                    "Failed to release rejected accepted skill consumer",
                    exc_info=True,
                )
            except BaseException as cleanup_exc:
                if deferred_interrupt is None:
                    deferred_interrupt = cleanup_exc
                logger.warning(
                    "Rejected accepted skill consumer cleanup interrupted",
                )
        elif materializer is None and sandbox_id is not None and provider is not None:
            try:
                await asyncio.to_thread(provider.release, sandbox_id)
            except Exception:
                logger.warning(
                    "Failed to release rejected accepted sandbox",
                    exc_info=True,
                )
            except BaseException as cleanup_exc:
                if deferred_interrupt is None:
                    deferred_interrupt = cleanup_exc
                logger.warning(
                    "Rejected accepted sandbox cleanup interrupted",
                )
        if materializer is not None and materialization_lease is not None:
            try:
                await materializer.release(materialization_lease)
            except Exception:
                logger.warning(
                    "Failed to release rejected accepted materialization",
                    exc_info=True,
                )
            except BaseException as cleanup_exc:
                if deferred_interrupt is None:
                    deferred_interrupt = cleanup_exc
                logger.warning(
                    "Rejected accepted materialization cleanup interrupted",
                )
        if deferred_interrupt is not None:
            raise deferred_interrupt
        raise AcceptedSkillSandboxBindingError(
            "accepted_skill_snapshot_materialization_failed",
        ) from None


async def _publish_accepted_sandbox_lifecycle(
    event_appender: Any | None,
    session: AcceptedSandboxSessionBridge,
    *,
    start_index: int,
) -> int:
    """Publish newly observed safe diagnostics without making them authority."""

    observations = session.lifecycle_observations
    for observation in observations[start_index:]:
        logger.info(
            "Accepted sandbox lifecycle run_id=%s kind=%s provider_kind=%s qualification_scope=%s reason_code=%s evidence_digest=%s",
            observation.run_id,
            observation.kind.value,
            observation.provider_kind,
            observation.qualification_scope,
            observation.reason_code,
            observation.execution_evidence_digest,
        )
        if event_appender is None:
            continue
        try:
            await event_appender.put(
                thread_id=event_appender.authority.thread_id,
                run_id=observation.run_id,
                event_type=SANDBOX_LIFECYCLE_EVENT.event_type,
                category=SANDBOX_LIFECYCLE_EVENT.category,
                content=observation.to_persisted(),
                metadata={},
            )
        except Exception:
            logger.warning(
                "Accepted sandbox lifecycle observation could not be persisted run_id=%s kind=%s",
                observation.run_id,
                observation.kind.value,
            )
    return len(observations)


def _project_background_tasks(task_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Build the bounded model-state projection without trusting display names."""
    return [
        {
            "task_id": row["id"],
            "task_name": neutralize_untrusted_tags(str(row["task_name"])),
            "status": row["status"],
            "updated_at": row["updated_at"],
        }
        for row in task_rows
    ]


async def _persist_delivery_receipt(
    event_store: Any,
    *,
    thread_id: str,
    run_id: str,
    content: dict[str, Any],
) -> bool:
    """Persist a terminal receipt with short bounded retries.

    The owning worker still knows the real terminal outcome and renews its
    lease while this coroutine runs. Retrying here handles transient event
    store failures without handing a successful run to orphan recovery, which
    cannot reconstruct either the terminal status or the detailed receipt.
    """
    attempts = len(_DELIVERY_RECEIPT_RETRY_DELAYS_SECONDS) + 1
    for attempt in range(attempts):
        try:
            await event_store.put_if_absent(
                thread_id=thread_id,
                run_id=run_id,
                event_type="run.delivery",
                category="outputs",
                content=content,
            )
            return True
        except RuntimeEventOwnershipLost:
            raise
        except Exception as exc:
            failure = map_runtime_failure(
                code="delivery_receipt_write_failed",
                error=exc,
            )
            if attempt == attempts - 1:
                logger.warning(
                    "Delivery receipt write failed run_id=%s attempts=%d code=%s error_class=%s correlation_id=%s; applying terminal delivery semantics without a receipt",
                    run_id,
                    attempts,
                    failure.code,
                    failure.error_class,
                    failure.correlation_id,
                )
                return False
            delay = _DELIVERY_RECEIPT_RETRY_DELAYS_SECONDS[attempt]
            logger.warning(
                "Delivery receipt write failed run_id=%s attempt=%d/%d retry_seconds=%.1f code=%s error_class=%s correlation_id=%s",
                run_id,
                attempt + 1,
                attempts,
                delay,
                failure.code,
                failure.error_class,
                failure.correlation_id,
            )
            await asyncio.sleep(delay)

    return False  # pragma: no cover - loop always returns


_DELIVERY_INCOMPLETE_ERROR = "Artifact delivery incomplete: no produced output artifact was presented"
_DELIVERY_RECEIPT_FAILED_ERROR = "Artifact delivery verification failed: terminal delivery receipt could not be persisted"


def _empty_delivery_content() -> dict[str, Any]:
    return {"presented": 0, "paths": [], "by_tool": {}}


def _presented_path_covers_output(presented_path: str, produced_path: str) -> bool:
    presented_path = presented_path.rstrip("/")
    return bool(presented_path) and (produced_path == presented_path or produced_path.startswith(f"{presented_path}/"))


def _delivery_content_with_outputs(
    content: dict[str, Any],
    produced_paths: list[str],
) -> dict[str, Any]:
    """Attach a delivery verdict when this run created or modified outputs."""
    if not produced_paths:
        return content

    presented_paths = content.get("by_tool", {}).get("present_files", [])
    matched_paths = [produced_path for produced_path in produced_paths if any(_presented_path_covers_output(presented_path, produced_path) for presented_path in presented_paths)]
    satisfied = bool(matched_paths)
    return {
        **content,
        "verification": {
            "source": "outputs_changed",
            "requirement": "present_files_matches_produced_output",
        },
        "produced_paths": produced_paths,
        "presented_paths": presented_paths,
        "matched_paths": matched_paths,
        "stage": "presented" if satisfied else ("mismatched" if presented_paths else "not_started"),
        "satisfied": satisfied,
    }


def _delivery_error(content: dict[str, Any]) -> str | None:
    """Return the terminal error when no changed output was presented."""
    if not content.get("produced_paths") or content.get("satisfied") is True:
        return None
    return _DELIVERY_INCOMPLETE_ERROR


def _workspace_excluded_dir_names(app_config: AppConfig | None) -> frozenset[str]:
    """Directory names workspace snapshots must skip for this deployment.

    The tool-output budget middleware externalizes oversized tool outputs into
    a storage subdir under outputs (default ``.tool-results``). Those files are
    process feedback referenced from the budget preview via ``read_file``, not
    deliverables: counting them as produced artifacts would fail run delivery
    verification for any run that externalized a tool output without also
    presenting a real artifact. The default name is excluded by the scanner
    itself; a custom ``tool_output.storage_subdir`` (a single-segment name,
    enforced by ``ToolOutputConfig`` so the scanner's dir-name pruning always
    matches) is threaded through the snapshot capture here so before/after
    diffs stay consistent.
    """
    storage_subdir = app_config.tool_output.storage_subdir if app_config is not None else TOOL_RESULTS_DIRNAME
    return frozenset({storage_subdir})


async def _produced_output_paths(
    before: WorkspaceSnapshot | None,
    *,
    thread_id: str,
    user_id: str | None,
    extra_excluded_dir_names: frozenset[str] | None = None,
) -> list[str]:
    """Detect regular output files created or modified by this run."""
    if before is None:
        return []
    try:
        after = await capture_workspace_snapshot(
            thread_id,
            user_id=user_id,
            include_text=False,
            extra_excluded_dir_names=extra_excluded_dir_names,
        )
        return get_changed_output_paths(before, after)
    except Exception:
        logger.warning(
            "Could not detect produced output artifacts for run thread %s",
            thread_id,
            exc_info=True,
        )
        return []


# Keep this streaming policy separate from middleware write-authorization sets.
_LARGE_FILE_TOOL_NAMES = frozenset({"str_replace", "write_file"})
_LARGE_FILE_TOOL_BATCH_SIZE = 32


@dataclass
class _LargeFileToolChunkBatcher:
    """Batch file-body argument deltas to avoid quadratic browser parsing.

    Normal assistant text and non-file tool calls remain token-streamed. Large
    file arguments still update progressively, but in bounded batches instead
    of forcing the browser to reparse the growing JSON on every model token.
    """

    batch_size: int = _LARGE_FILE_TOOL_BATCH_SIZE
    tool_names: dict[tuple[str, str, str], str] = field(default_factory=dict)
    pending_identity: tuple[str, str, str] | None = None
    pending_message: Any | None = None
    pending_metadata: dict[str, Any] = field(default_factory=dict)
    pending_count: int = 0

    def push(self, chunk: Any) -> list[Any]:
        if not isinstance(chunk, tuple) or len(chunk) != 2:
            return [*self.flush(), chunk]

        message, metadata = chunk
        message_id = getattr(message, "id", None)
        tool_call_chunks = getattr(message, "tool_call_chunks", None)
        if not isinstance(message_id, str) or not message_id or not isinstance(tool_call_chunks, list) or len(tool_call_chunks) != 1:
            return [*self.flush(), chunk]

        tool_chunk = tool_call_chunks[0]
        if not isinstance(tool_chunk, dict):
            return [*self.flush(), chunk]
        index = tool_chunk.get("index")
        tool_call_id = tool_chunk.get("id")
        if isinstance(index, int):
            discriminator = f"index:{index}"
        elif isinstance(tool_call_id, str) and tool_call_id:
            discriminator = f"id:{tool_call_id}"
        else:
            discriminator = "single"
        raw_namespace = None
        if isinstance(metadata, dict):
            raw_namespace = metadata.get("langgraph_checkpoint_ns") or metadata.get("checkpoint_ns")
        namespace = raw_namespace if isinstance(raw_namespace, str) else ""
        identity = (namespace, message_id, discriminator)
        name_fragment = tool_chunk.get("name")
        tool_name = self.tool_names.get(identity, "")
        if tool_name not in _LARGE_FILE_TOOL_NAMES and isinstance(name_fragment, str) and name_fragment:
            tool_name += name_fragment
            if any(candidate.startswith(tool_name) for candidate in _LARGE_FILE_TOOL_NAMES):
                self.tool_names[identity] = tool_name
            else:
                self.tool_names.pop(identity, None)
        # Batching starts only after the accumulated name matches; split or
        # incomplete name fragments stream per-chunk until then.
        if tool_name not in _LARGE_FILE_TOOL_NAMES:
            return [*self.flush(), chunk]

        model_copy = getattr(message, "model_copy", None)
        if not callable(model_copy):
            return [*self.flush(), chunk]
        additional_kwargs = getattr(message, "additional_kwargs", None)
        sanitized_additional_kwargs = additional_kwargs
        if isinstance(additional_kwargs, dict) and ("function_call" in additional_kwargs or "tool_calls" in additional_kwargs):
            sanitized_additional_kwargs = {key: value for key, value in additional_kwargs.items() if key not in {"function_call", "tool_calls"}}
        has_non_tool_payload = bool(getattr(message, "content", None) or sanitized_additional_kwargs or getattr(message, "usage_metadata", None) or getattr(message, "response_metadata", None))
        outputs: list[Any] = []
        if self.pending_identity is not None and self.pending_identity != identity:
            outputs.extend(self.flush())
        if has_non_tool_payload:
            visible_message = model_copy(
                update={
                    "additional_kwargs": sanitized_additional_kwargs,
                    "invalid_tool_calls": [],
                    "tool_call_chunks": [],
                    "tool_calls": [],
                }
            )
            outputs.append((visible_message, metadata))

        tool_only_message = model_copy(
            update={
                "additional_kwargs": {},
                "content": "",
                "invalid_tool_calls": [],
                "response_metadata": {},
                "tool_calls": [],
                "usage_metadata": None,
            }
        )
        self.pending_identity = identity
        self.pending_message = tool_only_message if self.pending_message is None else self.pending_message + tool_only_message
        if isinstance(metadata, dict):
            self.pending_metadata.update(metadata)
        self.pending_count += 1
        if self.pending_count >= self.batch_size:
            outputs.extend(self.flush())
        return outputs

    def flush(self) -> list[Any]:
        if self.pending_message is None:
            return []
        chunk = (self.pending_message, self.pending_metadata)
        self.pending_identity = None
        self.pending_message = None
        self.pending_metadata = {}
        self.pending_count = 0
        return [chunk]

    def finish(self) -> list[Any]:
        """Flush and release identities at a values or end-of-stream boundary.

        A regular batch-size or interleaved-mode flush must retain identities
        because continuation chunks commonly omit the tool name.
        """
        chunks = self.flush()
        self.tool_names.clear()
        return chunks


# Runtime-context keys the worker owns outright. A same-named key in the
# caller's ``config['context']`` is dropped rather than merged: the Gateway
# strips ``__``-prefixed keys in build_run_config, but embedded harness callers
# have no such filter and ``deerflow_trace_id`` carries no prefix to be caught
# by it anyway.
_SERVER_OWNED_RUNTIME_CONTEXT_KEYS: Final[frozenset[str]] = frozenset(
    {
        CURRENT_RUN_PRE_EXISTING_MESSAGE_IDS_KEY,
        DEERFLOW_TRACE_METADATA_KEY,
        "__deerflow_accepted_parent_batch_context_v1",
        "__deerflow_accepted_sandbox_session_v1",
        "__deerflow_recovery_executor_v1",
    }
)

# Safe current-executor evidence for an exact-two recovery. This never
# replaces the accepted admission actor stored in TrustedRunContextV1.
RECOVERY_EXECUTOR_CONTEXT_KEY: Final[str] = "__deerflow_recovery_executor_v1"


def _build_runtime_context(
    thread_id: str,
    run_id: str,
    caller_context: Any | None,
    app_config: AppConfig | None = None,
    task_store: Any | None = None,
    extensions: Any | None = None,
    authorization_provider: AuthorizationProvider | None = None,
) -> dict[str, Any]:
    """Build the dict that becomes ``ToolRuntime.context`` for the run.

    Always includes ``thread_id`` and ``run_id``. Additional keys from the caller's
    ``config['context']`` (e.g. ``agent_name`` for the bootstrap flow — issue #2677)
    are merged in but never override ``thread_id``/``run_id`` or the server-owned
    keys in ``_SERVER_OWNED_RUNTIME_CONTEXT_KEYS``. The resolved ``AppConfig`` is
    added by the worker so tools can consume it without ambient global lookups.

    langgraph 1.1+ surfaces this as ``runtime.context`` via the parent runtime stored
    under ``config['configurable']['__pregel_runtime']`` — see
    ``langgraph.pregel.main`` where ``parent_runtime.merge(...)`` is invoked.
    """
    runtime_ctx: dict[str, Any] = {"thread_id": thread_id, "run_id": run_id}
    if isinstance(caller_context, dict):
        for key, value in caller_context.items():
            if key in _SERVER_OWNED_RUNTIME_CONTEXT_KEYS:
                continue
            runtime_ctx.setdefault(key, value)
    if app_config is not None:
        runtime_ctx["app_config"] = app_config
    if task_store is not None:
        from deerflow_extension_api import EXTENSION_TASK_STORE_KEY

        runtime_ctx[EXTENSION_TASK_STORE_KEY] = task_store
    # Publish the run's extension snapshot so work dispatched during graph
    # execution (task delegation) binds the same generation the lead agent was
    # built with, instead of re-reading a singleton that may have been replaced
    # mid-run. Written after the caller merge and popped when absent, because a
    # caller-supplied value for this host-internal key is never authoritative.
    from deerflow.extensions import EXTENSION_SNAPSHOT_CONTEXT_KEY

    if extensions is not None:
        runtime_ctx[EXTENSION_SNAPSHOT_CONTEXT_KEY] = extensions
    else:
        runtime_ctx.pop(EXTENSION_SNAPSHOT_CONTEXT_KEY, None)
    from deerflow.authz.runtime import AUTHORIZATION_PROVIDER_CONTEXT_KEY

    if authorization_provider is not None:
        runtime_ctx[AUTHORIZATION_PROVIDER_CONTEXT_KEY] = authorization_provider
    else:
        runtime_ctx.pop(AUTHORIZATION_PROVIDER_CONTEXT_KEY, None)
    from deerflow.runtime.constraints import (
        INVOCATION_CONSTRAINTS_CONTEXT_KEY,
        SUBAGENT_RESERVATION_CONTEXT_KEY,
    )

    # These objects exist only after accepted evidence passes the construction
    # fence below. A caller-provided value, including an in-process one, is not
    # an accepted projection or reservation.
    runtime_ctx.pop(INVOCATION_CONSTRAINTS_CONTEXT_KEY, None)
    runtime_ctx.pop(SUBAGENT_RESERVATION_CONTEXT_KEY, None)
    from deerflow.extensions.mcp import MCP_INVOCATION_FACTS_CONTEXT_KEY

    runtime_ctx.pop(MCP_INVOCATION_FACTS_CONTEXT_KEY, None)
    from deerflow.runtime.accepted_invocation import (
        INVOCATION_IDENTITY_CONTEXT_KEY,
        INVOCATION_ORIGIN_CONTEXT_KEY,
        TRUSTED_RUN_CONTEXT_KEY,
    )

    runtime_ctx.pop(INVOCATION_IDENTITY_CONTEXT_KEY, None)
    runtime_ctx.pop(INVOCATION_ORIGIN_CONTEXT_KEY, None)
    runtime_ctx.pop(TRUSTED_RUN_CONTEXT_KEY, None)
    runtime_ctx.pop(RECOVERY_EXECUTOR_CONTEXT_KEY, None)
    runtime_ctx.pop(TENANT_REFERENCE_CONTEXT_KEY, None)
    from deerflow.runtime.assembly_evidence import strip_assembly_evidence_requirement

    strip_assembly_evidence_requirement(runtime_ctx)
    from deerflow.runtime.tool_evidence import strip_tool_evidence_context

    # Tool evidence objects are executable host capabilities. A request may
    # use a lookalike key but can never inject a sink, fence, or anchor.
    strip_tool_evidence_context(runtime_ctx)
    from deerflow.subagents.batch_acceptance import (
        strip_parent_batch_acceptance_context,
    )

    strip_parent_batch_acceptance_context(runtime_ctx)
    runtime_ctx.pop("authz_attributes", None)
    return runtime_ctx


@dataclass(frozen=True)
class RunContext:
    """Infrastructure dependencies for a single agent run.

    Groups checkpointer, store, and persistence-related singletons so that
    ``run_agent`` (and any future callers) receive one object instead of a
    growing list of keyword arguments.
    """

    checkpointer: Any
    store: Any | None = field(default=None)
    event_store: Any | None = field(default=None)
    run_events_config: Any | None = field(default=None)
    thread_store: Any | None = field(default=None)
    mcp_task_repo: Any | None = field(default=None)
    app_config: AppConfig | None = field(default=None)
    authorization_provider: AuthorizationProvider | None = field(default=None)
    # Server-owned identity resolved once at process startup. Recovery compares
    # accepted evidence to this reference before resolving material or starting
    # any model/tool work.
    tenant: TenantReferenceV1 | None = field(default=None)
    extensions: Any | None = field(default=None)
    capability_manifest_digest: str | None = field(default=None)
    checkpoint_channel_mode: CheckpointChannelMode = "full"
    # Delta snapshot cadence frozen at startup; ``None`` means "not frozen in
    # this process" (embedded/tests) and resolves to the config default.
    checkpoint_snapshot_frequency: int | None = None
    on_run_completed: Any | None = field(default=None)
    # Restart-only seam: resolve current agent material once for a persisted
    # accepted revision. The accepting process uses the captured material on
    # RunRecord and does not call this resolver.
    agent_revision_resolver: Any | None = field(default=None, repr=False)
    # Host-owned clock used by both accepted-constraint worker fences.
    constraint_clock: Any | None = field(default=None, repr=False)
    # Exact-two recovery only: called after accepted material and assembly
    # revalidation, but before the durable graph-dispatch marker and any
    # model/tool work. The callback waits for RunManager to validate/release
    # the returned decision.
    execution_recovery_gate: (
        Callable[
            [RunRecord, object],
            Awaitable[ExecutionRecoveryDecision],
        ]
        | None
    ) = field(default=None, repr=False)
    # Exact-two recovery only: the currently authenticated internal executor,
    # separate from the original accepted actor in trusted_context.
    recovery_executor: VerifiedActorContextV1 | None = field(
        default=None,
        repr=False,
    )
    # Startup-frozen private policy keyring. Accepted rows persist only its
    # public key id; recovery must find that exact historical key here.
    execution_policy_keyring: Any | None = field(default=None, repr=False)

    def __post_init__(self) -> None:
        if self.recovery_executor is not None and not isinstance(
            self.recovery_executor,
            VerifiedActorContextV1,
        ):
            raise TypeError("recovery_executor must be VerifiedActorContextV1 or None")


def _install_runtime_context(config: dict, runtime_context: dict[str, Any]) -> None:
    from deerflow.runtime.assembly_evidence import (
        REQUIRE_ASSEMBLY_EVIDENCE_CONTEXT_KEY,
        strip_assembly_evidence_requirement,
    )

    configurable = config.get("configurable")
    if isinstance(configurable, dict):
        strip_assembly_evidence_requirement(configurable)
        from deerflow.runtime.tool_evidence import strip_tool_evidence_context

        strip_tool_evidence_context(configurable)
        from deerflow.subagents.batch_acceptance import (
            strip_parent_batch_acceptance_context,
        )

        strip_parent_batch_acceptance_context(configurable)
    existing_context = config.get("context")
    if isinstance(existing_context, dict):
        from deerflow.sandbox.accepted_material import (
            ACCEPTED_SANDBOX_SESSION_CONTEXT_KEY,
            strip_accepted_sandbox_session,
        )

        strip_accepted_sandbox_session(existing_context)
        strip_assembly_evidence_requirement(existing_context)
        from deerflow.runtime.tool_evidence import (
            TOOL_EVIDENCE_CONTEXT_KEY,
            TOOL_EVIDENCE_SINK_KEY,
            strip_tool_evidence_context,
        )

        strip_tool_evidence_context(existing_context)
        from deerflow.subagents.batch_acceptance import (
            PARENT_BATCH_ACCEPTANCE_CONTEXT_KEY,
            strip_parent_batch_acceptance_context,
        )

        strip_parent_batch_acceptance_context(existing_context)
        if REQUIRE_ASSEMBLY_EVIDENCE_CONTEXT_KEY in runtime_context:
            existing_context[REQUIRE_ASSEMBLY_EVIDENCE_CONTEXT_KEY] = runtime_context[REQUIRE_ASSEMBLY_EVIDENCE_CONTEXT_KEY]
        existing_context.setdefault("thread_id", runtime_context["thread_id"])
        existing_context.setdefault("run_id", runtime_context["run_id"])
        # Assigned, not setdefault: this is a server-owned key, the same rule
        # _bind_trace_id applies to the runtime context and the run metadata. A
        # deerflow_trace_id the caller put in body.config.context is an echo of
        # a past output, not an input, and leaving it would make this one dict
        # disagree with the response header and the logs.
        if DEERFLOW_TRACE_METADATA_KEY in runtime_context:
            existing_context[DEERFLOW_TRACE_METADATA_KEY] = runtime_context[DEERFLOW_TRACE_METADATA_KEY]
        if "app_config" in runtime_context:
            existing_context["app_config"] = runtime_context["app_config"]
        from deerflow.authz.runtime import AUTHORIZATION_PROVIDER_CONTEXT_KEY

        if AUTHORIZATION_PROVIDER_CONTEXT_KEY in runtime_context:
            existing_context[AUTHORIZATION_PROVIDER_CONTEXT_KEY] = runtime_context[AUTHORIZATION_PROVIDER_CONTEXT_KEY]
        if CURRENT_RUN_PRE_EXISTING_MESSAGE_IDS_KEY in runtime_context:
            existing_context[CURRENT_RUN_PRE_EXISTING_MESSAGE_IDS_KEY] = runtime_context[CURRENT_RUN_PRE_EXISTING_MESSAGE_IDS_KEY]
        from deerflow.runtime.agent_revision import RESOLVED_AGENT_MATERIAL_CONTEXT_KEY

        for internal_key in (
            RESOLVED_AGENT_MATERIAL_CONTEXT_KEY,
            PARENT_BATCH_ACCEPTANCE_CONTEXT_KEY,
            ACCEPTED_SANDBOX_SESSION_CONTEXT_KEY,
            "accepted_agent_revision_digest",
            "accepted_extension_generation",
            "accepted_extension_manifest_digest",
            "accepted_extension_artifact_manifest_digest",
            "accepted_extension_configuration_digest",
            "accepted_execution_budget",
            "execution_policy_keyring",
        ):
            if internal_key in runtime_context:
                existing_context[internal_key] = runtime_context[internal_key]
        from deerflow.runtime.constraints import (
            INVOCATION_CONSTRAINTS_CONTEXT_KEY,
            SUBAGENT_RESERVATION_CONTEXT_KEY,
        )

        for internal_key in (
            INVOCATION_CONSTRAINTS_CONTEXT_KEY,
            SUBAGENT_RESERVATION_CONTEXT_KEY,
            TENANT_REFERENCE_CONTEXT_KEY,
        ):
            if internal_key in runtime_context:
                existing_context[internal_key] = runtime_context[internal_key]
            else:
                existing_context.pop(internal_key, None)
        from deerflow.extensions.mcp import MCP_INVOCATION_FACTS_CONTEXT_KEY

        if MCP_INVOCATION_FACTS_CONTEXT_KEY in runtime_context:
            existing_context[MCP_INVOCATION_FACTS_CONTEXT_KEY] = runtime_context[MCP_INVOCATION_FACTS_CONTEXT_KEY]
        else:
            existing_context.pop(MCP_INVOCATION_FACTS_CONTEXT_KEY, None)
        from deerflow.runtime.accepted_invocation import (
            INVOCATION_IDENTITY_CONTEXT_KEY,
            INVOCATION_ORIGIN_CONTEXT_KEY,
            TRUSTED_RUN_CONTEXT_KEY,
        )

        for internal_key in (
            INVOCATION_IDENTITY_CONTEXT_KEY,
            INVOCATION_ORIGIN_CONTEXT_KEY,
            TRUSTED_RUN_CONTEXT_KEY,
        ):
            if internal_key in runtime_context:
                existing_context[internal_key] = runtime_context[internal_key]
            else:
                existing_context.pop(internal_key, None)
        for internal_key in (
            TOOL_EVIDENCE_CONTEXT_KEY,
            TOOL_EVIDENCE_SINK_KEY,
        ):
            if internal_key in runtime_context:
                existing_context[internal_key] = runtime_context[internal_key]
        if INVOCATION_ORIGIN_CONTEXT_KEY in runtime_context:
            existing_context.pop("authz_attributes", None)
            for compatibility_key in (
                "user_id",
                "user_role",
                "oauth_provider",
                "oauth_id",
                "channel_user_id",
                "is_internal",
            ):
                if compatibility_key in runtime_context:
                    existing_context[compatibility_key] = runtime_context[compatibility_key]
                else:
                    existing_context.pop(compatibility_key, None)
        return

    config["context"] = dict(runtime_context)


def _install_pinned_agent_facts(runtime_context: dict[str, Any], material: Any) -> None:
    """Replace mutable factory inputs with the facts covered by the revision."""
    defaults = material.runtime_defaults
    for key in (
        "agent_name",
        "is_bootstrap",
        "thinking_enabled",
        "reasoning_effort",
        "is_plan_mode",
        "subagent_enabled",
        "max_concurrent_subagents",
        "max_total_subagents",
        "non_interactive",
        "channel_name",
    ):
        if key not in defaults:
            continue
        value = defaults[key]
        if value is None:
            runtime_context.pop(key, None)
        else:
            runtime_context[key] = value
    selected_model = material.model_profile.get("name")
    if isinstance(selected_model, str) and selected_model:
        runtime_context["model_name"] = selected_model
        runtime_context.pop("model", None)


def _compute_agent_factory_supports_app_config(agent_factory: Any) -> bool:
    try:
        return "app_config" in inspect.signature(agent_factory).parameters
    except (TypeError, ValueError):
        return False


@lru_cache(maxsize=128)
def _cached_agent_factory_supports_app_config(agent_factory: Any) -> bool:
    return _compute_agent_factory_supports_app_config(agent_factory)


def _agent_factory_supports_app_config(agent_factory: Any) -> bool:
    try:
        return _cached_agent_factory_supports_app_config(agent_factory)
    except TypeError:
        # Some callable instances are unhashable; fall back to a direct check.
        return _compute_agent_factory_supports_app_config(agent_factory)


def _split_agent_factory_result(agent_result: Any) -> tuple[Any, Any | None]:
    """Split a host assembly while retaining bare-graph compatibility."""

    try:
        from deerflow.agents.lead_agent.agent import split_agent_factory_result
    except Exception:
        return agent_result, None
    return split_agent_factory_result(agent_result)


class _SubagentEventBuffer:
    """Buffer subagent ``task_*`` step events and flush them in one locked batch (#3779).

    The live SSE bridge already forwards these events for real-time display; this
    additionally writes them so the subtask card's step history survives a reload.

    ``RunEventStore.put`` is documented as a low-frequency path — on Postgres each
    call opens its own transaction and takes a per-thread advisory lock. A deep
    subagent (``general-purpose`` runs up to ``max_turns=150``) emits hundreds of
    ``task_running`` steps on the hot stream loop, so persisting each with
    ``put()`` would serialize against the run's own message-batch writer. This
    accumulates recognized subagent events and writes them with ``put_batch``,
    which acquires the lock once per batch, honoring the store's contract.

    Best-effort: a missing store (run_events not configured) or an unrecognized
    chunk is a no-op, flush failures are logged but never propagate into the
    stream loop, and terminal ``subagent.end`` events flush eagerly so a completed
    subagent's step history is durable promptly rather than only at run end.
    """

    #: Flush once this many events are buffered, bounding memory and reload lag on
    #: a single deep subagent without paying a per-step lock.
    FLUSH_THRESHOLD = 25

    def __init__(self, event_store: Any | None, thread_id: str, run_id: str) -> None:
        self._event_store = event_store
        self._thread_id = thread_id
        self._run_id = run_id
        self._pending: list[dict[str, Any]] = []

    async def add(self, chunk: Any) -> None:
        """Buffer one custom stream chunk; flush on a terminal event or threshold."""
        if self._event_store is None:
            return
        # Lazy import: importing deerflow.subagents at module load triggers its
        # package __init__ (executor → agents → tools → task_tool), which imports
        # back from deerflow.subagents and deadlocks at gateway startup. Deferring
        # it to call time (after all modules are loaded) breaks that cycle.
        from deerflow.subagents.step_events import subagent_run_event

        record = subagent_run_event(chunk)
        if record is None:
            return
        self._pending.append({"thread_id": self._thread_id, "run_id": self._run_id, **record})
        if record["event_type"] == "subagent.end" or len(self._pending) >= self.FLUSH_THRESHOLD:
            await self.flush()

    async def flush(self) -> None:
        """Persist buffered events in one ``put_batch`` call; swallow store errors."""
        if self._event_store is None or not self._pending:
            return
        batch = self._pending
        self._pending = []
        try:
            await self._event_store.put_batch(batch)
        except RuntimeEventOwnershipLost:
            self._pending = batch + self._pending
            raise
        except Exception as exc:
            # Re-buffer the failed batch (ahead of any events queued since) so a
            # transient store error does not silently drop subagent step events.
            self._pending = batch + self._pending
            failure = map_runtime_failure(
                code="subagent_event_write_failed",
                error=exc,
            )
            logger.warning(
                "Subagent event write failed run_id=%s events=%d code=%s error_class=%s correlation_id=%s",
                self._run_id,
                len(batch),
                failure.code,
                failure.error_class,
                failure.correlation_id,
            )


def _bind_trace_id(config: dict[str, Any], runtime_ctx: dict[str, Any]) -> str:
    """Record the current request trace id on the runtime context and metadata.

    The ContextVar is the only source. A ``deerflow_trace_id`` the caller sent
    in ``config["metadata"]`` is overwritten rather than read: honouring it
    would let the persisted run disagree with the ``X-Trace-Id`` and the log
    lines the same request already produced, which is the correlation the id
    exists to provide in the first place.

    The two destinations serve different purposes. ``runtime_ctx`` is the
    carrier across boundaries the ContextVar does not cross -- subagent
    delegation, the memory update running on a Timer/executor thread -- while
    ``config["metadata"]`` is persisted with the checkpoint and is what makes a
    finished run traceable after the fact.
    """
    trace_id = ensure_trace_id()
    runtime_ctx[DEERFLOW_TRACE_METADATA_KEY] = trace_id
    incoming_metadata = config.get("metadata")
    # Replaced rather than mutated through: this mapping can be shared with the
    # caller's request body.
    merged_metadata = dict(incoming_metadata) if isinstance(incoming_metadata, dict) else {}
    merged_metadata[DEERFLOW_TRACE_METADATA_KEY] = trace_id
    config["metadata"] = merged_metadata
    return trace_id


async def run_agent(
    bridge: StreamBridge,
    run_manager: RunManager,
    record: RunRecord,
    *,
    ctx: RunContext,
    agent_factory: Any,
    graph_input: dict,
    config: dict,
    stream_modes: list[str] | None = None,
    stream_subgraphs: bool = False,
    interrupt_before: list[str] | Literal["*"] | None = None,
    interrupt_after: list[str] | Literal["*"] | None = None,
) -> None:
    """Execute an agent in the background, publishing events to *bridge*."""

    # Unpack infrastructure dependencies from RunContext.
    checkpointer = ctx.checkpointer
    store = ctx.store
    event_store = ctx.event_store
    run_events_config = ctx.run_events_config
    thread_store = ctx.thread_store
    terminal_status_kwargs = {"persist": False} if event_store is not None else {}

    run_id = record.run_id
    thread_id = record.thread_id

    from deerflow_extension_api import ExtensionData, TaskInfo

    from deerflow.extensions import get_loaded_extensions
    from deerflow.extensions.notify import (
        lead_task_id,
        lead_task_outcome,
        notify_task_start,
        notify_task_stop,
    )
    from deerflow.persistence.thread_meta.base import ThreadMetaRunProjection

    extensions = ctx.extensions if ctx.extensions is not None else get_loaded_extensions()
    task_store: ExtensionData | None = None
    task_info: TaskInfo | None = None
    deferred_stop_interrupt: BaseException | None = None

    def _defer_stop_interrupt(exc: BaseException) -> None:
        """Retain the first control-flow interruption until cleanup is done."""

        nonlocal deferred_stop_interrupt
        if deferred_stop_interrupt is None:
            deferred_stop_interrupt = exc

    async def _await_terminal_cleanup(
        awaitable: Awaitable[Any],
        *,
        interrupted_result: Any = None,
        interrupt_current: bool = False,
        propagate_inner_interrupt: bool = False,
    ) -> Any:
        """Finish one cleanup await despite repeated caller cancellation.

        Observer hooks remain interruptible so shutdown cannot wait forever on
        third-party code. All ownership and resource cleanup is shielded and
        drained before the first interruption is re-raised.
        """

        cleanup_task = asyncio.ensure_future(awaitable)
        if interrupt_current and deferred_stop_interrupt is not None:
            cleanup_task.cancel()
        while True:
            try:
                return await asyncio.shield(cleanup_task)
            except Exception:
                raise
            except BaseException as exc:
                _defer_stop_interrupt(exc)
                if interrupt_current and not cleanup_task.done():
                    cleanup_task.cancel()
                if not cleanup_task.done():
                    continue
                if cleanup_task.cancelled():
                    if propagate_inner_interrupt:
                        cleanup_task.result()
                    return interrupted_result
                try:
                    return cleanup_task.result()
                except Exception:
                    raise
                except BaseException as cleanup_exc:
                    _defer_stop_interrupt(cleanup_exc)
                    if propagate_inner_interrupt:
                        raise cleanup_exc
                    return interrupted_result

    pre_run_checkpoint_id: str | None = None
    pre_run_workspace_snapshot: WorkspaceSnapshot | None = None
    workspace_changes_user_id: str | None = None
    workspace_excluded_dir_names: frozenset[str] | None = None
    snapshot_capture_failed = False
    llm_error_fallback_message: str | None = None
    checkpoint_rollback_completed = False
    # Message ids checkpointed *before* this run started. The stream loop uses
    # this set to mask out ``deerflow_error_fallback`` markers that belong to
    # earlier runs on the same thread — without it, one stale fallback in
    # history would mark every subsequent run on this thread as ``error``.
    pre_existing_message_ids: set[str] = set()

    # Bound agent graph accessor + captured pre-run rollback point; assigned
    # inside the try block so the finally rollback path can fork the pre-run
    # checkpoint lineage (see below).
    accessor: CheckpointStateAccessor | None = None
    rollback_point: RollbackPoint | None = None
    journal = None
    event_appender: Any | None = None
    terminal_failure: RuntimeFailureV1 | None = None
    runtime_event_authority_rejected = False
    runtime_ctx: dict[str, Any] | None = None
    runtime: Any | None = None
    agent: Any | None = None
    runnable_configs: list[dict[str, Any]] = [config]
    goal_evaluator_model: Any | None = None
    delivery_content: dict[str, Any] | None = None
    produced_output_paths: list[str] | None = None
    # Journal construction moved ahead of preflight so every terminal run can
    # emit a receipt. Completion persistence keeps its prior boundary: before
    # #4272 the journal did not exist until preflight had succeeded, so early
    # checkpoint failures / cancellation while waiting did not write an empty
    # completion snapshot into RunStore.
    persist_completion = False
    # Buffers subagent step events for batched persistence (#3779); assigned once
    # streaming starts and flushed in the finally block. Pre-bound to None so the
    # finally is safe even if an exception fires before streaming begins.
    subagent_events: _SubagentEventBuffer | None = None
    started = False
    accepted_constraints = None
    accepted_for_cleanup = record.accepted_invocation
    requires_assembly_evidence = accepted_for_cleanup is not None and run_manager.requires_assembly_evidence
    requires_tool_receipt_evidence = accepted_for_cleanup is not None and accepted_for_cleanup.tool_receipt_evidence_version in (1, 2, 3)
    assembly_evidence_bound = False
    pinned_material_for_cleanup = accepted_for_cleanup.agent_revision.material if accepted_for_cleanup is not None else None
    skill_binding_user_id: str | None = None
    materialization: _AcceptedMaterializationResult | None = None
    materialization_evidence = None
    accepted_sandbox_session: AcceptedSandboxSessionBridge | None = None
    accepted_sandbox_lifecycle_count = 0
    dispatch_ledger = None
    thread_projection_owner_id: str | None = None
    thread_projection_active_state_version: int | None = None

    async def _publish_accepted_sandbox_lifecycle_during_cleanup() -> None:
        nonlocal accepted_sandbox_lifecycle_count, deferred_stop_interrupt

        if accepted_sandbox_session is None:
            return
        try:
            accepted_sandbox_lifecycle_count = await _publish_accepted_sandbox_lifecycle(
                event_appender,
                accepted_sandbox_session,
                start_index=accepted_sandbox_lifecycle_count,
            )
        except BaseException as exc:
            if deferred_stop_interrupt is None:
                deferred_stop_interrupt = exc
                logger.warning(
                    "Accepted sandbox lifecycle publication interrupted for run %s; completing terminal cleanup first",
                    run_id,
                )
            else:
                logger.warning(
                    "Accepted sandbox lifecycle publication failed while another terminal interruption was pending for run %s",
                    run_id,
                    exc_info=True,
                )

    async def _finish_cancellation(
        action: str,
        *,
        restore_checkpoint: bool = True,
    ) -> None:
        nonlocal checkpoint_rollback_completed
        if requires_assembly_evidence and not assembly_evidence_bound:
            restore_checkpoint = False
        await run_manager.set_finalizing(run_id, True)
        if action == "rollback":
            await run_manager.set_status(
                run_id,
                RunStatus.error,
                error="Rolled back by user",
                lifecycle_type=LifecycleType.cancelled,
                **terminal_status_kwargs,
            )
            if record.ownership_lost:
                return
            if not restore_checkpoint:
                return
            try:
                checkpoint_rollback_completed = await _rollback_to_pre_run_checkpoint(
                    accessor=accessor,
                    checkpointer=checkpointer,
                    thread_id=thread_id,
                    run_id=run_id,
                    rollback_point=rollback_point,
                    snapshot_capture_failed=snapshot_capture_failed,
                )
                logger.info(
                    "Run %s rolled back to pre-run checkpoint %s",
                    run_id,
                    pre_run_checkpoint_id,
                )
            except Exception:
                logger.warning(
                    "Run %s cancellation rollback failed",
                    run_id,
                    exc_info=True,
                )
        else:
            await run_manager.set_status(
                run_id,
                RunStatus.interrupted,
                lifecycle_type=LifecycleType.cancelled,
                **terminal_status_kwargs,
            )
            logger.info("Run %s was cancelled", run_id)

    try:
        normalized_stream_modes = normalize_stream_modes(stream_modes)
        requested_modes: set[str] = set(normalized_stream_modes)
        lg_modes = to_langgraph_stream_modes(normalized_stream_modes)
        # Initialize the run-scoped journal before any fallible or cancellable
        # preflight work. Every terminal run with an event store must reach the
        # shared finally block with a journal available for its run.delivery
        # receipt, including checkpoint validation failures and cancellation
        # while waiting for an earlier run to finish finalizing.
        if event_store is not None:
            from deerflow.runtime.journal import RunJournal

            owner_id = record.owner_worker_id
            lease_epoch = record.state_version
            if not isinstance(owner_id, str) or not owner_id or type(lease_epoch) is not int:
                raise RuntimeEventOwnershipLost("runtime_event_authority_unavailable")
            authority = RuntimeEventAuthority(
                tenant=ctx.tenant,
                thread_id=thread_id,
                run_id=run_id,
                owner_id=owner_id,
                lease_epoch=lease_epoch,
            )

            async def _current_runtime_event_authority() -> RuntimeEventAuthority:
                current_owner = record.owner_worker_id
                current_epoch = record.state_version
                if record.ownership_lost or not isinstance(current_owner, str) or not current_owner or type(current_epoch) is not int:
                    raise RuntimeEventOwnershipLost("runtime_event_ownership_lost")
                return RuntimeEventAuthority(
                    tenant=ctx.tenant,
                    thread_id=thread_id,
                    run_id=run_id,
                    owner_id=current_owner,
                    lease_epoch=current_epoch,
                )

            async def _process_local_authority_is_current(
                candidate: RuntimeEventAuthority,
            ) -> bool:
                return bool(
                    not record.ownership_lost
                    and candidate.run_id == record.run_id
                    and candidate.thread_id == record.thread_id
                    and candidate.owner_id == record.owner_worker_id
                    and candidate.lease_epoch == record.state_version
                    and candidate.tenant == ctx.tenant
                )

            event_appender = FencedRunEventAppender(
                event_store,
                authority,
                process_local_validator=(None if run_manager.heartbeat_enabled else _process_local_authority_is_current),
                authority_provider=_current_runtime_event_authority,
            )
            journal = RunJournal(
                run_id=run_id,
                thread_id=thread_id,
                event_store=event_appender,
                track_token_usage=getattr(run_events_config, "track_token_usage", True),
                progress_reporter=lambda snapshot: run_manager.update_run_progress(run_id, **snapshot),
            )

        accepted_tenant = accepted_for_cleanup.tenant if accepted_for_cleanup is not None else None
        if accepted_for_cleanup is not None and accepted_tenant != ctx.tenant:
            error = "Accepted invocation tenant does not match this deployment"
            await run_manager.set_status_if_not_cancelled(
                run_id,
                RunStatus.error,
                error=error,
                stop_reason="tenant_identity_mismatch",
                **terminal_status_kwargs,
            )
            await bridge.publish(
                run_id,
                "error",
                {
                    "message": error,
                    "name": "TenantIdentityMismatchError",
                },
            )
            return

        if accepted_for_cleanup is not None and accepted_for_cleanup.extension_artifact_manifest_digest is not None:
            process_tuple = (
                int(getattr(extensions, "generation", -1)),
                ctx.capability_manifest_digest,
                getattr(extensions, "artifact_manifest_digest", None),
                getattr(extensions, "extension_configuration_digest", None),
            )
            accepted_tuple = (
                accepted_for_cleanup.extension_generation,
                accepted_for_cleanup.extension_manifest_digest,
                accepted_for_cleanup.extension_artifact_manifest_digest,
                accepted_for_cleanup.extension_configuration_digest,
            )
            if process_tuple != accepted_tuple:
                error = "Accepted extension provenance does not match this process"
                await run_manager.set_status_if_not_cancelled(
                    run_id,
                    RunStatus.error,
                    error=error,
                    stop_reason="extension_provenance_mismatch",
                    **terminal_status_kwargs,
                )
                await bridge.publish(
                    run_id,
                    "error",
                    {
                        "message": error,
                        "name": "ExtensionProvenanceMismatchError",
                    },
                )
                return

        # Keep cancellable preflight work under the worker's terminal guard so
        # cancellation cannot strand a pending RunRecord or stream subscriber.
        if ctx.mcp_task_repo is not None and record.user_id is not None:
            try:
                task_rows = await ctx.mcp_task_repo.list_by_thread(
                    thread_id,
                    user_id=record.user_id,
                    limit=20,
                    tenant_digest=ctx.mcp_task_repo.tenant.digest,
                )
                graph_input = {
                    **graph_input,
                    "background_tasks": _project_background_tasks(task_rows),
                }
            except Exception:
                logger.warning("Run %s: failed to project MCP task state", run_id, exc_info=True)

        await run_manager.wait_for_prior_finalizing(
            thread_id,
            run_id,
            abort_event=record.abort_event,
        )

        if pinned_material_for_cleanup is not None:
            skill_binding_user_id = record.user_id or get_effective_user_id()
            await _await_accepted_skill_projection_claim(
                user_id=skill_binding_user_id,
                thread_id=thread_id,
                run_id=run_id,
                material=pinned_material_for_cleanup,
                abort_event=record.abort_event,
            )

        task_id = lead_task_id(run_id)
        if extensions.needs_task_store:
            task_store = ExtensionData(task_id)
        mode = ctx.checkpoint_channel_mode
        inject_checkpoint_mode(config, mode)
        checkpoint_config = {
            "configurable": {
                "thread_id": thread_id,
                "checkpoint_ns": "",
            }
        }
        checkpoint_preflight_configs = [checkpoint_config]
        if checkpointer is not None:
            configurable = config["configurable"]
            selected_configurable = {
                "thread_id": thread_id,
                "checkpoint_ns": configurable.get("checkpoint_ns", ""),
            }
            for selector_key in ("checkpoint_id", "checkpoint_map"):
                if selector_key in configurable:
                    selected_configurable[selector_key] = configurable[selector_key]
            selected_checkpoint_config = {
                "configurable": selected_configurable,
            }
            if selected_checkpoint_config != checkpoint_config:
                checkpoint_preflight_configs.append(selected_checkpoint_config)
            if not requires_assembly_evidence:
                for preflight_config in checkpoint_preflight_configs:
                    await aensure_checkpoint_mode_compatible(
                        checkpointer,
                        preflight_config,
                        mode,
                    )

        persist_completion = True

        if event_store is not None:
            workspace_changes_user_id = get_effective_user_id()
            # Resolved once per run so the pre-run snapshot, the post-run
            # delivery scan, and the workspace-changes scan all agree on the
            # same exclusion set.
            workspace_excluded_dir_names = _workspace_excluded_dir_names(ctx.app_config)
            try:
                pre_run_workspace_snapshot = await capture_workspace_snapshot(
                    thread_id,
                    user_id=workspace_changes_user_id,
                    extra_excluded_dir_names=workspace_excluded_dir_names,
                )
            except Exception:
                logger.warning(
                    "Could not capture pre-run workspace snapshot for run %s",
                    run_id,
                    exc_info=True,
                )

        # 2. Publish metadata — useStream needs both run_id AND thread_id
        await bridge.publish(
            run_id,
            "metadata",
            {
                "run_id": run_id,
                "thread_id": thread_id,
            },
        )

        # 3. Build the agent
        from langchain_core.runnables import RunnableConfig
        from langgraph.runtime import Runtime

        # Inject runtime context so middlewares and tools (via ToolRuntime.context) can
        # access thread-level data. langgraph-cli does this automatically; we must do it
        # manually here because we drive the graph through ``agent.astream(config=...)``
        # without passing the official ``context=`` parameter.
        runtime_ctx = _build_runtime_context(
            thread_id,
            run_id,
            config.get("context"),
            ctx.app_config,
            task_store,
            extensions,
            ctx.authorization_provider,
        )
        if ctx.recovery_executor is not None:
            if not record.execution_takeover:
                raise ValueError("recovery executor evidence requires execution takeover")
            runtime_ctx[RECOVERY_EXECUTOR_CONTEXT_KEY] = ctx.recovery_executor
        deerflow_trace_id = _bind_trace_id(config, runtime_ctx)
        # Expose the run-scoped journal under a sentinel key so middleware can
        # write audit events (e.g. SafetyFinishReasonMiddleware recording
        # suppressed tool calls). Double-underscore prefix marks it as a
        # runtime-internal channel; user code must not depend on the key name.
        if journal is not None:
            runtime_ctx["__run_journal"] = journal
        _install_runtime_context(config, runtime_ctx)
        runtime = Runtime(context=cast(Any, runtime_ctx), store=store)
        skill_binding_user_id = resolve_runtime_user_id(runtime)
        config.setdefault("configurable", {})["__pregel_runtime"] = runtime

        # Inject RunJournal as a LangChain callback handler.
        # on_llm_end captures token usage; on_chain_start/end captures lifecycle.
        if journal is not None:
            config.setdefault("callbacks", []).append(journal)

        # Inject Langfuse trace-attribute metadata so the langchain CallbackHandler
        # can lift session_id / user_id / trace_name / tags onto the root trace.
        # Shared helper with ``DeerFlowClient.stream`` so both entry points stay
        # in sync; caller-provided metadata wins via setdefault inside the helper.
        inject_langfuse_metadata(
            config,
            thread_id=thread_id,
            user_id=resolve_runtime_user_id(runtime),
            assistant_id=record.assistant_id,
            model_name=record.model_name,
            environment=os.environ.get("DEER_FLOW_ENV") or os.environ.get("ENVIRONMENT"),
            deerflow_trace_id=deerflow_trace_id,
        )

        # Resolve after runtime context installation so context/configurable reflect
        # the agent name that this run will actually execute.
        config.setdefault("run_name", resolve_root_run_name(config, record.assistant_id))
        initial_runnable_config = RunnableConfig(**config)
        runnable_configs.append(initial_runnable_config)

        def _continuation_runnable_config() -> RunnableConfig:
            continuation_config = dict(config)
            configurable = dict(continuation_config.get("configurable", {}) or {})
            configurable["checkpoint_ns"] = ""
            configurable.pop("checkpoint_id", None)
            configurable.pop("checkpoint_map", None)
            continuation_config["configurable"] = configurable
            continuation = RunnableConfig(**continuation_config)
            runnable_configs.append(continuation)
            return continuation

        async def _fail_unavailable_subagent_catalog() -> None:
            error = "Accepted subagent catalog is unavailable"
            await run_manager.set_status_if_not_cancelled(
                run_id,
                RunStatus.error,
                error=error,
                stop_reason="subagent_catalog_unavailable",
                **terminal_status_kwargs,
            )
            await bridge.publish(
                run_id,
                "error",
                {
                    "message": error,
                    "name": "SubagentCatalogUnavailableError",
                },
            )

        accepted = record.accepted_invocation
        if accepted is not None:
            from deerflow.runtime.accepted_invocation import (
                ResolvedAgentMaterialV1,
                ResolvedAgentRevision,
            )
            from deerflow.runtime.agent_revision import (
                RESOLVED_AGENT_MATERIAL_CONTEXT_KEY,
            )

            if accepted.agent_revision.subagent_catalog is None:
                await _fail_unavailable_subagent_catalog()
                return

            pinned_material = accepted.agent_revision.material
            if pinned_material is None:
                candidate = runtime_ctx.get(RESOLVED_AGENT_MATERIAL_CONTEXT_KEY)
                if isinstance(candidate, ResolvedAgentMaterialV1):
                    pinned_material = candidate
            if pinned_material is None and ctx.agent_revision_resolver is not None:
                resolved = ctx.agent_revision_resolver(record, config)
                if inspect.isawaitable(resolved):
                    resolved = await resolved
                if isinstance(resolved, ResolvedAgentRevision):
                    pinned_material = resolved.material
                elif isinstance(resolved, ResolvedAgentMaterialV1):
                    pinned_material = resolved
            if isinstance(pinned_material, ResolvedAgentMaterialV1):
                accepted_catalog = accepted.agent_revision.subagent_catalog
                accepted_scopes = accepted.agent_revision.skill_scopes
                if accepted_scopes is None:
                    await _fail_unavailable_subagent_catalog()
                    return
                # A restart resolver may rebuild the lead's other immutable
                # inputs from their normal stores, but managed definitions are
                # prospective state. Rebind the already-validated persisted
                # catalog before digest comparison; never compare it to live
                # managed rows.
                if pinned_material.subagent_catalog != accepted_catalog or pinned_material.skill_scopes != accepted_scopes:
                    pinned_material = replace(
                        pinned_material,
                        subagent_catalog=accepted_catalog,
                        skill_scopes=accepted_scopes,
                    )
            if isinstance(pinned_material, ResolvedAgentMaterialV1):
                pinned_material_for_cleanup = pinned_material
                try:
                    await asyncio.to_thread(pinned_material.verify_process_material)
                except Exception as exc:
                    from deerflow.runtime.subagent_snapshot import (
                        SubagentCatalogError,
                    )

                    if isinstance(exc, SubagentCatalogError):
                        stop_reason = exc.code
                        error = "Accepted subagent skill material is unavailable"
                        error_name = "SubagentSkillMaterialError"
                    else:
                        stop_reason = "agent_revision_drift"
                        error = "Accepted skill snapshot no longer matches captured material"
                        error_name = "AgentRevisionDriftError"
                    await run_manager.set_status_if_not_cancelled(
                        run_id,
                        RunStatus.error,
                        error=error,
                        stop_reason=stop_reason,
                        **terminal_status_kwargs,
                    )
                    await bridge.publish(
                        run_id,
                        "error",
                        {
                            "message": error,
                            "name": error_name,
                        },
                    )
                    return
            actual_revision = ResolvedAgentRevision.from_material(pinned_material) if isinstance(pinned_material, ResolvedAgentMaterialV1) else None
            if actual_revision is None or actual_revision.digest != accepted.agent_revision.digest:
                error = "Accepted agent revision no longer matches current resolved material"
                await run_manager.set_status_if_not_cancelled(
                    run_id,
                    RunStatus.error,
                    error=error,
                    stop_reason="agent_revision_drift",
                    **terminal_status_kwargs,
                )
                await bridge.publish(
                    run_id,
                    "error",
                    {"message": error, "name": "AgentRevisionDriftError"},
                )
                return
            # Bind the exact object that passed the digest check. The factory
            # consumes it directly and never performs a second mutable read.
            runtime_ctx[RESOLVED_AGENT_MATERIAL_CONTEXT_KEY] = pinned_material
            from deerflow.subagents.batch_acceptance import (
                PARENT_BATCH_ACCEPTANCE_CONTEXT_KEY,
            )

            runtime_ctx[PARENT_BATCH_ACCEPTANCE_CONTEXT_KEY] = accepted
            if accepted.tenant is not None:
                runtime_ctx[TENANT_REFERENCE_CONTEXT_KEY] = accepted.tenant
            runtime_ctx["accepted_agent_revision_digest"] = actual_revision.digest
            runtime_ctx["accepted_extension_generation"] = accepted.extension_generation
            if accepted.extension_manifest_digest is not None:
                runtime_ctx["accepted_extension_manifest_digest"] = accepted.extension_manifest_digest
            if accepted.extension_artifact_manifest_digest is not None:
                runtime_ctx["accepted_extension_artifact_manifest_digest"] = accepted.extension_artifact_manifest_digest
            if accepted.extension_configuration_digest is not None:
                runtime_ctx["accepted_extension_configuration_digest"] = accepted.extension_configuration_digest
            if accepted.tool_plane_revision is not None:
                runtime_ctx["accepted_tool_plane_revision"] = accepted.tool_plane_revision
            execution_budget = accepted.execution_budget
            if execution_budget is not None:
                from deerflow.runtime.events.catalog import (
                    EXECUTION_POLICY_DECISION_EVENT,
                )
                from deerflow.runtime.execution_policy import (
                    EXECUTION_POLICY_OBSERVER_CONTEXT_KEY,
                    ExecutionPolicyEvaluator,
                    ExecutionPolicyObservationV1,
                    ExecutionPolicyStateV1,
                    PolicyDecision,
                    ToolEquivalenceKeyring,
                    normalizer_manifest_digest,
                )
                from deerflow.runtime.runs.store.base import (
                    ApplyExecutionPolicyStateOutcome,
                )

                policy_keyring = ctx.execution_policy_keyring
                try:
                    if not isinstance(policy_keyring, ToolEquivalenceKeyring):
                        raise ExecutionPolicyError("policy_equivalence_key_unavailable")
                    policy_keyring.require_key(execution_budget.equivalence_key_id)
                    if execution_budget.equivalence_normalizer_manifest_digest != normalizer_manifest_digest():
                        raise ExecutionPolicyError("policy_equivalence_normalizer_unavailable")
                except ExecutionPolicyError as exc:
                    await run_manager.set_status_if_not_cancelled(
                        run_id,
                        RunStatus.error,
                        error="Accepted execution policy is unavailable",
                        stop_reason=exc.code,
                        **terminal_status_kwargs,
                    )
                    await bridge.publish(
                        run_id,
                        "error",
                        {
                            "message": "Accepted execution policy is unavailable",
                            "name": "ExecutionPolicyUnavailableError",
                        },
                    )
                    return
                runtime_ctx["accepted_execution_budget"] = execution_budget
                runtime_ctx["execution_policy_keyring"] = policy_keyring

                async def _flush_policy_decision_outbox(
                    state: ExecutionPolicyStateV1,
                ) -> ExecutionPolicyStateV1:
                    """Publish the row-backed outbox once under the live fence."""

                    if not state.decision_outbox:
                        return state
                    if event_store is None or event_appender is None:
                        raise ExecutionPolicyError("policy_state_inconsistent")
                    existing = await event_store.list_events(
                        thread_id,
                        run_id,
                        event_types=[EXECUTION_POLICY_DECISION_EVENT.event_type],
                        limit=65,
                    )
                    published_state_digests = {content.get("state_digest") for event in existing if isinstance((content := event.get("content")), dict)}
                    for pending in state.decision_outbox:
                        if pending.state_digest in published_state_digests:
                            continue
                        await event_appender.put(
                            event_type=EXECUTION_POLICY_DECISION_EVENT.event_type,
                            category=EXECUTION_POLICY_DECISION_EVENT.category,
                            content={
                                "version": 1,
                                "decision": pending.decision.value,
                                "reason_code": pending.reason_code,
                                "current": pending.current,
                                "limit": pending.limit,
                                "budget_digest": execution_budget.digest,
                                "state_digest": pending.state_digest,
                                "summary_key": pending.summary_key,
                            },
                            metadata={},
                        )
                    cleared = replace(state, decision_outbox=())
                    cleared_outcome = await run_manager.apply_execution_policy_state(
                        run_id,
                        expected_digest=state.digest,
                        state=cleared,
                    )
                    if cleared_outcome is not ApplyExecutionPolicyStateOutcome.applied:
                        raise ExecutionPolicyError("policy_state_inconsistent")
                    return cleared

                try:
                    if record.execution_policy_state_json is None:
                        if record.execution_policy_state_digest is not None:
                            raise ExecutionPolicyError("policy_state_inconsistent")
                        policy_state = ExecutionPolicyStateV1.initial(execution_budget)
                        initial_outcome = await run_manager.apply_execution_policy_state(
                            run_id,
                            expected_digest=None,
                            state=policy_state,
                        )
                        if initial_outcome is not ApplyExecutionPolicyStateOutcome.applied:
                            raise ExecutionPolicyError("policy_state_inconsistent")
                    else:
                        policy_state = ExecutionPolicyStateV1.from_json(record.execution_policy_state_json)
                        if policy_state.digest != record.execution_policy_state_digest or policy_state.budget_digest != execution_budget.digest:
                            raise ExecutionPolicyError("policy_state_inconsistent")
                    policy_state = await _flush_policy_decision_outbox(policy_state)
                    if policy_state.terminal_reason is not None:
                        raise ExecutionPolicyError(policy_state.terminal_reason)
                except ExecutionPolicyError as exc:
                    await run_manager.set_status_if_not_cancelled(
                        run_id,
                        RunStatus.error,
                        error="Accepted execution policy state is unavailable",
                        stop_reason=exc.code,
                        **terminal_status_kwargs,
                    )
                    await bridge.publish(
                        run_id,
                        "error",
                        {
                            "message": "Accepted execution policy state is unavailable",
                            "name": "ExecutionPolicyStateError",
                        },
                    )
                    return

                policy_lock = asyncio.Lock()
                evaluator = ExecutionPolicyEvaluator()

                async def _observe_execution_policy(
                    observation: ExecutionPolicyObservationV1,
                ):
                    nonlocal policy_state
                    if not isinstance(observation, ExecutionPolicyObservationV1):
                        raise TypeError("invalid execution policy observation")
                    async with policy_lock:
                        evaluation = evaluator.evaluate(
                            execution_budget,
                            policy_state,
                            observation,
                        )
                        outcome = await run_manager.apply_execution_policy_state(
                            run_id,
                            expected_digest=policy_state.digest,
                            state=evaluation.next_state,
                        )
                        if outcome is not ApplyExecutionPolicyStateOutcome.applied:
                            raise ExecutionPolicyError("policy_state_inconsistent")
                        policy_state = await _flush_policy_decision_outbox(evaluation.next_state)
                        if evaluation.decision is PolicyDecision.stop:
                            runtime_ctx["stop_reason"] = evaluation.reason_code
                            runtime_ctx["execution_policy_stopped"] = True
                        return evaluation

                runtime_ctx[EXECUTION_POLICY_OBSERVER_CONTEXT_KEY] = _observe_execution_policy
            from deerflow_extension_api import SafeContextReferenceV1, SealedOriginV1

            from deerflow.runtime.accepted_invocation import (
                INVOCATION_IDENTITY_CONTEXT_KEY,
                INVOCATION_ORIGIN_CONTEXT_KEY,
                TRUSTED_RUN_CONTEXT_KEY,
            )

            trusted_context = accepted.trusted_context
            if trusted_context is not None and trusted_context.runtime_reference_count and not trusted_context.runtime_state_complete:
                error = "Accepted runtime-only contributor context is unavailable after process recovery"
                await run_manager.set_status_if_not_cancelled(
                    run_id,
                    RunStatus.error,
                    error=error,
                    stop_reason="trusted_context_unavailable",
                    **terminal_status_kwargs,
                )
                await bridge.publish(
                    run_id,
                    "error",
                    {"message": error, "name": "TrustedRunContextUnavailableError"},
                )
                return
            bound_trusted_context = trusted_context.bind_run(run_id) if trusted_context is not None else None
            if bound_trusted_context is not None:
                runtime_ctx[TRUSTED_RUN_CONTEXT_KEY] = bound_trusted_context
                runtime_ctx[INVOCATION_IDENTITY_CONTEXT_KEY] = bound_trusted_context.identity
                runtime_ctx[INVOCATION_ORIGIN_CONTEXT_KEY] = bound_trusted_context.origin
            elif accepted.principal.identity is not None:
                runtime_ctx[INVOCATION_IDENTITY_CONTEXT_KEY] = accepted.principal.identity
            runtime_ctx.pop("authz_attributes", None)
            for field_name, field_value in (
                ("user_id", accepted.principal.user_id),
                ("user_role", accepted.principal.role),
                ("oauth_provider", accepted.principal.oauth_provider),
                ("oauth_id", accepted.principal.oauth_id),
                ("channel_user_id", accepted.principal.channel_user_id),
            ):
                if field_value is None:
                    runtime_ctx.pop(field_name, None)
                else:
                    runtime_ctx[field_name] = field_value
            runtime_ctx["is_internal"] = accepted.principal.is_internal
            if bound_trusted_context is None:
                runtime_ctx[INVOCATION_ORIGIN_CONTEXT_KEY] = SealedOriginV1(
                    source_kind=accepted.origin.source_kind,
                    references=tuple(
                        SafeContextReferenceV1(
                            key=key,
                            value=value,
                            storage_class="persistable",
                            purpose="correlation",
                        )
                        for key, value in sorted(accepted.origin.references.items())
                    ),
                    digest=accepted.base_origin_digest,
                )
            from deerflow.extensions.mcp import (
                MCP_INVOCATION_FACTS_CONTEXT_KEY,
                McpInvocationFacts,
            )

            runtime_ctx[MCP_INVOCATION_FACTS_CONTEXT_KEY] = McpInvocationFacts.from_accepted(accepted, run_id=run_id)
            _install_pinned_agent_facts(runtime_ctx, pinned_material)
            from deerflow.runtime.constraints import (
                INVOCATION_CONSTRAINTS_CONTEXT_KEY,
                SUBAGENT_RESERVATION_CONTEXT_KEY,
                InvocationSubagentDispatchLedger,
                validate_constraint_fence,
            )

            accepted_constraints = validate_constraint_fence(
                accepted,
                request_digest=record.request_digest,
                clock=ctx.constraint_clock,
            )
            if accepted_constraints is not None:
                runtime_ctx[INVOCATION_CONSTRAINTS_CONTEXT_KEY] = accepted_constraints
                limit = accepted_constraints.max_total_subagents
                if limit is not None:
                    runtime_ctx["max_total_subagents"] = limit
                    dispatch_ledger = InvocationSubagentDispatchLedger(limit)
                    runtime_ctx[SUBAGENT_RESERVATION_CONTEXT_KEY] = dispatch_ledger
            if run_manager.requires_assembly_evidence:
                from deerflow.runtime.assembly_evidence import (
                    install_assembly_evidence_requirement,
                )

                install_assembly_evidence_requirement(runtime_ctx)
            _install_runtime_context(config, runtime_ctx)
            _install_pinned_agent_facts(config["context"], pinned_material)
            if accepted_constraints is not None:
                config["context"][INVOCATION_CONSTRAINTS_CONTEXT_KEY] = accepted_constraints
                if accepted_constraints.max_total_subagents is not None:
                    config["context"]["max_total_subagents"] = accepted_constraints.max_total_subagents
                    config["context"][SUBAGENT_RESERVATION_CONTEXT_KEY] = runtime_ctx[SUBAGENT_RESERVATION_CONTEXT_KEY]
            initial_runnable_config = RunnableConfig(**config)

        if accepted is not None and pinned_material_for_cleanup is not None and pinned_material_for_cleanup.skill_snapshot is not None:
            from deerflow.runtime.kubernetes_qualification import (
                qualification_barrier,
                qualification_counter,
            )

            await qualification_counter("materialization_starts", record)
            await qualification_barrier(
                "accepted_before_materialization",
                record,
            )

            async def _validate_pending_material_claim(claim: object) -> bool:
                from deerflow.sandbox.accepted_material import (
                    AcceptedMaterialExecutionClaimV1,
                )

                if (
                    not isinstance(claim, AcceptedMaterialExecutionClaimV1)
                    or claim.run_id != run_id
                    or claim.owner_worker_id != record.owner_worker_id
                    or claim.state_version != record.state_version
                    or record.ownership_lost
                    or record.abort_event.is_set()
                ):
                    return False
                async with run_manager.hold_execution_fence(
                    run_id,
                    owner_worker_id=claim.owner_worker_id,
                    state_version=claim.state_version,
                    allowed_active_statuses=("pending", "running"),
                ) as active:
                    sampled = active
                return bool(sampled and not record.ownership_lost and not record.abort_event.is_set())

            raw_materialization = await _materialize_accepted_skill_projection(
                runtime,
                user_id=skill_binding_user_id,
                record=record,
                claim_validator=_validate_pending_material_claim,
            )
            if isinstance(raw_materialization, _AcceptedMaterializationResult):
                materialization = raw_materialization
            else:
                # Compatibility for focused worker tests that replace the helper
                # with the historical two-tuple seam.
                materialization_sandbox_id, materialization_evidence = raw_materialization
                from deerflow.sandbox import get_sandbox_provider

                materialization = _AcceptedMaterializationResult(
                    sandbox_id=materialization_sandbox_id,
                    evidence=materialization_evidence,
                    provider=(None if materialization_evidence is None else get_sandbox_provider()),
                )
            materialization_evidence = materialization.evidence
            if materialization_evidence is not None:
                if not run_manager.heartbeat_enabled:
                    raise AcceptedSkillExecutionFenceError(
                        "accepted_skill_execution_lease_unavailable",
                    )

        if materialization_evidence is None:
            start_outcome = await run_manager.try_start(run_id)
        else:
            start_outcome = await run_manager.try_start(
                run_id,
                execution_evidence=materialization_evidence,
            )
        if start_outcome is not RunStartOutcome.started:
            if record.abort_event.is_set():
                await _finish_cancellation(
                    record.abort_action,
                    restore_checkpoint=False,
                )
            return
        started = True
        if isinstance(record.owner_worker_id, str) and record.owner_worker_id and type(record.state_version) is int:
            thread_projection_owner_id = record.owner_worker_id
            thread_projection_active_state_version = record.state_version

        if materialization_evidence is not None:
            assert materialization is not None
            from deerflow.sandbox.accepted_material import (
                AcceptedExecutionEvidenceV1,
                AcceptedExecutionEvidenceV2,
                AcceptedMaterialExecutionClaimV1,
                AcceptedMaterialLeaseV1,
                AcceptedMaterialRequestV1,
                AcceptedMaterialRequestV2,
                AcceptedSandboxSession,
                install_accepted_sandbox_session,
            )

            neutral_tuple = (
                materialization.sandbox,
                materialization.materializer,
                materialization.lease,
                materialization.request,
            )
            if all(value is not None for value in neutral_tuple) and isinstance(
                materialization.evidence,
                (AcceptedExecutionEvidenceV1, AcceptedExecutionEvidenceV2),
            ):
                if not isinstance(
                    materialization.lease,
                    AcceptedMaterialLeaseV1,
                ) or not isinstance(
                    materialization.request,
                    (AcceptedMaterialRequestV1, AcceptedMaterialRequestV2),
                ):
                    raise AcceptedSkillExecutionFenceError(
                        "accepted_skill_execution_fence_failed",
                    )
                owner_worker_id = record.owner_worker_id
                state_version = record.state_version
                if not isinstance(owner_worker_id, str) or not owner_worker_id or type(state_version) is not int:
                    raise AcceptedSkillExecutionFenceError(
                        "accepted_skill_execution_fence_failed",
                    )
                running_claim = AcceptedMaterialExecutionClaimV1(
                    version=1,
                    tenant_digest=materialization.request.tenant.digest,
                    run_id=run_id,
                    owner_worker_id=owner_worker_id,
                    state_version=state_version,
                    execution_takeover=record.execution_takeover,
                    expected_materialization_digest=(materialization.evidence.materialization_digest if record.execution_takeover else None),
                )

                async def _validate_running_claim(
                    claim: AcceptedMaterialExecutionClaimV1,
                ) -> bool:
                    if claim is not running_claim or record.ownership_lost or record.abort_event.is_set():
                        return False
                    async with run_manager.hold_execution_fence(
                        run_id,
                        owner_worker_id=claim.owner_worker_id,
                        state_version=claim.state_version,
                    ) as active:
                        sampled = active
                    return bool(sampled and not record.ownership_lost and not record.abort_event.is_set())

                from deerflow.runtime.kubernetes_qualification import (
                    accepted_sandbox_qualification_candidate_enabled,
                    qualification_barrier,
                    qualification_counter,
                )
                from deerflow.runtime.tool_evidence import (
                    get_active_tool_receipt,
                )

                before_delegate = None
                if accepted_sandbox_qualification_candidate_enabled():

                    async def _qualification_before_delegate() -> None:
                        await qualification_counter(
                            "accepted_sandbox_validations",
                            record,
                        )
                        await qualification_barrier(
                            "accepted_sandbox_after_validation",
                            record,
                        )

                    before_delegate = _qualification_before_delegate

                def _active_tool_receipt_ref() -> str | None:
                    receipt = get_active_tool_receipt()
                    return None if receipt is None else receipt.receipt_id

                accepted_sandbox_session = install_accepted_sandbox_session(
                    runtime_ctx,
                    AcceptedSandboxSession(
                        sandbox=materialization.sandbox,
                        materializer=materialization.materializer,
                        lease=materialization.lease,
                        evidence=materialization.evidence,
                        execution_claim=running_claim,
                        run_fence_validator=_validate_running_claim,
                        before_delegate=before_delegate,
                        tool_receipt_ref_resolver=_active_tool_receipt_ref,
                    ),
                )
                _install_runtime_context(config, runtime_ctx)
                accepted_sandbox_lifecycle_count = await _publish_accepted_sandbox_lifecycle(
                    event_appender,
                    accepted_sandbox_session,
                    start_index=accepted_sandbox_lifecycle_count,
                )

                async def _renew_materialization() -> bool:
                    assert accepted_sandbox_session is not None
                    await accepted_sandbox_session.renew()
                    return True

            else:

                async def _renew_materialization() -> bool:
                    assert materialization is not None
                    return await materialization.renew()

            await run_manager.set_execution_lease_renewal(
                run_id,
                _renew_materialization,
            )

        if checkpointer is not None and getattr(
            run_manager,
            "heartbeat_enabled",
            False,
        ):
            from deerflow.runtime.checkpointer.fenced_saver import (
                FencedCheckpointSaver,
            )
            from deerflow.runtime.kubernetes_qualification import (
                qualification_counter,
            )

            checkpoint_owner_id = record.owner_worker_id
            if not isinstance(checkpoint_owner_id, str) or not checkpoint_owner_id or type(record.state_version) is not int:
                raise RuntimeError("run_checkpoint_ownership_fence_unavailable")
            checkpoint_state_version = record.state_version
            record.checkpoint_terminal_state_version = None
            record.checkpoint_execution_fence_revoked = False

            async def _checkpoint_rejected(operation: str) -> None:
                del operation
                await qualification_counter(
                    "checkpoint_stale_rejections",
                    record,
                )

            checkpointer = FencedCheckpointSaver(
                checkpointer,
                fence=lambda: run_manager.hold_execution_fence(
                    run_id,
                    owner_worker_id=checkpoint_owner_id,
                    state_version=checkpoint_state_version,
                    terminal_state_version=(record.checkpoint_terminal_state_version),
                    revoked=record.checkpoint_execution_fence_revoked,
                ),
                on_rejected=_checkpoint_rejected,
            )

        if materialization_evidence is not None:
            assert materialization is not None
            if record.ownership_lost or not await materialization.validate():
                raise AcceptedSkillExecutionFenceError(
                    "accepted_skill_execution_fence_failed",
                )
            from deerflow.runtime.kubernetes_qualification import (
                qualification_barrier,
                qualification_counter,
            )

            await qualification_counter("materialization_validations", record)
            await qualification_barrier(
                "post_materialization_before_checkpoint",
                record,
            )

        assembly_anchors = None
        if requires_assembly_evidence:
            from deerflow.runtime.assembly_evidence import (
                build_accepted_assembly_anchors,
            )

            if accepted is None or pinned_material_for_cleanup is None:
                raise AssemblyEvidenceError("assembly_descriptor_missing")
            assembly_anchors = build_accepted_assembly_anchors(
                run_id=record.run_id,
                accepted=accepted,
                material=pinned_material_for_cleanup,
                app_config=ctx.app_config,
                accepted_constraints=accepted_constraints,
            )

        agent_factory_kwargs: dict[str, Any] = {"config": initial_runnable_config}
        if ctx.app_config is not None and _agent_factory_supports_app_config(agent_factory):
            agent_factory_kwargs["app_config"] = ctx.app_config
        from deerflow.extensions import bind_agent_build_extensions

        with bind_agent_build_extensions(extensions):
            agent_result = agent_factory(**agent_factory_kwargs)
        agent, assembly_descriptor = _split_agent_factory_result(agent_result)

        if requires_assembly_evidence:
            from deerflow.runtime.assembly_evidence import (
                build_assembly_evidence,
            )

            if assembly_descriptor is None:
                raise AssemblyEvidenceError("assembly_descriptor_missing")
            if assembly_anchors is None:
                raise AssemblyEvidenceError("assembly_descriptor_missing")
            evidence = build_assembly_evidence(
                assembly_descriptor,
                anchors=assembly_anchors,
            )
            bind_outcome = await run_manager.bind_assembly_evidence(run_id, evidence)
            if bind_outcome in (
                BindAssemblyEvidenceOutcome.bound,
                BindAssemblyEvidenceOutcome.already_matching,
            ):
                assembly_evidence_bound = True
            elif bind_outcome is BindAssemblyEvidenceOutcome.mismatch:
                raise AssemblyEvidenceError("assembly_evidence_mismatch")
            else:
                record.ownership_lost = True
                raise AssemblyEvidenceError("assembly_evidence_fence_lost")

            if requires_tool_receipt_evidence:
                if event_store is None:
                    raise AssemblyEvidenceError("tool_receipt_sink_unavailable")
                from deerflow.runtime.tool_evidence import (
                    RunEventToolReceiptSink,
                    ToolEvidenceRuntimeBinding,
                    install_tool_evidence_context,
                )

                owner_id = record.owner_worker_id
                lease_epoch = record.state_version
                if not isinstance(owner_id, str) or not owner_id or type(lease_epoch) is not int:
                    raise AssemblyEvidenceError("tool_receipt_fence_unavailable")
                catalog = pinned_material_for_cleanup.subagent_catalog

                async def _receipt_ownership_lost(operation: str) -> None:
                    del operation
                    from deerflow.runtime.kubernetes_qualification import (
                        qualification_counter,
                    )

                    await qualification_counter(
                        "receipt_stale_rejections",
                        record,
                    )

                install_tool_evidence_context(
                    runtime_ctx,
                    binding=ToolEvidenceRuntimeBinding(
                        run_id=run_id,
                        execution_task_id=run_id,
                        execution_kind="lead",
                        subagent_name=None,
                        owner_id=owner_id,
                        lease_epoch=lease_epoch,
                        agent_revision_digest=evidence.accepted_agent_revision_digest,
                        assembly_fingerprint=evidence.fingerprint,
                        extension_generation=evidence.extension_generation,
                        capability_manifest_digest=(evidence.accepted_capability_manifest_digest),
                        artifact_manifest_digest=(evidence.accepted_artifact_manifest_digest),
                        extension_configuration_digest=(evidence.accepted_extension_configuration_digest),
                        subagent_catalog_digest=catalog.digest,
                        subagent_definition_digest=None,
                        tenant=evidence.tenant,
                    ),
                    sink=RunEventToolReceiptSink(
                        event_store,
                        on_ownership_lost=_receipt_ownership_lost,
                    ),
                )
                _install_runtime_context(config, runtime_ctx)

            if checkpointer is not None:
                for preflight_config in checkpoint_preflight_configs:
                    await aensure_checkpoint_mode_compatible(
                        checkpointer,
                        preflight_config,
                        mode,
                    )

        # A takeover worker is attached behind the manager's release barrier.
        # Only after accepted material and the rebuilt assembly have validated
        # may Gateway inspect the actual host-sealed tool recovery policy. The
        # gate owns any unsafe terminal CAS; no lifecycle observer, mutable
        # thread projection, graph, model, or tool work precedes this decision.
        execution_recovery_decision: ExecutionRecoveryDecision | None = None
        execution_dispatch_marked = False
        if record.execution_takeover:
            recovery_gate = ctx.execution_recovery_gate
            if recovery_gate is None:
                raise RuntimeError(
                    "execution_recovery_coordinator_unavailable",
                )
            execution_recovery_decision = await recovery_gate(
                record,
                assembly_descriptor,
            )
            if execution_recovery_decision.disposition in {
                ExecutionRecoveryDisposition.terminalize_checkpoint_unavailable,
                ExecutionRecoveryDisposition.terminalize_tool_attempt_indeterminate,
            }:
                raise _ExecutionRecoveryTerminalized(
                    execution_recovery_decision.disposition.value,
                )
            if execution_recovery_decision.disposition in {
                ExecutionRecoveryDisposition.resume_checkpoint,
                ExecutionRecoveryDisposition.resume_reconciled_tool,
            }:
                # Continue the stored graph state; applying accepted caller
                # input or attempting to rewrite the already-durable dispatch
                # singleton would duplicate/conflict with this turn.
                graph_input = None
                recovery_configurable = dict(
                    config.get("configurable", {}) or {},
                )
                recovery_configurable["checkpoint_ns"] = ""
                recovery_configurable.pop("checkpoint_id", None)
                recovery_configurable.pop("checkpoint_map", None)
                config["configurable"] = recovery_configurable
                initial_runnable_config = RunnableConfig(**config)
                execution_dispatch_marked = True

        if extensions.has_task_lifecycle:
            task_info = TaskInfo(
                task_id=task_id,
                run_id=run_id,
                thread_id=thread_id,
                kind="lead",
                agent_name=record.assistant_id,
                resumed=record.execution_takeover,
            )
            assert task_store is not None
            await notify_task_start(
                extensions,
                task_store,
                task_info,
                timeout=_EXTENSION_TASK_NOTIFY_TIMEOUT_SECONDS,
            )

        if not record.ownership_lost and thread_store is not None and thread_projection_owner_id is not None and thread_projection_active_state_version is not None:
            try:
                await thread_store.project_run(
                    ThreadMetaRunProjection(
                        run_id=run_id,
                        thread_id=thread_id,
                        owner_worker_id=thread_projection_owner_id,
                        active_state_version=(thread_projection_active_state_version),
                        status="running",
                    ),
                    user_id=record.user_id,
                )
            except Exception:
                logger.debug(
                    "Failed to project running thread_meta status for %s (non-fatal)",
                    thread_id,
                )

        accessor = CheckpointStateAccessor.bind(
            agent,
            checkpointer,
            store=store,
            mode=mode,
        )

        # Capture the pre-run rollback point (materialized state + raw pending
        # writes) before this run mutates the thread. Raw checkpoint blobs
        # cannot reconstruct Delta-channel messages (their checkpoints omit
        # channel_values), so rollback forks the pre-run lineage through the
        # graph and needs the materialized messages up front. Any capture
        # failure disables rollback: restoring an empty or partial message
        # history would silently truncate the thread.
        if checkpointer is not None:
            # A previous successful run may still be persisting duration
            # metadata after its active admission slot is released. Share its
            # checkpoint lock so the rollback snapshot and any resume rewrite
            # are one uninterrupted read/write sequence against the head.
            async with _checkpoint_thread_lock(thread_id):
                try:
                    rollback_point = await _capture_rollback_point(accessor, checkpointer, checkpoint_config)
                except Exception:
                    snapshot_capture_failed = True
                    logger.warning(
                        "Could not capture pre-run checkpoint snapshot for run %s",
                        run_id,
                        exc_info=True,
                    )
                if rollback_point is not None:
                    pre_run_checkpoint_id = rollback_point.config.get("configurable", {}).get("checkpoint_id")
                    pre_existing_message_ids = _collect_pre_existing_message_ids({"messages": list(rollback_point.messages)})

                # Resuming from an older checkpoint is a fork, and a delta fork
                # materializes the abandoned sibling's writes back into state
                # (#4458). Rewrite it as a linear head write *after* the rollback
                # point is captured, so cancel-with-rollback still restores the
                # real pre-run head rather than the rolled-back one.
                resumed_messages = await _linearize_delta_checkpoint_resume(
                    accessor=accessor,
                    checkpointer=checkpointer,
                    config=config,
                    thread_id=thread_id,
                    run_id=run_id,
                )
            if resumed_messages is not None:
                # The graph now starts from the selected state, so the
                # current-run message boundary is that state, not the head we
                # captured for rollback.
                pre_existing_message_ids = _collect_pre_existing_message_ids({"messages": list(resumed_messages)})
                initial_runnable_config = RunnableConfig(**config)
                runnable_configs.append(initial_runnable_config)

        runtime_ctx[CURRENT_RUN_PRE_EXISTING_MESSAGE_IDS_KEY] = frozenset(pre_existing_message_ids)
        _install_runtime_context(config, runtime_ctx)

        from deerflow.runtime.kubernetes_qualification import (
            qualification_barrier,
            qualification_counter,
        )

        await qualification_counter("checkpoint_preflight_starts", record)
        await qualification_barrier(
            "post_checkpoint_before_graph",
            record,
        )

        # Capture the effective (resolved) model name from the agent's metadata.
        # _resolve_model_name in agent.py may return the default model if the
        # requested name is not in the allowlist — this update ensures the
        # persisted model_name reflects the actual model used.
        if record.model_name is not None:
            resolved = getattr(agent, "metadata", {}) or {}
            if isinstance(resolved, dict):
                effective = resolved.get("model_name")
                if effective and effective != record.model_name:
                    await run_manager.update_model_name(record.run_id, effective)

        # 4. Attach checkpointer and store
        if checkpointer is not None:
            agent.checkpointer = checkpointer
        if store is not None:
            agent.store = store

        # 5. Set interrupt nodes
        if interrupt_before:
            agent.interrupt_before_nodes = interrupt_before
        if interrupt_after:
            agent.interrupt_after_nodes = interrupt_after

        logger.info(
            "Run %s: streaming with modes %s (requested: %s)",
            run_id,
            lg_modes,
            requested_modes,
        )

        # Buffer subagent step events and persist them in batches (#3779) instead
        # of one low-frequency put() per step on the hot stream loop. Flushed in
        # the finally block so buffered steps survive abort/exception paths too.
        subagent_events = _SubagentEventBuffer(event_appender, thread_id, run_id)

        def _get_goal_evaluator_model() -> Any:
            nonlocal goal_evaluator_model
            if goal_evaluator_model is None:
                goal_evaluator_model = create_goal_evaluator_model(
                    model_name=record.model_name,
                    app_config=ctx.app_config,
                )
            return goal_evaluator_model

        constraint_start_validated = False
        qualification_graph_start_recorded = False
        execution_recovery_resume_counted = False
        # Built once per run, not per _stream_once call: goal continuations
        # re-enter the stream and would otherwise discard the resolved seqs.
        seq_stamper = _build_seq_stamper(event_store, thread_id, journal) if "values" in requested_modes else None

        async def _stream_once(input_payload: Any, stream_config: RunnableConfig) -> None:
            nonlocal llm_error_fallback_message, constraint_start_validated, qualification_graph_start_recorded, execution_dispatch_marked, execution_recovery_resume_counted
            file_tool_chunk_batcher = _LargeFileToolChunkBatcher() if "values" in requested_modes else None
            try:
                async with _checkpoint_thread_lock(thread_id):
                    if not constraint_start_validated and accepted_constraints is not None:
                        from deerflow.runtime.constraints import (
                            validate_constraint_fence,
                        )

                        validate_constraint_fence(
                            accepted,
                            request_digest=record.request_digest,
                            clock=ctx.constraint_clock,
                        )
                        constraint_start_validated = True
                    if not execution_dispatch_marked:
                        if event_appender is None:
                            if record.execution_takeover:
                                raise RuntimeError(
                                    "execution_dispatch_marker_unavailable",
                                )
                        else:
                            dispatch_baseline_checkpoint_id = None
                            if checkpointer is not None:
                                dispatch_baseline_checkpoint_id = _checkpoint_id(
                                    await checkpointer.aget_tuple(
                                        checkpoint_config,
                                    )
                                )
                            await event_appender.put_if_absent(
                                thread_id=thread_id,
                                run_id=run_id,
                                event_type=(RUN_EXECUTION_STARTED_EVENT.event_type),
                                category=(RUN_EXECUTION_STARTED_EVENT.category),
                                content={
                                    "version": 1,
                                    "pre_graph_checkpoint_id": (dispatch_baseline_checkpoint_id),
                                },
                                metadata={},
                            )
                        execution_dispatch_marked = True
                        # This qualification-only seam is deliberately after
                        # the durable singleton and before every graph/model
                        # or takeover-resume counter. A replacement that sees
                        # the marker without a newer checkpoint must fail
                        # closed; it must not re-enter this barrier.
                        await qualification_barrier(
                            "post_dispatch_marker_before_graph",
                            record,
                        )
                    if record.execution_takeover and not execution_recovery_resume_counted:
                        from deerflow.runtime.kubernetes_qualification import (
                            qualification_counter,
                        )

                        await qualification_counter(
                            "execution_takeover_resumes",
                            record,
                        )
                        execution_recovery_resume_counted = True
                    if not qualification_graph_start_recorded:
                        from deerflow.runtime.kubernetes_qualification import (
                            qualification_counter,
                        )

                        await qualification_counter("graph_starts", record)
                        qualification_graph_start_recorded = True
                    if materialization_evidence is not None:
                        assert materialization is not None
                        if record.ownership_lost or not await materialization.validate():
                            raise AcceptedSkillExecutionFenceError(
                                "accepted_skill_execution_fence_failed",
                            )
                    observation_scope = nullcontext()
                    if journal is not None and accepted_for_cleanup is not None:
                        from deerflow.agents.memory.observations import (
                            bind_memory_observation_sink,
                        )

                        observation_scope = bind_memory_observation_sink(journal, ctx.tenant)
                    if len(lg_modes) == 1 and not stream_subgraphs:
                        # Single mode, no subgraphs: astream yields raw chunks
                        single_mode = lg_modes[0]
                        with observation_scope:
                            stream = agent.astream(input_payload, config=stream_config, stream_mode=single_mode)
                            broke_on_abort = False
                            try:
                                async for chunk in stream:
                                    if record.abort_event.is_set():
                                        broke_on_abort = True
                                        logger.info("Run %s abort requested — stopping", run_id)
                                        break
                                    llm_error_fallback_message = llm_error_fallback_message or _extract_llm_error_fallback_message(chunk, pre_existing_message_ids)
                                    sse_event = _lg_mode_to_sse_event(single_mode)
                                    single_payload = serialize(chunk, mode=single_mode)
                                    if single_mode == "values" and seq_stamper is not None:
                                        single_payload = await seq_stamper.stamp(single_payload)
                                    await bridge.publish(run_id, sse_event, single_payload)
                                    if single_mode == "custom":
                                        await subagent_events.add(chunk)
                            finally:
                                close_error = sys.exception()
                                try:
                                    await _close_agent_stream(stream)
                                except Exception:
                                    abort_requested = broke_on_abort or record.abort_event.is_set()
                                    if close_error is None and not abort_requested:
                                        raise
                                    if abort_requested:
                                        logger.warning("Could not close aborted agent stream for run %s", run_id, exc_info=True)
                                    else:
                                        logger.debug("Could not close agent stream for run %s", run_id, exc_info=True)
                        return
                    # Multiple modes or subgraphs: astream yields tuples
                    with observation_scope:
                        stream = agent.astream(
                            input_payload,
                            config=stream_config,
                            stream_mode=lg_modes,
                            subgraphs=stream_subgraphs,
                        )
                        broke_on_abort = False
                        try:
                            async for item in stream:
                                if record.abort_event.is_set():
                                    broke_on_abort = True
                                    logger.info("Run %s abort requested — stopping", run_id)
                                    break

                                mode, chunk, namespace = _unpack_stream_item(item, lg_modes, stream_subgraphs)
                                if mode is None:
                                    continue

                                if not namespace:
                                    # Only root-graph frames may decide the parent run's error
                                    # fallback: a delegated subagent's marked fallback is the
                                    # executor's to map (task_failed), not this run's.
                                    llm_error_fallback_message = llm_error_fallback_message or _extract_llm_error_fallback_message(chunk, pre_existing_message_ids)
                                await _publish_stream_item(
                                    bridge=bridge,
                                    run_id=run_id,
                                    mode=mode,
                                    chunk=chunk,
                                    namespace=namespace,
                                    file_tool_chunk_batcher=file_tool_chunk_batcher,
                                    subagent_events=subagent_events,
                                    seq_stamper=seq_stamper,
                                )
                        finally:
                            close_error = sys.exception()
                            try:
                                await _close_agent_stream(stream)
                            except Exception:
                                abort_requested = broke_on_abort or record.abort_event.is_set()
                                if close_error is None and not abort_requested:
                                    raise
                                if abort_requested:
                                    logger.warning("Could not close aborted agent stream for run %s", run_id, exc_info=True)
                                else:
                                    logger.debug("Could not close agent stream for run %s", run_id, exc_info=True)
            finally:
                stream_error = sys.exception()
                if file_tool_chunk_batcher is not None:
                    try:
                        for publish_chunk in file_tool_chunk_batcher.finish():
                            await bridge.publish(
                                run_id,
                                "messages",
                                serialize(publish_chunk, mode="messages"),
                            )
                    except Exception:
                        if stream_error is None:
                            raise
                        logger.debug(
                            "Could not flush pending file-tool chunks for run %s",
                            run_id,
                            exc_info=True,
                        )

        # 7. Stream the requested turn, then optionally continue hidden goal turns.
        # Clear any stale stop_reason before the first (user-visible) turn only.
        # Continuation turns preserve a cap reason from the user turn: a run that
        # hits a cap during the user turn IS capped even if hidden goal-evaluator
        # turns complete cleanly afterward (#4176 review).
        if isinstance(runtime.context, dict):
            runtime.context.pop("stop_reason", None)
        await _stream_once(graph_input, initial_runnable_config)
        while not record.abort_event.is_set() and not llm_error_fallback_message and (journal is None or not journal.had_llm_error_fallback):
            continuation_input = await _prepare_goal_continuation_input(
                bridge=bridge,
                accessor=accessor,
                checkpointer=checkpointer,
                thread_id=thread_id,
                run_id=run_id,
                model_name=record.model_name,
                app_config=ctx.app_config,
                evaluator_model_factory=_get_goal_evaluator_model,
                abort_event=record.abort_event,
                user_id=resolve_runtime_user_id(runtime),
                deerflow_trace_id=deerflow_trace_id,
                task_store=task_store,
                extensions=extensions,
            )
            if continuation_input is None or record.abort_event.is_set():
                break
            await _stream_once(continuation_input, _continuation_runnable_config())

        # 8. Final status
        if record.abort_event.is_set():
            await _finish_cancellation(record.abort_action)
        elif llm_error_fallback_message or (journal is not None and journal.had_llm_error_fallback):
            terminal_failure = map_runtime_failure(
                code="llm_provider_failed",
                error_class="LLMProviderFailure",
            )
            error_msg = terminal_failure.public_message
            await _ensure_finalizing_before_edit_failure(run_manager, record)
            cancel_action = await run_manager.set_status_if_not_cancelled(
                run_id,
                RunStatus.error,
                error=error_msg,
                **terminal_status_kwargs,
            )
            if cancel_action is not None:
                await _finish_cancellation(cancel_action)
        else:
            runtime_context = runtime.context if isinstance(runtime.context, dict) else None
            # Guard middlewares that hard-stop a run by stripping tool_calls
            # stamp stop_reason into runtime.context so the worker can surface
            # it on the run record:
            #   loop_detection      -> "loop_capped"
            #   token_budget        -> "token_capped"
            #   safety_finish_reason -> "safety_capped"
            #   subagent_limit       -> "subagent_limit_capped"
            #   model_length_finish_reason -> "model_length_capped"
            #
            # If more guards grow stop_reason semantics, consider a publish/
            # collect pattern (e.g. each guard middleware publishes its cap
            # reason to a dedicated runtime.context channel, and the worker
            # collects the most severe / first / all reasons) instead of each
            # guard writing directly to the same key.
            stop_reason = runtime_context.get("stop_reason") if runtime_context is not None else None
            produced_output_paths = await _produced_output_paths(
                pre_run_workspace_snapshot,
                thread_id=thread_id,
                user_id=workspace_changes_user_id,
                extra_excluded_dir_names=workspace_excluded_dir_names,
            )
            delivery_content = _delivery_content_with_outputs(
                journal.get_delivery_content() if journal is not None else _empty_delivery_content(),
                produced_output_paths,
            )
            delivery_error = _delivery_error(delivery_content)
            if delivery_error is not None:
                terminal_failure = map_runtime_failure(
                    code="artifact_delivery_incomplete",
                    error_class="ArtifactDeliveryFailure",
                )
            if accepted_sandbox_session is not None:
                # Success is not staged from a provider lease that was lost
                # after the final graph operation. The subsequent run-store
                # transition independently rechecks the SQL execution fence.
                await accepted_sandbox_session.validate()
            cancel_action = await run_manager.set_status_if_not_cancelled(
                run_id,
                RunStatus.error if delivery_error else RunStatus.success,
                error=delivery_error,
                stop_reason=stop_reason,
                **terminal_status_kwargs,
            )
            if cancel_action is not None:
                await _finish_cancellation(cancel_action)

    except _ExecutionRecoveryTerminalized as exc:
        # RunManager already committed the bounded terminal lifecycle under
        # this takeover's exact owner/epoch fence. The worker must release
        # material and end the stream without attempting a second status,
        # receipt outcome, checkpoint, or thread projection write.
        record.ownership_lost = True
        disposition = ExecutionRecoveryDisposition(str(exc))
        if disposition is ExecutionRecoveryDisposition.terminalize_tool_attempt_indeterminate:
            message = "Recovery stopped because a prior tool attempt could not be safely reconciled."
            stop_reason = RECOVERY_TOOL_ATTEMPT_INDETERMINATE_STOP_REASON
        else:
            message = "Recovery stopped because no safe durable execution checkpoint is available."
            stop_reason = RECOVERY_CHECKPOINT_UNAVAILABLE_STOP_REASON
        await bridge.publish(
            run_id,
            "error",
            {
                "message": message,
                "name": "ExecutionRecoveryError",
                "stop_reason": stop_reason,
            },
        )

    except asyncio.CancelledError:
        await _finish_cancellation(record.abort_action)

    except ConstraintFenceError as exc:
        error_msg = f"Invocation constraint fence failed: {exc.reason}"
        logger.warning("Run %s failed constraint fence: %s", run_id, exc.reason)
        await _ensure_finalizing_before_edit_failure(run_manager, record)
        cancel_action = await run_manager.set_status_if_not_cancelled(
            run_id,
            RunStatus.error,
            error=error_msg,
            stop_reason=exc.reason,
            **terminal_status_kwargs,
        )
        if cancel_action is not None:
            await _finish_cancellation(cancel_action)
        else:
            await bridge.publish(
                run_id,
                "error",
                {"message": error_msg, "name": "ConstraintFenceError"},
            )

    except AcceptedSkillExecutionFenceError as exc:
        reason = str(exc)
        error_msg = "Accepted sandbox material is no longer executable"
        logger.warning("Run %s failed accepted sandbox fence: %s", run_id, reason)
        await _ensure_finalizing_before_edit_failure(run_manager, record)
        cancel_action = await run_manager.set_status_if_not_cancelled(
            run_id,
            RunStatus.error,
            error=error_msg,
            stop_reason=reason,
            **terminal_status_kwargs,
        )
        if cancel_action is not None:
            await _finish_cancellation(cancel_action)
        else:
            await bridge.publish(
                run_id,
                "error",
                {
                    "message": error_msg,
                    "name": "AcceptedSkillExecutionFenceError",
                },
            )

    except AssemblyEvidenceError as exc:
        fence_lost = exc.code == "assembly_evidence_fence_lost"
        unavailable = exc.code in {
            "assembly_descriptor_missing",
        }
        stop_reason = "assembly_evidence_unavailable" if unavailable else "agent_assembly_drift"
        error_msg = "Agent assembly evidence is unavailable" if unavailable else "Agent assembly does not match the accepted durable execution"
        logger.warning("Run %s failed agent assembly evidence: %s", run_id, exc.code)
        if fence_lost:
            record.ownership_lost = True
            await bridge.publish(
                run_id,
                "error",
                {
                    "message": "Agent assembly evidence ownership fence was lost",
                    "name": "AssemblyEvidenceFenceLostError",
                },
            )
        else:
            await _ensure_finalizing_before_edit_failure(run_manager, record)
            cancel_action = await run_manager.set_status_if_not_cancelled(
                run_id,
                RunStatus.error,
                error=error_msg,
                stop_reason=stop_reason,
                **terminal_status_kwargs,
            )
            if cancel_action is not None:
                await _finish_cancellation(cancel_action)
            else:
                await bridge.publish(
                    run_id,
                    "error",
                    {
                        "message": error_msg,
                        "name": "AssemblyEvidenceError",
                    },
                )

    except ExecutionPolicyError as exc:
        error_msg = "Accepted execution policy stopped this run"
        await _ensure_finalizing_before_edit_failure(run_manager, record)
        cancel_action = await run_manager.set_status_if_not_cancelled(
            run_id,
            RunStatus.error,
            error=error_msg,
            stop_reason=exc.code,
            **terminal_status_kwargs,
        )
        if cancel_action is not None:
            await _finish_cancellation(cancel_action)
        else:
            await bridge.publish(
                run_id,
                "error",
                {
                    "message": error_msg,
                    "name": "ExecutionPolicyError",
                    "stop_reason": exc.code,
                },
            )

    except RuntimeEventOwnershipLost:
        runtime_event_authority_rejected = True
        logger.warning(
            "Run %s stopped after its runtime-event write authority changed",
            run_id,
        )

    except Exception as exc:
        terminal_failure = map_runtime_failure(
            code="run_execution_failed",
            error=exc,
        )
        error_msg = terminal_failure.public_message
        logger.error(
            "Run failed run_id=%s code=%s error_class=%s correlation_id=%s",
            run_id,
            terminal_failure.code,
            terminal_failure.error_class,
            terminal_failure.correlation_id,
        )
        await _ensure_finalizing_before_edit_failure(run_manager, record)
        cancel_action = await run_manager.set_status_if_not_cancelled(
            run_id,
            RunStatus.error,
            error=error_msg,
            **terminal_status_kwargs,
        )
        if cancel_action is not None:
            await _finish_cancellation(cancel_action)
        else:
            await bridge.publish(
                run_id,
                "error",
                {
                    "message": error_msg,
                    "name": "RuntimeFailure",
                },
            )

    finally:
        active_terminal_exception = sys.exception()
        if active_terminal_exception is not None and not isinstance(
            active_terminal_exception,
            Exception,
        ):
            _defer_stop_interrupt(active_terminal_exception)
        if accepted_sandbox_session is not None:
            await _await_terminal_cleanup(
                _publish_accepted_sandbox_lifecycle_during_cleanup(),
                interrupt_current=True,
            )
        if started and getattr(run_manager, "heartbeat_enabled", False) and not record.ownership_lost:
            try:
                refreshed_cancel = await _await_terminal_cleanup(
                    run_manager.refresh_owned_cancellation(run_id),
                )
            except Exception as exc:
                failure = map_runtime_failure(
                    code="runtime_event_authority_refresh_failed",
                    error=exc,
                )
                logger.warning(
                    "Runtime-event authority refresh failed run_id=%s code=%s error_class=%s correlation_id=%s",
                    run_id,
                    failure.code,
                    failure.error_class,
                    failure.correlation_id,
                )
                refreshed_cancel = None
            if refreshed_cancel is not None:
                await _await_terminal_cleanup(
                    _finish_cancellation(refreshed_cancel),
                )
            elif runtime_event_authority_rejected:
                record.ownership_lost = True

        if materialization_evidence is not None:
            try:
                await _await_terminal_cleanup(
                    run_manager.set_execution_lease_renewal(run_id, None),
                )
            except Exception:
                logger.warning(
                    "Failed to clear accepted sandbox renewal for run %s",
                    run_id,
                    exc_info=True,
                )
        if record.ownership_lost:
            logger.warning(
                "Skipping durable finalization for run %s because this worker no longer owns its lease",
                run_id,
            )

        checkpoint_access_authorized = not requires_assembly_evidence or assembly_evidence_bound

        if not record.ownership_lost and checkpoint_access_authorized and _is_edit_replay_run(record) and record.status != RunStatus.success:
            if not record.finalizing:
                await _await_terminal_cleanup(
                    run_manager.set_finalizing(run_id, True),
                )
            try:
                if not checkpoint_rollback_completed:
                    checkpoint_rollback_completed = await _await_terminal_cleanup(
                        _rollback_to_pre_run_checkpoint(
                            accessor=accessor,
                            checkpointer=checkpointer,
                            thread_id=thread_id,
                            run_id=run_id,
                            rollback_point=rollback_point,
                            snapshot_capture_failed=snapshot_capture_failed,
                        ),
                        interrupted_result=False,
                    )
                if checkpoint_rollback_completed:
                    await _await_terminal_cleanup(
                        _publish_restored_checkpoint_values(
                            bridge=bridge,
                            run_id=run_id,
                            accessor=accessor,
                            thread_id=thread_id,
                        ),
                    )
                    logger.info(
                        "Run %s edit replay restored pre-run checkpoint %s",
                        run_id,
                        pre_run_checkpoint_id,
                    )
            except Exception:
                logger.warning("Run %s edit replay rollback failed", run_id, exc_info=True)

        # Persist any subagent step events still buffered (#3779) — including on
        # abort/exception paths, where the stream loop broke before its own flush.
        if not record.ownership_lost and subagent_events is not None:
            try:
                await _await_terminal_cleanup(subagent_events.flush())
            except RuntimeEventOwnershipLost:
                record.ownership_lost = True

        if not record.ownership_lost and event_store is not None and pre_run_workspace_snapshot is not None:
            try:
                await _await_terminal_cleanup(
                    record_workspace_changes(
                        event_appender,
                        thread_id,
                        run_id,
                        pre_run_workspace_snapshot,
                        user_id=workspace_changes_user_id,
                        extra_excluded_dir_names=workspace_excluded_dir_names,
                    ),
                )
            except RuntimeEventOwnershipLost:
                record.ownership_lost = True
            except Exception as exc:
                failure = map_runtime_failure(
                    code="workspace_event_write_failed",
                    error=exc,
                )
                logger.warning(
                    "Workspace event write failed run_id=%s code=%s error_class=%s correlation_id=%s",
                    run_id,
                    failure.code,
                    failure.error_class,
                    failure.correlation_id,
                )

        # Flush buffered journal events before the terminal receipt. The
        # receipt uses a run-scoped idempotent write shared with recovery, then
        # the staged terminal status is persisted. This ordering closes the
        # crash window where a terminal run could otherwise outlive its receipt.
        # A fenced worker leaves receipt recovery to the peer that claimed it.
        if not record.ownership_lost and journal is not None:
            try:
                await _await_terminal_cleanup(journal.flush())
            except RuntimeEventOwnershipLost:
                record.ownership_lost = True
            except Exception as exc:
                failure = map_runtime_failure(
                    code="run_journal_write_failed",
                    error=exc,
                )
                logger.warning(
                    "Run journal write failed run_id=%s code=%s error_class=%s correlation_id=%s",
                    run_id,
                    failure.code,
                    failure.error_class,
                    failure.correlation_id,
                )

            if not record.ownership_lost and delivery_content is None:
                if produced_output_paths is None:
                    produced_output_paths = await _await_terminal_cleanup(
                        _produced_output_paths(
                            pre_run_workspace_snapshot,
                            thread_id=thread_id,
                            user_id=workspace_changes_user_id,
                            extra_excluded_dir_names=workspace_excluded_dir_names,
                        ),
                        interrupted_result=[],
                    )
                delivery_content = _delivery_content_with_outputs(journal.get_delivery_content(), produced_output_paths)
            if not record.ownership_lost:
                try:
                    receipt_persisted = await _await_terminal_cleanup(
                        _persist_delivery_receipt(
                            event_appender,
                            thread_id=thread_id,
                            run_id=run_id,
                            content=delivery_content,
                        ),
                        interrupted_result=False,
                    )
                except RuntimeEventOwnershipLost:
                    record.ownership_lost = True
                    receipt_persisted = False
            if not record.ownership_lost and produced_output_paths and record.status == RunStatus.success and not receipt_persisted:
                terminal_failure = map_runtime_failure(
                    code="delivery_receipt_failed",
                    error_class="DeliveryReceiptFailure",
                )
                await _await_terminal_cleanup(
                    run_manager.set_status(
                        run_id,
                        RunStatus.error,
                        error=_DELIVERY_RECEIPT_FAILED_ERROR,
                        persist=False,
                    ),
                )

            if not record.ownership_lost:
                journal.record_terminal_summary(
                    status=record.status.value,
                    stop_reason=record.stop_reason,
                    failure=terminal_failure,
                )
                try:
                    await _await_terminal_cleanup(journal.flush())
                except RuntimeEventOwnershipLost:
                    record.ownership_lost = True
                except Exception as exc:
                    failure = map_runtime_failure(
                        code="terminal_summary_write_failed",
                        error=exc,
                    )
                    logger.warning(
                        "Terminal summary write failed run_id=%s code=%s error_class=%s correlation_id=%s",
                        run_id,
                        failure.code,
                        failure.error_class,
                        failure.correlation_id,
                    )

        # Persist run duration to checkpoint metadata while the durable run row
        # is still active. This keeps a peer checkpoint writer from entering
        # between the final graph output and the attribution write.
        if started and not record.ownership_lost and checkpoint_access_authorized and checkpointer is not None and record.status == RunStatus.success:
            try:
                created = datetime.fromisoformat(record.created_at.replace("Z", "+00:00"))
                updated = datetime.fromisoformat(record.updated_at.replace("Z", "+00:00"))
                # Match legacy history semantics: turn_duration is the whole
                # RunRecord lifetime in integer seconds, including admission
                # delay. Persist zero for sub-second successful turns.
                duration = max(0, int((updated - created).total_seconds()))
                await _await_terminal_cleanup(
                    _persist_run_duration(
                        checkpointer=checkpointer,
                        thread_id=thread_id,
                        run_id=run_id,
                        duration_seconds=duration,
                    ),
                )
            except Exception:
                logger.debug(
                    "Failed to persist run duration for thread %s run %s (non-fatal)",
                    thread_id,
                    run_id,
                )

        if not record.ownership_lost and event_store is not None and accepted_sandbox_session is not None:
            try:
                # Re-sample both authorities immediately before the durable
                # terminal CAS. Earlier validation cannot cover delivery,
                # lifecycle, or checkpoint awaits in this cleanup path.
                await _await_terminal_cleanup(
                    accepted_sandbox_session.validate(),
                    propagate_inner_interrupt=True,
                )
            except Exception:
                record.ownership_lost = True
                logger.warning(
                    "Skipping terminal persistence after accepted sandbox authority loss for run %s",
                    run_id,
                )
            except BaseException as exc:
                record.ownership_lost = True
                if deferred_stop_interrupt is None:
                    deferred_stop_interrupt = exc
                logger.warning(
                    "Accepted sandbox terminal validation interrupted for run %s; completing cleanup first",
                    run_id,
                )

        if not record.ownership_lost and event_store is not None:
            try:
                from deerflow.runtime.kubernetes_qualification import (
                    qualification_barrier,
                    qualification_counter,
                )

                await _await_terminal_cleanup(
                    qualification_counter("terminal_commit_attempts", record),
                )
                await _await_terminal_cleanup(
                    qualification_barrier(
                        "terminal_before_lifecycle_commit",
                        record,
                    ),
                )
                # Even after bounded receipt retries are exhausted, persist the
                # real worker outcome. Leaving a successful row inflight would
                # let lease recovery rewrite it as an error with a synthetic
                # zero receipt.
                if record.abort_event.is_set():
                    await _await_terminal_cleanup(
                        run_manager.persist_current_status(run_id),
                    )
                else:
                    cancel_action = await _await_terminal_cleanup(
                        run_manager.set_status_if_not_cancelled(
                            run_id,
                            record.status,
                            error=record.error,
                            stop_reason=record.stop_reason,
                        ),
                    )
                    if cancel_action is not None:
                        await _await_terminal_cleanup(
                            _finish_cancellation(cancel_action),
                        )
                        await _await_terminal_cleanup(
                            run_manager.persist_current_status(run_id),
                        )
            except Exception:
                logger.warning(
                    "Failed to persist terminal status for run %s after delivery receipt attempts",
                    run_id,
                    exc_info=True,
                )

        if not record.ownership_lost and journal is not None and persist_completion:
            try:
                # Persist token usage + convenience fields to RunStore
                completion = journal.get_completion_data()
                await _await_terminal_cleanup(
                    run_manager.update_run_completion(
                        run_id,
                        status=record.status.value,
                        **completion,
                    ),
                )
            except Exception:
                logger.warning(
                    "Failed to persist run completion for %s (non-fatal)",
                    run_id,
                    exc_info=True,
                )

        if started and not record.ownership_lost and checkpoint_access_authorized and checkpointer is not None and record.status == RunStatus.interrupted and not _is_edit_replay_run(record):
            try:
                await _await_terminal_cleanup(
                    run_manager.wait_for_prior_finalizing(thread_id, run_id),
                )
                has_later_started_run = await _await_terminal_cleanup(
                    run_manager.has_later_started_run(thread_id, run_id),
                    interrupted_result=True,
                )
                if not has_later_started_run:
                    await _await_terminal_cleanup(
                        _ensure_interrupted_title(
                            checkpointer=checkpointer,
                            thread_id=thread_id,
                            app_config=ctx.app_config,
                            graph_input=graph_input,
                        ),
                    )
            except Exception:
                logger.debug(
                    "Failed to generate interrupted title for thread %s (non-fatal)",
                    thread_id,
                )

        projected_title: str | None = None
        if started and not record.ownership_lost and checkpoint_access_authorized and checkpointer is not None:
            try:
                ckpt_config = {"configurable": {"thread_id": thread_id, "checkpoint_ns": ""}}
                ckpt_tuple = await _await_terminal_cleanup(
                    checkpointer.aget_tuple(ckpt_config),
                )
                if ckpt_tuple is not None:
                    ckpt = getattr(ckpt_tuple, "checkpoint", {}) or {}
                    title = ckpt.get("channel_values", {}).get("title")
                    if isinstance(title, str) and title:
                        projected_title = title
            except Exception:
                logger.debug(
                    "Failed to read projected title for thread %s (non-fatal)",
                    thread_id,
                )

        # Project title/status together only when this worker owns the exact
        # terminal capability and this run remains the latest admission.
        terminal_projection_owner_id = record.terminal_projection_owner_worker_id
        terminal_projection_active_version = record.terminal_projection_active_state_version
        if started and not record.ownership_lost and thread_store is not None and terminal_projection_owner_id is not None and terminal_projection_active_version is not None and type(record.checkpoint_terminal_state_version) is int:
            try:
                final_status = "idle" if record.status == RunStatus.success else record.status.value
                await _await_terminal_cleanup(
                    thread_store.project_run(
                        ThreadMetaRunProjection(
                            run_id=run_id,
                            thread_id=thread_id,
                            owner_worker_id=terminal_projection_owner_id,
                            active_state_version=(terminal_projection_active_version),
                            terminal_state_version=(record.checkpoint_terminal_state_version),
                            status=final_status,
                            display_name=projected_title,
                        ),
                        user_id=record.user_id,
                    ),
                )
            except Exception:
                logger.debug(
                    "Failed to project terminal thread_meta fields for %s (non-fatal)",
                    thread_id,
                )

        if not record.ownership_lost and ctx.on_run_completed is not None:
            try:
                await _await_terminal_cleanup(
                    ctx.on_run_completed(record),
                    interrupt_current=True,
                )
            except Exception:
                logger.warning(
                    "Run completion hook failed for %s (non-fatal)",
                    run_id,
                    exc_info=True,
                )
            except BaseException as exc:
                # A cancellation or other control-flow interruption must not
                # bypass stream closure and local reference cleanup.
                _defer_stop_interrupt(exc)
                logger.warning(
                    "Run completion hook interrupted for run %s; completing cleanup first",
                    run_id,
                )

        if not record.ownership_lost and task_info is not None and task_store is not None:
            # Keep the finalizing barrier held until stop observers finish, so
            # a same-thread replacement cannot overlap this task's lifecycle.
            try:
                await _await_terminal_cleanup(
                    notify_task_stop(
                        extensions,
                        task_store,
                        task_info,
                        lead_task_outcome(
                            aborted=(record.abort_event.is_set() or record.status == RunStatus.interrupted),
                            succeeded=record.status == RunStatus.success,
                        ),
                        timeout=_EXTENSION_TASK_NOTIFY_TIMEOUT_SECONDS,
                    ),
                    interrupt_current=True,
                )
            except Exception:
                logger.warning(
                    "Extension task-stop notification failed for run %s (non-fatal)",
                    run_id,
                    exc_info=True,
                )
            except BaseException as exc:
                # Cancellation here must not strand the finalizing barrier or
                # leave stream consumers waiting for the end frame.
                _defer_stop_interrupt(exc)
                logger.warning(
                    "Extension task-stop notification interrupted for run %s; completing cleanup first",
                    run_id,
                )
        if record.finalizing:
            await _await_terminal_cleanup(
                run_manager.set_finalizing(run_id, False),
            )

        from deerflow.runtime.skill_projection import get_skill_projection_coordinator

        projection_coordinator = get_skill_projection_coordinator()
        projection_token = None
        if skill_binding_user_id is not None:
            projection_token = projection_coordinator.token_for_consumer(
                user_id=skill_binding_user_id,
                thread_id=thread_id,
                run_id=run_id,
                consumer_id=f"run:{run_id}:lead",
            )
        if projection_token is not None:
            from deerflow.sandbox.sandbox_provider import (
                release_accepted_skill_consumer,
            )

            try:
                await _await_terminal_cleanup(
                    asyncio.to_thread(
                        release_accepted_skill_consumer,
                        projection_token,
                    ),
                )
            except Exception:
                logger.warning(
                    "Failed to release accepted skill projection consumer for run %s",
                    run_id,
                    exc_info=True,
                )
        elif skill_binding_user_id is not None:
            projection_coordinator.release_unactivated_run(
                user_id=skill_binding_user_id,
                thread_id=thread_id,
                run_id=run_id,
            )

        if accepted_sandbox_session is not None:
            try:
                await _await_terminal_cleanup(
                    accepted_sandbox_session.close(),
                )
            except Exception:
                logger.warning(
                    "Failed to release accepted materialization for run %s",
                    run_id,
                    exc_info=True,
                )
            except BaseException as exc:
                # Preserve cancellation/interrupt semantics, but do not let an
                # interrupted provider release bypass lifecycle publication or
                # the rest of the worker's terminal cleanup.
                _defer_stop_interrupt(exc)
                logger.warning(
                    "Accepted materialization cleanup interrupted for run %s; completing terminal cleanup first",
                    run_id,
                )
            await _await_terminal_cleanup(
                _publish_accepted_sandbox_lifecycle_during_cleanup(),
                interrupt_current=True,
            )
            if runtime_ctx is not None:
                from deerflow.sandbox.accepted_material import (
                    strip_accepted_sandbox_session,
                )

                strip_accepted_sandbox_session(runtime_ctx)
        elif materialization is not None:
            try:
                await _await_terminal_cleanup(materialization.release())
            except Exception:
                logger.warning(
                    "Failed to release accepted materialization for run %s",
                    run_id,
                    exc_info=True,
                )

        if pinned_material_for_cleanup is not None:
            try:
                await _await_terminal_cleanup(
                    asyncio.to_thread(
                        pinned_material_for_cleanup.release_process_material,
                    ),
                )
            except Exception:
                logger.warning(
                    "Failed to release accepted skill snapshot for run %s",
                    run_id,
                    exc_info=True,
                )
        if dispatch_ledger is not None:
            dispatch_ledger.close()
        if not record.ownership_lost:
            try:
                await _await_terminal_cleanup(bridge.publish_end(run_id))
            except BaseException as exc:
                if deferred_stop_interrupt is None:
                    deferred_stop_interrupt = exc
                else:
                    logger.warning(
                        "Stream end publication failed while another terminal interruption was pending for run %s",
                        run_id,
                        exc_info=True,
                    )

        if journal is not None:
            try:
                await _await_terminal_cleanup(
                    journal.close(flush=not record.ownership_lost),
                )
            except Exception:
                logger.warning("Failed to close journal for run %s", run_id, exc_info=True)

        _release_run_scoped_references(
            runnable_configs,
            runtime_ctx,
            journal,
        )
        # Drop graph and per-run payload references before the terminal worker
        # task itself becomes collectable.
        agent = None
        accessor = None
        runtime = None
        runtime_ctx = None
        rollback_point = None
        subagent_events = None
        goal_evaluator_model = None
        task_store = None
        task_info = None
        pre_run_workspace_snapshot = None
        delivery_content = None
        produced_output_paths = None
        graph_input = {}

        # Local housekeeping must run even when durable finalization was fenced.
        _create_contextless_task(bridge.cleanup(run_id, delay=60))
        _create_contextless_task(run_manager.cleanup(run_id))
        _schedule_terminal_cycle_collection()

        if deferred_stop_interrupt is not None:
            raise deferred_stop_interrupt


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _checkpoint_id(checkpoint_tuple: Any) -> str | None:
    config = getattr(checkpoint_tuple, "config", {}) or {}
    configurable = config.get("configurable", {}) if isinstance(config, dict) else {}
    checkpoint_id = configurable.get("checkpoint_id") if isinstance(configurable, dict) else None
    if isinstance(checkpoint_id, str):
        return checkpoint_id
    checkpoint = getattr(checkpoint_tuple, "checkpoint", {}) or {}
    if isinstance(checkpoint, dict) and isinstance(checkpoint.get("id"), str):
        return checkpoint["id"]
    return None


def _goal_instance_matches(left: GoalState | None, right: GoalState | None) -> bool:
    if not left or not right:
        return False
    same_status = left.get("status") == right.get("status") == "active"
    same_objective = left.get("objective") == right.get("objective")
    same_created_at = left.get("created_at") == right.get("created_at")
    return same_status and same_objective and same_created_at


async def _materialized_checkpoint_messages(accessor: CheckpointStateAccessor, thread_id: str) -> list[Any]:
    """Read ``messages`` through the mode-matched accessor.

    Raw ``channel_values`` reads see a sentinel in delta mode; only a
    materialized read reconstructs the list.  Raw checkpoint tuples remain
    valid for tuple-level metadata (checkpoint id, ``pending_writes``).
    """
    snapshot = await accessor.aget({"configurable": {"thread_id": thread_id, "checkpoint_ns": ""}})
    values = getattr(snapshot, "values", None) or {}
    messages = values.get("messages") if isinstance(values, dict) else None
    return list(messages) if isinstance(messages, list) else []


def _read_checkpoint_goal(checkpoint_tuple: Any) -> GoalState | None:
    checkpoint = getattr(checkpoint_tuple, "checkpoint", {}) or {}
    channel_values = checkpoint.get("channel_values", {}) if isinstance(checkpoint, dict) else {}
    raw_goal = channel_values.get("goal") if isinstance(channel_values, dict) else None
    return copy.deepcopy(raw_goal) if isinstance(raw_goal, dict) else None


def _has_durable_goal_turn_receipt(checkpoint_tuple: Any, messages: list[Any]) -> bool:
    """Return true when a completed visible assistant turn is safely checkpointed.

    ``pending_writes`` is the durability signal: a ``CheckpointTuple`` carries no
    ``tasks`` field (those live on a ``StateSnapshot``), so the presence of any
    queued writes is what tells us the turn is still in flight.
    """
    if _checkpoint_id(checkpoint_tuple) is None:
        return False
    if getattr(checkpoint_tuple, "pending_writes", None):
        return False
    visible_messages = []
    for message in messages:
        if _is_visible_message(message) and message_to_text(message).strip():
            visible_messages.append(message)
    if not visible_messages:
        return False
    return _message_type(visible_messages[-1]) == "ai"


def _stand_down_reason(goal: GoalState, evaluation: GoalEvaluation, no_progress_count: int) -> str | None:
    if evaluation["satisfied"]:
        return None
    if evaluation["blocker"] != "goal_not_met_yet":
        return f"blocked:{evaluation['blocker']}"
    # Default caps mirror should_continue_goal so the two gate functions agree on
    # a goal dict that is missing these fields.
    if int(goal.get("continuation_count", 0)) >= int(goal.get("max_continuations", DEFAULT_MAX_GOAL_CONTINUATIONS)):
        return "max_continuations_reached"
    if no_progress_count >= int(goal.get("max_no_progress_continuations", DEFAULT_MAX_NO_PROGRESS_CONTINUATIONS)):
        return "no_progress_detected"
    return None


async def _persist_goal_evaluation(
    *,
    bridge: StreamBridge,
    checkpointer: Any,
    thread_id: str,
    run_id: str,
    goal: GoalState,
    evaluation: GoalEvaluation,
    no_progress_count: int,
    continuation_count: int | None = None,
    stand_down_reason: str | None = None,
    evidence_signature: str = "",
) -> GoalState | None:
    try:
        async with goal_thread_lock(thread_id):
            checkpoint_tuple = await _call_checkpointer_method(
                checkpointer,
                "aget_tuple",
                "get_tuple",
                {"configurable": {"thread_id": thread_id, "checkpoint_ns": ""}},
            )
            if checkpoint_tuple is None:
                return None
            current_goal = _read_checkpoint_goal(checkpoint_tuple)
            if current_goal is None or not _goal_instance_matches(goal, current_goal):
                return None
            # Defensive: compute continuation_count from the fresh current_goal
            # inside the lock.  The caller computed it from a possibly-stale goal
            # snapshot; a racing continuation may have already bumped the count.
            if continuation_count is not None:
                current_count = int(current_goal.get("continuation_count", 0))
                continuation_count = max(continuation_count, current_count + 1)
            expected_checkpoint_id = _checkpoint_id(checkpoint_tuple)
            updated_goal = attach_goal_evaluation(
                current_goal,
                evaluation,
                run_id=run_id,
                continuation_count=continuation_count,
                no_progress_count=no_progress_count,
                stand_down_reason=stand_down_reason,
                evidence_signature=evidence_signature,
            )
            values = await write_thread_goal(
                checkpointer,
                thread_id,
                updated_goal,
                as_node="goal_evaluator",
                expected_checkpoint_id=expected_checkpoint_id,
            )
        await bridge.publish(run_id, "values", serialize(values, mode="values"))
        return updated_goal
    except GoalWriteConflict:
        return None
    except Exception:
        logger.warning("Could not persist goal evaluation for thread %s", thread_id, exc_info=True)
        return None


async def _reread_goal_and_checkpoint(checkpointer: Any, thread_id: str) -> tuple[GoalState | None, Any]:
    """Re-read the goal and latest checkpoint together for a concurrency re-check."""
    goal = await read_thread_goal(checkpointer, thread_id)
    checkpoint_tuple = await _call_checkpointer_method(
        checkpointer,
        "aget_tuple",
        "get_tuple",
        {"configurable": {"thread_id": thread_id, "checkpoint_ns": ""}},
    )
    return goal, checkpoint_tuple


async def _prepare_goal_continuation_input(
    *,
    bridge: StreamBridge,
    accessor: CheckpointStateAccessor,
    checkpointer: Any,
    thread_id: str,
    run_id: str,
    model_name: str | None,
    app_config: AppConfig | None,
    evaluator_model_factory: Any | None = None,
    abort_event: asyncio.Event | None = None,
    user_id: str | None = None,
    deerflow_trace_id: str | None = None,
    task_store: Any | None = None,
    extensions: Any | None = None,
) -> dict[str, Any] | None:
    """Evaluate the active goal and return a hidden continuation input if needed.

    NOTE: The re-reads below catch a racing user message or ``/goal clear``
    before we queue a continuation. Goal writes then serialize per thread and
    pass the checkpoint id they read from, so stale evaluator writes stand down
    instead of clobbering a newer goal change.
    """
    if checkpointer is None:
        return None
    if abort_event is not None and abort_event.is_set():
        return None

    try:
        goal = await read_thread_goal(checkpointer, thread_id)
    except Exception:
        logger.warning(
            "Could not read goal for thread %s after run %s",
            thread_id,
            run_id,
            exc_info=True,
        )
        return None
    if not goal or goal.get("status") != "active":
        return None

    async def _persist(
        goal: GoalState,
        evaluation: GoalEvaluation,
        no_progress_count: int,
        *,
        stand_down_reason: str | None = None,
        continuation_count: int | None = None,
    ) -> GoalState | None:
        """Record the evaluation against the still-current goal instance."""
        return await _persist_goal_evaluation(
            bridge=bridge,
            checkpointer=checkpointer,
            thread_id=thread_id,
            run_id=run_id,
            goal=goal,
            evaluation=evaluation,
            no_progress_count=no_progress_count,
            continuation_count=continuation_count,
            stand_down_reason=stand_down_reason,
            evidence_signature=evidence_signature,
        )

    try:
        checkpoint_tuple = await _call_checkpointer_method(
            checkpointer,
            "aget_tuple",
            "get_tuple",
            {"configurable": {"thread_id": thread_id, "checkpoint_ns": ""}},
        )
        if checkpoint_tuple is None:
            return None
        checkpoint_id_before = _checkpoint_id(checkpoint_tuple)
        messages = await _materialized_checkpoint_messages(accessor, thread_id)
        conversation_signature_before = visible_conversation_signature(messages)
        evidence_signature = latest_visible_assistant_signature(messages)

        if not _has_durable_goal_turn_receipt(checkpoint_tuple, messages):
            evaluation = GoalEvaluation(
                satisfied=False,
                blocker="run_failed",
                reason="No durable assistant end-of-turn receipt was available.",
                evidence_summary="",
            )
            no_progress_count = compute_no_progress_count(goal, evaluation, evidence_signature=evidence_signature)
            await _persist(
                goal,
                evaluation,
                no_progress_count,
                stand_down_reason="no_durable_end_of_turn",
            )
            return None

        if abort_event is not None and abort_event.is_set():
            return None
        evaluator_model = evaluator_model_factory() if evaluator_model_factory is not None else None
        evaluation = await evaluate_goal_completion(
            goal,
            messages,
            model=evaluator_model,
            model_name=model_name,
            app_config=app_config,
            thread_id=thread_id,
            user_id=user_id,
            deerflow_trace_id=deerflow_trace_id,
            task_store=task_store,
            extensions=extensions,
        )
        if abort_event is not None and abort_event.is_set():
            return None
    except Exception:
        logger.warning(
            "Goal evaluator failed for thread %s after run %s",
            thread_id,
            run_id,
            exc_info=True,
        )
        return None

    no_progress_count = compute_no_progress_count(goal, evaluation, evidence_signature=evidence_signature)

    # Re-check that neither the goal nor the visible conversation changed while the
    # evaluator ran — a user message or /goal clear racing the evaluation must win.
    try:
        current_goal, current_checkpoint_tuple = await _reread_goal_and_checkpoint(checkpointer, thread_id)
    except Exception:
        logger.warning(
            "Could not re-check goal state for thread %s after evaluation",
            thread_id,
            exc_info=True,
        )
        return None

    if not _goal_instance_matches(goal, current_goal) or current_checkpoint_tuple is None:
        return None

    checkpoint_changed = _checkpoint_id(current_checkpoint_tuple) != checkpoint_id_before
    messages_changed = visible_conversation_signature(await _materialized_checkpoint_messages(accessor, thread_id)) != conversation_signature_before
    if checkpoint_changed or messages_changed:
        await _persist(
            current_goal,
            evaluation,
            no_progress_count,
            stand_down_reason="thread_changed_after_evaluation",
        )
        return None

    if evaluation["satisfied"]:
        try:
            async with goal_thread_lock(thread_id):
                latest_checkpoint_tuple = await _call_checkpointer_method(
                    checkpointer,
                    "aget_tuple",
                    "get_tuple",
                    {"configurable": {"thread_id": thread_id, "checkpoint_ns": ""}},
                )
                if latest_checkpoint_tuple is None:
                    return None
                latest_goal = _read_checkpoint_goal(latest_checkpoint_tuple)
                if latest_goal is None or not _goal_instance_matches(goal, latest_goal):
                    return None
                values = await write_thread_goal(
                    checkpointer,
                    thread_id,
                    None,
                    as_node="goal_evaluator",
                    expected_checkpoint_id=_checkpoint_id(latest_checkpoint_tuple),
                )
            await bridge.publish(run_id, "values", serialize(values, mode="values"))
        except GoalWriteConflict:
            return None
        except Exception:
            logger.warning("Could not clear satisfied goal for thread %s", thread_id, exc_info=True)
        return None

    stand_down_reason = _stand_down_reason(goal, evaluation, no_progress_count)
    if stand_down_reason is not None or not should_continue_goal(goal, evaluation, no_progress_count=no_progress_count):
        await _persist(goal, evaluation, no_progress_count, stand_down_reason=stand_down_reason)
        return None

    next_count = int(goal.get("continuation_count", 0)) + 1
    updated_goal = await _persist(goal, evaluation, no_progress_count, continuation_count=next_count)
    if updated_goal is None:
        return None

    # Final guard: the persist above bumped the checkpoint id, so only the visible
    # conversation signature is meaningful for detecting a racing user turn here.
    try:
        latest_goal, latest_checkpoint_tuple = await _reread_goal_and_checkpoint(checkpointer, thread_id)
    except Exception:
        logger.warning(
            "Could not verify queued goal continuation for thread %s",
            thread_id,
            exc_info=True,
        )
        return None
    if not _goal_instance_matches(updated_goal, latest_goal) or latest_checkpoint_tuple is None:
        return None
    if visible_conversation_signature(await _materialized_checkpoint_messages(accessor, thread_id)) != conversation_signature_before:
        # Do not pass continuation_count here: the persist above already
        # committed it (as next_count). Re-passing next_count would make
        # _persist_goal_evaluation's race guard (#4088) see that same write as
        # a "current_count" bump and add another +1 on top of it, silently
        # double-counting this single continuation attempt against the
        # continuation budget even though it is being stood down, not
        # delivered. Omitting it leaves the already-committed count untouched,
        # matching every other stand-down call site in this function.
        await _persist(
            latest_goal,
            evaluation,
            no_progress_count,
            stand_down_reason="thread_changed_before_continuation",
        )
        return None

    logger.info(
        "Run %s continuing thread %s for active goal (%d/%d)",
        run_id,
        thread_id,
        updated_goal.get("continuation_count", next_count),
        updated_goal.get("max_continuations", 0),
    )
    return {"messages": [make_goal_continuation_message(updated_goal, evaluation)]}


def _is_edit_replay_run(record: RunRecord) -> bool:
    metadata = record.metadata or {}
    return metadata.get("replay_kind") == "edit"


async def _ensure_finalizing_before_edit_failure(run_manager: RunManager, record: RunRecord) -> None:
    if _is_edit_replay_run(record) and not record.finalizing:
        await run_manager.set_finalizing(record.run_id, True)


async def _publish_restored_checkpoint_values(
    *,
    bridge: StreamBridge,
    run_id: str,
    accessor: CheckpointStateAccessor | None,
    thread_id: str,
) -> None:
    if accessor is None:
        return
    snapshot = await accessor.aget({"configurable": {"thread_id": thread_id, "checkpoint_ns": ""}})
    values = getattr(snapshot, "values", None)
    if isinstance(values, dict):
        await bridge.publish(run_id, "values", serialize(values, mode="values"))


@dataclass(frozen=True)
class RollbackPoint:
    """Materialized pre-run state used to restore the thread after cancellation.

    Raw checkpoint blobs cannot reconstruct Delta-channel messages (their
    checkpoints omit the materialized value), so rollback preserves those
    messages plus delta mode's materialized non-message state in addition to
    the raw pending writes.
    """

    config: dict[str, Any]
    state_values: dict[str, Any]
    messages: tuple[Any, ...]
    metadata: dict[str, Any]
    pending_writes: tuple[tuple[str, str, Any], ...]


async def _capture_rollback_point(
    accessor: CheckpointStateAccessor,
    checkpointer: Any,
    read_config: dict[str, Any],
) -> RollbackPoint | None:
    """Materialize the pre-run checkpoint state and its raw pending writes.

    Returns ``None`` when the thread has no checkpoint yet; the caller keeps
    the existing delete/reset rollback contract for that case.
    """
    snapshot = await accessor.aget(read_config)
    snapshot_config = getattr(snapshot, "config", None) or {}
    configurable = snapshot_config.get("configurable") or {}
    if not configurable.get("checkpoint_id"):
        return None
    checkpoint_tuple = await _call_checkpointer_method(checkpointer, "aget_tuple", "get_tuple", snapshot_config)
    raw_values = getattr(snapshot, "values", None) or {}
    messages = raw_values.get("messages") if isinstance(raw_values, dict) else None
    state_values = copy.deepcopy({key: value for key, value in raw_values.items() if key != "messages"}) if accessor.mode == "delta" and isinstance(raw_values, dict) else {}
    return RollbackPoint(
        config={
            "configurable": {
                "thread_id": configurable.get("thread_id"),
                "checkpoint_ns": configurable.get("checkpoint_ns") or "",
                "checkpoint_id": configurable.get("checkpoint_id"),
            }
        },
        state_values=state_values,
        messages=tuple(messages or ()),
        metadata=dict(getattr(snapshot, "metadata", None) or {}),
        pending_writes=tuple(getattr(checkpoint_tuple, "pending_writes", ()) or ()),
    )


def _complete_state_replacement_values(
    *,
    mutation_graph: Any,
    selected_values: dict[str, Any],
    current_values: dict[str, Any],
    run_id: str,
    operation: str,
) -> dict[str, Any]:
    """Build a whole-state replacement through the graph's effective schema."""
    writable_fields = graph_writable_channels(mutation_graph)
    reducer_fields = graph_reducer_channels(mutation_graph)
    if writable_fields is None or reducer_fields is None:
        raise RuntimeError(f"Run {run_id} could not inspect the state schema for {operation}")

    replacement_values: dict[str, Any] = {}
    for field_name in writable_fields:
        if field_name in selected_values:
            replacement = copy.deepcopy(selected_values[field_name])
        elif field_name in current_values:
            # LangGraph has no public "unset channel" update. A fresh channel
            # exposes its schema default when one exists (for example [] / {});
            # optional and otherwise-unconstructible channels reset to None.
            channel = mutation_graph.channels.get(field_name)
            replacement = copy.deepcopy(channel.get()) if channel is not None and channel.is_available() else None
        else:
            continue
        replacement_values[field_name] = Overwrite(replacement) if field_name in reducer_fields else replacement
    return replacement_values


async def _linearize_delta_checkpoint_resume(
    *,
    accessor: CheckpointStateAccessor,
    checkpointer: Any,
    config: dict[str, Any],
    thread_id: str,
    run_id: str,
) -> list[Any] | None:
    """Replace a delta-mode checkpoint fork with an equivalent linear write.

    Resuming from an older checkpoint forks the lineage, and in ``delta`` mode
    the fork's state cannot be materialized correctly: the delta history walk
    collects **every** ``pending_writes`` entry stored on each on-path
    ancestor, but a shared parent also carries the writes of the sibling child
    that was abandoned. Those writes are replayed into the fork, so the run
    starts from a message list that still contains the answer it was supposed
    to replace — regenerating in a branched thread surfaced this as the old
    assistant message reappearing beside the new one after a reload (#4458).
    Reproduced on postgres, sqlite, and the in-memory saver; ``full`` mode is
    unaffected because its checkpoints carry complete ``channel_values`` and
    need no replay.

    The upstream contract (`BaseCheckpointSaver.get_delta_channel_history` and
    the savers overriding it) is where write-to-child ownership belongs, so
    this does not reimplement it. Instead the fork is expressed as what it
    means: materialize the requested checkpoint's state and write it with
    replace semantics on the **current head**, which has no other children,
    then run linearly. Every materialized channel is restored; channels that
    exist only on the newer head are reset to their schema default (or
    ``None`` when the channel has no constructible default). The abandoned
    turn stays in checkpoint history as the rewritten head's ancestry.

    Returns the materialized messages when the resume was linearized, or
    ``None`` when there was nothing to do (full mode, no checkpoint selector,
    a non-root namespace, or a selector that already names the head). Failures
    propagate: silently falling back to the fork would persist the corrupted
    history this exists to prevent. The worker call site holds
    ``_checkpoint_thread_lock`` across rollback capture and this rewrite; do
    not reacquire that non-reentrant lock inside this helper.
    """
    if checkpointer is None or accessor.mode != "delta":
        return None
    configurable = config.get("configurable")
    if not isinstance(configurable, dict):
        return None
    checkpoint_id = configurable.get("checkpoint_id")
    if not isinstance(checkpoint_id, str) or not checkpoint_id:
        return None
    if configurable.get("checkpoint_ns"):
        # Subgraph namespaces have their own lineage; the Gateway only selects
        # root checkpoints, so leave anything else untouched.
        return None

    head_config: dict[str, Any] = {"configurable": {"thread_id": thread_id, "checkpoint_ns": ""}}
    head = await accessor.aget(head_config)
    if _checkpoint_id(head) == checkpoint_id:
        # Selecting the head is already linear — no sibling can exist yet.
        return None

    source_config: dict[str, Any] = {
        "configurable": {
            "thread_id": thread_id,
            "checkpoint_ns": "",
            "checkpoint_id": checkpoint_id,
        }
    }
    snapshot = await accessor.aget(source_config)
    values = getattr(snapshot, "values", None) or {}
    messages = values.get("messages") if isinstance(values, dict) else None
    if not isinstance(messages, list):
        raise RuntimeError(f"Run {run_id} could not materialize resume checkpoint {checkpoint_id}")

    # Write through the thread's effective schema so every application and
    # middleware channel can be restored. Reducer channels need Overwrite to
    # replace their already-aggregated value instead of merging it again.
    mutation_graph = build_state_mutation_graph(
        "checkpoint_resume",
        accessor.mode,
        graph_state_schema(getattr(accessor, "graph", None)),
    )
    selected_values = dict(values)
    head_values = getattr(head, "values", None) or {}
    head_values = dict(head_values) if isinstance(head_values, dict) else {}
    replacement_values = _complete_state_replacement_values(
        mutation_graph=mutation_graph,
        selected_values=selected_values,
        current_values=head_values,
        run_id=run_id,
        operation="checkpoint resume",
    )

    mutation_accessor = CheckpointStateAccessor.bind(mutation_graph, checkpointer, mode=accessor.mode)
    await mutation_accessor.aupdate(head_config, replacement_values, as_node="checkpoint_resume")
    configurable.pop("checkpoint_id", None)
    configurable.pop("checkpoint_map", None)
    logger.info(
        "Run %s linearized a delta-mode resume of checkpoint %s onto thread %s",
        run_id,
        checkpoint_id,
        thread_id,
    )
    return list(messages)


async def _rollback_to_pre_run_checkpoint(
    *,
    accessor: CheckpointStateAccessor | None,
    checkpointer: Any,
    thread_id: str,
    run_id: str,
    rollback_point: RollbackPoint | None,
    snapshot_capture_failed: bool,
) -> bool:
    """Restore the complete pre-run state and report whether it completed.

    Full mode forks the captured pre-run checkpoint and overwrites messages;
    all other channels inherit from that parent. Delta mode cannot safely fork
    once the cancelled path has attached writes to the same parent, so it
    replaces every captured channel on the current head instead. Both writes
    use a state-only mutation graph whose synthetic ``rollback_restore`` node
    finishes immediately and schedules no agent work.
    """
    if checkpointer is None:
        logger.info("Run %s rollback requested but no checkpointer is configured", run_id)
        return False

    if snapshot_capture_failed:
        logger.warning("Run %s rollback skipped: pre-run checkpoint capture failed", run_id)
        return False

    if rollback_point is None:
        await _call_checkpointer_method(checkpointer, "adelete_thread", "delete_thread", thread_id)
        logger.info("Run %s rollback reset thread %s to empty state", run_id, thread_id)
        return True

    configurable = rollback_point.config.get("configurable", {})
    if not configurable.get("checkpoint_id"):
        logger.warning("Run %s rollback skipped: pre-run checkpoint has no checkpoint id", run_id)
        return False

    if accessor is None:
        # Unreachable in practice: a rollback point can only be captured
        # through the bound accessor. Stay fail-closed.
        logger.warning("Run %s rollback skipped: agent accessor unavailable", run_id)
        return False

    # Compile with the thread's effective schema so middleware-contributed
    # channels survive (the base ThreadState fallback would silently drop
    # them).
    mutation_graph = build_state_mutation_graph(
        "rollback_restore",
        accessor.mode,
        graph_state_schema(getattr(accessor, "graph", None)),
    )
    mutation_accessor = CheckpointStateAccessor.bind(mutation_graph, checkpointer, mode=accessor.mode)
    if accessor.mode == "delta":
        # A delta rollback fork has the same write-ownership problem as a
        # checkpoint resume: the captured parent now carries writes from the
        # cancelled sibling. Restore linearly on the current head instead.
        restore_config: dict[str, Any] = {"configurable": {"thread_id": thread_id, "checkpoint_ns": ""}}
        current = await accessor.aget(restore_config)
        raw_current_values = getattr(current, "values", None) or {}
        current_values = dict(raw_current_values) if isinstance(raw_current_values, dict) else {}
        selected_values = copy.deepcopy(rollback_point.state_values)
        selected_values["messages"] = list(rollback_point.messages)
        replacement_values = _complete_state_replacement_values(
            mutation_graph=mutation_graph,
            selected_values=selected_values,
            current_values=current_values,
            run_id=run_id,
            operation="rollback",
        )
    else:
        restore_config = rollback_point.config
        replacement_values = {"messages": Overwrite(list(rollback_point.messages))}

    restored_config = await mutation_accessor.aupdate(
        restore_config,
        replacement_values,
        as_node="rollback_restore",
    )
    if not isinstance(restored_config, dict):
        raise RuntimeError(f"Run {run_id} rollback restore returned invalid config: expected dict")
    restored_configurable = restored_config.get("configurable", {})
    if not isinstance(restored_configurable, dict):
        raise RuntimeError(f"Run {run_id} rollback restore returned invalid config payload")
    restored_checkpoint_id = restored_configurable.get("checkpoint_id")
    if not restored_checkpoint_id:
        raise RuntimeError(f"Run {run_id} rollback restore did not return checkpoint_id")

    pending_writes = rollback_point.pending_writes
    if not pending_writes:
        return True

    writes_by_task: dict[str, list[tuple[str, Any]]] = {}
    for item in pending_writes:
        if not isinstance(item, (tuple, list)) or len(item) != 3:
            raise RuntimeError(f"Run {run_id} rollback failed: pending_write is not a 3-tuple: {item!r}")
        task_id, channel, value = item
        if not isinstance(channel, str):
            raise RuntimeError(f"Run {run_id} rollback failed: pending_write has non-string channel: task_id={task_id!r}, channel={channel!r}")
        writes_by_task.setdefault(str(task_id), []).append((channel, value))

    for task_id, writes in writes_by_task.items():
        await _call_checkpointer_method(
            checkpointer,
            "aput_writes",
            "put_writes",
            restored_config,
            writes,
            task_id=task_id,
        )
    return True


def _new_checkpoint_marker() -> dict[str, str]:
    marker = empty_checkpoint()
    return {"id": marker["id"], "ts": marker["ts"]}


def _bump_channel_version(checkpointer: Any, current_version: Any) -> Any:
    """Return a strictly-different next version for a checkpoint channel.

    DB-backed LangGraph savers (PostgresSaver / v4 SqliteSaver blob layout)
    persist channel blobs keyed by ``channel_versions[<channel>]``, so the
    new value MUST differ from the prior value. We delegate to the
    checkpointer's ``get_next_version`` when available — that is the canonical
    versioning scheme each saver picks (int, monotonic float, or
    UUID-shaped string). When the checkpointer doesn't expose it (or it
    returns ``None``/an unchanged value), fall back to a defensive bump that
    still guarantees inequality.
    """
    get_next_version = getattr(checkpointer, "get_next_version", None)
    if callable(get_next_version):
        try:
            next_version = get_next_version(current_version, None)
        except Exception:
            next_version = None
        if next_version is not None and next_version != current_version:
            return next_version
        # fall through to defensive bump

    if isinstance(current_version, bool):
        # ``bool`` is a subclass of ``int``; treat True/False as 1/0 instead of
        # adding to the boolean itself, which would produce an int anyway but
        # via a path that surprises readers.
        return int(current_version) + 1
    if isinstance(current_version, int):
        return current_version + 1
    if isinstance(current_version, float):
        # Match LangGraph's default float versioning (monotonic increment).
        return current_version + 1.0
    if isinstance(current_version, str):
        try:
            return str(int(current_version) + 1)
        except ValueError:
            return f"{current_version}.1"
    return 1


def _checkpoint_identity(ckpt_tuple: Any | None, checkpoint: dict[str, Any]) -> str | None:
    tuple_config = getattr(ckpt_tuple, "config", {}) or {}
    tuple_configurable = tuple_config.get("configurable", {}) if isinstance(tuple_config, dict) else {}
    if isinstance(tuple_configurable, dict):
        checkpoint_id = tuple_configurable.get("checkpoint_id")
        if isinstance(checkpoint_id, str) and checkpoint_id:
            return checkpoint_id
    checkpoint_id = checkpoint.get("id")
    return checkpoint_id if isinstance(checkpoint_id, str) and checkpoint_id else None


def _checkpoint_namespace(ckpt_tuple: Any | None) -> str:
    tuple_config = getattr(ckpt_tuple, "config", {}) or {}
    tuple_configurable = tuple_config.get("configurable", {}) if isinstance(tuple_config, dict) else {}
    checkpoint_ns = tuple_configurable.get("checkpoint_ns", "") if isinstance(tuple_configurable, dict) else ""
    return checkpoint_ns if isinstance(checkpoint_ns, str) else ""


def _graph_input_messages(graph_input: Any | None) -> list[Any]:
    if not isinstance(graph_input, dict):
        return []
    messages = graph_input.get("messages")
    if isinstance(messages, list):
        return messages
    if isinstance(messages, tuple):
        return list(messages)
    return []


def _title_generation_state(channel_values: dict[str, Any], graph_input: Any | None) -> dict[str, Any]:
    state = dict(channel_values)
    messages = state.get("messages")
    if not messages:
        fallback_messages = _graph_input_messages(graph_input)
        if fallback_messages:
            state["messages"] = fallback_messages
    return state


def valid_duration_entry(run_id: Any, duration_seconds: Any) -> bool:
    """Check that (run_id, duration_seconds) is a well-formed duration entry."""
    return isinstance(run_id, str) and bool(run_id) and isinstance(duration_seconds, int) and not isinstance(duration_seconds, bool)


RUN_MESSAGE_IDS_METADATA_KEY = "run_message_ids"


def valid_run_message_id_entry(message_id: Any, run_id: Any) -> bool:
    """Check that a persisted legacy message-to-run attribution is well formed."""
    return isinstance(message_id, str) and bool(message_id) and isinstance(run_id, str) and bool(run_id)


async def persist_run_history_metadata(
    *,
    checkpointer: Any,
    thread_id: str,
    durations: dict[str, int] | None = None,
    message_run_ids: dict[str, str] | None = None,
) -> bool:
    """Merge validated run history indexes into a metadata-only checkpoint.

    Durations accumulate so the history fast path can serve every known turn
    from the latest checkpoint. Legacy AI-message attributions are persisted
    alongside them for every audited AI ID, including boundary fallbacks whose
    event lookup was exhaustively empty. The full mapping is deliberate: it is
    both the exact-attribution cache and the negative-result coverage proof.
    While the materialized message set at the head remains unchanged, later
    reads query only uncached IDs. This metadata-only merge retains existing
    entries, so compaction timing or historical migration may leave stale IDs;
    reads ignore them because they only consult IDs in the materialized history.
    """
    duration_updates = {run_id: max(0, duration_seconds) for run_id, duration_seconds in (durations or {}).items() if valid_duration_entry(run_id, duration_seconds)}
    message_run_id_updates = {message_id: run_id for message_id, run_id in (message_run_ids or {}).items() if valid_run_message_id_entry(message_id, run_id)}
    if not duration_updates and not message_run_id_updates:
        return False

    ckpt_config = {"configurable": {"thread_id": thread_id, "checkpoint_ns": ""}}
    async with _checkpoint_thread_lock(thread_id):
        for _attempt in range(3):
            ckpt_tuple = await _call_checkpointer_method(checkpointer, "aget_tuple", "get_tuple", ckpt_config)
            if ckpt_tuple is None:
                return False

            checkpoint = dict(getattr(ckpt_tuple, "checkpoint", {}) or {})
            metadata = dict(getattr(ckpt_tuple, "metadata", {}) or {})
            raw_run_durations = metadata.get("run_durations")
            run_durations = {key: value for key, value in raw_run_durations.items() if valid_duration_entry(key, value)} if isinstance(raw_run_durations, dict) else {}
            raw_message_run_ids = metadata.get(RUN_MESSAGE_IDS_METADATA_KEY)
            run_message_ids = {message_id: run_id for message_id, run_id in raw_message_run_ids.items() if valid_run_message_id_entry(message_id, run_id)} if isinstance(raw_message_run_ids, dict) else {}
            changed_durations = {run_id: duration for run_id, duration in duration_updates.items() if run_durations.get(run_id) != duration}
            changed_message_run_ids = {message_id: run_id for message_id, run_id in message_run_id_updates.items() if run_message_ids.get(message_id) != run_id}
            if not changed_durations and not changed_message_run_ids:
                return False

            run_durations.update(changed_durations)
            run_message_ids.update(changed_message_run_ids)
            parent_checkpoint_id = _checkpoint_identity(ckpt_tuple, checkpoint)
            latest_tuple = await _call_checkpointer_method(checkpointer, "aget_tuple", "get_tuple", ckpt_config)
            latest_checkpoint = dict(getattr(latest_tuple, "checkpoint", {}) or {}) if latest_tuple is not None else {}
            if _checkpoint_identity(latest_tuple, latest_checkpoint) != parent_checkpoint_id:
                continue

            checkpoint.update(_new_checkpoint_marker())
            metadata["source"] = "update"
            prev_step = metadata.get("step")
            metadata["step"] = (prev_step + 1) if isinstance(prev_step, int) else 1
            metadata["run_durations"] = run_durations
            if run_message_ids:
                metadata[RUN_MESSAGE_IDS_METADATA_KEY] = run_message_ids
            else:
                metadata.pop(RUN_MESSAGE_IDS_METADATA_KEY, None)
            metadata["writes"] = {
                "runtime_run_duration": {
                    "run_ids": sorted(changed_durations),
                    "message_ids": sorted(changed_message_run_ids),
                }
            }

            checkpoint_ns = _checkpoint_namespace(ckpt_tuple)
            write_config = {
                "configurable": {
                    "thread_id": thread_id,
                    "checkpoint_ns": checkpoint_ns,
                    "checkpoint_id": parent_checkpoint_id,
                }
            }
            await _call_checkpointer_method(
                checkpointer,
                "aput",
                "put",
                write_config,
                checkpoint,
                metadata,
                {},
            )
            return True
    return False


async def persist_run_durations(
    *,
    checkpointer: Any,
    thread_id: str,
    durations: dict[str, int],
) -> bool:
    """Merge validated run durations into a metadata-only checkpoint."""
    return await persist_run_history_metadata(
        checkpointer=checkpointer,
        thread_id=thread_id,
        durations=durations,
    )


async def _persist_run_duration(
    *,
    checkpointer: Any,
    thread_id: str,
    run_id: str,
    duration_seconds: int,
) -> None:
    """Persist one completed run duration in the thread checkpoint metadata."""
    await persist_run_durations(
        checkpointer=checkpointer,
        thread_id=thread_id,
        durations={run_id: duration_seconds},
    )


async def _ensure_interrupted_title(
    *,
    checkpointer: Any,
    thread_id: str,
    app_config: AppConfig | None,
    graph_input: Any | None = None,
) -> str | None:
    """Persist a local fallback title for interrupted first-turn runs.

    Returns the title that is now persisted (existing or newly written), or
    ``None`` when no checkpoint is available or no title text can be derived.
    Idempotent: re-invoking against a checkpoint that already carries a title
    short-circuits without writing a new checkpoint.
    """
    from deerflow.agents.middlewares.title_middleware import TitleMiddleware

    middleware = TitleMiddleware(app_config=app_config) if app_config is not None else TitleMiddleware()
    ckpt_config = {"configurable": {"thread_id": thread_id, "checkpoint_ns": ""}}

    for _attempt in range(3):
        ckpt_tuple = await _call_checkpointer_method(checkpointer, "aget_tuple", "get_tuple", ckpt_config)
        checkpoint = copy.deepcopy(getattr(ckpt_tuple, "checkpoint", {}) or {}) if ckpt_tuple is not None else empty_checkpoint()
        channel_values = dict(checkpoint.get("channel_values", {}) or {})
        existing_title = channel_values.get("title")
        if existing_title:
            return existing_title

        result = middleware._generate_title_result(
            _title_generation_state(channel_values, graph_input),
            allow_partial_exchange=True,
        )
        title = result.get("title") if isinstance(result, dict) else None
        if not title:
            return None

        # ``empty_checkpoint()`` creates a fresh id every time; only real tuples
        # carry an identity stable enough for the stale-snapshot comparison.
        base_identity = _checkpoint_identity(ckpt_tuple, checkpoint) if ckpt_tuple is not None else None
        latest_tuple = await _call_checkpointer_method(checkpointer, "aget_tuple", "get_tuple", ckpt_config)
        latest_checkpoint = copy.deepcopy(getattr(latest_tuple, "checkpoint", {}) or {}) if latest_tuple is not None else empty_checkpoint()
        latest_identity = _checkpoint_identity(latest_tuple, latest_checkpoint) if latest_tuple is not None else None
        if base_identity is None:
            if latest_identity is not None:
                continue
        elif latest_identity != base_identity:
            continue

        checkpoint = latest_checkpoint
        channel_values = dict(checkpoint.get("channel_values", {}) or {})
        existing_title = channel_values.get("title")
        if existing_title:
            return existing_title

        channel_values["title"] = title
        marker = _new_checkpoint_marker()
        checkpoint.update({"id": marker["id"], "ts": marker["ts"], "channel_values": channel_values})

        # Bump ``channel_versions["title"]`` and declare the bump in ``new_versions``
        # so DB-backed savers (SqliteSaver v4 / PostgresSaver) actually persist the
        # new blob — those savers strip inline ``channel_values`` from ``put`` and
        # only write blobs for channels listed in ``new_versions``. The legacy
        # single-table sqlite saver ignores ``new_versions`` and inlines the
        # snapshot, so this path is correct for both layouts. Mirrors
        # ``_rollback_to_pre_run_checkpoint`` in the same file.
        channel_versions = dict(checkpoint.get("channel_versions", {}) or {})
        next_title_version = _bump_channel_version(checkpointer, channel_versions.get("title"))
        channel_versions["title"] = next_title_version
        checkpoint["channel_versions"] = channel_versions

        metadata = dict(getattr(latest_tuple, "metadata", {}) or {})
        metadata["source"] = "update"
        prev_step = metadata.get("step")
        metadata["step"] = (prev_step + 1) if isinstance(prev_step, int) else 1
        metadata["writes"] = {"runtime_interrupt_title": {"title": title}}

        checkpoint_ns = _checkpoint_namespace(latest_tuple)
        # Parent to the checkpoint this write was derived from - a parentless
        # raw write would sever Delta-channel replay ancestry (and truncate
        # full-mode history walks).
        write_config = {
            "configurable": {
                "thread_id": thread_id,
                "checkpoint_ns": checkpoint_ns,
                "checkpoint_id": latest_identity,
            }
        }
        await _call_checkpointer_method(
            checkpointer,
            "aput",
            "put",
            write_config,
            checkpoint,
            metadata,
            {"title": next_title_version},
        )
        return title

    return None


def _lg_mode_to_sse_event(mode: str) -> str:
    """Map LangGraph internal stream_mode name to SSE event name.

    LangGraph's ``astream(stream_mode="messages")`` produces message
    tuples.  The SSE protocol calls this ``messages-tuple`` when the
    client explicitly requests it, but the default SSE event name used
    by LangGraph Platform is simply ``"messages"``.
    """
    # All LG modes map 1:1 to SSE event names — "messages" stays "messages"
    return mode


def _error_fallback_message_from_metadata(metadata: dict[str, Any], content: Any) -> str:
    detail = metadata.get("error_detail")
    if isinstance(detail, str) and detail.strip():
        return detail.strip()
    reason = metadata.get("error_reason")
    if isinstance(reason, str) and reason.strip():
        return reason.strip()
    if isinstance(content, str) and content.strip():
        return content.strip()[:2000]
    return "LLM provider failed after retries"


def _message_id(obj: Any) -> str | None:
    """Best-effort extraction of a stable message id from a message-like object."""
    msg_id = getattr(obj, "id", None)
    if isinstance(msg_id, str) and msg_id:
        return msg_id
    if isinstance(obj, dict):
        raw = obj.get("id")
        if isinstance(raw, str) and raw:
            return raw
    return None


def _try_extract_from_message(obj: Any, pre_existing_ids: set[str] | None = None) -> str | None:
    """Try to extract fallback marker from a single message object or dict.

    Messages whose id appears in ``pre_existing_ids`` are skipped — those are
    history checkpointed by a *prior* run on this thread and any fallback
    marker on them was already accounted for when that earlier run finished.
    Without this filter, a single past run that ended with a fallback marker
    would mark every subsequent run on the same thread as ``error``, because
    LangGraph replays the full message history through ``stream_mode="values"``.
    """
    if pre_existing_ids:
        msg_id = _message_id(obj)
        if msg_id is not None and msg_id in pre_existing_ids:
            return None

    additional_kwargs = getattr(obj, "additional_kwargs", None)
    if isinstance(additional_kwargs, dict) and additional_kwargs.get("deerflow_error_fallback"):
        return _error_fallback_message_from_metadata(additional_kwargs, getattr(obj, "content", None))

    if isinstance(obj, dict):
        nested_kwargs = obj.get("additional_kwargs")
        if isinstance(nested_kwargs, dict) and nested_kwargs.get("deerflow_error_fallback"):
            return _error_fallback_message_from_metadata(nested_kwargs, obj.get("content"))
    return None


def _extract_llm_error_fallback_message(value: Any, pre_existing_ids: set[str] | None = None) -> str | None:
    """Find LLM fallback markers in streamed LangGraph chunks.

    Error fallback messages returned by model-call middleware are not guaranteed
    to pass through LLM end callbacks, but they do appear in graph state chunks.

    Messages whose id appears in ``pre_existing_ids`` are ignored — they are
    history from prior runs on the same thread (LangGraph replays the full
    messages channel in ``stream_mode="values"`` chunks), and any error
    fallback in that history was already resolved when its run finished.
    """
    # Fast path: large state chunks produced by stream_mode="values" have a
    # top-level "messages" list. Scanning only that list avoids expensive deep
    # recursion into large state dicts.
    if isinstance(value, dict):
        messages = value.get("messages")
        if isinstance(messages, (list, tuple)):
            for msg in messages:
                result = _try_extract_from_message(msg, pre_existing_ids)
                if result is not None:
                    return result
            # Fallback marker is attached to an AI message in the messages
            # channel; it will never appear elsewhere in a values chunk.
            return None
        # No top-level "messages" — this is likely an "updates" chunk (small
        # dict keyed by node name). Fall through to deep walk, which is cheap
        # for these payloads.

    # Deep walk for updates / messages / tuple / list modes. Payloads are
    # small, so full recursion is acceptable here.
    seen: set[int] = set()

    def walk(obj: Any) -> str | None:
        oid = id(obj)
        if oid in seen:
            return None
        seen.add(oid)

        result = _try_extract_from_message(obj, pre_existing_ids)
        if result is not None:
            return result

        if isinstance(obj, dict):
            for item in obj.values():
                result = walk(item)
                if result is not None:
                    return result
            return None

        if isinstance(obj, (list, tuple, set)):
            for item in obj:
                result = walk(item)
                if result is not None:
                    return result
        return None

    return walk(value)


def _collect_pre_existing_message_ids(values: Any) -> set[str]:
    """Collect stable message IDs from graph-materialized channel values."""
    if not isinstance(values, dict):
        return set()
    messages = values.get("messages")
    if not isinstance(messages, (list, tuple)):
        return set()
    return {message_id for message in messages if (message_id := _message_id(message)) is not None}


def _unpack_stream_item(
    item: Any,
    lg_modes: list[str],
    stream_subgraphs: bool,
) -> tuple[str | None, Any, tuple[str, ...]]:
    """Unpack a multi-mode or subgraph stream item into (mode, chunk, namespace).

    ``namespace`` is the subgraph namespace tuple LangGraph prefixes onto each
    frame when ``subgraphs=True``; it is empty for root-graph frames. Delegated
    subagent graphs inherit the parent's checkpoint namespace (see
    ``subagents/executor.py``), so their frames arrive here with a non-empty
    namespace and must not be mistaken for root frames.

    Returns ``(None, None, ())`` if the item cannot be parsed.
    """
    if stream_subgraphs:
        if isinstance(item, tuple) and len(item) == 3:
            ns, mode, chunk = item
            namespace = tuple(str(part) for part in ns) if isinstance(ns, (list, tuple)) else (str(ns),)
            return str(mode), chunk, namespace
        if isinstance(item, tuple) and len(item) == 2:
            mode, chunk = item
            return str(mode), chunk, ()
        return None, None, ()

    if isinstance(item, tuple) and len(item) == 2:
        mode, chunk = item
        return str(mode), chunk, ()

    # Fallback: single-element output from first mode
    return lg_modes[0] if lg_modes else None, item, ()


def _compose_sse_event(sse_event: str, namespace: tuple[str, ...]) -> str:
    """Namespace-qualified SSE event name, LangGraph Platform style.

    Root frames keep the bare event name; subgraph frames become
    ``mode|ns1|ns2`` so clients can tell them apart. The LangGraph SDK parses
    exactly this shape (``event.split("|").slice(1)``) and routes
    subagent-namespaced values away from the thread view.
    """
    if not namespace:
        return sse_event
    return "|".join((sse_event, *namespace))


#: Sentinel generation for an identity no lookup has missed at yet. Below every
#: real generation, so it never reads as "already asked at this generation".
_NEVER_LOOKED_UP = -1


def _NO_FEED_WRITES() -> int:  # noqa: N802 — a callable constant, not a class
    """Generation source for a path with no journal: the feed never moves."""
    return 0


class _MessageSeqStamper:
    """Attach each already-persisted message's feed seq to a ``values`` frame.

    A checkpoint carries no seq of its own and loses messages to summarization,
    so a client merging it with the seq-ordered thread feed cannot place a
    surviving old message once the feed's loaded page window no longer reaches
    back to it (#4666). The seq exists in the event store keyed by message
    identity; this carries it to the client and writes nothing back to the
    checkpoint.

    Cost is bounded to frames that introduce identities it has not resolved
    yet: a resolved seq is final (the feed's earliest-seq-wins rule), so it is
    never looked up twice, and in a real run the only frame that pays for a
    query is the one where compaction brings older messages back into view.

    A miss, unlike a hit, is only provisional. A message this run produces
    reaches a frame before ``RunJournal`` flushes it, so it legitimately misses
    and would stay unstamped for the whole run if that answer were kept — the
    long run that then rolls past the history page and compacts is exactly the
    case this stamper exists for. Misses are therefore re-asked, but only once
    the feed has actually gained rows, which *feed_generation* reports; frames
    alone never trigger a retry. A failed lookup is a miss under the same rule,
    so a transient store error costs one generation, not the run.

    An unstamped message needs no seq while it is still streaming: appending it
    at the tail is already its correct position.
    """

    __slots__ = ("_store", "_thread_id", "_user_id", "_seqs", "_missing", "_feed_generation")

    def __init__(self, event_store: Any, thread_id: str, *, feed_generation: Callable[[], int] | None = None) -> None:
        self._store = event_store
        self._thread_id = thread_id
        # Without a generation source (no journal on this path) nothing can
        # report a feed write, so a miss stays cached rather than being re-asked
        # on every frame: a constant reads as "the feed has not moved".
        self._feed_generation = feed_generation if feed_generation is not None else _NO_FEED_WRITES
        # Soft-resolved once at build time, like the worker's write paths: a
        # launch path that never inherits the auth contextvar (e.g. a
        # null-owner scheduled task) writes rows with no user_id, so the
        # lookup filters by the same id the writes stamped — or not at all —
        # instead of the db store's strict AUTO default raising per frame.
        user = get_current_user()
        self._user_id: str | None = str(user.id) if user is not None else None
        self._seqs: dict[str, int] = {}
        # identity -> the feed generation its last lookup missed at.
        self._missing: dict[str, int] = {}

    async def stamp(self, payload: Any) -> Any:
        if self._store is None or not isinstance(payload, Mapping):
            return payload
        messages = payload.get("messages")
        if not isinstance(messages, list) or not messages:
            return payload

        identities = [message_identity(m) if isinstance(m, Mapping) else None for m in messages]
        # Read before the lookup, never after: a write landing mid-query then
        # advances past the generation the miss is recorded under, so the next
        # frame re-asks. The reverse order could bury such a write.
        generation = self._feed_generation()
        unresolved = {i for i in identities if i is not None and i not in self._seqs and self._missing.get(i, _NEVER_LOOKED_UP) != generation}
        if unresolved:
            try:
                found = await self._store.get_message_seqs(self._thread_id, sorted(unresolved), user_id=self._user_id)
            except Exception:
                # Placement is an enhancement: a client without seq falls back
                # to its own ordering rule. Never fail the frame over it.
                logger.warning("Failed to resolve message seqs for thread %s", self._thread_id, exc_info=True)
                found = {}
            self._seqs.update(found)
            self._missing.update(dict.fromkeys(unresolved - found.keys(), generation))

        stamped = [attach_message_seq(message, seq) if identity is not None and (seq := self._seqs.get(identity)) is not None else message for message, identity in zip(messages, identities, strict=True)]
        return {**payload, "messages": stamped}


def _build_seq_stamper(event_store: Any, thread_id: str, journal: Any) -> _MessageSeqStamper:
    """Build the run's stamper, reading feed writes from *journal*.

    The journal owns the writes that turn a lookup miss into a hit, so it is
    also what can tell the stamper that a cached miss is worth re-asking. A run
    without one has no writer to report, and the stamper falls back to keeping
    its misses.
    """
    return _MessageSeqStamper(
        event_store,
        thread_id,
        feed_generation=(lambda: journal.feed_generation) if journal is not None else None,
    )


async def _publish_stream_item(
    *,
    bridge: Any,
    run_id: str,
    mode: str,
    chunk: Any,
    namespace: tuple[str, ...],
    file_tool_chunk_batcher: Any,
    subagent_events: Any,
    seq_stamper: Any = None,
) -> None:
    """Publish one stream frame, preserving the subgraph namespace.

    A subgraph frame published under a bare event name impersonates the root
    graph: a delegated subagent's ``values`` snapshot then replaces the whole
    thread view in SDK clients and its token chunks flood the parent message
    stream (#4399). Subgraph frames therefore keep their namespace in the event
    name and bypass the root-only consumers (file-tool chunk batcher, subagent
    event persistence — task_* lifecycle events are root frames already).
    """
    sse_event = _compose_sse_event(_lg_mode_to_sse_event(mode), namespace)
    if namespace:
        await bridge.publish(run_id, sse_event, serialize(chunk, mode=mode))
        return
    if file_tool_chunk_batcher is not None and mode != "messages":
        pending_chunks = file_tool_chunk_batcher.finish() if mode == "values" else file_tool_chunk_batcher.flush()
        for publish_chunk in pending_chunks:
            await bridge.publish(run_id, "messages", serialize(publish_chunk, mode="messages"))
    chunks_to_publish = file_tool_chunk_batcher.push(chunk) if mode == "messages" and file_tool_chunk_batcher is not None else [chunk]
    for publish_chunk in chunks_to_publish:
        payload = serialize(publish_chunk, mode=mode)
        if mode == "values" and seq_stamper is not None:
            # Root frames only: a subagent's snapshot is not part of this
            # thread's feed ordering (the namespaced branch returned above).
            payload = await seq_stamper.stamp(payload)
        await bridge.publish(run_id, sse_event, payload)
    if mode == "custom":
        await subagent_events.add(chunk)
