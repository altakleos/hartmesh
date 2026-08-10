"""Run lifecycle service layer.

Centralizes the business logic for creating runs, formatting SSE
frames, and consuming stream bridge events.  Router modules
(``thread_runs``, ``runs``) are thin HTTP handlers that delegate here.
"""

from __future__ import annotations

import asyncio
import json
import logging
import re
import threading
import uuid
from collections.abc import AsyncIterator, Mapping
from contextlib import asynccontextmanager, contextmanager
from types import SimpleNamespace
from typing import Any

from deerflow_extension_api import (
    ActingServiceV1,
    EffectiveSubjectV1,
    InvocationIdentityV1,
    NamespacedContextReferenceV1,
    OriginContributionRequestV1,
    PrincipalProjectionV1,
    ResolvedAgentRevisionReferenceV1,
    ResolvedProfileRevisionReferenceV1,
    RunContextContributionRequestV1,
    SafeContextReferenceV1,
    SealedOriginV1,
    TrustedRunContextV1,
    validate_model_profile_identifier,
)
from fastapi import HTTPException, Request
from langchain_core.messages import BaseMessage
from langchain_core.messages.utils import convert_to_messages
from langgraph.types import Command

from app.gateway.auth_disabled import AUTH_DISABLED_USER_ID, AUTH_SOURCE_AUTH_DISABLED, AUTH_SOURCE_INTERNAL
from app.gateway.deps import (
    get_checkpointer,
    get_local_provider,
    get_run_context,
    get_run_manager,
    get_stream_bridge,
    get_thread_store,
)
from app.gateway.internal_auth import (
    INTERNAL_SYSTEM_ROLE,
    get_internal_user,
    get_trusted_internal_owner_user_id,
)
from app.gateway.run_models import RunCreateRequest
from app.gateway.utils import sanitize_log_param
from app.runtime.authorization import ProviderInvocationAuthorization
from app.runtime.idempotency import (
    SYSTEM_TASK_OWNER,
    CanonicalCallerIntent,
    EffectiveExecutionProjection,
    canonical_request_digest,
    canonical_request_value,
    normalize_external_key,
    scope_for_channel,
    scope_for_http,
    scope_for_scheduler,
    scope_for_service,
)
from app.runtime.invocation import (
    DurableAdmission,
    InternalAdmissionIdentity,
    InternalCancelRequest,
    InternalLaunchIntent,
    InternalNativeChannelFacts,
    InternalSourceKind,
    InvocationAuthorizationOutcome,
    InvocationPrincipal,
    InvocationRuntime,
    NotFoundOrInvisible,
    PreparedLaunch,
    TaskFactory,
    WorkerCoroutine,
    thaw_host_value,
)
from app.runtime.native_binding import InternalVerifiedNativeBindingKind
from app.runtime.service_identity import validate_persisted_service_id
from app.runtime.visibility import ObservationVisibilityResolver, ServiceObservationGrant
from deerflow.agents.middlewares.dynamic_context_middleware import _DYNAMIC_CONTEXT_REMINDER_KEY, _REMINDER_DATE_KEY
from deerflow.agents.middlewares.view_image_middleware import _IMAGE_CONTEXT_MESSAGE_MARKER_KEY
from deerflow.config.agents_config import validate_agent_name
from deerflow.config.app_config import get_app_config
from deerflow.config.database_config import resolve_checkpoint_graph_cache_max
from deerflow.diagnostics import bounded_diagnostic
from deerflow.persistence.thread_meta import ThreadMetaAlreadyExistsError
from deerflow.runtime import (
    END_SENTINEL,
    HEARTBEAT_SENTINEL,
    ORPHAN_RECOVERY_STOP_REASON,
    CancelOutcome,
    CheckpointStateAccessor,
    ConflictError,
    DisconnectMode,
    RunContext,
    RunManager,
    RunRecord,
    RunStatus,
    StreamBridge,
    StreamGap,
    ThreadOperationKind,
    UnsupportedStrategyError,
    build_state_mutation_graph,
    run_agent,
)
from deerflow.runtime.accepted_invocation import (
    INVOCATION_IDENTITY_CONTEXT_KEY,
    INVOCATION_ORIGIN_CONTEXT_KEY,
    TRUSTED_RUN_CONTEXT_KEY,
    AcceptedInvocation,
    InvocationOrigin,
    PrincipalProjection,
    ResolvedAgentMaterialV1,
    canonical_digest,
)
from deerflow.runtime.agent_revision import (
    RESOLVED_AGENT_MATERIAL_CONTEXT_KEY,
    resolve_agent_revision,
)
from deerflow.runtime.checkpoint_mode import (
    INTERNAL_CHECKPOINT_MODE_KEY,
    CheckpointModeMismatchError,
    checkpoint_tuple_uses_delta,
    inject_checkpoint_mode,
)
from deerflow.runtime.checkpoint_state import graph_state_schema
from deerflow.runtime.goal import goal_thread_lock
from deerflow.runtime.journal import build_checkpoint_history_seed_events
from deerflow.runtime.runs.lifecycle_query import (
    LifecyclePage,
    LifecycleQuery,
    LifecycleVisibilityScope,
)
from deerflow.runtime.runs.manager import IdempotencyConflictError
from deerflow.runtime.runs.naming import resolve_root_run_name
from deerflow.runtime.runs.store.base import AdmissionOutcome, CancellationRequestOutcome
from deerflow.runtime.secret_context import (
    LegacyRunMetadataSecretError,
    redact_config_secrets,
    validate_run_metadata_secrets,
)
from deerflow.runtime.stream_modes import normalize_stream_modes
from deerflow.runtime.user_context import DEFAULT_USER_ID, reset_current_user, set_current_user
from deerflow.utils.messages import ORIGINAL_USER_CONTENT_KEY
from deerflow.utils.thread_id import validate_thread_id

logger = logging.getLogger(__name__)


def _invocation_principal_from_projection(
    principal: PrincipalProjection,
    *,
    visibility_prevalidated: bool = False,
) -> InvocationPrincipal:
    return InvocationPrincipal(
        user_id=principal.user_id,
        role=principal.role,
        oauth_provider=principal.oauth_provider,
        oauth_id=principal.oauth_id,
        channel_user_id=principal.channel_user_id,
        is_internal=principal.is_internal,
        visibility_prevalidated=visibility_prevalidated,
        identity=principal.identity,
    )


async def invocation_principal_from_request(
    request: Request,
    *,
    user_id: str | None = None,
    visibility_prevalidated: bool = False,
) -> InvocationPrincipal:
    """Project the authenticated request principal for runtime authorization."""
    user = getattr(getattr(request, "state", None), "user", None)
    resolved_user_id = user_id if user_id is not None else getattr(user, "id", None)
    auth_source = getattr(getattr(request, "state", None), "auth_source", None)
    if resolved_user_id is None and auth_source == AUTH_SOURCE_AUTH_DISABLED:
        resolved_user_id = AUTH_DISABLED_USER_ID
    user_id_value = str(resolved_user_id) if resolved_user_id is not None else None
    role = getattr(user, "system_role", None)
    identity = None
    if role == INTERNAL_SYSTEM_ROLE:
        owner_user_id = get_trusted_internal_owner_user_id(request)
        if owner_user_id is not None:
            owner = await resolve_trusted_internal_owner_for_attribution(
                request,
                owner_user_id,
            )
            if owner is None:
                raise ValueError("trusted internal invocation owner could not be revalidated")
            identity = InvocationIdentityV1(
                effective_subject=EffectiveSubjectV1(
                    kind="human",
                    subject_id=owner_user_id,
                    role=getattr(owner, "system_role", None),
                    oauth_provider=getattr(owner, "oauth_provider", None),
                    oauth_id=getattr(owner, "oauth_id", None),
                ),
                acting_service=ActingServiceV1(service_id="gateway-internal"),
            )
        else:
            identity = InvocationIdentityV1(
                effective_subject=EffectiveSubjectV1(
                    kind="service",
                    subject_id="gateway-internal",
                    role="service",
                )
            )
    if user_id_value is not None:
        if identity is None:
            subject_kind = "service" if role == "service" else "human"
            identity = InvocationIdentityV1(
                effective_subject=EffectiveSubjectV1(
                    kind=subject_kind,
                    subject_id=user_id_value,
                    role=("service" if subject_kind == "service" else role),
                    oauth_provider=getattr(user, "oauth_provider", None),
                    oauth_id=getattr(user, "oauth_id", None),
                )
            )
    return InvocationPrincipal(
        user_id=user_id_value,
        role=role,
        oauth_provider=getattr(user, "oauth_provider", None),
        oauth_id=getattr(user, "oauth_id", None),
        is_internal=identity is not None and identity.effective_subject.kind == "service",
        visibility_prevalidated=visibility_prevalidated,
        identity=identity,
    )


@asynccontextmanager
async def reserve_checkpoint_write(
    request: Request,
    thread_id: str,
    *,
    user_id: str | None = None,
) -> AsyncIterator[None]:
    """Serialize an out-of-run checkpoint writer against all thread operations."""
    run_manager = get_run_manager(request)
    async with goal_thread_lock(thread_id):
        async with run_manager.reserve_thread_operation(
            thread_id,
            kind=ThreadOperationKind.checkpoint_write,
            user_id=user_id,
        ):
            yield


_TERMINAL_RUN_STATUSES = {
    RunStatus.success,
    RunStatus.error,
    RunStatus.timeout,
    RunStatus.interrupted,
}

_THREAD_METADATA_SETUP_TIMEOUT_SECONDS = 5.0
_PREGRAPH_FINALIZE_TIMEOUT_SECONDS = 5.0

_SERVER_OWNED_MESSAGE_METADATA_KEYS = frozenset(
    {
        _DYNAMIC_CONTEXT_REMINDER_KEY,
        _REMINDER_DATE_KEY,
        _IMAGE_CONTEXT_MESSAGE_MARKER_KEY,
    }
)


# ---------------------------------------------------------------------------
# SSE formatting
# ---------------------------------------------------------------------------


def format_sse(event: str, data: Any, *, event_id: str | None = None) -> str:
    """Format a single SSE frame.

    Field order: ``event:`` -> ``data:`` -> ``id:`` (optional) -> blank line.
    This matches the LangGraph Platform wire format consumed by the
    ``useStream`` React hook and the Python ``langgraph-sdk`` SSE decoder.
    """
    payload = json.dumps(data, default=str, ensure_ascii=False)
    parts = [f"event: {event}", f"data: {payload}"]
    if event_id:
        parts.append(f"id: {event_id}")
    parts.append("")
    parts.append("")
    return "\n".join(parts)


def _run_is_terminal(record: RunRecord) -> bool:
    return record.status in _TERMINAL_RUN_STATUSES


def _consume_task_result(task: asyncio.Task) -> None:
    """Retrieve a detached task's exception without propagating cancellation."""
    if not task.cancelled():
        task.exception()


def _log_thread_metadata_failure(
    error: BaseException,
    *,
    code: str,
    thread_id: str,
) -> None:
    """Emit bounded ownership-metadata diagnostics without exception text."""

    diagnostic = bounded_diagnostic(
        code=code,
        operation="ensure_thread_metadata",
        error=error,
        capability_id="thread_meta",
    )
    logger.warning(
        "thread metadata operation failed code=%s operation=%s error_class=%s capability_id=%s thread_id=%s correlation_id=%s",
        diagnostic.code,
        diagnostic.operation,
        diagnostic.error_class,
        diagnostic.capability_id,
        sanitize_log_param(thread_id),
        diagnostic.correlation_id,
        extra={
            "diagnostic_code": diagnostic.code,
            "operation": diagnostic.operation,
            "exception_class": diagnostic.error_class,
            "capability_id": diagnostic.capability_id,
            "correlation_id": diagnostic.correlation_id,
        },
    )


def _log_thread_metadata_task_result(task: asyncio.Task, *, thread_id: str) -> None:
    """Log detached metadata setup failures while ignoring cancellation."""
    if task.cancelled():
        return
    try:
        task.result()
    except asyncio.CancelledError:
        return
    except Exception as exc:
        _log_thread_metadata_failure(
            exc,
            code="thread_metadata_detached_failure",
            thread_id=thread_id,
        )


def _log_pregraph_stream_failure(
    error: BaseException,
    *,
    operation: str,
    run_id: str,
) -> None:
    """Emit bounded diagnostics when a pre-graph terminal stream write fails."""

    diagnostic = bounded_diagnostic(
        code="pregraph_stream_finalize_failed",
        operation=operation,
        error=error,
        capability_id="stream_bridge",
    )
    logger.warning(
        "pre-graph stream operation failed code=%s operation=%s error_class=%s capability_id=%s run_id=%s correlation_id=%s",
        diagnostic.code,
        diagnostic.operation,
        diagnostic.error_class,
        diagnostic.capability_id,
        sanitize_log_param(run_id),
        diagnostic.correlation_id,
        extra={
            "diagnostic_code": diagnostic.code,
            "operation": diagnostic.operation,
            "exception_class": diagnostic.error_class,
            "capability_id": diagnostic.capability_id,
            "correlation_id": diagnostic.correlation_id,
        },
    )


async def _finalize_pregraph_stream(
    bridge: StreamBridge,
    record: RunRecord,
    *,
    error_message: str | None,
) -> None:
    """Close both live and late stream consumers without entering graph preflight."""

    if error_message is not None:
        try:
            await bridge.publish(
                record.run_id,
                "error",
                {
                    "message": error_message,
                    "name": "RunStartupError",
                },
            )
        except Exception as exc:
            _log_pregraph_stream_failure(
                exc,
                operation="publish_error",
                run_id=record.run_id,
            )
    try:
        await bridge.publish_end(record.run_id)
    except Exception as exc:
        _log_pregraph_stream_failure(
            exc,
            operation="publish_end",
            run_id=record.run_id,
        )
        return
    cleanup = asyncio.create_task(bridge.cleanup(record.run_id, delay=60))
    cleanup.add_done_callback(_consume_task_result)


class _ThreadOwnershipConflict(RuntimeError):
    """An admitted run cannot execute against another owner's thread state."""


async def _ensure_thread_metadata(
    run_ctx: RunContext,
    record: RunRecord,
    *,
    owner_user_id: str | None,
) -> None:
    """Ensure an admitted run's thread exists without delaying task attachment."""
    thread_store = run_ctx.thread_store
    record_owner_user_id = getattr(record, "user_id", None)
    if owner_user_id and record_owner_user_id and owner_user_id != record_owner_user_id:
        raise _ThreadOwnershipConflict("thread ownership conflict")
    effective_owner_user_id = owner_user_id or record_owner_user_id
    owner_kwargs = {"user_id": effective_owner_user_id} if effective_owner_user_id else {}
    existing = await thread_store.get(record.thread_id, **owner_kwargs)
    if existing is None and effective_owner_user_id:
        await thread_store.claim_unowned(record.thread_id, effective_owner_user_id)
        existing = await thread_store.get(record.thread_id, **owner_kwargs)
        if existing is None and await thread_store.get(record.thread_id, user_id=None) is not None:
            raise _ThreadOwnershipConflict("thread ownership conflict")
    if existing is None:
        try:
            await thread_store.create(
                record.thread_id,
                assistant_id=record.assistant_id,
                metadata=record.metadata,
                **owner_kwargs,
            )
        except ThreadMetaAlreadyExistsError:
            if effective_owner_user_id:
                await thread_store.claim_unowned(
                    record.thread_id,
                    effective_owner_user_id,
                )
            existing = await thread_store.get(record.thread_id, **owner_kwargs)
            if existing is None:
                raise _ThreadOwnershipConflict("thread ownership conflict") from None


async def _terminal_record_stream_missing(bridge: StreamBridge, record: RunRecord) -> bool:
    """True when a terminal run has no retained stream on bridges that can tell."""
    if not _run_is_terminal(record):
        return False
    stream_exists = getattr(bridge, "stream_exists", None)
    if stream_exists is None:
        return False
    try:
        return not bool(await stream_exists(record.run_id))
    except Exception:
        logger.debug(
            "Failed to probe stream existence for terminal run %s",
            sanitize_log_param(record.run_id),
            exc_info=True,
        )
        return False


async def _orphan_recovery_observed_after_heartbeat(
    record: RunRecord,
    run_mgr: RunManager,
) -> bool:
    """Return whether durable orphan recovery is the consumer's liveness edge.

    A normal terminal status is not sufficient: the producer persists status
    before publishing its final error/data frames and END. Orphan recovery is
    different because the producer is known to be gone and the durable
    ``stop_reason`` is written atomically with the terminal status. Only that
    explicit signal may synthesize END after a heartbeat.
    """
    if not record.store_only:
        return False
    refreshed = await run_mgr.get(record.run_id, user_id=record.user_id)
    return refreshed is not None and _run_is_terminal(refreshed) and refreshed.stop_reason == ORPHAN_RECOVERY_STOP_REASON


# ---------------------------------------------------------------------------
# Input / config helpers
# ---------------------------------------------------------------------------


def _strip_external_message_metadata(message: Any) -> Any:
    """Remove server-owned metadata from an untrusted input message."""
    if not isinstance(message, BaseMessage):
        return message
    additional_kwargs = dict(message.additional_kwargs)
    additional_kwargs.pop(ORIGINAL_USER_CONTENT_KEY, None)
    for key in _SERVER_OWNED_MESSAGE_METADATA_KEYS:
        additional_kwargs.pop(key, None)
    if additional_kwargs == message.additional_kwargs:
        return message
    return message.model_copy(update={"additional_kwargs": additional_kwargs})


def normalize_input(raw_input: dict[str, Any] | None, *, trusted_internal: bool = False) -> dict[str, Any]:
    """Convert LangGraph Platform input format to LangChain state dict.

    Delegates dict→message coercion to ``langchain_core.messages.utils.convert_to_messages``
    so that ``additional_kwargs`` (e.g. uploaded-file metadata — gh #3132), ``id``,
    ``name``, and non-human roles (ai/system/tool) survive unchanged.  An earlier
    hand-rolled version only forwarded ``content`` and collapsed every role to
    ``HumanMessage``, which silently stripped frontend-supplied attachments.

    Malformed message dicts (missing ``role``/``type``/``content``, unsupported
    role, etc.) raise ``HTTPException(400)`` with the offending index, instead
    of bubbling up as a 500.  The gateway is a system boundary, so per-entry
    validation errors are the right shape for clients to retry against.

    ``original_user_content``, dynamic-context reminder markers, and the
    transient view-image context marker are server-owned. External callers
    cannot supply them; trusted internal channel calls may preserve metadata
    they added before invoking this boundary.
    """
    if raw_input is None:
        return {}
    messages = raw_input.get("messages")
    if messages and isinstance(messages, list):
        converted: list[Any] = []
        for index, msg in enumerate(messages):
            if isinstance(msg, BaseMessage):
                converted.append(msg)
            elif isinstance(msg, dict):
                try:
                    converted.extend(convert_to_messages([msg]))
                except (ValueError, TypeError, NotImplementedError) as exc:
                    raise HTTPException(
                        status_code=400,
                        detail=f"Invalid message at input.messages[{index}]: {exc}",
                    ) from exc
            else:
                converted.append(msg)
        if not trusted_internal:
            converted = [_strip_external_message_metadata(message) for message in converted]
        return {**raw_input, "messages": converted}
    return raw_input


_DEFAULT_ASSISTANT_ID = "lead_agent"


# Whitelist of run-context keys that the langgraph-compat layer forwards from
# ``body.context`` into the run config. ``config["context"]`` exists in
# LangGraph >=0.6, but these values must be written to both ``configurable``
# (for legacy ``_get_runtime_config`` consumers) and ``context`` because
# LangGraph >=1.1.9 no longer makes ``ToolRuntime.context`` fall back to
# ``configurable`` for consumers like ``setup_agent``.
_CONTEXT_CONFIGURABLE_KEYS: frozenset[str] = frozenset(
    {
        "model_name",
        "mode",
        "thinking_enabled",
        "reasoning_effort",
        "is_plan_mode",
        "subagent_enabled",
        "max_concurrent_subagents",
        "max_total_subagents",
        "agent_name",
        "is_bootstrap",
    }
)

# Keys honored only for internally-authenticated callers (the scheduler path).
# ``non_interactive`` strips ``ask_clarification`` from the lead-agent toolset;
# arbitrary HTTP/IM clients must not be able to force autonomous execution.
_CONTEXT_INTERNAL_CALLER_KEYS: frozenset[str] = frozenset({"non_interactive"})

# Server-owned authorization identity fields. These must never be accepted from
# client-supplied ``body.config.context`` or ``body.config.configurable``. They
# are either produced by Gateway auth state, admitted from a separately
# authenticated internal request channel, or reserved for LangGraph Server.
#   ``is_internal``             — derived from ``request.state.auth_source``
#   ``authz_attributes``        — Phase 1A has no Gateway-side producer; cleared.
#   ``channel_user_id``         — accepted only from trusted internal context.
#   ``langgraph_auth_user*``    — populated only by LangGraph Server auth.
_SERVER_OWNED_AUTHZ_CONTEXT_KEYS: frozenset[str] = frozenset(
    {
        "user_role",
        "oauth_provider",
        "oauth_id",
        "is_internal",
        "authz_attributes",
        "channel_user_id",
        "langgraph_auth_user",
        "langgraph_auth_user_id",
        INVOCATION_IDENTITY_CONTEXT_KEY,
        INVOCATION_ORIGIN_CONTEXT_KEY,
        TRUSTED_RUN_CONTEXT_KEY,
    }
)

# Keys forwarded from ``body.context`` into ``config['context']`` ONLY (the
# runtime context that becomes ``ToolRuntime.context`` / ``runtime.context``),
# never into ``config['configurable']``. These are read by tools and
# middlewares from ``runtime.context`` and have no reason to live in
# ``configurable`` — and ``configurable`` is persisted in checkpoints, so
# keeping secrets like ``github_token`` out of it avoids writing a
# short-lived installation token into the checkpoint store.
#
#   ``github_token``         — App installation token minted by the GitHub
#                              channel; the bash tool exposes it as
#                              ``GH_TOKEN``/``GITHUB_TOKEN`` so ``gh`` and
#                              ``git`` push as the bot, not the host user.
#   ``disable_clarification`` — set for non-interactive channels (GitHub
#                              webhooks) so ClarificationMiddleware proceeds
#                              instead of dead-ending the run.
_CONTEXT_RUNTIME_ONLY_KEYS: frozenset[str] = frozenset({"github_token", "disable_clarification"})


def strip_internal_context_keys(config: dict[str, Any]) -> None:
    """Drop internal-only keys a non-internal caller smuggled into the run config.

    Gating :func:`merge_run_context_overrides` is not enough on its own:
    ``build_run_config`` copies a client-supplied ``body.config['context']`` /
    ``body.config['configurable']`` verbatim, so the same keys must be scrubbed
    from both sections after the config is assembled.
    """
    for section in ("context", "configurable"):
        value = config.get(section)
        if isinstance(value, dict):
            for key in _CONTEXT_INTERNAL_CALLER_KEYS:
                value.pop(key, None)


def merge_run_context_overrides(config: dict[str, Any], context: Mapping[str, Any] | None, *, internal: bool = False) -> None:
    """Merge whitelisted keys from ``body.context`` into both ``config['configurable']``
    and ``config['context']`` so they are visible to legacy configurable readers and
    to LangGraph ``ToolRuntime.context`` consumers (e.g. the ``setup_agent`` tool —
    see issue #2677).

    ``user_id`` is intentionally propagated into ``config['context']`` in addition to
    the whitelisted keys, so non-web callers (e.g. IM channels) that supply identity in
    ``body.context`` keep it on ``ToolRuntime.context``. It is merged with
    ``setdefault`` so a server-authenticated id stamped by
    :func:`inject_authenticated_user_context` always wins over the client-supplied one.

    :data:`_CONTEXT_INTERNAL_CALLER_KEYS` are also forwarded when ``internal``
    is True; for non-internal callers those keys are dropped from client requests
    by :func:`strip_internal_context_keys`.

    A second set of keys (``_CONTEXT_RUNTIME_ONLY_KEYS`` — e.g. ``github_token``,
    ``disable_clarification``) is forwarded into ``config['context']`` only, never
    ``configurable``. These are secrets / runtime flags read by tools and middlewares
    from ``runtime.context``; keeping them out of ``configurable`` avoids persisting a
    short-lived token in the checkpoint store.
    """
    if not context:
        return
    configurable = config.setdefault("configurable", {})
    runtime_context = config.setdefault("context", {})
    keys = _CONTEXT_CONFIGURABLE_KEYS | _CONTEXT_INTERNAL_CALLER_KEYS if internal else _CONTEXT_CONFIGURABLE_KEYS
    for key in keys:
        if key in context:
            if isinstance(configurable, dict):
                configurable.setdefault(key, context[key])
            if isinstance(runtime_context, dict):
                runtime_context.setdefault(key, context[key])
    # Context-only keys (secrets / runtime flags) land in ``config['context']``
    # only — never ``configurable`` (which is persisted in checkpoints).
    for key in _CONTEXT_RUNTIME_ONLY_KEYS:
        if key in context and isinstance(runtime_context, dict):
            runtime_context.setdefault(key, context[key])
    if "user_id" in context and isinstance(runtime_context, dict):
        runtime_context.setdefault("user_id", context["user_id"])


async def resolve_trusted_internal_owner_for_attribution(request: Request, owner_user_id: str | None) -> Any | None:
    """Resolve the DeerFlow user used only for trusted internal attribution."""

    if not owner_user_id:
        return None
    user = getattr(request.state, "user", None)
    if getattr(user, "system_role", None) != INTERNAL_SYSTEM_ROLE:
        return None
    try:
        return await get_local_provider().get_user(owner_user_id)
    except Exception:
        logger.exception("Failed to resolve trusted internal owner %s", sanitize_log_param(owner_user_id))
        return None


def inject_authenticated_user_context(
    config: dict[str, Any],
    request: Request,
    *,
    internal_owner_user: Any | None = None,
    request_context: Mapping[str, Any] | None = None,
) -> None:
    """Stamp the authenticated user into the run context for background tools.

    Tool execution may happen after the request handler has returned, so tools
    that persist user-scoped files should not rely only on ambient ContextVars.
    The value comes from server-side auth state, never from client context.

    ``request_context.channel_user_id`` is the sole exception: it is honored
    only after ``request.state.auth_source`` proves the caller is internal.
    Values copied through the free-form RunnableConfig are always cleared.
    """

    # --- Server-owned authorization identity fields ---
    # Clear any client-forged values from both config sections, then write the
    # authoritative is_internal. This runs before ALL early returns so that
    # even user_id-is-None paths get a defined is_internal value.
    runtime_context = config.setdefault("context", {})
    if not isinstance(runtime_context, dict):
        raise TypeError("run context must be a mapping")
    for key in _SERVER_OWNED_AUTHZ_CONTEXT_KEYS:
        runtime_context.pop(key, None)
    configurable = config.get("configurable")
    if isinstance(configurable, dict):
        for key in _SERVER_OWNED_AUTHZ_CONTEXT_KEYS:
            configurable.pop(key, None)
    auth_source = getattr(getattr(request, "state", None), "auth_source", None)
    runtime_context["is_internal"] = auth_source == AUTH_SOURCE_INTERNAL
    if auth_source == AUTH_SOURCE_INTERNAL and request_context is not None:
        channel_user_id = request_context.get("channel_user_id")
        if channel_user_id is not None:
            runtime_context["channel_user_id"] = channel_user_id

    user = getattr(request.state, "user", None)
    user_id = getattr(user, "id", None)
    if user_id is None:
        return

    if getattr(user, "system_role", None) == INTERNAL_SYSTEM_ROLE:
        runtime_context = config.setdefault("context", {})
        if not isinstance(runtime_context, dict):
            return
        if internal_owner_user is None:
            runtime_context.pop("user_role", None)
            runtime_context.pop("oauth_provider", None)
            runtime_context.pop("oauth_id", None)
            return
        owner_user_id = getattr(internal_owner_user, "id", None)
        if owner_user_id is not None:
            runtime_context["user_id"] = str(owner_user_id)
        runtime_context["user_role"] = getattr(internal_owner_user, "system_role", None)
        runtime_context["oauth_provider"] = getattr(internal_owner_user, "oauth_provider", None)
        runtime_context["oauth_id"] = getattr(internal_owner_user, "oauth_id", None)
        return

    runtime_context = config.setdefault("context", {})
    if isinstance(runtime_context, dict):
        runtime_context["user_id"] = str(user_id)
        runtime_context["user_role"] = getattr(user, "system_role", None)
        runtime_context["oauth_provider"] = getattr(user, "oauth_provider", None)
        runtime_context["oauth_id"] = getattr(user, "oauth_id", None)


def resolve_agent_factory(assistant_id: str | None):
    """Resolve the agent factory callable from config.

    Custom agents are implemented as ``lead_agent`` + an ``agent_name``
    injected into ``configurable`` or ``context`` — see
    :func:`build_run_config`.  All ``assistant_id`` values therefore map to the
    same factory; the routing happens inside ``make_lead_agent`` when it reads
    ``cfg["agent_name"]``.
    """
    from deerflow.agents.lead_agent.agent import make_lead_agent

    return make_lead_agent


# Lead-agent recursion budget bounds. The Gateway must NOT trust a
# client-supplied ``recursion_limit`` verbatim: an arbitrarily large value lets
# a single run execute unbounded LangGraph super-steps (each at least one LLM
# call), enabling runaway API cost / DoS. ``_DEFAULT_RECURSION_LIMIT`` is the
# server default when the client sends nothing; the hard ceiling any client
# value is clamped to is configurable via ``AppConfig.max_recursion_limit``.
_DEFAULT_RECURSION_LIMIT = 100
_DEFAULT_MAX_RECURSION_LIMIT = 1000


def _resolve_max_recursion_limit() -> int:
    """Resolve the clamp ceiling from ``AppConfig.max_recursion_limit``.

    Falls back to ``_DEFAULT_MAX_RECURSION_LIMIT`` when the app config cannot be
    loaded (e.g. no ``config.yaml`` in a bare unit-test environment) so that the
    clamp still applies rather than crashing the run-config assembly.
    """
    try:
        return get_app_config().max_recursion_limit
    except Exception:
        return _DEFAULT_MAX_RECURSION_LIMIT


def _clamp_recursion_limit(value: Any, max_limit: int) -> int:
    """Clamp a client-supplied ``recursion_limit`` into a safe server range.

    Non-integer values (including ``bool``, an ``int`` subclass) and non-positive
    values fall back to ``_DEFAULT_RECURSION_LIMIT``; valid positive integers are
    capped at ``max_limit`` (from ``AppConfig.max_recursion_limit``).
    """
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        return _DEFAULT_RECURSION_LIMIT
    return min(value, max_limit)


def build_run_config(
    thread_id: str,
    request_config: dict[str, Any] | None,
    metadata: dict[str, Any] | None,
    *,
    assistant_id: str | None = None,
) -> dict[str, Any]:
    """Build a RunnableConfig dict for the agent.

    When *assistant_id* refers to a custom agent (anything other than
    ``"lead_agent"`` / ``None``), the name is forwarded as ``agent_name`` in
    both ``configurable`` and ``context`` so it is visible to legacy
    configurable readers and to LangGraph ``ToolRuntime.context`` consumers
    (e.g. the ``setup_agent`` tool, which since LangGraph >=1.1.9 no longer
    falls back from ``context`` to ``configurable``).  An explicit
    ``agent_name`` in either container takes precedence over the value
    derived from ``assistant_id``.  ``make_lead_agent`` reads this key to
    load the matching ``agents/<name>/SOUL.md`` and per-agent config —
    without it the agent silently runs as the default lead agent.

    This mirrors the channel manager's ``_resolve_run_params`` logic so that
    the LangGraph Platform-compatible HTTP API and the IM channel path behave
    identically.
    """
    # Lead-agent recursion budget (LangGraph super-steps for the lead graph
    # only). Independent of subagent depth: a `task()` dispatch runs the whole
    # subagent inside ONE lead tools-node step, and subagents enforce their own
    # limit via `subagents.max_turns`. Do not conflate this 100 with the
    # general-purpose subagent's max_turns.
    config: dict[str, Any] = {"recursion_limit": _DEFAULT_RECURSION_LIMIT}
    if request_config:
        # LangGraph >= 0.6.0 introduced ``context`` as the preferred way to
        # pass thread-level data and rejects requests that include both
        # ``configurable`` and ``context``.  If the caller already sends
        # ``context``, honour it and skip our own ``configurable`` dict.
        if "context" in request_config:
            if "configurable" in request_config:
                logger.warning(
                    "build_run_config: client sent both 'context' and 'configurable'; preferring 'context' (LangGraph >= 0.6.0). thread_id=%s, caller_configurable keys=%s",
                    thread_id,
                    list((request_config.get("configurable") or {}).keys()),
                )
            context_value = request_config["context"]
            if context_value is None:
                context = {}
            elif isinstance(context_value, Mapping):
                # Strip caller-supplied ``__``-prefixed keys: those are the
                # harness's private run-context channels (skill secret-binding
                # sources, the active-secret set, the run journal). A caller must
                # not be able to seed them and forge internal state — e.g. a
                # forged ``__slash_skill_secret_source`` would otherwise bypass the
                # skill enabled/allowlist/declaration gates (#3938). Legitimate
                # caller keys (``secrets``, ``user_id``, model overrides) never use
                # the ``__`` prefix.
                context = {key: value for key, value in context_value.items() if not (isinstance(key, str) and key.startswith("__"))}
            else:
                raise ValueError("request config 'context' must be a mapping or null.")
            context["thread_id"] = thread_id
            config["context"] = context
            # The checkpointer always scopes state by configurable["thread_id"],
            # regardless of whether the caller drives the run via context (e.g.
            # request-scoped secrets, #3861). thread_id comes from the URL path,
            # not caller config, so mirror it here while keeping secret-bearing
            # context keys out of configurable.
            config["configurable"] = {"thread_id": thread_id}
        else:
            configurable = {"thread_id": thread_id}
            configurable.update(request_config.get("configurable") or {})
            configurable["thread_id"] = thread_id
            config["configurable"] = configurable
        for k, v in request_config.items():
            if k not in ("configurable", "context"):
                config[k] = v
        # Never trust a client-supplied recursion_limit verbatim: clamp it to a
        # safe server range so a single run cannot execute unbounded LangGraph
        # super-steps (runaway LLM cost / DoS). Applied after the passthrough so
        # it overrides whatever the client sent.
        if "recursion_limit" in request_config:
            max_limit = _resolve_max_recursion_limit()
            clamped = _clamp_recursion_limit(request_config["recursion_limit"], max_limit)
            if clamped != request_config["recursion_limit"]:
                logger.warning(
                    "build_run_config: clamped client recursion_limit %r -> %d (max %d). thread_id=%s",
                    request_config["recursion_limit"],
                    clamped,
                    max_limit,
                    thread_id,
                )
            config["recursion_limit"] = clamped
    else:
        config["configurable"] = {"thread_id": thread_id}

    # Inject custom agent name when the caller specified a non-default assistant.
    # Honour an explicit agent_name in either runtime options container.
    if assistant_id and assistant_id != _DEFAULT_ASSISTANT_ID:
        normalized = assistant_id.strip().lower().replace("_", "-")
        if not normalized or not re.fullmatch(r"[a-z0-9-]+", normalized):
            raise ValueError(f"Invalid assistant_id {assistant_id!r}: must contain only letters, digits, and hyphens after normalization.")
        configurable = config.setdefault("configurable", {})
        runtime_context = config.setdefault("context", {})
        explicit_agent_name: str | None = None
        if isinstance(configurable, dict) and isinstance(configurable.get("agent_name"), str):
            explicit_agent_name = configurable["agent_name"]
        elif isinstance(runtime_context, dict) and isinstance(runtime_context.get("agent_name"), str):
            explicit_agent_name = runtime_context["agent_name"]
        effective_agent_name = explicit_agent_name or normalized
        if isinstance(configurable, dict):
            configurable["agent_name"] = effective_agent_name
        if isinstance(runtime_context, dict):
            runtime_context["agent_name"] = effective_agent_name
        config.setdefault("run_name", resolve_root_run_name(config, normalized))
    for section in ("configurable", "context"):
        external_values = config.get(section)
        if isinstance(external_values, dict):
            external_values.pop(INTERNAL_CHECKPOINT_MODE_KEY, None)

    if metadata:
        config.setdefault("metadata", {}).update(metadata)
    return config


def build_checkpoint_state_mutation_accessor(
    request: Request,
    *,
    thread_id: str,
    as_node: str,
    checkpoint_id: str | None = None,
    state_schema: Any | None = None,
) -> tuple[CheckpointStateAccessor, dict[str, Any]]:
    """Build a state-only graph whose writer node finishes immediately.

    ``state_schema`` should be the thread's effective schema (from
    :func:`graph_state_schema` on the assistant graph) whenever the write
    carries materialized state; with the base-schema fallback, channels
    contributed by custom middleware are silently discarded.
    """
    mode = getattr(request.app.state, "checkpoint_channel_mode", "full")
    config: dict[str, Any] = {
        "configurable": {
            "thread_id": thread_id,
            "checkpoint_ns": "",
        }
    }
    if checkpoint_id is not None:
        config["configurable"]["checkpoint_id"] = checkpoint_id
    inject_checkpoint_mode(config, mode)

    graph = build_state_mutation_graph(as_node, mode, state_schema)
    accessor = CheckpointStateAccessor.bind(
        graph,
        get_checkpointer(request),
        store=getattr(request.app.state, "store", None),
        mode=mode,
    )
    return accessor, config


# Cache of factory-built accessor graphs. Accessor operations (aget_state /
# aupdate_state) never execute graph nodes or middleware, so per-request
# variations (user, model, skills) cannot affect materialization semantics;
# the compiled graph is stable per (assistant_id, mode, snapshot_frequency,
# app_config). The
# factory and app_config identities are re-validated on every call so patched
# factories take effect immediately and a config.yaml hot-reload (which
# rebuilds the AppConfig object) never serves a stale compiled graph — the
# cached reference keeps the old config alive, so id-reuse cannot produce a
# false hit. Bounded: cleared when too many distinct assistants appear. The
# cap is configurable (database.checkpoint_graph_cache.accessor_graph_max)
# and re-read on every eviction check, so a hot-reload takes effect without
# a restart.
_STATE_ACCESSOR_GRAPH_CACHE_MAX = 64
_state_accessor_graph_cache: dict[tuple[str | None, str, int | None], tuple[Any, Any, Any]] = {}


def _accessor_graph_cache_max(app_config: Any) -> int:
    return resolve_checkpoint_graph_cache_max(
        getattr(app_config, "database", None),
        "accessor_graph_max",
        _STATE_ACCESSOR_GRAPH_CACHE_MAX,
    )


def _state_accessor_graph(agent_factory: Any, assistant_id: str | None, mode: str, snapshot_frequency: int | None, config: dict[str, Any]) -> Any:
    app_config = (config.get("context") or {}).get("app_config")
    key = (assistant_id, mode, snapshot_frequency)
    cached = _state_accessor_graph_cache.get(key)
    if cached is not None and cached[0] is agent_factory and cached[1] is app_config:
        return cached[2]
    if len(_state_accessor_graph_cache) >= _accessor_graph_cache_max(app_config):
        _state_accessor_graph_cache.clear()
    graph = agent_factory(config=config)
    _state_accessor_graph_cache[key] = (agent_factory, app_config, graph)
    return graph


class _RawCheckpointSnapshot:
    """StateSnapshot-shaped view over a raw checkpoint tuple (full mode only).

    ``next``/``tasks`` are not derivable without the compiled graph and
    degrade to empty; everything the read endpoints serialize (values,
    metadata, config ancestry, created_at) comes straight from the tuple.
    """

    __slots__ = ("checkpoint_exists", "config", "values", "metadata", "parent_config", "created_at", "tasks", "tasks_known", "next")

    def __init__(self, config: dict[str, Any], tup: Any | None) -> None:
        self.checkpoint_exists = tup is not None
        self.config = getattr(tup, "config", None) or config
        checkpoint = getattr(tup, "checkpoint", None) or {}
        self.values = dict(checkpoint.get("channel_values") or {})
        self.metadata = dict(getattr(tup, "metadata", None) or {})
        self.parent_config = getattr(tup, "parent_config", None)
        self.created_at = checkpoint.get("ts") or self.metadata.get("created_at", "")
        self.tasks: tuple = ()
        self.tasks_known = False
        self.next: tuple = ()


class _RawCheckpointReadAccessor:
    """Degraded full-mode read accessor for when the agent factory is down.

    Full-mode checkpoints persist complete ``channel_values``, so reads do not
    need the compiled graph. The fail-closed delta gate still applies: delta
    checkpoints are rejected with :class:`CheckpointModeMismatchError` instead
    of being served as partial state. Writes are unsupported — mutation paths
    keep using the graph-backed accessor.
    """

    def __init__(self, checkpointer: Any, mode: str) -> None:
        self.checkpointer = checkpointer
        self.mode = mode

    @staticmethod
    def _gate(tup: Any) -> None:
        if checkpoint_tuple_uses_delta(tup):
            raise CheckpointModeMismatchError("Thread requires delta mode; materialize and convert its checkpoints before using full mode.")

    async def aget(self, config: dict[str, Any]) -> _RawCheckpointSnapshot:
        tup = await self.checkpointer.aget_tuple(config)
        self._gate(tup)
        return _RawCheckpointSnapshot(config, tup)

    async def ahistory(self, config: dict[str, Any], *, limit: int | None = None) -> list[_RawCheckpointSnapshot]:
        if limit is not None and limit <= 0:
            return []
        result: list[_RawCheckpointSnapshot] = []
        before = None
        walk_config = config
        if config.get("configurable", {}).get("checkpoint_id"):
            # Pregel's get_state_history treats config.checkpoint_id as the
            # inclusive start of the walk, while alist(before=...) is
            # exclusive — fetch the anchor explicitly so the degraded path
            # matches the graph path.
            before = config
            walk_config = {
                **config,
                "configurable": {k: v for k, v in config.get("configurable", {}).items() if k != "checkpoint_id"},
            }
            anchor = await self.checkpointer.aget_tuple(before)
            self._gate(anchor)
            if anchor is not None:
                result.append(_RawCheckpointSnapshot(config, anchor))
        if limit is None or len(result) < limit:
            remaining = None if limit is None else limit - len(result)
            async for tup in self.checkpointer.alist(walk_config, before=before, limit=remaining):
                self._gate(tup)
                result.append(_RawCheckpointSnapshot(config, tup))
                if limit is not None and len(result) >= limit:
                    break
        return result


def build_checkpoint_state_accessor(
    request: Request,
    *,
    thread_id: str,
    assistant_id: str | None = None,
    checkpoint_id: str | None = None,
) -> tuple[CheckpointStateAccessor, dict[str, Any]]:
    """Build the mode-selected lead graph used for materialized checkpoint state."""
    ctx = get_run_context(request)
    config = build_run_config(thread_id, None, None, assistant_id=assistant_id)
    configurable = config.setdefault("configurable", {})
    configurable["checkpoint_ns"] = ""
    if checkpoint_id is not None:
        configurable["checkpoint_id"] = checkpoint_id

    if ctx.app_config is not None:
        config.setdefault("context", {})["app_config"] = ctx.app_config
    authorization_provider = getattr(ctx, "authorization_provider", None)
    if authorization_provider is not None:
        from deerflow.authz.runtime import AUTHORIZATION_PROVIDER_CONTEXT_KEY

        config.setdefault("context", {})[AUTHORIZATION_PROVIDER_CONTEXT_KEY] = authorization_provider
    inject_checkpoint_mode(config, ctx.checkpoint_channel_mode)

    agent_factory = resolve_agent_factory(assistant_id)
    try:
        graph = _state_accessor_graph(agent_factory, assistant_id, ctx.checkpoint_channel_mode, getattr(ctx, "checkpoint_snapshot_frequency", None), config)
    except Exception:
        if ctx.checkpoint_channel_mode != "full":
            # Delta materialization needs the graph's channel table; there is
            # no degraded path. Surface the factory failure as-is.
            raise
        # Full-mode checkpoints carry complete channel_values: degrade to raw
        # checkpointer reads so state endpoints survive a broken agent factory
        # (bad model config, MCP server down, misconfigured skill).
        logger.warning(
            "Agent factory unavailable for thread %s; falling back to raw checkpointer reads",
            thread_id,
            exc_info=True,
        )
        return _RawCheckpointReadAccessor(ctx.checkpointer, ctx.checkpoint_channel_mode), config
    accessor = CheckpointStateAccessor.bind(
        graph,
        ctx.checkpointer,
        store=ctx.store,
        mode=ctx.checkpoint_channel_mode,
    )
    return accessor, config


async def resolve_thread_assistant_id(
    request: Request,
    thread_id: str,
    *,
    fail_closed: bool = False,
) -> str | None:
    """Return the assistant_id recorded in thread metadata, or ``None``.

    Missing records degrade to ``None`` (the default lead agent). Store
    failures do the same for read callers, while mutation callers set
    ``fail_closed`` so they cannot compile a write graph with the wrong schema.
    """
    from app.gateway.deps import get_thread_store

    try:
        thread_store = get_thread_store(request)
        record = await thread_store.get(thread_id)
    except Exception:
        logger.warning("Failed to resolve assistant_id for thread %s", thread_id, exc_info=True)
        if fail_closed:
            raise
        return None
    return record.get("assistant_id") if isinstance(record, dict) else None


async def build_thread_checkpoint_state_accessor(
    request: Request,
    *,
    thread_id: str,
    checkpoint_id: str | None = None,
    fail_closed: bool = False,
) -> tuple[CheckpointStateAccessor, dict[str, Any]]:
    """Single resolution boundary for state endpoints.

    Thread metadata -> assistant_id -> effective assistant graph. Materializing
    with the default lead schema would drop channels contributed by a custom
    ``AgentMiddleware.state_schema`` from the response.
    """
    assistant_id = await resolve_thread_assistant_id(request, thread_id, fail_closed=fail_closed)
    return build_checkpoint_state_accessor(
        request,
        thread_id=thread_id,
        assistant_id=assistant_id,
        checkpoint_id=checkpoint_id,
    )


async def build_thread_checkpoint_state_mutation_accessor(
    request: Request,
    *,
    thread_id: str,
    as_node: str,
    checkpoint_id: str | None = None,
) -> tuple[CheckpointStateAccessor, dict[str, Any]]:
    """Mutation accessor compiled with the thread's effective state schema.

    Derives the schema through :func:`build_thread_checkpoint_state_accessor`
    so writes carrying materialized state do not silently discard
    extension-owned channels.
    """
    read_accessor, _read_config = await build_thread_checkpoint_state_accessor(
        request,
        thread_id=thread_id,
        checkpoint_id=checkpoint_id,
        fail_closed=True,
    )
    state_schema = graph_state_schema(getattr(read_accessor, "graph", None))
    return build_checkpoint_state_mutation_accessor(
        request,
        thread_id=thread_id,
        as_node=as_node,
        checkpoint_id=checkpoint_id,
        state_schema=state_schema,
    )


async def apply_checkpoint_to_run_config(
    config: dict[str, Any],
    *,
    body: Any,
    thread_id: str,
    request: Request,
) -> None:
    """Validate an optional run checkpoint and attach it to RunnableConfig."""
    checkpoint = getattr(body, "checkpoint", None)
    checkpoint_id = getattr(body, "checkpoint_id", None)
    checkpoint_ns = ""
    checkpoint_map = None

    if checkpoint:
        if not isinstance(checkpoint, Mapping):
            raise HTTPException(status_code=400, detail="checkpoint must be an object")
        checkpoint_thread_id = checkpoint.get("thread_id")
        if checkpoint_thread_id is not None and str(checkpoint_thread_id) != thread_id:
            raise HTTPException(status_code=400, detail="checkpoint thread_id does not match request thread_id")
        raw_checkpoint_id = checkpoint.get("checkpoint_id")
        if raw_checkpoint_id:
            checkpoint_id = str(raw_checkpoint_id)
        raw_checkpoint_ns = checkpoint.get("checkpoint_ns")
        if raw_checkpoint_ns is not None:
            checkpoint_ns = str(raw_checkpoint_ns)
        checkpoint_map = checkpoint.get("checkpoint_map")

    if not checkpoint_id:
        return

    read_config: dict[str, Any] = {
        "configurable": {
            "thread_id": thread_id,
            "checkpoint_ns": checkpoint_ns,
            "checkpoint_id": str(checkpoint_id),
        }
    }
    if checkpoint_map is not None:
        read_config["configurable"]["checkpoint_map"] = checkpoint_map

    checkpointer = get_checkpointer(request)
    try:
        checkpoint_tuple = await checkpointer.aget_tuple(read_config)
    except Exception as exc:
        logger.exception("Failed to validate checkpoint %s for thread %s", checkpoint_id, sanitize_log_param(thread_id))
        raise HTTPException(status_code=500, detail="Failed to validate checkpoint") from exc
    if checkpoint_tuple is None:
        raise HTTPException(status_code=404, detail=f"Checkpoint {checkpoint_id} not found")

    configurable = config.setdefault("configurable", {})
    if not isinstance(configurable, dict):
        raise HTTPException(status_code=400, detail="request config configurable must be an object")
    configurable["thread_id"] = thread_id
    configurable["checkpoint_ns"] = checkpoint_ns
    configurable["checkpoint_id"] = str(checkpoint_id)
    if checkpoint_map is not None:
        configurable["checkpoint_map"] = checkpoint_map


async def ensure_checkpoint_history_seeded(
    request: Request,
    *,
    thread_id: str,
    assistant_id: str | None,
) -> None:
    """Backfill an empty run-event feed from an existing checkpoint head.

    No-op unless the feed is empty AND a checkpoint head with messages
    exists — i.e. a legacy checkpoint-only thread facing its first journaled
    run. This is a migration shim: remove it once pre-journal threads are no
    longer a supported upgrade source. The info log on a successful seed is
    the observability hook for that decision — when it stops appearing, the
    shim is dead.
    """
    event_store = request.app.state.run_event_store
    # The emptiness check is deliberately thread-scoped, never user-scoped:
    # seed rows may be stamped with a different principal (NULL for ownerless
    # seeds, or another user on a shared NULL-owner thread), so a user-scoped
    # query would miss them and re-seed a duplicate history per principal.
    # Passing user_id=None also opts out of AUTO resolution explicitly, which
    # would raise when no user contextvar is set (e.g. the scheduler launch
    # path for ownerless internal tasks).
    if await event_store.list_messages(thread_id, limit=1, user_id=None):
        return

    checkpoint_config = {
        "configurable": {
            "thread_id": thread_id,
            "checkpoint_ns": "",
        }
    }
    if await get_checkpointer(request).aget_tuple(checkpoint_config) is None:
        return

    accessor, config = build_checkpoint_state_accessor(
        request,
        thread_id=thread_id,
        assistant_id=assistant_id,
    )
    snapshot = await accessor.aget(config)
    values = getattr(snapshot, "values", None)
    messages = values.get("messages") if isinstance(values, dict) else None
    if not isinstance(messages, list) or not messages:
        return

    events = build_checkpoint_history_seed_events(
        messages,
        thread_id=thread_id,
        run_id_prefix=f"checkpoint-seed-{thread_id}",
    )
    if not events:
        return
    await event_store.put_batch(events)
    logger.info("Seeded %d checkpoint-history events for thread %s", len(events), thread_id)


# ---------------------------------------------------------------------------
# Run lifecycle
# ---------------------------------------------------------------------------


def _bounded_source_value(value: Any) -> str | int | bool | None:
    if value is None or isinstance(value, (bool, int)):
        return value
    rendered = str(value)
    encoded = rendered.encode("utf-8")[:1024]
    return encoded.decode("utf-8", errors="ignore")


def _base_origin_references(
    intent: InternalLaunchIntent,
    *,
    include_verified_binding: bool = True,
) -> dict[str, str | int | bool | None]:
    if intent.source_kind is InternalSourceKind.scheduled_task:
        return {
            "task_id": _bounded_source_value(intent.trusted_task_id),
            "task_run_id": _bounded_source_value(intent.task_run_id),
            "trigger": _bounded_source_value(intent.scheduled_trigger),
        }
    if intent.source_kind is InternalSourceKind.native_channel:
        facts = intent.native_channel
        if facts is None:
            return {}
        references: dict[str, str | int | bool | None] = {
            "provider": _bounded_source_value(facts.provider),
            "connection_id": _bounded_source_value(facts.connection_id),
            "workspace_id": _bounded_source_value(facts.workspace_id),
            "chat_id": _bounded_source_value(facts.chat_id),
            "topic_id": _bounded_source_value(facts.topic_id),
            "provider_message_id": _bounded_source_value(facts.provider_message_id),
            "channel_user_id": _bounded_source_value(facts.channel_user_id),
        }
        if include_verified_binding and facts.verified_binding is not None:
            references["binding_kind"] = _bounded_source_value(facts.verified_binding.kind.value)
            references["binding_reference"] = _bounded_source_value(facts.verified_binding.reference)
        return references
    if intent.source_kind is InternalSourceKind.service:
        return {"service_id": _bounded_source_value(intent.trusted_service_id)}
    return {}


def _origin_request_references(references: Mapping[str, Any]) -> tuple[SafeContextReferenceV1, ...]:
    return tuple(
        SafeContextReferenceV1(
            key=key,
            value=value,
            storage_class="persistable",
            purpose="correlation",
        )
        for key, value in sorted(references.items())
    )


def _contribution_json(composed: Any) -> tuple[dict[str, Any], ...]:
    return tuple(
        {
            "contribution_id": item.contribution_id,
            "namespace": item.namespace,
            "key": item.reference.key,
            "value": item.reference.value,
            "storage_class": item.reference.storage_class,
            "purpose": item.reference.purpose,
        }
        for item in (
            *composed.persistable,
            *(handle for handle in getattr(composed, "secret_handles", ()) if handle.reference.storage_class == "persistable"),
        )
    )


def _trusted_contribution_references(
    composed: Any,
    *,
    capability_kind: str,
) -> tuple[
    tuple[NamespacedContextReferenceV1, ...],
    tuple[NamespacedContextReferenceV1, ...],
    tuple[NamespacedContextReferenceV1, ...],
]:
    def convert(items: Any) -> tuple[NamespacedContextReferenceV1, ...]:
        return tuple(
            NamespacedContextReferenceV1(
                capability_id=f"{capability_kind}:{item.contribution_id}",
                namespace=item.namespace,
                reference=item.reference,
            )
            for item in items
        )

    return (
        convert(composed.persistable),
        convert(getattr(composed, "runtime_only", ())),
        convert(getattr(composed, "secret_handles", ())),
    )


_EFFECTIVE_EXECUTION_PROJECTION_KEY = "__accepted_request_projection_v1"
_KEYED_CONFIG_KEYS = frozenset({"recursion_limit", "configurable", "context"})
_KEYED_CONFIGURABLE_KEYS = frozenset(
    {
        "thread_id",
        "checkpoint_id",
        "checkpoint_ns",
        "checkpoint_map",
        *_CONTEXT_CONFIGURABLE_KEYS,
        *_CONTEXT_INTERNAL_CALLER_KEYS,
    }
)
_KEYED_CONTEXT_KEYS = frozenset(
    {
        *_CONTEXT_CONFIGURABLE_KEYS,
        *_CONTEXT_INTERNAL_CALLER_KEYS,
        *_CONTEXT_RUNTIME_ONLY_KEYS,
        "thread_id",
        "user_id",
        "channel_user_id",
        "channel_name",
    }
)
_DIGEST_CONTEXT_KEYS = frozenset(
    {
        "model_name",
        "mode",
        "thinking_enabled",
        "reasoning_effort",
        "is_plan_mode",
        "subagent_enabled",
        "max_concurrent_subagents",
        "max_total_subagents",
        "agent_name",
        "is_bootstrap",
        "non_interactive",
        "disable_clarification",
    }
)


def _keyed_request_error(detail: str) -> HTTPException:
    return HTTPException(status_code=422, detail=detail)


def _validate_keyed_request_shape(intent: InternalLaunchIntent) -> None:
    if intent.metadata:
        raise _keyed_request_error("Idempotency-Key does not support arbitrary metadata")
    if intent.command is not None:
        if not isinstance(intent.command, Mapping) or set(intent.command) != {"resume"}:
            raise _keyed_request_error("Idempotency-Key supports only command.resume")
    config = intent.config or {}
    if not isinstance(config, Mapping):
        raise _keyed_request_error("keyed request config must be an object")
    unknown_config = set(config) - _KEYED_CONFIG_KEYS
    if unknown_config:
        raise _keyed_request_error(f"Idempotency-Key cannot classify config keys: {', '.join(sorted(unknown_config))}")
    trusted_internal_keys = {"non_interactive", "disable_clarification"}
    for section_name, allowed in (
        ("configurable", _KEYED_CONFIGURABLE_KEYS),
        ("context", _KEYED_CONTEXT_KEYS),
    ):
        section = config.get(section_name)
        if section is None:
            continue
        if not isinstance(section, Mapping):
            raise _keyed_request_error(f"keyed request config.{section_name} must be an object")
        unknown = set(section) - allowed
        if unknown:
            raise _keyed_request_error(f"Idempotency-Key cannot classify config.{section_name} keys: {', '.join(sorted(unknown))}")
        if intent.source_kind is InternalSourceKind.http and (forged := set(section) & trusted_internal_keys):
            raise _keyed_request_error(f"Idempotency-Key cannot accept trusted internal config.{section_name} keys: {', '.join(sorted(forged))}")
    context = intent.context or {}
    if not isinstance(context, Mapping):
        raise _keyed_request_error("keyed request context must be an object")
    unknown_context = set(context) - _KEYED_CONTEXT_KEYS
    if unknown_context:
        raise _keyed_request_error(f"Idempotency-Key cannot classify context keys: {', '.join(sorted(unknown_context))}")
    if intent.source_kind is InternalSourceKind.http and (forged := set(context) & trusted_internal_keys):
        raise _keyed_request_error(f"Idempotency-Key cannot accept trusted internal context keys: {', '.join(sorted(forged))}")
    try:
        canonical_request_digest(
            {
                "input": canonical_request_value(intent.input),
                "command": canonical_request_value(intent.command),
                "checkpoint": canonical_request_value(intent.checkpoint),
            }
        )
    except (TypeError, ValueError) as exc:
        raise _keyed_request_error(f"Idempotency-Key request cannot be projected: {exc}") from exc


def _requested_agent_id(intent: InternalLaunchIntent) -> str:
    if intent.source_kind is InternalSourceKind.native_channel and intent.native_channel is not None:
        return intent.native_channel.resolved_agent_name or "default"
    context = intent.context or {}
    if intent.source_kind is InternalSourceKind.http and context.get("is_bootstrap") is True:
        # Bootstrap routing is selected by both the mode bit and its validated
        # agent name. Keep the name in caller intent so two different
        # bootstrap agents cannot share an idempotency key merely because both
        # used the bootstrap path. The separator cannot collide with a valid
        # agent name (agent names are alphanumeric/hyphen only).
        agent_name = context.get("agent_name")
        return f"bootstrap:{agent_name}" if isinstance(agent_name, str) else "bootstrap"
    assistant_id = intent.assistant_id
    if assistant_id in (None, _DEFAULT_ASSISTANT_ID):
        return "default"
    return assistant_id.strip().lower().replace("_", "-")


def _caller_execution_context(intent: InternalLaunchIntent) -> dict[str, Any]:
    requested_config = intent.config or {}
    if "context" in requested_config:
        configured = requested_config.get("context")
    else:
        configured = requested_config.get("configurable")
    configured = configured if isinstance(configured, Mapping) else {}
    body_context = intent.context if isinstance(intent.context, Mapping) else {}
    result = {key: configured[key] for key in sorted(_DIGEST_CONTEXT_KEYS) if key in configured and configured[key] is not None}
    for key in sorted(_DIGEST_CONTEXT_KEYS):
        if key not in result and key in body_context and body_context[key] is not None:
            result[key] = body_context[key]
    if intent.source_kind is InternalSourceKind.http:
        # External agent hints are normalized into ``agent_selector`` below;
        # these raw context aliases never independently bind a revision.
        result.pop("agent_name", None)
        result.pop("is_bootstrap", None)
    return result


def _caller_checkpoint_selection(intent: InternalLaunchIntent) -> dict[str, Any]:
    requested_config = intent.config or {}
    configurable = requested_config.get("configurable") if "context" not in requested_config else None
    configured = configurable if isinstance(configurable, Mapping) else {}
    inherited = {key: configured[key] for key in ("checkpoint_id", "checkpoint_ns", "checkpoint_map") if key in configured and configured[key] is not None}

    checkpoint_id: Any = intent.checkpoint_id
    checkpoint = intent.checkpoint
    if checkpoint:
        checkpoint_id = checkpoint.get("checkpoint_id") or checkpoint_id
        if checkpoint_id:
            selected = {
                "checkpoint_id": str(checkpoint_id),
                "checkpoint_ns": str(checkpoint.get("checkpoint_ns") or ""),
            }
            if checkpoint.get("checkpoint_map") is not None:
                selected["checkpoint_map"] = checkpoint["checkpoint_map"]
            return selected
    elif checkpoint_id:
        return {"checkpoint_id": str(checkpoint_id), "checkpoint_ns": ""}
    return inherited


def _canonical_caller_intent(intent: InternalLaunchIntent) -> CanonicalCallerIntent:
    if intent.command and intent.command.get("resume") is not None:
        input_projection: dict[str, Any] = {
            "kind": "resume",
            "value": canonical_request_value(intent.command["resume"]),
        }
    else:
        graph_input = normalize_input(
            thaw_host_value(intent.input),
            trusted_internal=intent.source_kind is not InternalSourceKind.http,
        )
        input_projection = {
            "kind": "graph",
            "value": canonical_request_value(graph_input),
        }
    requested_config = intent.config or {}
    recursion_limit = requested_config.get("recursion_limit") if "recursion_limit" in requested_config else None
    return CanonicalCallerIntent(
        {
            "thread": ({"selection": "explicit", "thread_id": intent.thread_id} if intent.thread_id_explicit else {"selection": "server_assigned"}),
            "agent_selector": _requested_agent_id(intent),
            "input": input_projection,
            "multitask_strategy": intent.multitask_strategy,
            "checkpoint": canonical_request_value(_caller_checkpoint_selection(intent)),
            "interrupt_before": canonical_request_value(intent.interrupt_before),
            "interrupt_after": canonical_request_value(intent.interrupt_after),
            "execution_context": canonical_request_value(_caller_execution_context(intent)),
            # A missing or explicit-null limit both select the documented
            # Gateway default. Every other supplied value remains caller
            # intent; server clamping belongs only to the effective projection.
            "recursion_limit": ({"selection": "default"} if recursion_limit is None else {"selection": "explicit", "value": canonical_request_value(recursion_limit)}),
        }
    )


def _effective_execution_projection(
    intent: InternalLaunchIntent,
    *,
    accepted: AcceptedInvocation,
    graph_input: Any,
    config: Mapping[str, Any],
) -> EffectiveExecutionProjection:
    configurable = config.get("configurable") if isinstance(config.get("configurable"), Mapping) else {}
    runtime_context = config.get("context") if isinstance(config.get("context"), Mapping) else {}
    execution_context: dict[str, Any] = {}
    for key in sorted(_DIGEST_CONTEXT_KEYS):
        if key in runtime_context:
            execution_context[key] = runtime_context[key]
        elif key in configurable:
            execution_context[key] = configurable[key]
    input_projection = {"resume": canonical_request_value(intent.command["resume"])} if intent.command and intent.command.get("resume") is not None else canonical_request_value(graph_input)
    return EffectiveExecutionProjection(
        {
            "accepted_digest_semantics": "canonical_execution_v2",
            "thread_id": accepted.thread_id,
            "agent_selector": _requested_agent_id(intent),
            "agent_revision_digest": accepted.agent_revision.digest,
            "principal_digest": accepted.principal_digest,
            "base_origin_digest": accepted.base_origin_digest,
            "accepted_context_digest": accepted.accepted_context_digest,
            "runtime_identity_digest": accepted.runtime_identity_digest,
            "contributor_execution_digest": accepted.contributor_execution_digest,
            "extension_generation": accepted.extension_generation,
            "input": input_projection,
            "command": canonical_request_value(intent.command),
            "multitask_strategy": intent.multitask_strategy,
            "checkpoint": canonical_request_value({key: configurable[key] for key in ("checkpoint_id", "checkpoint_ns", "checkpoint_map") if key in configurable}),
            "interrupt_before": canonical_request_value(intent.interrupt_before),
            "interrupt_after": canonical_request_value(intent.interrupt_after),
            "execution_context": canonical_request_value(execution_context),
            "recursion_limit": config.get("recursion_limit"),
        }
    )


async def _principal_projection_for_intent(
    request: Any,
    intent: InternalLaunchIntent,
    *,
    owner_user_id: str | None,
) -> PrincipalProjection:
    request_user = getattr(getattr(request, "state", None), "user", None)
    request_role = getattr(request_user, "system_role", None)
    internal = request_role == INTERNAL_SYSTEM_ROLE
    if owner_user_id is not None and internal:
        owner = await resolve_trusted_internal_owner_for_attribution(request, owner_user_id)
        if owner is None:
            raise ValueError("trusted internal launch owner could not be revalidated")
        if intent.source_kind is InternalSourceKind.native_channel:
            facts = intent.native_channel
            if facts is None or not facts.provider:
                raise ValueError("native channel identity requires a trusted provider")
            acting_service = ActingServiceV1(service_id=f"channel:{facts.provider}")
        elif intent.source_kind is InternalSourceKind.scheduled_task:
            acting_service = ActingServiceV1(service_id="scheduler")
        else:
            acting_service = ActingServiceV1(service_id="gateway-internal")
        identity = InvocationIdentityV1(
            effective_subject=EffectiveSubjectV1(
                kind="human",
                subject_id=owner_user_id,
                role=getattr(owner, "system_role", None),
                oauth_provider=getattr(owner, "oauth_provider", None),
                oauth_id=getattr(owner, "oauth_id", None),
            ),
            acting_service=acting_service,
        )
        return PrincipalProjection(
            user_id=owner_user_id,
            role=getattr(owner, "system_role", None),
            oauth_provider=getattr(owner, "oauth_provider", None),
            oauth_id=getattr(owner, "oauth_id", None),
            channel_user_id=(intent.context or {}).get("channel_user_id"),
            is_internal=False,
            identity=identity,
        )
    request_user_id = getattr(request_user, "id", None)
    auth_source = getattr(getattr(request, "state", None), "auth_source", None)
    if request_user_id is None and auth_source == AUTH_SOURCE_AUTH_DISABLED:
        request_user_id = AUTH_DISABLED_USER_ID
    if internal:
        if intent.source_kind is InternalSourceKind.scheduled_task and intent.scheduled_system_owned:
            identity = InvocationIdentityV1(effective_subject=EffectiveSubjectV1(kind="service", subject_id="scheduler", role="service"))
        elif intent.source_kind is InternalSourceKind.scheduled_task:
            raise ValueError("scheduled task launch requires a persisted owner or explicit system ownership")
        elif intent.source_kind is InternalSourceKind.http:
            # An authenticated internal HTTP caller is a service subject only
            # when it is not representing a human. Owner-attributed requests
            # take the branch above and must revalidate that human first.
            identity = InvocationIdentityV1(
                effective_subject=EffectiveSubjectV1(
                    kind="service",
                    subject_id="gateway-internal",
                    role="service",
                )
            )
        else:
            raise ValueError("internal launch without a represented owner requires an explicit service subject")
    elif intent.source_kind is InternalSourceKind.service:
        service_id = intent.trusted_service_id
        if not service_id:
            raise ValueError("service launch requires an authenticated service subject")
        if request_role != "service" or request_user_id is None or str(request_user_id) != service_id:
            raise ValueError("service launch requires one matching authenticated service identity")
        identity = InvocationIdentityV1(effective_subject=EffectiveSubjectV1(kind="service", subject_id=service_id, role="service"))
    else:
        if request_user_id is None:
            raise ValueError("invocation requires an authenticated effective subject")
        subject_kind = "service" if request_role == "service" else "human"
        identity = InvocationIdentityV1(
            effective_subject=EffectiveSubjectV1(
                kind=subject_kind,
                subject_id=str(request_user_id),
                role=("service" if subject_kind == "service" else request_role),
                oauth_provider=getattr(request_user, "oauth_provider", None),
                oauth_id=getattr(request_user, "oauth_id", None),
            )
        )
    return PrincipalProjection(
        user_id=None if internal else (str(request_user_id) if request_user_id is not None else None),
        role=None if internal else request_role,
        oauth_provider=None if internal else getattr(request_user, "oauth_provider", None),
        oauth_id=None if internal else getattr(request_user, "oauth_id", None),
        channel_user_id=(intent.context or {}).get("channel_user_id") if internal else None,
        is_internal=identity.effective_subject.kind == "service",
        identity=identity,
    )


def _base_origin_digest(
    intent: InternalLaunchIntent,
    *,
    include_verified_binding: bool = True,
) -> str:
    origin = InvocationOrigin(
        source_kind=intent.source_kind.value,
        references=_base_origin_references(
            intent,
            include_verified_binding=include_verified_binding,
        ),
    )
    return canonical_digest({"version": 1, "origin": origin.base_json()})


_PROCESS_MATERIAL_CLEANUP_TIMEOUT_SECONDS = 5.0


async def _release_process_material_bounded(material: ResolvedAgentMaterialV1) -> None:
    """Release process material off-loop without letting cancellation orphan it."""
    cleanup = asyncio.create_task(asyncio.to_thread(material.release_process_material))
    deadline = asyncio.get_running_loop().time() + _PROCESS_MATERIAL_CLEANUP_TIMEOUT_SECONDS
    cancellation: asyncio.CancelledError | None = None
    while not cleanup.done():
        remaining = deadline - asyncio.get_running_loop().time()
        if remaining <= 0:
            cleanup.add_done_callback(_consume_task_result)
            logger.warning(
                "Accepted agent material cleanup exceeded its bounded deadline error_class=TimeoutError",
            )
            break
        try:
            await asyncio.wait_for(asyncio.shield(cleanup), timeout=remaining)
        except asyncio.CancelledError as exc:
            cancellation = cancellation or exc
        except TimeoutError:
            cleanup.add_done_callback(_consume_task_result)
            logger.warning(
                "Accepted agent material cleanup exceeded its bounded deadline error_class=TimeoutError",
            )
            break
        except Exception as exc:
            logger.warning(
                "Accepted agent material cleanup failed error_class=%s",
                type(exc).__name__,
            )
            break
    if cancellation is not None:
        raise cancellation


class _RevisionResolutionOwnership:
    """Transfer a resolver-produced process-material lease without a cancel gap."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._abandoned = False
        self._resolved: Any | None = None

    def resolve(self, config: dict[str, Any], *, app_config: Any, user_id: str | None) -> Any:
        revision = resolve_agent_revision(
            config,
            app_config=app_config,
            user_id=user_id,
        )
        release_material: ResolvedAgentMaterialV1 | None = None
        with self._lock:
            if self._abandoned:
                release_material = revision.material
            else:
                self._resolved = revision
        if isinstance(release_material, ResolvedAgentMaterialV1):
            try:
                release_material.release_process_material()
            except Exception as exc:  # pragma: no cover - defensive cleanup diagnostic
                logger.warning(
                    "Late accepted agent material cleanup failed error_class=%s",
                    type(exc).__name__,
                )
        return revision

    def abandon(self) -> ResolvedAgentMaterialV1 | None:
        with self._lock:
            self._abandoned = True
            revision = self._resolved
            self._resolved = None
        material = getattr(revision, "material", None)
        return material if isinstance(material, ResolvedAgentMaterialV1) else None

    def take(self, revision: Any) -> Any:
        with self._lock:
            resolved = self._resolved
            self._resolved = None
        return resolved if resolved is not None else revision


async def _resolve_agent_revision_cancellation_safe(
    config: dict[str, Any],
    *,
    app_config: Any,
    user_id: str | None,
) -> Any:
    ownership = _RevisionResolutionOwnership()
    resolution = asyncio.create_task(
        asyncio.to_thread(
            ownership.resolve,
            config,
            app_config=app_config,
            user_id=user_id,
        )
    )
    try:
        revision = await asyncio.shield(resolution)
    except asyncio.CancelledError:
        material = ownership.abandon()
        resolution.add_done_callback(_consume_task_result)
        if material is not None:
            try:
                await _release_process_material_bounded(material)
            except asyncio.CancelledError:
                pass
        raise
    return ownership.take(revision)


async def _seal_accepted_invocation(
    *,
    request: Any,
    intent: InternalLaunchIntent,
    config: dict[str, Any],
    graph_input: Any,
    owner_user_id: str | None,
    run_ctx: RunContext,
) -> AcceptedInvocation:
    runtime_context = config.get("context") if isinstance(config.get("context"), dict) else {}
    principal = await _principal_projection_for_intent(
        request,
        intent,
        owner_user_id=owner_user_id,
    )
    base_references = _base_origin_references(intent)
    app_state = getattr(getattr(request, "app", None), "state", None)
    contributor_host = getattr(app_state, "contributor_host", None)
    empty_contributor_digest = canonical_digest({"version": 1, "execution": []})
    if contributor_host is None:
        origin_contributions = SimpleNamespace(
            persistable=(),
            runtime_only=(),
            secret_handles=(),
            execution_digest=empty_contributor_digest,
            diagnostics=(),
        )
    else:
        origin_contributions = await contributor_host.contribute_origin(
            OriginContributionRequestV1(
                source_kind=intent.source_kind.value,
                authenticated_subject_reference=principal.user_id,
                source_references=_origin_request_references(base_references),
                identity=principal.identity,
            )
        )
    for diagnostic in origin_contributions.diagnostics:
        logger.warning(
            "Optional invocation contributor omitted capability_id=%s contribution_id=%s diagnostic_code=%s error_class=%s correlation_id=%s",
            diagnostic.capability_id,
            diagnostic.contribution_id,
            diagnostic.diagnostic_code,
            diagnostic.error_class,
            diagnostic.correlation_id,
        )
    origin_persistable, origin_runtime_only, origin_secret_handles = _trusted_contribution_references(
        origin_contributions,
        capability_kind="origin_contributor",
    )
    origin = InvocationOrigin(
        source_kind=intent.source_kind.value,
        references=base_references,
        contributor_references=_contribution_json(origin_contributions),
    )

    app_config = run_ctx.app_config or get_app_config()
    revision = await _resolve_agent_revision_cancellation_safe(
        config,
        app_config=app_config,
        user_id=principal.user_id,
    )
    if revision.material is None:  # pragma: no cover - resolver contract
        raise RuntimeError("accepted agent revision is missing captured material")
    # Publish the process-local lease into the already-scrubbed host context as
    # soon as it exists. The normalizer releases it if a later contributor or
    # sealing step fails before a PreparedLaunch can transfer ownership.
    runtime_context[RESOLVED_AGENT_MATERIAL_CONTEXT_KEY] = revision.material
    config["context"] = runtime_context

    public_principal = PrincipalProjectionV1(
        user_id=principal.user_id,
        role=principal.role,
        oauth_provider=principal.oauth_provider,
        oauth_id=principal.oauth_id,
        channel_user_id=principal.channel_user_id,
        is_internal=principal.is_internal,
        identity=principal.identity,
    )
    public_origin_references = _origin_request_references(base_references)
    origin_contributor_references = (
        *origin_persistable,
        *origin_runtime_only,
        *origin_secret_handles,
    )
    final_origin_digest = canonical_digest(
        {
            "version": 1,
            "source_kind": origin.source_kind,
            "references": [
                {
                    "key": reference.key,
                    "value": reference.value,
                    "storage_class": reference.storage_class,
                    "purpose": reference.purpose,
                }
                for reference in public_origin_references
            ],
            "contributor_references": [reference.to_json() for reference in origin_contributor_references],
        }
    )
    public_origin = SealedOriginV1(
        source_kind=origin.source_kind,
        references=public_origin_references,
        digest=final_origin_digest,
        contributor_references=origin_contributor_references,
    )
    if contributor_host is None:
        context_contributions = SimpleNamespace(
            persistable=(),
            runtime_only=(),
            secret_handles=(),
            execution_digest=empty_contributor_digest,
            diagnostics=(),
        )
    else:
        try:
            context_contributions = await contributor_host.contribute_run_context(
                RunContextContributionRequestV1(
                    principal=public_principal,
                    origin=public_origin,
                    thread_id=intent.thread_id,
                    agent_revision=ResolvedAgentRevisionReferenceV1(
                        agent_id=revision.agent_id,
                        digest=revision.digest,
                    ),
                    external_key_reference=(normalize_external_key(intent.external_key) if intent.external_key is not None else None),
                )
            )
        except (Exception, asyncio.CancelledError):
            try:
                await _release_unattached_agent_material(config)
            except asyncio.CancelledError:
                pass
            raise
    for diagnostic in context_contributions.diagnostics:
        logger.warning(
            "Optional invocation contributor omitted capability_id=%s contribution_id=%s diagnostic_code=%s error_class=%s correlation_id=%s",
            diagnostic.capability_id,
            diagnostic.contribution_id,
            diagnostic.diagnostic_code,
            diagnostic.error_class,
            diagnostic.correlation_id,
        )
    context_persistable, context_runtime_only, context_secret_handles = _trusted_contribution_references(
        context_contributions,
        capability_kind="run_context_contributor",
    )

    contributor_execution_digest = canonical_digest(
        {
            "version": 1,
            "origin": origin_contributions.execution_digest,
            "run_context": context_contributions.execution_digest,
        }
    )
    context_references = {
        key: runtime_context[key]
        for key in (
            "non_interactive",
            "is_plan_mode",
            "subagent_enabled",
            "max_concurrent_subagents",
            "max_total_subagents",
        )
        if key in runtime_context
    }
    extensions = getattr(app_state, "extensions", None)
    extension_generation = int(getattr(extensions, "generation", 0))
    capability_manifest = getattr(app_state, "capability_manifest", None)
    extension_manifest_digest = getattr(capability_manifest, "digest", None)
    if principal.identity is None:  # pragma: no cover - new acceptance contract
        raise RuntimeError("accepted principal is missing split identity")
    model_profile = revision.material.model_profile
    profile_id = str(model_profile.get("name") or "default")
    external_key_reference = normalize_external_key(intent.external_key) if intent.external_key is not None else None
    trusted_context = TrustedRunContextV1(
        identity=principal.identity,
        origin=public_origin,
        thread_id=intent.thread_id,
        external_key_reference=external_key_reference,
        agent_revision=ResolvedAgentRevisionReferenceV1(
            agent_id=revision.agent_id,
            digest=revision.digest,
        ),
        profile_revision=ResolvedProfileRevisionReferenceV1(
            profile_id=profile_id,
            digest=canonical_digest({"version": 1, "model_profile": model_profile}),
        ),
        extension_generation=extension_generation,
        extension_manifest_digest=extension_manifest_digest,
        persistable_references=(*origin_persistable, *context_persistable),
        runtime_only_references=(*origin_runtime_only, *context_runtime_only),
        secret_handles=(*origin_secret_handles, *context_secret_handles),
    )
    accepted = AcceptedInvocation.seal(
        principal=principal,
        origin=origin,
        thread_id=intent.thread_id,
        context_references=context_references,
        agent_revision=revision,
        normalized_input=({"resume": canonical_request_value(intent.command["resume"])} if intent.command and intent.command.get("resume") is not None else canonical_request_value(graph_input)),
        execution_options={
            "multitask_strategy": intent.multitask_strategy,
            "interrupt_before": intent.interrupt_before,
            "interrupt_after": intent.interrupt_after,
            "checkpoint_id": intent.checkpoint_id,
            "recursion_limit": config.get("recursion_limit"),
        },
        extension_generation=extension_generation,
        extension_manifest_digest=extension_manifest_digest,
        contributor_execution_digest=contributor_execution_digest,
        trusted_context=trusted_context,
    )
    # These objects are server-owned and installed after all caller context is
    # scrubbed. The worker and delegated subagents inherit the same accepted
    # revision/generation for construction and audit.
    runtime_context[RESOLVED_AGENT_MATERIAL_CONTEXT_KEY] = revision.material
    runtime_context["accepted_agent_revision_digest"] = revision.digest
    runtime_context["accepted_extension_generation"] = extension_generation
    if extension_manifest_digest is not None:
        runtime_context["accepted_extension_manifest_digest"] = extension_manifest_digest
    config["context"] = runtime_context
    return accepted


async def _release_unattached_agent_material(config: dict[str, Any]) -> None:
    """Release a process-local snapshot lease that never reached a worker."""
    runtime_context = config.get("context")
    material = runtime_context.get(RESOLVED_AGENT_MATERIAL_CONTEXT_KEY) if isinstance(runtime_context, dict) else None
    if isinstance(material, ResolvedAgentMaterialV1):
        runtime_context.pop(RESOLVED_AGENT_MATERIAL_CONTEXT_KEY, None)
        await _release_process_material_bounded(material)


class _GatewayLaunchNormalizer:
    """Translate a finite internal intent into the current Gateway run plan."""

    def __init__(
        self,
        request: Request,
        *,
        trust_internal_launch_facts: bool = False,
    ) -> None:
        self._request = request
        self._trust_internal_launch_facts = trust_internal_launch_facts
        self._identified: dict[int, tuple[InternalLaunchIntent, InternalAdmissionIdentity]] = {}

    def _owner_user_id(self, intent: InternalLaunchIntent) -> str | None:
        if self._trust_internal_launch_facts and intent.source_kind in {
            InternalSourceKind.scheduled_task,
            InternalSourceKind.native_channel,
            InternalSourceKind.service,
        }:
            return intent.owner_user_id
        return get_trusted_internal_owner_user_id(self._request)

    @staticmethod
    def _validate_native_channel_facts(intent: InternalLaunchIntent) -> InternalNativeChannelFacts:
        facts = intent.native_channel
        if facts is None or not facts.provider or not facts.chat_id or not facts.channel_user_id:
            raise ValueError("native channel launch requires authenticated provider, chat, and sender facts")
        if facts.resolved_assistant_id != intent.assistant_id:
            raise ValueError("native channel launch assistant does not match its resolved route")
        context = intent.context or {}
        if context.get("channel_user_id") != facts.channel_user_id:
            raise ValueError("native channel launch sender does not match its authenticated source facts")
        if context.get("channel_name") not in (None, facts.provider):
            raise ValueError("native channel launch provider does not match its authenticated source facts")
        if context.get("agent_name") != facts.resolved_agent_name:
            raise ValueError("native channel launch agent does not match its resolved route")
        binding = facts.verified_binding
        if binding is not None:
            if binding.kind is InternalVerifiedNativeBindingKind.connection:
                if not facts.connection_id or binding.reference != facts.connection_id:
                    raise ValueError("native channel connection binding conflicts with its verified source facts")
            elif binding.kind is InternalVerifiedNativeBindingKind.webhook_route:
                if facts.connection_id is not None:
                    raise ValueError("native channel launch has conflicting verified binding sources")
            else:  # pragma: no cover - enum construction is closed
                raise ValueError("native channel launch has an unsupported verified binding")
        if intent.external_key is not None and binding is None:
            raise ValueError("keyed native-channel admission requires a verified source binding")
        return facts

    def _metadata(self, intent: InternalLaunchIntent) -> dict[str, Any]:
        metadata = thaw_host_value(intent.metadata or {})
        if not self._trust_internal_launch_facts or intent.source_kind is not InternalSourceKind.scheduled_task:
            return metadata
        if not intent.trusted_task_id or not intent.task_run_id or intent.scheduled_trigger not in {"scheduled", "manual"}:
            raise ValueError("scheduled task launch requires trusted task, occurrence, and trigger facts")
        metadata.update(
            scheduled_task_id=intent.trusted_task_id,
            scheduled_task_run_id=intent.task_run_id,
            scheduled_trigger=intent.scheduled_trigger,
        )
        return metadata

    @contextmanager
    def scope(self, intent: InternalLaunchIntent):
        owner_user_id = self._owner_user_id(intent)
        token = set_current_user(SimpleNamespace(id=owner_user_id)) if owner_user_id else None
        try:
            yield
        finally:
            cached = self._identified.get(id(intent))
            if cached is not None and cached[0] is intent:
                self._identified.pop(id(intent), None)
            if token is not None:
                reset_current_user(token)

    async def identify(self, intent: InternalLaunchIntent) -> InternalAdmissionIdentity | None:
        if intent.external_key is None:
            return None
        _validate_keyed_request_shape(intent)
        try:
            external_key = normalize_external_key(intent.external_key)
            owner_user_id = self._owner_user_id(intent)
            principal = await _principal_projection_for_intent(
                self._request,
                intent,
                owner_user_id=owner_user_id,
            )
            if intent.source_kind is InternalSourceKind.http:
                subject_id = principal.user_id
                if subject_id is None:
                    raise ValueError("keyed HTTP admission requires an authenticated server subject")
                auth_source = getattr(getattr(self._request, "state", None), "auth_source", None)
                configured_kind = getattr(getattr(self._request, "state", None), "principal_kind", None)
                if auth_source == AUTH_SOURCE_AUTH_DISABLED:
                    principal_kind = "default-user"
                elif configured_kind in {"user", "service"}:
                    principal_kind = configured_kind
                else:
                    principal_kind = "service" if principal.role == "service" else "user"
                external_scope = scope_for_http(principal_kind, subject_id)
            elif intent.source_kind is InternalSourceKind.native_channel:
                facts = self._validate_native_channel_facts(intent)
                binding = facts.verified_binding
                if binding is None:  # guarded by _validate_native_channel_facts
                    raise ValueError("keyed native-channel admission requires a verified source binding")
                external_scope = scope_for_channel(
                    facts.provider,
                    binding.reference,
                    facts.workspace_id or "",
                    facts.chat_id,
                    binding_kind=binding.kind.value,
                )
            elif intent.source_kind is InternalSourceKind.scheduled_task:
                self._metadata(intent)
                if owner_user_id is not None:
                    scope_owner = owner_user_id
                elif intent.scheduled_system_owned:
                    scope_owner = SYSTEM_TASK_OWNER
                else:
                    raise ValueError("keyed scheduled admission requires a persisted owner")
                external_scope = scope_for_scheduler(scope_owner, str(intent.trusted_task_id))
            elif intent.source_kind is InternalSourceKind.service:
                if not self._trust_internal_launch_facts or not intent.trusted_service_id or principal.user_id != intent.trusted_service_id or principal.role != "service":
                    raise ValueError("service admission requires one matching authenticated service identity")
                external_scope = scope_for_service(intent.trusted_service_id)
            else:  # pragma: no cover - closed enum
                raise ValueError(f"unsupported invocation source {intent.source_kind}")
        except ValueError as exc:
            if intent.source_kind is InternalSourceKind.http:
                raise _keyed_request_error(str(exc)) from exc
            raise
        identity = InternalAdmissionIdentity(
            external_scope=external_scope,
            external_key=external_key,
            principal_digest=canonical_digest({"version": 1, "principal": principal.to_json()}),
            base_origin_digest=_base_origin_digest(intent),
            thread_id=intent.thread_id if intent.thread_id_explicit else None,
            requested_agent_id=_requested_agent_id(intent),
            caller_intent=_canonical_caller_intent(intent),
            user_id=principal.user_id,
            principal=_invocation_principal_from_projection(principal),
        )
        # Retain the intent object alongside its identity: a strong reference
        # prevents Python from recycling the object ID before normalization.
        self._identified[id(intent)] = (intent, identity)
        return identity

    async def validate_replay(
        self,
        intent: InternalLaunchIntent,
        identity: InternalAdmissionIdentity,
        record: RunRecord,
    ) -> None:
        cached = self._identified.get(id(intent))
        if cached is not None and cached[0] is intent:
            self._identified.pop(id(intent), None)
        accepted = record.accepted_invocation
        if accepted is None:
            raise IdempotencyConflictError("The retained run has no accepted invocation evidence")
        if identity.principal_digest != accepted.principal_digest:
            raise IdempotencyConflictError("Idempotency key has contradictory authenticated principal evidence")
        if identity.base_origin_digest != accepted.base_origin_digest:
            facts = intent.native_channel
            accepted_references = accepted.origin.references
            legacy_connection_evidence = (
                intent.source_kind is InternalSourceKind.native_channel
                and facts is not None
                and facts.verified_binding is not None
                and facts.verified_binding.kind is InternalVerifiedNativeBindingKind.connection
                and "binding_kind" not in accepted_references
                and "binding_reference" not in accepted_references
                and _base_origin_digest(intent, include_verified_binding=False) == accepted.base_origin_digest
            )
            if not legacy_connection_evidence:
                raise IdempotencyConflictError("Idempotency key has contradictory authenticated source evidence")
        if identity.thread_id is not None and identity.thread_id != record.thread_id:
            raise IdempotencyConflictError("Idempotency key is bound to a different thread")
        caller_intent = identity.caller_intent
        stored_caller_intent = record.caller_intent_json
        if caller_intent is None or not isinstance(stored_caller_intent, Mapping):
            raise IdempotencyConflictError("The retained run predates canonical caller-intent evidence")
        try:
            persisted = CanonicalCallerIntent.from_persisted(stored_caller_intent)
        except (TypeError, ValueError) as exc:
            raise IdempotencyConflictError("The retained run has invalid caller-intent evidence") from exc
        if record.caller_intent_digest_version != caller_intent.digest_version or record.caller_intent_digest != caller_intent.digest or persisted.digest != record.caller_intent_digest:
            raise IdempotencyConflictError("Idempotency key was already used for a different request")

    async def normalize(self, intent: InternalLaunchIntent) -> PreparedLaunch:
        cached = self._identified.pop(id(intent), None)
        identity = cached[1] if cached is not None and cached[0] is intent else None
        if self._trust_internal_launch_facts and intent.source_kind is InternalSourceKind.native_channel:
            self._validate_native_channel_facts(intent)
        try:
            validate_thread_id(intent.thread_id)
        except ValueError as exc:
            raise HTTPException(status_code=422, detail=str(exc)) from exc

        metadata = self._metadata(intent)
        config_metadata = thaw_host_value(intent.config.get("metadata")) if isinstance(intent.config, Mapping) else None
        try:
            validate_run_metadata_secrets(metadata)
            validate_run_metadata_secrets(config_metadata)
        except LegacyRunMetadataSecretError as exc:
            raise HTTPException(status_code=422, detail=str(exc)) from exc

        stream_modes = normalize_stream_modes(thaw_host_value(intent.stream_mode))
        bridge = get_stream_bridge(self._request)
        run_mgr = get_run_manager(self._request)
        run_ctx = get_run_context(self._request)
        disconnect = DisconnectMode.cancel if intent.on_disconnect == "cancel" else DisconnectMode.continue_

        body_context = intent.context or {}
        model_name = body_context.get("model_name")
        if model_name is not None:
            try:
                model_name = validate_model_profile_identifier(model_name, field_name="context.model_name profile identifier")
            except ValueError as exc:
                raise HTTPException(status_code=422, detail=str(exc)) from exc
        # Validate model against the allowlist when a model_name is provided.
        if model_name and get_app_config().get_model_config(model_name) is None:
            raise HTTPException(
                status_code=400,
                detail=f"Model {model_name!r} is not in the configured model allowlist",
            )

        owner_user_id = self._owner_user_id(intent)
        # Stateless run endpoints carry thread_id in the request *body*, so the
        # @require_permission(owner_check=True) decorator -- which resolves
        # ownership from the path param -- cannot protect them. Enforce thread
        # ownership before admission. Internal channel runs act on behalf of the
        # connection owner carried in X-DeerFlow-Owner-User-Id, so they remain
        # scoped exclusively to that owner instead of inheriting the internal
        # carrier account's thread access.
        user = getattr(self._request.state, "user", None)
        if user is not None:
            access_owner_user_id = owner_user_id if owner_user_id and getattr(user, "system_role", None) == INTERNAL_SYSTEM_ROLE else str(user.id)
            allowed = await run_ctx.thread_store.check_access(
                intent.thread_id,
                access_owner_user_id,
            )
            if not allowed:
                raise HTTPException(status_code=404, detail=f"Thread {intent.thread_id} not found")

        agent_factory = resolve_agent_factory(intent.assistant_id)
        is_internal_caller = getattr(getattr(self._request, "state", None), "auth_source", None) == AUTH_SOURCE_INTERNAL
        if intent.command and intent.command.get("resume") is not None:
            graph_input = Command(resume=thaw_host_value(intent.command["resume"]))
        else:
            graph_input = normalize_input(
                thaw_host_value(intent.input),
                trusted_internal=is_internal_caller,
            )
        config = build_run_config(
            intent.thread_id,
            thaw_host_value(intent.config),
            metadata,
            assistant_id=intent.assistant_id,
        )
        await apply_checkpoint_to_run_config(
            config,
            body=intent,
            thread_id=intent.thread_id,
            request=self._request,
        )
        # Merge DeerFlow-specific context overrides into both ``configurable``
        # and ``context``. Only agent-relevant keys are forwarded.
        merge_run_context_overrides(config, intent.context, internal=is_internal_caller)
        if not is_internal_caller:
            # ``intent.config`` is free-form and copied by ``build_run_config``;
            # scrub any internal-only keys smuggled there.
            strip_internal_context_keys(config)
        internal_owner_user = await resolve_trusted_internal_owner_for_attribution(
            self._request,
            owner_user_id,
        )
        inject_authenticated_user_context(
            config,
            self._request,
            internal_owner_user=internal_owner_user,
            request_context=intent.context,
        )
        if not is_internal_caller:
            # External agent/config values remain hints until this server-side
            # route resolution. A body/config value cannot stamp the accepted
            # agent revision independently of assistant_id.
            resolved_agent_name = intent.assistant_id if intent.assistant_id not in (None, _DEFAULT_ASSISTANT_ID) else None
            resolved_bootstrap = False
            if resolved_agent_name is None and body_context.get("is_bootstrap") is True:
                try:
                    resolved_agent_name = validate_agent_name(body_context.get("agent_name"))
                except ValueError as exc:
                    raise HTTPException(status_code=422, detail=str(exc)) from exc
                resolved_bootstrap = resolved_agent_name is not None
            for section_name in ("context", "configurable"):
                section = config.get(section_name)
                if not isinstance(section, dict):
                    continue
                section.pop("agent_name", None)
                section["is_bootstrap"] = resolved_bootstrap
                if resolved_agent_name is not None:
                    section["agent_name"] = resolved_agent_name

        try:
            accepted_invocation = await _seal_accepted_invocation(
                request=self._request,
                intent=intent,
                config=config,
                graph_input=graph_input,
                owner_user_id=owner_user_id,
                run_ctx=run_ctx,
            )
        except Exception:
            await _release_unattached_agent_material(config)
            raise
        # Start authorization receives a canonical digest for every launch,
        # including unkeyed calls. Only keyed admission persists the internal
        # projection and uses it for replay comparison.
        try:
            effective_execution = _effective_execution_projection(
                intent,
                accepted=accepted_invocation,
                graph_input=graph_input,
                config=config,
            )
        except Exception:
            await _release_unattached_agent_material(config)
            raise
        caller_intent = identity.caller_intent if identity is not None else None

        entered_run_agent = False

        async def run_after_metadata_body(record: RunRecord) -> None:
            nonlocal entered_run_agent
            requires_owned_metadata = bool(owner_user_id or getattr(record, "user_id", None))
            metadata_task = asyncio.create_task(
                _ensure_thread_metadata(
                    run_ctx,
                    record,
                    owner_user_id=owner_user_id,
                )
            )
            abort_task = asyncio.create_task(record.abort_event.wait())
            metadata_failure_logged = False
            startup_failure: str | None = None
            abort_before_metadata = False
            try:
                done, _ = await asyncio.wait(
                    (metadata_task, abort_task),
                    timeout=_THREAD_METADATA_SETUP_TIMEOUT_SECONDS,
                    return_when=asyncio.FIRST_COMPLETED,
                )
                if abort_task in done:
                    abort_before_metadata = True
                elif metadata_task in done:
                    try:
                        metadata_task.result()
                    except asyncio.CancelledError:
                        pass
                    except _ThreadOwnershipConflict as exc:
                        metadata_failure_logged = True
                        _log_thread_metadata_failure(
                            exc,
                            code="thread_ownership_conflict",
                            thread_id=intent.thread_id,
                        )
                        startup_failure = "Thread ownership conflict prevented execution"
                    except Exception as exc:
                        metadata_failure_logged = True
                        _log_thread_metadata_failure(
                            exc,
                            code="thread_metadata_setup_failed",
                            thread_id=intent.thread_id,
                        )
                        if requires_owned_metadata:
                            startup_failure = "Thread ownership metadata was unavailable before execution"
                elif abort_task not in done:
                    _log_thread_metadata_failure(
                        TimeoutError("thread metadata setup deadline elapsed"),
                        code="thread_metadata_setup_timeout",
                        thread_id=intent.thread_id,
                    )
                    if requires_owned_metadata:
                        startup_failure = "Thread ownership metadata was unavailable before execution"
            finally:
                if metadata_task.done():
                    if not metadata_failure_logged:
                        _log_thread_metadata_task_result(
                            metadata_task,
                            thread_id=intent.thread_id,
                        )
                else:
                    metadata_task.cancel()
                    metadata_task.add_done_callback(
                        lambda task: _log_thread_metadata_task_result(
                            task,
                            thread_id=intent.thread_id,
                        )
                    )
                if not abort_task.done():
                    abort_task.cancel()
                    abort_task.add_done_callback(_consume_task_result)
            if startup_failure is not None:
                failure_retained = await run_mgr.fail_start_if_pending(
                    record.run_id,
                    error=startup_failure,
                )
                if not failure_retained:
                    await run_mgr.finalize_pending_cancellation(record.run_id)
                await _finalize_pregraph_stream(
                    bridge,
                    record,
                    error_message=(startup_failure if failure_retained else None),
                )
                return
            if abort_before_metadata:
                await run_mgr.finalize_pending_cancellation(record.run_id)
                await _finalize_pregraph_stream(
                    bridge,
                    record,
                    error_message=None,
                )
                return
            entered_run_agent = True
            await run_agent(
                bridge,
                run_mgr,
                record,
                ctx=run_ctx,
                agent_factory=agent_factory,
                graph_input=graph_input,
                config=config,
                stream_modes=list(stream_modes),
                stream_subgraphs=intent.stream_subgraphs,
                interrupt_before=thaw_host_value(intent.interrupt_before),
                interrupt_after=thaw_host_value(intent.interrupt_after),
            )

        async def run_after_metadata(record: RunRecord) -> None:
            try:
                await run_after_metadata_body(record)
            except asyncio.CancelledError:
                if entered_run_agent or not record.abort_event.is_set():
                    raise

                async def finalize_cancelled_pregraph() -> None:
                    await run_mgr.finalize_pending_cancellation(record.run_id)
                    await _finalize_pregraph_stream(
                        bridge,
                        record,
                        error_message=None,
                    )

                finalizer = asyncio.create_task(finalize_cancelled_pregraph())
                finalizer.add_done_callback(_consume_task_result)
                deadline = asyncio.get_running_loop().time() + _PREGRAPH_FINALIZE_TIMEOUT_SECONDS
                while not finalizer.done():
                    remaining = deadline - asyncio.get_running_loop().time()
                    if remaining <= 0:
                        finalizer.cancel()
                        _log_pregraph_stream_failure(
                            TimeoutError("pre-graph cancellation finalizer deadline elapsed"),
                            operation="cancelled_worker_finalize",
                            run_id=record.run_id,
                        )
                        return
                    try:
                        await asyncio.wait_for(
                            asyncio.shield(finalizer),
                            timeout=remaining,
                        )
                    except asyncio.CancelledError:
                        continue
                    except TimeoutError as exc:
                        finalizer.cancel()
                        _log_pregraph_stream_failure(
                            exc,
                            operation="cancelled_worker_finalize",
                            run_id=record.run_id,
                        )
                        return
                finalizer.result()

        try:
            prepared = PreparedLaunch(
                thread_id=intent.thread_id,
                assistant_id=intent.assistant_id,
                on_disconnect=disconnect,
                metadata=metadata,
                kwargs={
                    # The stored kwargs are echoed by the run API, so persist a
                    # secret-redacted config while retaining live secrets above.
                    "input": thaw_host_value(intent.input),
                    "config": redact_config_secrets(thaw_host_value(intent.config)),
                    **({_EFFECTIVE_EXECUTION_PROJECTION_KEY: effective_execution.to_persisted()} if identity is not None else {}),
                },
                multitask_strategy=intent.multitask_strategy,
                model_name=model_name,
                user_id=accepted_invocation.principal.user_id,
                worker=run_after_metadata,
                accepted_invocation=accepted_invocation,
                external_scope=identity.external_scope if identity is not None else None,
                external_key=identity.external_key if identity is not None else None,
                request_digest=effective_execution.digest,
                request_digest_version=effective_execution.digest_version,
                caller_intent_json=caller_intent.to_persisted() if caller_intent is not None else None,
                caller_intent_digest=caller_intent.digest if caller_intent is not None else None,
                caller_intent_digest_version=caller_intent.digest_version if caller_intent is not None else None,
                principal=_invocation_principal_from_projection(accepted_invocation.principal),
            )
        except Exception:
            await _release_unattached_agent_material(config)
            raise
        return prepared


class _GatewayDurableRuns:
    """Gateway adapter over the harness run manager and admission lock."""

    def __init__(self, request: Request) -> None:
        self._request = request
        self._projection_reservations: dict[int, object] = {}
        self._projection_supersessions: dict[int, object] = {}

    @asynccontextmanager
    async def admission_scope(self, thread_id: str):
        async with goal_thread_lock(thread_id):
            yield

    async def prepare_admission(self, launch: PreparedLaunch) -> None:
        run_manager = get_run_manager(self._request)
        launch_identity = id(launch)
        if launch.external_scope is not None and launch.external_key is not None:
            existing = await run_manager.get_by_external_identity(
                launch.external_scope,
                launch.external_key,
                user_id=launch.user_id,
            )
            if existing is not None:
                # The optimistic lookup raced a creator. Let the atomic store
                # classify equal replay versus digest conflict; neither case
                # is blocked by the creator's still-live projection.
                return

        accepted = launch.accepted_invocation
        material = accepted.agent_revision.material if accepted is not None else None
        if material is not None:
            from deerflow.runtime.skill_projection import (
                SkillProjectionBusyError,
                SkillProjectionEvidence,
                get_skill_projection_coordinator,
            )

            snapshot = material.skill_snapshot
            try:
                reservation = get_skill_projection_coordinator().reserve_admission(
                    user_id=launch.user_id or DEFAULT_USER_ID,
                    thread_id=launch.thread_id,
                    reservation_id=f"admission:{uuid.uuid4().hex}",
                    snapshot_id=None if snapshot is None else snapshot.snapshot_id,
                    evidence=SkillProjectionEvidence.from_snapshot(snapshot),
                )
            except SkillProjectionBusyError as exc:
                coordinator = get_skill_projection_coordinator()
                replacement = launch.multitask_strategy in ("interrupt", "rollback")
                if not replacement:
                    raise ConflictError(
                        "Thread has an invocation-owned skill projection",
                    ) from exc
                try:
                    supersession = coordinator.fence_committed_owner(
                        user_id=launch.user_id or DEFAULT_USER_ID,
                        thread_id=launch.thread_id,
                    )
                except SkillProjectionBusyError:
                    raise ConflictError(
                        "Thread has an invocation-owned skill projection",
                    ) from exc
                self._projection_supersessions[launch_identity] = supersession
            else:
                self._projection_reservations[launch_identity] = reservation
        try:
            await ensure_checkpoint_history_seeded(
                self._request,
                thread_id=launch.thread_id,
                assistant_id=launch.assistant_id,
            )
        except (Exception, asyncio.CancelledError):
            reservation = self._projection_reservations.pop(launch_identity, None)
            self._projection_supersessions.pop(launch_identity, None)
            if reservation is not None:
                from deerflow.runtime.skill_projection import get_skill_projection_coordinator

                get_skill_projection_coordinator().abort_admission(reservation)
            raise

    async def find_by_external_identity(
        self,
        identity: InternalAdmissionIdentity,
    ) -> RunRecord | None:
        return await get_run_manager(self._request).get_by_external_identity(
            identity.external_scope,
            identity.external_key,
            user_id=identity.user_id,
        )

    @staticmethod
    async def _terminalize_unattached_candidate(
        run_manager,
        candidate_run_id: str,
    ) -> None:
        fail_start = getattr(run_manager, "fail_start_if_pending", None)
        if not callable(fail_start):
            return
        cleanup = asyncio.create_task(
            fail_start(
                candidate_run_id,
                error="worker_attachment_failed",
            ),
            name=f"deerflow-abort-admission-handoff-{candidate_run_id}",
        )
        while not cleanup.done():
            try:
                await asyncio.shield(cleanup)
            except asyncio.CancelledError:
                continue
        cleanup.result()

    async def admit(
        self,
        launch: PreparedLaunch,
        *,
        candidate_run_id: str,
    ) -> DurableAdmission | RunRecord:
        run_manager = get_run_manager(self._request)
        launch_identity = id(launch)
        reservation = self._projection_reservations.pop(launch_identity, None)
        supersession = self._projection_supersessions.pop(launch_identity, None)
        try:
            if launch.external_scope is None:
                record = await run_manager.create_or_reject(
                    launch.thread_id,
                    launch.assistant_id,
                    candidate_run_id=candidate_run_id,
                    on_disconnect=launch.on_disconnect,
                    metadata=thaw_host_value(launch.metadata),
                    kwargs=thaw_host_value(launch.kwargs),
                    multitask_strategy=launch.multitask_strategy,
                    model_name=launch.model_name,
                    user_id=launch.user_id,
                    accepted_invocation=launch.accepted_invocation,
                )
                if reservation is not None:
                    from deerflow.runtime.skill_projection import get_skill_projection_coordinator

                    get_skill_projection_coordinator().promote_admission(
                        reservation,
                        run_id=record.run_id,
                    )
                elif supersession is not None:
                    from deerflow.runtime.skill_projection import (
                        SkillProjectionEvidence,
                        get_skill_projection_coordinator,
                    )

                    snapshot = launch.accepted_invocation.agent_revision.material.skill_snapshot
                    get_skill_projection_coordinator().promote_supersession(
                        supersession,
                        run_id=record.run_id,
                        snapshot_id=None if snapshot is None else snapshot.snapshot_id,
                        evidence=SkillProjectionEvidence.from_snapshot(snapshot),
                    )
                return record
            if launch.external_key is None or launch.request_digest is None or launch.request_digest_version is None or launch.caller_intent_json is None or launch.caller_intent_digest is None or launch.caller_intent_digest_version is None:
                raise RuntimeError("keyed launch is missing canonical admission evidence")
            admission = await run_manager.ensure_or_reject(
                launch.thread_id,
                launch.assistant_id,
                candidate_run_id=candidate_run_id,
                on_disconnect=launch.on_disconnect,
                metadata=thaw_host_value(launch.metadata),
                kwargs=thaw_host_value(launch.kwargs),
                multitask_strategy=launch.multitask_strategy,
                model_name=launch.model_name,
                user_id=launch.user_id,
                accepted_invocation=launch.accepted_invocation,
                external_scope=launch.external_scope,
                external_key=launch.external_key,
                request_digest=launch.request_digest,
                request_digest_version=launch.request_digest_version,
                caller_intent_json=thaw_host_value(launch.caller_intent_json),
                caller_intent_digest=launch.caller_intent_digest,
                caller_intent_digest_version=launch.caller_intent_digest_version,
            )
            if reservation is not None:
                from deerflow.runtime.skill_projection import get_skill_projection_coordinator

                coordinator = get_skill_projection_coordinator()
                if admission.outcome is AdmissionOutcome.created:
                    coordinator.promote_admission(
                        reservation,
                        run_id=admission.record.run_id,
                    )
                else:
                    coordinator.abort_admission(reservation)
            elif supersession is not None and admission.outcome is AdmissionOutcome.created:
                from deerflow.runtime.skill_projection import (
                    SkillProjectionEvidence,
                    get_skill_projection_coordinator,
                )

                snapshot = launch.accepted_invocation.agent_revision.material.skill_snapshot
                get_skill_projection_coordinator().promote_supersession(
                    supersession,
                    run_id=admission.record.run_id,
                    snapshot_id=None if snapshot is None else snapshot.snapshot_id,
                    evidence=SkillProjectionEvidence.from_snapshot(snapshot),
                )
            return DurableAdmission(record=admission.record, outcome=admission.outcome)
        except BaseException:
            from deerflow.runtime.skill_projection import get_skill_projection_coordinator

            coordinator = get_skill_projection_coordinator()
            if reservation is not None:
                coordinator.abort_admission(reservation)
            try:
                await self._terminalize_unattached_candidate(
                    run_manager,
                    candidate_run_id,
                )
            finally:
                # The manager must first prove terminal state or retain the
                # exact candidate in its compensator. Only then may this
                # process release the accepted execution material.
                coordinator.release_unactivated_run(
                    user_id=launch.user_id or DEFAULT_USER_ID,
                    thread_id=launch.thread_id,
                    run_id=candidate_run_id,
                )
            raise

    async def attach_worker(
        self,
        record: RunRecord,
        worker: WorkerCoroutine,
        task_factory: TaskFactory,
    ) -> asyncio.Task[None]:
        return await get_run_manager(self._request).attach_worker_once(
            record.run_id,
            worker,
            task_factory,
        )

    async def fail_start(self, record: RunRecord, error: str) -> None:
        from deerflow.runtime.skill_projection import get_skill_projection_coordinator

        try:
            await get_run_manager(self._request).fail_start_if_pending(
                record.run_id,
                error=error,
            )
        finally:
            get_skill_projection_coordinator().release_unactivated_run(
                user_id=record.user_id or DEFAULT_USER_ID,
                thread_id=record.thread_id,
                run_id=record.run_id,
            )

    async def cancel_start(self, record: RunRecord) -> None:
        """Cancel one admitted row that never transferred to a worker."""

        from deerflow.runtime.skill_projection import get_skill_projection_coordinator

        try:
            await get_run_manager(self._request).cancel_start_if_pending(
                record.run_id,
            )
        finally:
            get_skill_projection_coordinator().release_unactivated_run(
                user_id=record.user_id or DEFAULT_USER_ID,
                thread_id=record.thread_id,
                run_id=record.run_id,
            )

    async def observe(
        self,
        run_id: str,
        principal: InvocationPrincipal,
    ) -> RunRecord | None:
        user_id = None if principal.visibility_prevalidated or principal.role == "admin" else principal.user_id
        record = await get_run_manager(self._request).get(
            run_id,
            user_id=user_id,
        )
        # RunManager applies ``user_id`` while hydrating from its durable
        # store, but an already-local record is returned before that store
        # filter runs. Recheck the owner here for facades that have not
        # already completed a route/thread visibility decision.
        if record is not None and record.operation_kind is not ThreadOperationKind.run:
            return None
        if record is not None and user_id is not None and record.user_id != user_id:
            return None
        return record

    async def observe_granted(
        self,
        run_id: str,
        grant: ServiceObservationGrant,
    ) -> RunRecord | None:
        record = await get_run_manager(self._request).get(
            run_id,
            user_id=None,
        )
        if record is None or record.operation_kind is not ThreadOperationKind.run:
            return None
        return record if grant.permits(record) else None

    async def context_visible(
        self,
        thread_id: str,
        principal: InvocationPrincipal,
    ) -> bool:
        if principal.visibility_prevalidated:
            return True
        user_id = None if principal.role == "admin" else principal.user_id
        thread_store = get_thread_store(self._request)
        if user_id is None:
            if await thread_store.get(thread_id, user_id=None) is not None:
                return True
        elif await thread_store.check_access(
            thread_id,
            user_id,
            require_existing=True,
        ):
            return True
        # Preserve read access for legacy contexts that predate thread_meta,
        # while keeping a truly unknown context indistinguishable from one
        # owned by somebody else.
        records = await get_run_manager(self._request).list_by_thread(
            thread_id,
            user_id=user_id,
            limit=1,
        )
        return bool(records)

    async def context_visible_granted(
        self,
        thread_id: str,
        scope: LifecycleVisibilityScope,
    ) -> bool:
        return await get_run_manager(self._request).context_visible_in_scope(
            thread_id,
            scope,
        )

    async def query_lifecycle(self, query: LifecycleQuery) -> LifecyclePage:
        return await get_run_manager(self._request).query_lifecycle(query)

    async def cancel(
        self,
        cancel_request: InternalCancelRequest,
    ) -> CancelOutcome | CancellationRequestOutcome:
        if cancel_request.expected_state_version is not None:
            user_id = None if cancel_request.principal.visibility_prevalidated or cancel_request.principal.role == "admin" else cancel_request.principal.user_id
            return await get_run_manager(self._request).request_cancel_fenced(
                cancel_request.run_id,
                action=cancel_request.action,
                expected_state_version=cancel_request.expected_state_version,
                user_id=user_id,
            )
        return await get_run_manager(self._request).cancel(
            cancel_request.run_id,
            action=cancel_request.action,
        )


def _build_invocation_authorization(request: Any) -> ProviderInvocationAuthorization:
    app_state = getattr(getattr(request, "app", None), "state", None)
    settings = getattr(app_state, "invocation_authorization_config", None)
    if settings is None:
        from deerflow.config.authorization_config import InvocationOperationsAuthorizationConfig

        settings = InvocationOperationsAuthorizationConfig()
    resolver = getattr(app_state, "authorization_provider_resolver", None)

    def resolve():
        if resolver is None:
            raise RuntimeError("Gateway authorization provider resolver is unavailable")
        return resolver.resolve(get_app_config().authorization)

    return ProviderInvocationAuthorization(settings, resolve)


def _build_invocation_constraints(request: Any):
    from app.runtime.constraints import ProviderInvocationConstraints

    app_state = getattr(getattr(request, "app", None), "state", None)
    return ProviderInvocationConstraints(
        getattr(app_state, "invocation_constraints_host", None),
        getattr(app_state, "capability_health_monitor", None),
    )


def raise_for_invocation_authorization(
    result: Any,
    *,
    operation: str,
) -> None:
    """Translate finite internal authorization failures at the HTTP facade."""
    if result is InvocationAuthorizationOutcome.denied:
        raise HTTPException(status_code=403, detail=f"Invocation {operation} denied")
    if result is InvocationAuthorizationOutcome.indeterminate:
        raise HTTPException(
            status_code=503,
            detail=f"Invocation {operation} authorization indeterminate",
        )


def invocation_observation_enabled(request: Any) -> bool:
    """Return the Gateway's startup-snapshotted observe opt-in."""
    app_state = getattr(getattr(request, "app", None), "state", None)
    settings = getattr(app_state, "invocation_authorization_config", None)
    return settings is not None and settings.observe_enabled is True


def _observation_visibility(request: Any) -> ObservationVisibilityResolver | None:
    """Return the application-owned service visibility resolver, when configured."""

    app_state = getattr(getattr(request, "app", None), "state", None)
    return getattr(app_state, "service_observation_visibility_resolver", None)


async def authorize_context_observation(
    request: Request,
    thread_id: str,
    principal: InvocationPrincipal,
) -> None:
    """Authorize one already-visible context feed with the coherent provider."""
    decision = await _build_invocation_authorization(request).authorize_context_observe(
        thread_id,
        principal,
    )
    raise_for_invocation_authorization(decision.outcome, operation="observe")


def build_invocation_runtime(request: Request) -> InvocationRuntime:
    """Construct a request-scoped runtime from typed Gateway adapters."""
    return InvocationRuntime(
        normalizer=_GatewayLaunchNormalizer(request),
        runs=_GatewayDurableRuns(request),
        authorization=_build_invocation_authorization(request),
        constraints=_build_invocation_constraints(request),
        visibility=_observation_visibility(request),
        admission_fence=request.app.state.runtime_readiness,
    )


def build_scheduled_invocation_runtime(app: Any) -> InvocationRuntime:
    """Construct the scheduler's process-internal application runtime."""
    request = SimpleNamespace(
        app=app,
        headers={},
        state=SimpleNamespace(
            user=get_internal_user(),
            auth_source=AUTH_SOURCE_INTERNAL,
        ),
        cookies={},
    )
    return InvocationRuntime(
        normalizer=_GatewayLaunchNormalizer(
            request,
            trust_internal_launch_facts=True,
        ),
        runs=_GatewayDurableRuns(request),
        authorization=_build_invocation_authorization(request),
        constraints=_build_invocation_constraints(request),
        visibility=_observation_visibility(request),
        admission_fence=app.state.runtime_readiness,
    )


def build_channel_invocation_runtime(app: Any) -> InvocationRuntime:
    """Construct the native-channel process-internal application runtime."""
    request = SimpleNamespace(
        app=app,
        headers={},
        state=SimpleNamespace(
            user=get_internal_user(),
            auth_source=AUTH_SOURCE_INTERNAL,
        ),
        cookies={},
    )
    return InvocationRuntime(
        normalizer=_GatewayLaunchNormalizer(
            request,
            trust_internal_launch_facts=True,
        ),
        runs=_GatewayDurableRuns(request),
        authorization=_build_invocation_authorization(request),
        constraints=_build_invocation_constraints(request),
        visibility=_observation_visibility(request),
        admission_fence=app.state.runtime_readiness,
    )


def build_service_invocation_runtime(
    app: Any,
    *,
    authenticated_service_id: str,
) -> InvocationRuntime:
    """Construct an embedded-service runtime with a host-owned identity."""

    authenticated_service_id = validate_persisted_service_id(authenticated_service_id)
    request = SimpleNamespace(
        app=app,
        headers={},
        state=SimpleNamespace(
            user=SimpleNamespace(
                id=authenticated_service_id,
                system_role="service",
                oauth_provider=None,
                oauth_id=None,
            ),
            auth_source=AUTH_SOURCE_INTERNAL,
            principal_kind="service",
        ),
        cookies={},
    )
    return InvocationRuntime(
        normalizer=_GatewayLaunchNormalizer(
            request,
            trust_internal_launch_facts=True,
        ),
        runs=_GatewayDurableRuns(request),
        authorization=_build_invocation_authorization(request),
        constraints=_build_invocation_constraints(request),
        visibility=_observation_visibility(request),
        admission_fence=app.state.runtime_readiness,
    )


def _launch_intent(
    body: RunCreateRequest,
    thread_id: str,
    *,
    external_key: str | None = None,
    thread_id_explicit: bool = True,
) -> InternalLaunchIntent:
    return InternalLaunchIntent(
        thread_id=thread_id,
        assistant_id=getattr(body, "assistant_id", None),
        input=getattr(body, "input", None),
        command=getattr(body, "command", None),
        metadata=getattr(body, "metadata", None),
        config=getattr(body, "config", None),
        context=getattr(body, "context", None),
        checkpoint_id=getattr(body, "checkpoint_id", None),
        checkpoint=getattr(body, "checkpoint", None),
        interrupt_before=getattr(body, "interrupt_before", None),
        interrupt_after=getattr(body, "interrupt_after", None),
        stream_mode=getattr(body, "stream_mode", None),
        stream_subgraphs=getattr(body, "stream_subgraphs", False),
        on_disconnect=getattr(body, "on_disconnect", "cancel"),
        multitask_strategy=getattr(body, "multitask_strategy", "reject"),
        external_key=external_key,
        thread_id_explicit=thread_id_explicit,
    )


def _http_idempotency_key(request: Request) -> str | None:
    headers = getattr(request, "headers", {})
    for name, value in headers.items():
        if str(name).lower() == "idempotency-key":
            return value
    return None


async def start_run(
    body: RunCreateRequest,
    thread_id: str,
    request: Request,
    *,
    thread_id_explicit: bool = True,
) -> RunRecord:
    """FastAPI compatibility adapter for application-owned invocation launch."""
    try:
        validate_thread_id(thread_id)
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    runtime = build_invocation_runtime(request)
    try:
        receipt = await runtime.launch(
            _launch_intent(
                body,
                thread_id,
                external_key=_http_idempotency_key(request),
                thread_id_explicit=thread_id_explicit,
            )
        )
    except ConflictError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    except UnsupportedStrategyError as exc:
        raise HTTPException(status_code=501, detail=str(exc)) from exc
    raise_for_invocation_authorization(receipt, operation="start")
    if receipt is NotFoundOrInvisible.not_found_or_invisible:
        raise HTTPException(status_code=404, detail="Invocation not found")
    return receipt.record


async def sse_consumer(
    bridge: StreamBridge,
    record: RunRecord,
    request: Request,
    run_mgr: RunManager,
):
    """Async generator that yields SSE frames from the bridge.

    The ``finally`` block implements ``on_disconnect`` semantics:
    - ``cancel``: abort the background task on client disconnect.
    - ``continue``: let the task run; events are discarded.
    """
    last_event_id = request.headers.get("Last-Event-ID")
    if await _terminal_record_stream_missing(bridge, record):
        yield format_sse("end", None)
        return

    gap_emitted = False
    try:
        async for entry in bridge.subscribe(record.run_id, last_event_id=last_event_id):
            if await request.is_disconnected():
                break

            if isinstance(entry, StreamGap):
                gap_emitted = True
                yield format_sse(
                    "gap",
                    {
                        "code": "stream_replay_gap",
                        "run_id": record.run_id,
                        "requested_event_id": entry.requested_event_id,
                        "earliest_available_event_id": entry.earliest_available_event_id,
                        "latest_available_event_id": entry.latest_available_event_id,
                        "recovery": "reload_durable_state",
                    },
                )
                return

            if entry is HEARTBEAT_SENTINEL:
                if await _orphan_recovery_observed_after_heartbeat(record, run_mgr):
                    yield format_sse("end", None)
                    return
                yield ": heartbeat\n\n"
                continue

            if entry is END_SENTINEL:
                yield format_sse("end", None, event_id=entry.id or None)
                return

            yield format_sse(entry.event, entry.data, event_id=entry.id or None)

    finally:
        # store_only records are cross-worker observation handles. An explicit
        # cancel-then-stream action has already persisted its request before
        # subscribing; a plain join disconnect must not invent a new
        # cancellation request. Only apply on_disconnect to locally-owned runs.
        if not gap_emitted and not record.store_only and record.status in (RunStatus.pending, RunStatus.running):
            if record.on_disconnect == DisconnectMode.cancel:
                await run_mgr.cancel(record.run_id)


async def wait_for_run_completion(
    bridge: StreamBridge,
    record: RunRecord,
    request: Request,
    run_mgr: RunManager,
) -> bool:
    """Block until the run publishes ``END_SENTINEL``, honouring on_disconnect.

    The non-streaming ``/wait`` endpoints used to ``await record.task``
    directly with no disconnect handling.  When the client (or an
    intermediate HTTP proxy) timed out during a long tool call such as
    ``pip install``, the handler would swallow ``CancelledError`` and
    serialize whatever checkpoint happened to exist — masking a half-finished
    run as a normal completion (issue #3265).

    This helper consumes the same bridge that ``sse_consumer`` does so the
    wait path shares its disconnect semantics: each wake-up polls
    ``request.is_disconnected()``; on a real disconnect it cancels the
    background run when ``record.on_disconnect`` is ``cancel``.  The bridge's
    heartbeat sentinels guarantee at least one wake-up per
    ``heartbeat_interval`` even when the agent emits no events for a while.

    Returns:
        ``True`` when ``END_SENTINEL`` was observed (run reached a terminal
        state), ``False`` when the loop exited because the client
        disconnected.  Callers must skip checkpoint serialization on
        ``False`` so a partial checkpoint is not returned as a normal
        response.
    """
    completed = False
    if await _terminal_record_stream_missing(bridge, record):
        return True

    resume_from_event_id: str | None = None
    try:
        while True:
            gap_seen = False
            async for entry in bridge.subscribe(record.run_id, last_event_id=resume_from_event_id):
                # END_SENTINEL means the run reached a terminal state; honour it
                # even if the client just disconnected so the caller still serializes
                # the real final checkpoint.
                if entry is END_SENTINEL:
                    completed = True
                    return True
                if isinstance(entry, StreamGap):
                    # The wait API only needs terminal completion, not a complete
                    # event replay. Resume at the retained tail rather than
                    # treating a bridge gap as a client disconnect.
                    resume_from_event_id = entry.latest_available_event_id
                    gap_seen = True
                    break
                if entry is HEARTBEAT_SENTINEL and await _orphan_recovery_observed_after_heartbeat(record, run_mgr):
                    completed = True
                    return True
                if await request.is_disconnected():
                    return False
                # Heartbeats and regular events: keep waiting for END_SENTINEL.
            if not gap_seen:
                return completed
    finally:
        if not completed and record.status in (RunStatus.pending, RunStatus.running):
            if record.on_disconnect == DisconnectMode.cancel:
                await run_mgr.cancel(record.run_id)
