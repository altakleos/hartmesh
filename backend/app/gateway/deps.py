"""Centralized accessors for singleton objects stored on ``app.state``.

**Getters** (used by routers): raise 503 when a required dependency is
missing, except ``get_store`` which returns ``None``.

``AppConfig`` is intentionally *not* cached on ``app.state``. Routers and the
run path resolve it through :func:`deerflow.config.app_config.get_app_config`,
which performs mtime-based hot reload, so edits to ``config.yaml`` take
effect on the next request without a process restart. The engines created in
:func:`langgraph_runtime` (stream bridge, persistence, checkpointer, store,
run-event store) accept a ``startup_config`` snapshot — they are
restart-required by design and stay bound to that snapshot to keep the live
process consistent with itself.

Initialization is handled directly in ``app.py`` via :class:`AsyncExitStack`.
"""

from __future__ import annotations

import asyncio
import copy
import logging
import os
from collections.abc import AsyncGenerator, Callable
from contextlib import AsyncExitStack, asynccontextmanager
from dataclasses import replace
from typing import TYPE_CHECKING, Any, TypeVar, cast

from fastapi import FastAPI, HTTPException, Request
from langgraph.types import Checkpointer

from app.mcp_tasks.service import McpTaskService
from deerflow.community.browser_automation.session import browser_multi_worker_error
from deerflow.config.app_config import AppConfig, get_app_config
from deerflow.persistence.feedback import FeedbackRepository
from deerflow.persistence.mcp_tasks import McpTaskRepository
from deerflow.runtime import (
    ORPHAN_RECOVERY_STOP_REASON,
    STARTUP_ORPHAN_RECOVERY_ERROR,
    RunContext,
    RunManager,
    StreamBridge,
)
from deerflow.runtime.events.store.base import RunEventStore
from deerflow.runtime.runs.store.base import RecoveryPolicy, RunStore
from deerflow.runtime.tenant_identity import TenantIdentityV1, TenantSubsystem

logger = logging.getLogger(__name__)

# Upper bound (seconds) for draining in-flight runs during shutdown, before the
# AsyncExitStack tears down the checkpointer (and its connection pool). Kept
# local to avoid an app -> deps -> app import cycle. This is a *separate* budget
# from ``app.gateway.app._SHUTDOWN_HOOK_TIMEOUT_SECONDS`` (currently also 5.0s,
# which bounds channel-service stop): the two govern independent teardown steps
# and may diverge, but both count toward the lifespan shutdown window — revisit
# them together if their sum must stay within the server's graceful-shutdown
# timeout.
_RUN_DRAIN_TIMEOUT_SECONDS = 5.0
_EXECUTION_RECOVERY_CLAIMS_ENV = "HARTMESH_EXECUTION_RECOVERY_CLAIMS_ENABLED"


def _execution_recovery_claims_enabled() -> bool:
    """Resolve the reversible process-local claim kill switch."""

    value = os.environ.get(_EXECUTION_RECOVERY_CLAIMS_ENV, "false")
    normalized = value.strip().lower()
    if normalized in {"1", "true", "yes", "on"}:
        return True
    if normalized in {"0", "false", "no", "off"}:
        return False
    raise RuntimeError(
        f"{_EXECUTION_RECOVERY_CLAIMS_ENV} must be a boolean",
    )


def _exact_two_execution_takeover_eligible(record: RunRecord) -> bool:
    """Keep post-dispatch exact-two takeover unavailable.

    Absence of accepted-skill evidence does not prove absence of ordinary AIO
    sandbox use, durable delivery state, or other process-local execution
    state. A future adapter may replace this kill switch only after it binds a
    linearizable per-request owner/epoch gate and reconstructible recovery
    inputs before the first external side effect.
    """

    del record
    return False


def _execution_recovery_manager_options(
    *,
    exact_two_profile: bool,
    takeover_callback: Callable[[RunRecord], Any],
) -> dict[str, Any]:
    """Return startup-frozen manager options for the qualified profile."""

    if not exact_two_profile:
        return {}
    return {
        "on_execution_takeover": takeover_callback,
        "admission_recovery_policy": (RecoveryPolicy.exact_two_takeover_v1),
        "execution_recovery_claims_enabled": (_execution_recovery_claims_enabled()),
        "execution_takeover_eligibility": (_exact_two_execution_takeover_eligible),
    }


async def _launch_execution_recovery_worker(
    app: FastAPI,
    manager: RunManager,
    record: RunRecord,
    decision_gate: Callable[
        [RunRecord, object],
        Any,
    ],
) -> None:
    """Attach one app-scoped worker behind the manager release barrier.

    Parsing the bounded payload is safe before release. All materialization,
    agent-factory resolution, checkpoint/event access, and graph work happens
    inside ``released_worker`` after the manager has verified the attachment
    against the won owner/epoch and set the release event.
    """

    from langgraph.types import Command

    from app.gateway.services import normalize_input, resolve_agent_factory
    from deerflow.runtime import (
        ExecutionRecoveryDisposition,
        ExecutionRecoveryPayloadV1,
        run_agent,
    )

    payload_json = record.recovery_payload_json
    if payload_json is None:
        raise ValueError("recovery_payload_unavailable")
    payload = ExecutionRecoveryPayloadV1.from_persisted(payload_json)
    accepted = record.accepted_invocation
    if accepted is None:
        raise ValueError("recovery_accepted_invocation_unavailable")
    configurable = payload.config.get("configurable")
    if not isinstance(configurable, dict) or configurable.get("thread_id") != record.thread_id:
        raise ValueError("recovery_payload_thread_mismatch")

    async def verified_gate(
        claimed_record: RunRecord,
        assembly_descriptor: object,
    ):
        decision = await decision_gate(
            claimed_record,
            assembly_descriptor,
        )
        terminal = decision.disposition in {
            ExecutionRecoveryDisposition.terminalize_checkpoint_unavailable,
            ExecutionRecoveryDisposition.terminalize_tool_attempt_indeterminate,
        }
        if terminal:
            committed = await manager.terminalize_execution_takeover(
                claimed_record,
                decision,
            )
            if not committed:
                claimed_record.ownership_lost = True
                raise RuntimeError(
                    "execution_recovery_terminal_fence_lost",
                )
            await _project_recovered_threads_error(
                app.state.thread_store,
                [claimed_record],
            )
            return decision
        if not await manager.validate_execution_recovery_decision(
            claimed_record,
            decision,
        ):
            claimed_record.ownership_lost = True
            raise RuntimeError("execution_recovery_decision_fence_lost")
        return decision

    async def released_worker() -> None:
        await record.execution_recovery_release_event.wait()
        config = copy.deepcopy(dict(payload.config))
        if payload.input_kind == "command_resume":
            graph_input = Command(
                resume=copy.deepcopy(payload.input_value),
            )
        else:
            graph_input = normalize_input(
                copy.deepcopy(payload.input_value),
                trusted_internal=accepted.principal.is_internal,
            )
        run_context = replace(
            get_app_run_context(app),
            execution_recovery_gate=verified_gate,
        )
        agent_factory = resolve_agent_factory(record.assistant_id)
        before = payload.interrupt_before
        after = payload.interrupt_after
        await run_agent(
            app.state.stream_bridge,
            manager,
            record,
            ctx=run_context,
            agent_factory=agent_factory,
            graph_input=graph_input,
            config=config,
            stream_modes=list(payload.stream_modes),
            stream_subgraphs=payload.stream_subgraphs,
            interrupt_before=list(before) if isinstance(before, tuple) else before,
            interrupt_after=list(after) if isinstance(after, tuple) else after,
        )

    worker = released_worker()
    try:
        await manager.attach_worker_once(
            record.run_id,
            worker,
            asyncio.create_task,
        )
    except BaseException:
        worker.close()
        raise


def _browser_tools_enabled_in_config(config: AppConfig) -> bool:
    """Return whether process-local agentic browser sessions are configured."""
    get_tool_config = getattr(config, "get_tool_config", None)
    if callable(get_tool_config):
        return get_tool_config("browser_navigate") is not None
    return any(getattr(tool, "name", None) == "browser_navigate" for tool in (getattr(config, "tools", None) or []))


def _enforce_postgres_for_multi_worker(config: AppConfig) -> None:
    """Refuse unsafe multi-process configurations before persistence starts.

    Multi-instance scheduler recovery also needs the durable run ownership
    contract even when each Pod runs a single Gateway worker.

    1. The background scheduler must be disabled for ordinary multi-worker
       mode. ``scheduler.multi_instance`` opts into the lease-aware path.
    2. Process-local browser sessions must be disabled. Browser tools keep
       Chromium and Playwright objects in one worker's memory, while ordinary
       uvicorn dispatch provides no thread-id affinity.
    3. The DB backend must be Postgres — SQLite write-locks cannot support
       concurrent multi-process access.
    4. ``run_events.backend`` must be ``db``. Memory and JSONL stores are
       process-local, so workers cannot enforce a shared singleton receipt.
    5. ``run_ownership.heartbeat_enabled`` must be True — without heartbeat,
       every run has a NULL lease, so reconciliation treats all inflight
       runs as orphans and Worker B would kill Worker A's live runs on
       every rolling update or scale-up.

    This gate runs once at startup before any persistence engine is
    initialised so the error message is clear and the process exits
    immediately.
    """
    try:
        workers = int(os.environ.get("GATEWAY_WORKERS", "1"))
    except (TypeError, ValueError):
        workers = 1

    scheduler = getattr(config, "scheduler", None)
    multi_instance_requested = bool(getattr(scheduler, "multi_instance", False))
    multi_instance_scheduler = bool(getattr(scheduler, "enabled", False) and multi_instance_requested)

    backend = getattr(config.database, "backend", None)
    run_events_backend = getattr(getattr(config, "run_events", None), "backend", None)
    run_ownership = getattr(config, "run_ownership", None)

    if multi_instance_requested and backend != "postgres":
        raise SystemExit(f"scheduler.multi_instance=true requires database.backend='postgres'. database.backend is '{backend}'. Set scheduler.multi_instance=false or configure Postgres.")
    if multi_instance_requested and run_events_backend != "db":
        raise SystemExit(f"scheduler.multi_instance=true requires run_events.backend='db'. run_events.backend is '{run_events_backend}'. Set scheduler.multi_instance=false or configure run_events.backend: db.")
    if multi_instance_requested and (run_ownership is None or not run_ownership.heartbeat_enabled):
        raise SystemExit("scheduler.multi_instance=true requires run_ownership.heartbeat_enabled=true so peer runs retain a valid lease. Set scheduler.multi_instance=false or enable run ownership heartbeats.")

    if workers <= 1:
        return

    if config.scheduler.enabled and not multi_instance_scheduler:
        raise SystemExit(f"GATEWAY_WORKERS={workers} cannot run with scheduler.enabled=true because each worker starts its own scheduler. Set GATEWAY_WORKERS=1, scheduler.multi_instance=true, or scheduler.enabled=false.")

    if _browser_tools_enabled_in_config(config):
        raise SystemExit(browser_multi_worker_error(workers))

    if backend != "postgres":
        raise SystemExit(f"GATEWAY_WORKERS={workers} requires database.backend='postgres', but database.backend is '{backend}'. SQLite cannot support concurrent multi-process access. Set GATEWAY_WORKERS=1 or switch to Postgres.")

    if run_events_backend != "db":
        raise SystemExit(
            f"GATEWAY_WORKERS={workers} requires run_events.backend='db', but run_events.backend is '{run_events_backend}'. "
            "Memory and JSONL event stores are process-local, so delivery receipt singleton guarantees cannot hold across workers. "
            "Set GATEWAY_WORKERS=1 or configure run_events.backend: db."
        )

    if run_ownership is None or not run_ownership.heartbeat_enabled:
        raise SystemExit(
            f"GATEWAY_WORKERS={workers} requires run_ownership.heartbeat_enabled=true. "
            "Without heartbeat, every run has a NULL lease, so reconciliation "
            "treats all inflight runs as orphans — Worker B would kill Worker A's "
            "live runs on every rolling update or scale-up. "
            "Set run_ownership.heartbeat_enabled=true in config.yaml."
        )


def _validate_agent_storage(config: AppConfig) -> None:
    """Fail fast on an agent-storage backend the database cannot support.

    ``agent_storage.backend: db`` needs a durable, shared SQL database — a
    ``memory`` database is per-process, so custom-agent and managed-subagent
    definitions would silently diverge across nodes (and there is no SQL URL
    to open). Mirrors deermem's create_storage fail-fast and the multi-worker
    gate above.

    Also warns when a multi-worker Postgres deployment leaves agent storage on
    ``file``: custom agents created on one node's local disk are invisible to
    the others, exactly the divergence the db backend exists to fix.
    """
    agent_storage = getattr(config, "agent_storage", None)
    backend = getattr(agent_storage, "backend", "file")
    db_backend = getattr(getattr(config, "database", None), "backend", None)
    if backend == "db" and db_backend not in ("sqlite", "postgres"):
        raise SystemExit(
            f"agent_storage.backend='db' requires database.backend to be 'sqlite' or 'postgres', "
            f"but database.backend is '{db_backend}'. A 'memory' database is per-process and cannot "
            "share agent definitions across nodes. Set database.backend, or use agent_storage.backend='file'."
        )
    try:
        workers = int(os.environ.get("GATEWAY_WORKERS", "1"))
    except (TypeError, ValueError):
        workers = 1
    if workers > 1 and db_backend == "postgres" and backend == "file":
        logger.warning(
            "GATEWAY_WORKERS=%s with database.backend='postgres' but agent_storage.backend='file': "
            "custom agents and managed subagents are stored per-node on local disk and are not visible "
            "across workers/nodes. Set agent_storage.backend='db' to share them.",
            workers,
        )


async def _drain_inflight_runs(run_manager: RunManager) -> bool:
    """Drain in-flight runs before the checkpointer is torn down (issue #3373).

    Shields the (internally-bounded) drain so that even if the lifespan
    coroutine is itself cancelled mid-shutdown — a second SIGINT or the server's
    graceful-shutdown timeout, i.e. the same signal storm behind #3373 — the
    checkpointer pool is not closed while run tasks are still writing
    checkpoints. On such a cancellation we let the already-running drain finish
    (it is bounded by ``RunManager.shutdown``'s own timeout) and then propagate
    the cancellation.
    """
    drain = asyncio.create_task(run_manager.shutdown(timeout=_RUN_DRAIN_TIMEOUT_SECONDS))
    try:
        return await asyncio.shield(drain)
    except asyncio.CancelledError:
        # Re-shield so this second wait does not abandon the in-flight drain;
        # it is bounded, so this cannot hang. Then re-raise to honour shutdown.
        try:
            await asyncio.shield(drain)
        except Exception:
            logger.exception("In-flight run drain failed after shutdown cancellation")
        raise
    except Exception:
        logger.exception("Failed to drain in-flight runs during shutdown")
        return False


async def _publish_recovered_run_stream_end(
    bridge: StreamBridge,
    recovered_runs: list[RunRecord],
    *,
    cleanup_delay: float = 60.0,
    on_cleanup_scheduled: Callable[[str, asyncio.Task[None]], None] | None = None,
) -> list[tuple[str, asyncio.Task[None]]]:
    """Terminate retained streams for runs recovered as orphaned."""
    cleanup_tasks: list[tuple[str, asyncio.Task[None]]] = []
    for record in recovered_runs:
        stream_exists = getattr(bridge, "stream_exists", None)
        if stream_exists is not None:
            try:
                if not await stream_exists(record.run_id):
                    logger.debug(
                        "Skipping recovered stream end for %s: stream already expired",
                        record.run_id,
                    )
                    continue
            except Exception:
                logger.debug(
                    "Failed to check recovered stream existence for %s",
                    record.run_id,
                    exc_info=True,
                )
        try:
            await bridge.publish_end(record.run_id)
        except Exception:
            logger.warning(
                "Failed to publish recovered run stream end for %s",
                record.run_id,
                exc_info=True,
            )
            continue
        task = asyncio.create_task(bridge.cleanup(record.run_id, delay=cleanup_delay))
        task.add_done_callback(lambda task, run_id=record.run_id: _log_recovered_stream_cleanup_result(task, run_id))
        cleanup_tasks.append((record.run_id, task))
        if on_cleanup_scheduled is not None:
            on_cleanup_scheduled(record.run_id, task)
    return cleanup_tasks


def _log_recovered_stream_cleanup_result(task: asyncio.Task[None], run_id: str) -> None:
    if task.cancelled():
        return
    try:
        task.result()
    except Exception:
        logger.warning("Failed to clean up recovered run stream for %s", run_id, exc_info=True)


async def _flush_recovered_stream_cleanups(
    bridge: StreamBridge,
    cleanup_tasks: dict[asyncio.Task[None], str],
    *,
    timeout: float = 1.0,
) -> None:
    """Cancel delayed cleanups and delete their streams before bridge shutdown."""
    pending = [(task, run_id) for task, run_id in cleanup_tasks.items() if not task.done()]
    if not pending:
        return
    for task, _run_id in pending:
        task.cancel()
    await asyncio.gather(*(task for task, _run_id in pending), return_exceptions=True)

    run_ids = list(dict.fromkeys(run_id for _task, run_id in pending))
    try:
        results = await asyncio.wait_for(
            asyncio.gather(
                *(bridge.cleanup(run_id, delay=0) for run_id in run_ids),
                return_exceptions=True,
            ),
            timeout=max(0.0, timeout),
        )
    except TimeoutError:
        logger.warning(
            "Immediate recovered stream cleanup exceeded %.1fs for run_ids=%s; bridge TTL remains the final safety net",
            timeout,
            run_ids,
        )
    else:
        for run_id, result in zip(run_ids, results):
            if isinstance(result, BaseException):
                logger.warning(
                    "Failed to immediately clean up recovered run stream for %s: %r",
                    run_id,
                    result,
                )


if TYPE_CHECKING:
    from app.gateway.auth.local_provider import LocalAuthProvider
    from app.gateway.auth.models import User
    from app.gateway.auth.repositories.sqlite import SQLiteUserRepository
    from deerflow.persistence.thread_meta.base import ThreadMetaStore
    from deerflow.runtime import RunRecord


T = TypeVar("T")


async def _project_recovered_threads_error(
    thread_store: ThreadMetaStore,
    recovered_runs: list[RunRecord],
) -> None:
    """Project recovered terminal state through the durable authority seam."""

    from deerflow.persistence.thread_meta import ThreadMetaRunProjection

    for record in recovered_runs:
        owner_worker_id = getattr(
            record,
            "recovery_projection_owner_worker_id",
            None,
        )
        active_state_version = getattr(
            record,
            "recovery_projection_active_state_version",
            None,
        )
        terminal_state_version = getattr(
            record,
            "checkpoint_terminal_state_version",
            None,
        )
        if not isinstance(owner_worker_id, str) or not owner_worker_id or type(active_state_version) is not int or type(terminal_state_version) is not int:
            logger.warning(
                "Skipped recovered thread projection for run %s without an exact terminal authority capability",
                record.run_id,
            )
            continue
        try:
            await thread_store.project_run(
                ThreadMetaRunProjection(
                    run_id=record.run_id,
                    thread_id=record.thread_id,
                    owner_worker_id=owner_worker_id,
                    active_state_version=active_state_version,
                    terminal_state_version=terminal_state_version,
                    status="error",
                ),
                user_id=None,
            )
        except Exception:
            logger.warning(
                "Failed to project recovered run %s into thread %s",
                record.run_id,
                record.thread_id,
                exc_info=True,
            )


# Private compatibility alias for downstream test fixtures. It now uses the
# same authority-bound path as periodic recovery rather than a startup-only
# read/update exception.
async def _mark_latest_startup_recovered_threads_error(
    run_manager: RunManager,
    thread_store: ThreadMetaStore,
    recovered_runs: list[RunRecord],
) -> None:
    del run_manager
    await _project_recovered_threads_error(thread_store, recovered_runs)


async def _terminalize_recovered_runs(
    bridge: StreamBridge,
    recovered_runs: list[RunRecord],
    *,
    cleanup_delay: float,
    on_cleanup_scheduled: Callable[[str, asyncio.Task[None]], None] | None = None,
) -> list[tuple[str, asyncio.Task[None]]]:
    """Publish terminal markers and schedule retained-stream cleanup."""
    return await _publish_recovered_run_stream_end(
        bridge,
        recovered_runs,
        cleanup_delay=cleanup_delay,
        on_cleanup_scheduled=on_cleanup_scheduled,
    )


def get_config() -> AppConfig:
    """Return the freshest ``AppConfig`` for the current request.

    Routes through :func:`deerflow.config.app_config.get_app_config`, which
    honours runtime ``ContextVar`` overrides and reloads ``config.yaml`` from
    disk when its mtime changes. ``AppConfig`` is not cached on ``app.state``
    at all — the only startup-time snapshot lives as a local
    ``startup_config`` variable inside ``lifespan()`` and is passed
    explicitly into :func:`langgraph_runtime` for the engines that are
    restart-required by design. Routing every request through
    :func:`get_app_config` closes the bytedance/deer-flow issue #3107 BUG-001
    split-brain where the worker / lead-agent thread saw a stale startup
    snapshot.

    Hot-reload boundary: fields backed by startup-time singletons
    (engines, sandbox provider, IM channels, logging handler) require a
    process restart to change at runtime. The authoritative list lives in
    :mod:`deerflow.config.reload_boundary` and is mirrored by the
    standardised ``"startup-only:"`` prefix on the matching
    ``Field(description=...)`` in :class:`AppConfig` — IDE hover on those
    fields will surface the boundary inline. See
    ``backend/CLAUDE.md`` "Config Hot-Reload Boundary" for the operator
    summary.

    Any failure to materialise the config (missing file, permission denied,
    YAML parse error, validation error) is reported as 503 — semantically
    "the gateway cannot serve requests without a usable configuration" — and
    logged with the original exception so operators have something to debug.
    """
    try:
        return get_app_config()
    except Exception as exc:  # noqa: BLE001 - request boundary: log and degrade gracefully
        logger.exception("Failed to load AppConfig at request time")
        raise HTTPException(status_code=503, detail="Configuration not available") from exc


@asynccontextmanager
async def langgraph_runtime(app: FastAPI, startup_config: AppConfig) -> AsyncGenerator[None, None]:
    """Bootstrap and tear down all LangGraph runtime singletons.

    ``startup_config`` is the ``AppConfig`` snapshot taken once during
    ``lifespan()`` for one-shot infrastructure bootstrap. The engines and
    stores constructed here (stream bridge, persistence engine, checkpointer,
    store, run-event store) are restart-required by design — they hold live
    connections, file handles, or singleton providers — so they bind to this
    snapshot and survive across `config.yaml` edits. Request-time consumers
    must still go through :func:`get_config` for any field that should be
    hot-reloadable. See ``backend/CLAUDE.md`` "Config Hot-Reload Boundary".

    The matching ``run_events_config`` is frozen onto ``app.state`` so
    :func:`get_run_context` pairs a freshly-loaded ``AppConfig`` with the
    *startup-time* run-events configuration the underlying ``event_store``
    was built from — otherwise the runtime could end up combining a live
    new ``run_events_config`` with an event store still bound to the
    previous backend.

    Usage in ``app.py``::

        async with langgraph_runtime(app, startup_config):
            yield
    """
    from deerflow.persistence.engine import (
        close_engine,
        get_session_factory,
        init_engine_from_config,
    )
    from deerflow.runtime import make_store, make_stream_bridge
    from deerflow.runtime.checkpoint_mode import (
        freeze_checkpoint_channel_mode,
        freeze_checkpoint_snapshot_frequency,
    )
    from deerflow.runtime.checkpointer.async_provider import make_checkpointer
    from deerflow.runtime.events.store import make_run_event_store

    # ------------------------------------------------------------------
    # Multi-worker safety gate: reject SQLite when GATEWAY_WORKERS > 1.
    # SQLite write-locks cannot support concurrent multi-process access.
    # ------------------------------------------------------------------
    _enforce_postgres_for_multi_worker(startup_config)
    # Reject agent_storage.backend='db' on a non-durable database, and warn on
    # node-divergent file storage under multi-worker Postgres.
    _validate_agent_storage(startup_config)

    async with AsyncExitStack() as stack:
        # Lifecycle and system-model hooks can originate on isolated subagent
        # loops. Bind them to the Gateway's serving loop before any runtime
        # dependency starts, then reset the binding last through the exit
        # stack. Registering the callback synchronously here also covers every
        # startup-failure and cancellation path below.
        try:
            from deerflow.extensions.notify import (
                reset_extension_notify_loop,
                set_extension_notify_loop,
            )

            set_extension_notify_loop(asyncio.get_running_loop())
        except Exception:
            logger.exception("Failed to register the extension notify loop; sync observations will be dropped")
        else:

            def reset_notify_loop_safely() -> None:
                try:
                    reset_extension_notify_loop()
                except Exception:
                    logger.debug(
                        "Failed to reset the extension notify loop (non-fatal)",
                        exc_info=True,
                    )

            stack.callback(reset_notify_loop_safely)

        config = startup_config
        tenant_identity = getattr(app.state, "tenant_identity", None)
        if not isinstance(tenant_identity, TenantIdentityV1):
            raise RuntimeError("Gateway tenant identity was not resolved during application construction")
        tenant_reference = tenant_identity.to_persisted_reference()
        app.state.checkpoint_channel_mode = freeze_checkpoint_channel_mode(config.database.checkpoint_channel_mode)
        app.state.checkpoint_snapshot_frequency = freeze_checkpoint_snapshot_frequency(config.database.checkpoint_delta.snapshot_frequency)
        redis_tenant_namespace = tenant_identity.namespace(TenantSubsystem.REDIS)
        app.state.redis_tenant_namespace = redis_tenant_namespace

        # Bind durable schema identity before constructing Redis consumers.
        # A legacy prefix is authoritative only when the explicit migration
        # command stored it alongside this schema's tenant binding.
        stack.push_async_callback(close_engine)
        from deerflow.deployment.topology import (
            DeploymentProfile,
            coerce_deployment_profile,
        )

        deployment_profile = coerce_deployment_profile(
            getattr(getattr(config, "deployment", None), "profile", None),
        )
        is_multi_gateway_profile = deployment_profile is DeploymentProfile.durable_two_gateway_v1
        if is_multi_gateway_profile:
            await init_engine_from_config(config.database, migration_mode="verify")
        else:
            await init_engine_from_config(config.database)
        sf = get_session_factory()
        from deerflow.runtime.tenant_identity import LegacyRedisPrefixRecordV1

        legacy_redis_prefixes = LegacyRedisPrefixRecordV1()
        if sf is not None:
            from deerflow.persistence.credential_audit import (
                CredentialAuditRepository,
            )
            from deerflow.persistence.tenant_binding import (
                ensure_schema_tenant_binding,
            )

            app.state.tenant_schema_binding = await ensure_schema_tenant_binding(
                sf,
                tenant_identity,
            )
            legacy_redis_prefixes = app.state.tenant_schema_binding.legacy_redis_prefixes

        if is_multi_gateway_profile:
            if sf is None:
                raise RuntimeError("topology_dependency_not_shared")
            from datetime import UTC, datetime

            from app.mcp_tasks.replay_commitment import (
                McpTaskReplayKeyringConfirmation,
            )
            from deerflow.deployment.topology import (
                TOPOLOGY_HEARTBEAT_INTERVAL_SECONDS,
                TOPOLOGY_LIVE_TTL_SECONDS,
                ReplicaRegistrationV1,
                TopologyHeartbeatSupervisor,
                TopologyStartupFactsV1,
                build_topology_fingerprint,
            )
            from deerflow.persistence.topology import PostgresTopologyRegistry

            topology_facts = TopologyStartupFactsV1.from_environment()
            replay_keyring_confirmation = getattr(
                app.state,
                "mcp_task_replay_keyring_confirmation",
                None,
            )
            if not isinstance(
                replay_keyring_confirmation,
                McpTaskReplayKeyringConfirmation,
            ):
                raise RuntimeError("topology_dependency_not_shared")
            topology_fingerprint = build_topology_fingerprint(
                facts=topology_facts,
                tenant_digest=tenant_identity.digest,
                redis_namespace_digest=redis_tenant_namespace.digest,
                capability_manifest=app.state.capability_manifest,
                config=config,
                mcp_task_replay_keyring_confirmation_version=(replay_keyring_confirmation.version),
                mcp_task_replay_keyring_confirmation_digest=(replay_keyring_confirmation.digest),
            )
            topology_registration = ReplicaRegistrationV1(
                replica_id=topology_facts.replica_id,
                topology_fingerprint=topology_fingerprint,
                started_at=datetime.now(UTC),
                heartbeat_at=datetime.now(UTC),
            )
            topology_supervisor = TopologyHeartbeatSupervisor(
                registry=PostgresTopologyRegistry(
                    sf,
                    live_ttl_seconds=TOPOLOGY_LIVE_TTL_SECONDS,
                ),
                registration=topology_registration,
                heartbeat_interval_seconds=TOPOLOGY_HEARTBEAT_INTERVAL_SECONDS,
            )
            app.state.topology_supervisor = topology_supervisor
            app.state.topology_registration = topology_registration
            stack.push_async_callback(topology_supervisor.close)
            await topology_supervisor.start()
        else:
            app.state.topology_supervisor = None
            app.state.topology_registration = None

        # Sandbox providers are lazy process singletons, so bind their Redis
        # ownership factory before the first request can construct one. This is
        # the same immutable namespace object used by eager Gateway factories.
        from deerflow.community.aio_sandbox.ownership.factory import (
            bind_ownership_tenant_namespace,
            resolve_ownership_config,
            resolve_ownership_key_prefix,
            unbind_ownership_tenant_namespace,
        )
        from deerflow.runtime.checkpoint_cache.provider import (
            checkpoint_cache_key_prefix,
        )

        bind_ownership_tenant_namespace(
            redis_tenant_namespace,
            legacy_redis_prefixes=legacy_redis_prefixes,
        )
        stack.callback(
            unbind_ownership_tenant_namespace,
            redis_tenant_namespace,
        )
        ownership_config = resolve_ownership_config(
            getattr(getattr(config, "sandbox", None), "ownership", None),
            stream_bridge=getattr(config, "stream_bridge", None),
        )
        topology_redis_key_prefixes: list[str] = []
        if ownership_config.type == "redis":
            ownership_key_prefix = resolve_ownership_key_prefix(
                ownership_config,
                tenant_namespace=redis_tenant_namespace,
                legacy_redis_prefixes=legacy_redis_prefixes,
            )
            topology_redis_key_prefixes.append(ownership_key_prefix)
        if getattr(config.database, "checkpoint_cache", None) is not None:
            checkpoint_key_prefix = checkpoint_cache_key_prefix(
                config,
                redis_tenant_namespace,
                legacy_redis_prefixes,
            )
            if getattr(config.database.checkpoint_cache, "type", None) == "redis":
                topology_redis_key_prefixes.append(checkpoint_key_prefix)
        app.state.topology_redis_key_prefixes = tuple(topology_redis_key_prefixes)
        app.state.stream_bridge = await stack.enter_async_context(
            make_stream_bridge(
                config,
                tenant_namespace=redis_tenant_namespace,
                legacy_redis_prefixes=legacy_redis_prefixes,
            )
        )

        app.state.checkpointer = await stack.enter_async_context(
            make_checkpointer(
                config,
                tenant_namespace=redis_tenant_namespace,
                legacy_redis_prefixes=legacy_redis_prefixes,
            )
        )
        app.state.store = await stack.enter_async_context(make_store(config))

        # Initialize repositories — one get_session_factory() call for all.
        if sf is not None:
            from deerflow.persistence.feedback import FeedbackRepository
            from deerflow.persistence.personal_access_tokens import PersonalAccessTokenRepository
            from deerflow.persistence.run import RunRepository

            app.state.run_store = RunRepository(sf, tenant=tenant_reference)
            app.state.feedback_repo = FeedbackRepository(sf)
            from app.gateway.auth.pat import PAT_LAST_USED_WRITE_INTERVAL_SECONDS

            app.state.credential_audit_repo = CredentialAuditRepository(
                sf,
                tenant=tenant_reference,
            )
            app.state.pat_repo = PersonalAccessTokenRepository(
                sf,
                tenant=tenant_reference,
                audit_repository=app.state.credential_audit_repo,
                last_used_write_interval_seconds=PAT_LAST_USED_WRITE_INTERVAL_SECONDS,
            )
        else:
            from deerflow.persistence.credential_audit import (
                InMemoryCredentialAuditRepository,
            )
            from deerflow.runtime.runs.store.memory import MemoryRunStore

            app.state.run_store = MemoryRunStore(tenant=tenant_reference)
            app.state.feedback_repo = None
            app.state.credential_audit_repo = InMemoryCredentialAuditRepository(tenant=tenant_reference)
            # Memory backend has no durable PAT store, so Bearer credentials
            # cannot be validated there and are rejected by the middleware.
            app.state.pat_repo = None

        await app.state.run_store.initialize_lifecycle()
        if is_multi_gateway_profile:
            from deerflow.deployment.topology import (
                validate_multi_gateway_run_store,
            )

            validate_multi_gateway_run_store(app.state.run_store)
        deployment_reporter = getattr(app.state, "deployment_reporter", None)
        if deployment_reporter is not None:
            app.state.deployment_reporter = deployment_reporter.with_runtime_store(
                profile=config.deployment.profile,
                database_backend=config.database.backend,
                atomic_lifecycle=bool(getattr(app.state.run_store, "durable_lifecycle", False)),
            )

        # Services are app-scoped. Capture this app's immutable extension set
        # once and close over the same object for teardown; the process-wide
        # singleton may be replaced by another app/test before shutdown.
        from deerflow.extensions import EMPTY_EXTENSIONS, record_runtime_diagnostics
        from deerflow.extensions.gateway import start_services, stop_services

        extensions = getattr(app.state, "extensions", EMPTY_EXTENSIONS)
        attempted_services: list[tuple[str, Any]] = []

        async def stop_extension_services() -> None:
            record_runtime_diagnostics(
                await stop_services(
                    extensions,
                    service_entries=attempted_services,
                )
            )

        # Register cleanup before starting: start() can partially acquire
        # resources and then fail or be cancelled.
        stack.push_async_callback(stop_extension_services)
        record_runtime_diagnostics(
            await start_services(
                extensions,
                config,
                sf,
                attempted_services=attempted_services,
            )
        )

        from deerflow.persistence.thread_meta import make_thread_store

        app.state.thread_store = make_thread_store(
            sf,
            app.state.store,
            run_store=app.state.run_store,
        )
        if sf is not None:
            from deerflow.persistence.mcp_tasks import McpTaskRepository
            from deerflow.persistence.scheduled_task_runs import (
                ScheduledTaskRunRepository,
            )
            from deerflow.persistence.scheduled_tasks import ScheduledTaskRepository
            from deerflow.persistence.subagent_batches import SubagentBatchRepository

            app.state.scheduled_task_repo = ScheduledTaskRepository(
                sf,
                run_repository=app.state.run_store,
            )
            app.state.scheduled_task_run_repo = ScheduledTaskRunRepository(
                sf,
                run_repository=app.state.run_store,
            )
            app.state.mcp_task_repo = McpTaskRepository(
                sf,
                tenant=tenant_reference,
            )
            await app.state.mcp_task_repo.verify_schema_writer_compatibility()
            app.state.subagent_batch_repo = SubagentBatchRepository(
                sf,
                tenant=tenant_reference,
            )
            await app.state.subagent_batch_repo.verify_schema_writer_compatibility()
        else:
            app.state.mcp_task_repo = None
            app.state.subagent_batch_repo = None
            app.state.scheduled_task_repo = None
            app.state.scheduled_task_run_repo = None

        # Run event store. The store and the matching ``run_events_config`` are
        # both frozen at startup so ``get_run_context`` does not combine a
        # freshly-reloaded ``AppConfig.run_events`` with a store still bound to
        # the previous backend.
        run_events_config = getattr(config, "run_events", None)
        app.state.run_events_config = run_events_config
        app.state.run_event_store = make_run_event_store(
            run_events_config,
            run_store=app.state.run_store,
            tenant=tenant_reference,
        )

        # RunManager with store backing for persistence
        run_ownership_config = getattr(config, "run_ownership", None)
        sb_config = getattr(config, "stream_bridge", None)
        cleanup_delay = getattr(sb_config, "recovered_stream_cleanup_delay_seconds", 60.0) if sb_config else 60.0
        recovered_stream_cleanup_tasks: dict[asyncio.Task[None], str] = {}

        def track_recovered_stream_cleanup(
            run_id: str,
            task: asyncio.Task[None],
        ) -> None:
            recovered_stream_cleanup_tasks[task] = run_id
            task.add_done_callback(lambda completed: recovered_stream_cleanup_tasks.pop(completed, None))

        async def terminalize_recovered_runs(recovered_runs: list[RunRecord]) -> None:
            await _terminalize_recovered_runs(
                app.state.stream_bridge,
                recovered_runs,
                cleanup_delay=cleanup_delay,
                on_cleanup_scheduled=track_recovered_stream_cleanup,
            )
            await _project_recovered_threads_error(
                app.state.thread_store,
                recovered_runs,
            )

        manager_kwargs: dict[str, Any] = {
            "store": app.state.run_store,
            "run_ownership_config": run_ownership_config,
            "event_store": app.state.run_event_store,
            "on_orphans_recovered": terminalize_recovered_runs,
            "tenant": tenant_reference,
        }
        recovery_coordinator = None
        if is_multi_gateway_profile:
            from app.gateway.run_recovery import (
                GatewayExecutionRecoveryCoordinator,
            )

            async def execution_takeover(record: RunRecord):
                if recovery_coordinator is None:
                    raise RuntimeError(
                        "execution_recovery_coordinator_unavailable",
                    )
                return await recovery_coordinator.recover(record)

            manager_kwargs.update(
                _execution_recovery_manager_options(
                    exact_two_profile=True,
                    takeover_callback=execution_takeover,
                )
            )
        app.state.run_manager = RunManager(**manager_kwargs)
        if is_multi_gateway_profile:
            recovery_coordinator = GatewayExecutionRecoveryCoordinator(
                run_store=app.state.run_store,
                event_store=app.state.run_event_store,
                checkpointer=app.state.checkpointer,
                worker_launcher=lambda record, decision_gate: _launch_execution_recovery_worker(
                    app,
                    app.state.run_manager,
                    record,
                    decision_gate,
                ),
            )
        app.state.execution_recovery_coordinator = recovery_coordinator

        # Claimed execution epochs need renewal while startup scans and
        # reconstructs later rows. Register teardown before starting so a
        # partial startup failure cannot strand the renewal task.
        stack.push_async_callback(app.state.run_manager.stop_heartbeat)
        await app.state.run_manager.start_heartbeat()

        # Startup recovery: mark inflight runs whose lease has expired as error.
        # In single-worker mode (SQLite / backend=memory), no run has a lease, so
        # all inflight rows are reclaimed (unchanged behaviour). In multi-worker
        # mode (Postgres), only runs with an expired lease are reclaimed; runs
        # owned by another live worker are skipped.
        from deerflow.utils.time import now_iso

        recovered_runs = await app.state.run_manager.reconcile_orphaned_inflight_runs(
            error=STARTUP_ORPHAN_RECOVERY_ERROR,
            # Exact-two SQL recovery derives both eligibility and its default
            # created-at bound from the database clock. A pod-authored bound
            # could hide stale rows when that pod's wall clock lags.
            before=(None if is_multi_gateway_profile else now_iso()),
            stop_reason=ORPHAN_RECOVERY_STOP_REASON,
        )
        await _terminalize_recovered_runs(
            app.state.stream_bridge,
            recovered_runs,
            cleanup_delay=cleanup_delay,
            on_cleanup_scheduled=track_recovered_stream_cleanup,
        )
        await _project_recovered_threads_error(
            app.state.thread_store,
            recovered_runs,
        )

        # Transfer ownership out of the surrounding context manager before the
        # application shutdown coordinator runs.  If admission or a producer
        # cannot quiesce by the absolute deadline, closing these callbacks would
        # race their database/checkpointer use.  In that unsafe case the
        # coordinator deliberately leaves them for process reclamation instead
        # of allowing ``AsyncExitStack.__aexit__`` to close them implicitly.
        runtime_resource_stack = stack.pop_all()
        runtime_close_task: asyncio.Task[None] | None = None

        async def close_runtime_dependencies() -> None:
            """Close stream/checkpoint/store/database resources once."""

            nonlocal runtime_close_task
            if runtime_close_task is None:

                async def close_owned_resources() -> None:
                    try:
                        await _flush_recovered_stream_cleanups(
                            app.state.stream_bridge,
                            recovered_stream_cleanup_tasks,
                            timeout=1.0,
                        )
                    finally:
                        await runtime_resource_stack.aclose()

                runtime_close_task = asyncio.create_task(
                    close_owned_resources(),
                    name="gateway-runtime-dependencies-close",
                )
            await runtime_close_task

        app.state.close_runtime_dependencies = close_runtime_dependencies

        try:
            yield
        finally:
            # Drain in-flight run tasks BEFORE the AsyncExitStack tears down the
            # checkpointer (and its connection pool). A run still mid-graph would
            # otherwise leak into asyncio.run() shutdown, where langgraph's
            # _checkpointer_put_after_previous aput races the closed pool and
            # raises PoolClosed (issue #3373).
            run_manager = getattr(app.state, "run_manager", None)
            if run_manager is not None:
                coordinator = getattr(app.state, "shutdown_coordinator", None)
                if coordinator is not None:
                    # Production lifespan owns the complete ordered sequence.
                    await coordinator.shutdown()
                else:
                    # Direct context-manager users (mostly focused tests) do
                    # not construct the application coordinator.
                    if await _drain_inflight_runs(run_manager):
                        await close_runtime_dependencies()
                    else:
                        logger.warning("Runtime dependencies retained because run drain was not proven")


def build_multi_gateway_topology_service_registry():
    """Register every service surface composed by the exact Gateway profile."""

    from deerflow.deployment.topology import TopologyServiceRegistry

    registry = TopologyServiceRegistry()
    registry.register(
        "accepted_materializer",
        construction_ref="deerflow.sandbox.accepted_material",
    )
    registry.register(
        "agent_store",
        construction_ref="deerflow.persistence.agents:make_agent_store",
    )
    registry.register(
        "capability_health_monitor",
        construction_ref="app.gateway.app:create_app",
    )
    registry.register(
        "channel_service",
        construction_ref="app.channels.service:start_channel_service",
    )
    registry.register(
        "checkpoint_cache",
        construction_ref="deerflow.runtime.checkpoint_cache.provider",
    )
    registry.register(
        "checkpointer",
        construction_ref="deerflow.runtime.checkpointer.async_provider:make_checkpointer",
    )
    registry.register(
        "configuration_snapshot",
        construction_ref="app.gateway.app:lifespan",
    )
    registry.register(
        "credential_audit_repo",
        construction_ref=("deerflow.persistence.credential_audit:CredentialAuditRepository"),
    )
    registry.register(
        "extension_services",
        construction_ref="deerflow.extensions.gateway:start_services",
    )
    registry.register(
        "feedback_repo",
        construction_ref="deerflow.persistence.feedback:FeedbackRepository",
    )
    registry.register(
        "inbound_dedupe_store",
        construction_ref="app.channels.dedupe_store:PostgresInboundDedupeStore",
    )
    registry.register(
        "langgraph_store",
        construction_ref="deerflow.runtime:make_store",
    )
    registry.register(
        "llm_call_limiter",
        construction_ref="deerflow.agents.middlewares.llm_error_handling_middleware:_ProcessWideLimiter",
    )
    registry.register(
        "mcp_task_repo",
        construction_ref="deerflow.persistence.mcp_tasks:McpTaskRepository",
    )
    registry.register(
        "mcp_task_service",
        construction_ref="app.mcp_tasks.service:McpTaskService",
    )
    registry.register(
        "personal_access_token_repo",
        construction_ref="deerflow.persistence.personal_access_tokens:PersonalAccessTokenRepository",
    )
    registry.register(
        "memory_manager",
        construction_ref="deerflow.agents.memory:get_memory_manager",
    )
    registry.register(
        "migration_verifier",
        construction_ref="deerflow.persistence.engine:init_engine_from_config",
    )
    registry.register(
        "orphan_reconciler",
        construction_ref="deerflow.runtime.runs.manager:RunManager",
    )
    registry.register(
        "persistence_engine",
        construction_ref="deerflow.persistence.engine:init_engine_from_config",
    )
    registry.register(
        "provisioner",
        construction_ref="deerflow.community.aio_sandbox:AioSandboxProvider",
    )
    registry.register(
        "run_event_store",
        construction_ref="deerflow.runtime.events.store:make_run_event_store",
    )
    registry.register(
        "run_manager",
        construction_ref="deerflow.runtime.runs.manager:RunManager",
    )
    registry.register(
        "run_store",
        construction_ref="deerflow.persistence.run:RunRepository",
    )
    registry.register(
        "runtime_readiness",
        construction_ref="app.runtime.readiness:RuntimeReadinessCoordinator",
    )
    registry.register(
        "sandbox_provider",
        construction_ref="deerflow.sandbox.sandbox_provider",
    )
    registry.register(
        "sandbox_reconciler",
        construction_ref="deerflow.community.aio_sandbox",
    )
    registry.register(
        "scheduled_task_repo",
        construction_ref="deerflow.persistence.scheduled_tasks:ScheduledTaskRepository",
    )
    registry.register(
        "scheduled_task_run_repo",
        construction_ref="deerflow.persistence.scheduled_task_runs:ScheduledTaskRunRepository",
    )
    registry.register(
        "scheduled_task_service",
        construction_ref="app.scheduler.service:ScheduledTaskService",
    )
    registry.register(
        "stream_bridge",
        construction_ref="deerflow.runtime:make_stream_bridge",
    )
    registry.register(
        "stream_cleanup",
        construction_ref="deerflow.runtime.stream_bridge",
    )
    registry.register(
        "subagent_batch_repo",
        construction_ref="deerflow.persistence.subagent_batches:SubagentBatchRepository",
    )
    registry.register(
        "subagent_batch_service",
        construction_ref="app.subagent_batches:SubagentBatchService",
    )
    registry.register(
        "thread_store",
        construction_ref="deerflow.persistence.thread_meta:make_thread_store",
    )
    registry.register(
        "topology_registry",
        construction_ref="deerflow.persistence.topology:PostgresTopologyRegistry",
    )
    registry.register(
        "user_store",
        construction_ref="app.gateway.auth.repositories",
    )
    registry.register(
        "webhook_ingress",
        construction_ref="app.gateway.github.webhook_auth",
    )
    return registry


# ---------------------------------------------------------------------------
# Getters – called by routers per-request
# ---------------------------------------------------------------------------


def _require(attr: str, label: str) -> Callable[[Request], T]:
    """Create a FastAPI dependency that returns ``app.state.<attr>`` or 503."""

    def dep(request: Request) -> T:
        val = getattr(request.app.state, attr, None)
        if val is None:
            raise HTTPException(status_code=503, detail=f"{label} not available")
        return cast(T, val)

    dep.__name__ = dep.__qualname__ = f"get_{attr}"
    return dep


get_stream_bridge: Callable[[Request], StreamBridge] = _require("stream_bridge", "Stream bridge")
get_run_manager: Callable[[Request], RunManager] = _require("run_manager", "Run manager")
get_checkpointer: Callable[[Request], Checkpointer] = _require("checkpointer", "Checkpointer")
get_run_event_store: Callable[[Request], RunEventStore] = _require("run_event_store", "Run event store")
get_feedback_repo: Callable[[Request], FeedbackRepository] = _require("feedback_repo", "Feedback")
get_run_store: Callable[[Request], RunStore] = _require("run_store", "Run store")


def get_store(request: Request):
    """Return the global store (may be ``None`` if not configured)."""
    return getattr(request.app.state, "store", None)


def get_thread_store(request: Request) -> ThreadMetaStore:
    """Return the thread metadata store (SQL or memory-backed)."""
    val = getattr(request.app.state, "thread_store", None)
    if val is None:
        raise HTTPException(status_code=503, detail="Thread metadata store not available")
    return val


def get_scheduled_task_repo(request: Request):
    val = getattr(request.app.state, "scheduled_task_repo", None)
    if val is None:
        raise HTTPException(status_code=503, detail="Scheduled task repo not available")
    return val


def get_scheduled_task_run_repo(request: Request):
    val = getattr(request.app.state, "scheduled_task_run_repo", None)
    if val is None:
        raise HTTPException(status_code=503, detail="Scheduled task run repo not available")
    return val


def get_scheduled_task_service(request: Request):
    val = getattr(request.app.state, "scheduled_task_service", None)
    if val is None:
        raise HTTPException(status_code=503, detail="Scheduled task service not available")
    return val


def get_mcp_task_repo(request: Request) -> McpTaskRepository:
    """Return the configured MCP task repository or fail as unavailable."""

    val = getattr(request.app.state, "mcp_task_repo", None)
    if val is None:
        raise HTTPException(status_code=503, detail="MCP task repo not available")
    return val


def get_mcp_task_service(request: Request) -> McpTaskService:
    """Return the configured MCP task service or fail as unavailable."""

    val = getattr(request.app.state, "mcp_task_service", None)
    if val is None:
        raise HTTPException(status_code=503, detail="MCP task service not available")
    return val


def get_subagent_batch_repo(request: Request):
    val = getattr(request.app.state, "subagent_batch_repo", None)
    if val is None:
        raise HTTPException(status_code=503, detail="Subagent batch repository not available")
    return val


def get_subagent_batch_service(request: Request):
    val = getattr(request.app.state, "subagent_batch_service", None)
    if val is None:
        raise HTTPException(status_code=503, detail="Subagent batch service not available")
    return val


def get_app_run_context(app: FastAPI) -> RunContext:
    """Build a request-independent :class:`RunContext` from app singletons.

    Returns a *base* context with infrastructure dependencies. The
    ``app_config`` field is resolved live so per-run fields (e.g.
    ``models[*].max_tokens``) follow ``config.yaml`` edits; the
    ``event_store`` / ``run_events_config`` pair stays frozen to the snapshot
    captured in :func:`langgraph_runtime` so callers never see a store bound
    to one backend paired with a config pointing at another.
    """
    app_config = get_config()
    authorization_config = getattr(app_config, "authorization", None)
    resolver = getattr(app.state, "authorization_provider_resolver", None)
    if resolver is None:
        if getattr(authorization_config, "enabled", False) is True:
            raise HTTPException(status_code=503, detail="Authorization provider resolver not available")
        authorization_provider = None
    elif authorization_config is None:
        authorization_provider = None
    else:
        authorization_provider = resolver.resolve(authorization_config).provider

    def _resolve_recovered_agent_revision(record, config):
        """Rebuild lead material while retaining accepted subagent facts."""

        from deerflow.runtime.agent_revision import resolve_agent_revision

        accepted = getattr(record, "accepted_invocation", None)
        accepted_revision = getattr(accepted, "agent_revision", None)
        return resolve_agent_revision(
            config,
            app_config=app_config,
            user_id=getattr(record, "user_id", None),
            accepted_subagent_catalog=getattr(
                accepted_revision,
                "subagent_catalog",
                None,
            ),
            accepted_skill_scopes=getattr(
                accepted_revision,
                "skill_scopes",
                None,
            ),
        )

    tenant_identity = getattr(app.state, "tenant_identity", None)
    if not isinstance(tenant_identity, TenantIdentityV1):
        raise RuntimeError("Gateway tenant identity was not resolved during application construction")

    return RunContext(
        checkpointer=getattr(app.state, "checkpointer", None),
        store=getattr(app.state, "store", None),
        event_store=getattr(app.state, "run_event_store", None),
        run_events_config=getattr(app.state, "run_events_config", None),
        checkpoint_channel_mode=getattr(app.state, "checkpoint_channel_mode", "full"),
        checkpoint_snapshot_frequency=getattr(app.state, "checkpoint_snapshot_frequency", None),
        thread_store=getattr(app.state, "thread_store", None),
        mcp_task_repo=getattr(app.state, "mcp_task_repo", None),
        app_config=app_config,
        authorization_provider=authorization_provider,
        tenant=tenant_identity.to_persisted_reference(),
        extensions=getattr(app.state, "extensions", None),
        capability_manifest_digest=getattr(
            getattr(app.state, "capability_manifest", None),
            "digest",
            None,
        ),
        on_run_completed=getattr(app.state, "scheduled_task_service", None).handle_run_completion if getattr(app.state, "scheduled_task_service", None) is not None else None,
        constraint_clock=getattr(
            getattr(app.state, "invocation_constraints_host", None),
            "clock",
            None,
        ),
        agent_revision_resolver=_resolve_recovered_agent_revision,
    )


def get_run_context(request: Request) -> RunContext:
    """Build the same app-scoped context for an ordinary HTTP request."""

    return get_app_run_context(request.app)


# ---------------------------------------------------------------------------
# Auth helpers (used by authz.py and auth middleware)
# ---------------------------------------------------------------------------

# Cached singletons to avoid repeated instantiation per request
_cached_local_provider: LocalAuthProvider | None = None
_cached_repo: SQLiteUserRepository | None = None


def get_local_provider() -> LocalAuthProvider:
    """Get or create the cached LocalAuthProvider singleton.

    Must be called after ``init_engine_from_config()`` — the shared
    session factory is required to construct the user repository.
    """
    global _cached_local_provider, _cached_repo
    if _cached_repo is None:
        from app.gateway.auth.repositories.sqlite import SQLiteUserRepository
        from deerflow.persistence.engine import get_session_factory

        sf = get_session_factory()
        if sf is None:
            raise RuntimeError("get_local_provider() called before init_engine_from_config(); cannot access users table")
        _cached_repo = SQLiteUserRepository(sf)
    if _cached_local_provider is None:
        from app.gateway.auth.local_provider import LocalAuthProvider

        _cached_local_provider = LocalAuthProvider(repository=_cached_repo)
    return _cached_local_provider


def get_pat_repo(request: Request):
    """Return the personal-access-token repository from app state.

    Raises 503 when the process runs on the memory backend (no durable PAT
    storage), so PAT management routes fail explicitly instead of silently
    accepting tokens nobody can validate.
    """
    pat_repo = getattr(request.app.state, "pat_repo", None)
    if pat_repo is None:
        raise HTTPException(status_code=503, detail="Personal access tokens require a configured database")
    return pat_repo


async def get_current_user_from_request(request: Request):
    """Get the current authenticated user from the request cookie.

    Raises HTTPException 401 if not authenticated.
    """
    state = getattr(request, "state", None)
    state_user = getattr(state, "user", None)
    from app.gateway.auth_disabled import (
        AUTH_SOURCE_AUTH_DISABLED,
        AUTH_SOURCE_INTERNAL,
        AUTH_SOURCE_PAT,
        AUTH_SOURCE_SESSION,
    )

    if state_user is not None and getattr(state, "auth_source", None) in {
        AUTH_SOURCE_SESSION,
        AUTH_SOURCE_AUTH_DISABLED,
        AUTH_SOURCE_INTERNAL,
        AUTH_SOURCE_PAT,
    }:
        return state_user

    from app.gateway.auth import decode_token
    from app.gateway.auth.errors import (
        AuthErrorCode,
        AuthErrorResponse,
        TokenError,
        token_error_to_code,
    )

    access_token = request.cookies.get("access_token")
    if not access_token:
        raise HTTPException(
            status_code=401,
            detail=AuthErrorResponse(code=AuthErrorCode.NOT_AUTHENTICATED, message="Not authenticated").model_dump(),
        )

    payload = decode_token(access_token)
    if isinstance(payload, TokenError):
        raise HTTPException(
            status_code=401,
            detail=AuthErrorResponse(
                code=token_error_to_code(payload),
                message=f"Token error: {payload.value}",
            ).model_dump(),
        )

    provider = get_local_provider()
    user = await provider.get_user(payload.sub)
    if user is None:
        raise HTTPException(
            status_code=401,
            detail=AuthErrorResponse(code=AuthErrorCode.USER_NOT_FOUND, message="User not found").model_dump(),
        )

    # Token version mismatch → password was changed, token is stale
    if user.token_version != payload.ver:
        raise HTTPException(
            status_code=401,
            detail=AuthErrorResponse(
                code=AuthErrorCode.TOKEN_INVALID,
                message="Token revoked (password changed)",
            ).model_dump(),
        )

    return user


async def is_admin_user(request: Request) -> bool:
    """Return whether the authenticated caller is an admin user.

    ``AuthMiddleware`` normally stamps ``request.state.user`` before the request
    reaches a router. Falling back to the strict dependency keeps the route safe
    in tests or alternative ASGI compositions that mount a router without the
    global middleware.

    Centralising this here means a future change to the admin definition (e.g.
    allowing an internal system role, adding audit logging, or switching to a
    permission-based check) lands in one place instead of drifting across the
    per-router copies that previously existed in ``mcp``, ``channel_connections``
    and ``channels``.
    """
    # PAT credentials never carry admin capability: no scope in the PAT
    # universe grants it, so an admin's automation token must not unlock
    # admin-only routes (skill installs, integration credentials, MCP config).
    from app.gateway.auth_disabled import AUTH_SOURCE_PAT

    if getattr(request.state, "auth_source", None) == AUTH_SOURCE_PAT:
        return False
    user = getattr(request.state, "user", None)
    if user is None:
        user = await get_current_user_from_request(request)

    return getattr(user, "system_role", None) == "admin"


async def require_admin_user(request: Request, *, detail: str) -> User:
    """Require the authenticated caller to be an admin user.

    ``detail`` is the route-specific 403 message. The shared predicate keeps
    read-side redaction and write authorization on the same admin definition.
    """

    user = getattr(request.state, "user", None)
    if user is None:
        user = await get_current_user_from_request(request)
    if getattr(user, "system_role", None) != "admin":
        raise HTTPException(status_code=403, detail=detail)
    return user


async def get_optional_user_from_request(request: Request):
    """Get optional authenticated user from request.

    Returns None if not authenticated.
    """
    try:
        return await get_current_user_from_request(request)
    except HTTPException:
        return None


async def get_current_user(request: Request) -> str | None:
    """Extract user_id from request cookie, or None if not authenticated.

    Thin adapter that returns the string id for callers that only need
    identification (e.g., ``feedback.py``). Full-user callers should use
    ``get_current_user_from_request`` or ``get_optional_user_from_request``.
    """
    user = await get_optional_user_from_request(request)
    return str(user.id) if user else None
