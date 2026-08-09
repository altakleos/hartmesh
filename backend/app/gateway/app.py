import asyncio
import logging
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from app.gateway.auth_disabled import warn_if_auth_disabled_enabled
from app.gateway.auth_middleware import AuthMiddleware
from app.gateway.browser_capability import ensure_browser_runtime_available
from app.gateway.config import get_gateway_config
from app.gateway.csrf_middleware import CORS_EXPOSED_HEADERS, CSRFMiddleware, get_configured_cors_origins
from app.gateway.deps import langgraph_runtime
from app.gateway.routers import (
    agents,
    artifacts,
    assistants_compat,
    auth,
    browser,
    channel_connections,
    channels,
    console,
    features,
    feedback,
    github_webhooks,
    input_polish,
    integrations,
    mcp,
    memory,
    models,
    runs,
    runtime_api,
    scheduled_tasks,
    skills,
    suggestions,
    thread_runs,
    threads,
    uploads,
)
from app.gateway.runtime_http import install_runtime_error_handlers
from app.gateway.trace_middleware import TraceMiddleware, resolve_trace_enabled
from deerflow.config import app_config as deerflow_app_config
from deerflow.logging_config import DEFAULT_LOG_DATE_FORMAT, DEFAULT_LOG_FORMAT, configure_logging
from deerflow.tracing.monocle import setup_monocle_tracing_if_enabled
from deerflow.uploads.manager import cleanup_stale_upload_staging_files

AppConfig = deerflow_app_config.AppConfig
get_app_config = deerflow_app_config.get_app_config

# Default logging; lifespan overrides from config.yaml log_level.
logging.basicConfig(
    level=logging.INFO,
    format=DEFAULT_LOG_FORMAT,
    datefmt=DEFAULT_LOG_DATE_FORMAT,
)

logger = logging.getLogger(__name__)

# Compatibility name for tests/extensions that imported the former per-hook
# timeout. It now supplies only the default channel phase budget; the
# coordinator owns all actual deadline accounting.
_SHUTDOWN_HOOK_TIMEOUT_SECONDS = 5.0


async def _ensure_admin_user(app: FastAPI) -> None:
    """Startup hook: handle first boot and migrate orphan threads otherwise.

    After admin creation, migrate orphan threads from the LangGraph
    store (metadata.user_id unset) to the admin account. This is the
    "no-auth → with-auth" upgrade path: users who ran DeerFlow without
    authentication have existing LangGraph thread data that needs an
    owner assigned.
        First boot (no admin exists):
            - Does NOT create any user accounts automatically.
            - The operator must visit ``/setup`` to create the first admin.

    Subsequent boots (admin already exists):
      - Runs the one-time "no-auth → with-auth" orphan thread migration for
        existing LangGraph thread metadata that has no user_id.

    No SQL persistence migration is needed: the four user_id columns
    (threads_meta, runs, run_events, feedback) only come into existence
    alongside the auth module via create_all, so freshly created tables
    never contain NULL-owner rows.
    """
    from sqlalchemy import select

    from app.gateway.deps import get_local_provider
    from deerflow.persistence.engine import get_session_factory
    from deerflow.persistence.user.model import UserRow

    try:
        provider = get_local_provider()
    except RuntimeError:
        # Auth persistence may not be initialized in some test/boot paths.
        # Skip admin migration work rather than failing gateway startup.
        logger.warning("Auth persistence not ready; skipping admin bootstrap check")
        return

    sf = get_session_factory()
    if sf is None:
        return

    admin_count = await provider.count_admin_users()

    if admin_count == 0:
        logger.info("=" * 60)
        logger.info("  First boot detected — no admin account exists.")
        logger.info("  Visit /setup to complete admin account creation.")
        logger.info("=" * 60)
        return

    # Admin already exists — run orphan thread migration for any
    # LangGraph thread metadata that pre-dates the auth module.
    async with sf() as session:
        stmt = select(UserRow).where(UserRow.system_role == "admin").limit(1)
        row = (await session.execute(stmt)).scalar_one_or_none()

    if row is None:
        return  # Should not happen (admin_count > 0 above), but be safe.

    admin_id = str(row.id)

    # LangGraph store orphan migration — non-fatal.
    # This covers the "no-auth → with-auth" upgrade path for users
    # whose existing LangGraph thread metadata has no user_id set.
    store = getattr(app.state, "store", None)
    if store is not None:
        try:
            migrated = await _migrate_orphaned_threads(store, admin_id)
            if migrated:
                logger.info("Migrated %d orphan LangGraph thread(s) to admin", migrated)
        except Exception:
            logger.exception("LangGraph thread migration failed (non-fatal)")


async def _iter_store_items(store, namespace, *, page_size: int = 500):
    """Paginated async iterator over a LangGraph store namespace.

    Replaces the old hardcoded ``limit=1000`` call with a cursor-style
    loop so that environments with more than one page of orphans do
    not silently lose data. Terminates when a page is empty OR when a
    short page arrives (indicating the last page).
    """
    offset = 0
    while True:
        batch = await store.asearch(namespace, limit=page_size, offset=offset)
        if not batch:
            return
        for item in batch:
            yield item
        if len(batch) < page_size:
            return
        offset += page_size


async def _migrate_orphaned_threads(store, admin_user_id: str) -> int:
    """Migrate LangGraph store threads with no user_id to the given admin.

    Uses cursor pagination so all orphans are migrated regardless of
    count. Returns the number of rows migrated.
    """
    migrated = 0
    async for item in _iter_store_items(store, ("threads",)):
        metadata = item.value.get("metadata", {})
        if not metadata.get("user_id"):
            metadata["user_id"] = admin_user_id
            item.value["metadata"] = metadata
            await store.aput(("threads",), item.key, item.value)
            migrated += 1
    return migrated


async def _warm_memory_retrieval(manager) -> None:
    """Rebuild the derived retrieval index without delaying Gateway readiness."""
    try:
        rebuilt = await asyncio.to_thread(manager.warm_retrieval)
        if rebuilt:
            logger.info("Memory retrieval index rebuilt successfully")
        else:
            logger.warning("Memory retrieval index rebuild failed; scoped searches will retry lazily")
    except Exception:
        logger.warning("Memory retrieval index rebuild skipped", exc_info=True)


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    """Application lifespan handler."""

    # Load config and check necessary environment variables at startup.
    # `startup_config` is a local snapshot used only for one-shot bootstrap
    # work (logging level, langgraph_runtime engines, channels). Request-time
    # config resolution always routes through `get_app_config()` in
    # `app/gateway/deps.py::get_config()` so `config.yaml` edits become
    # visible without a process restart. We deliberately do NOT cache this
    # snapshot on `app.state` to keep that contract enforceable.
    try:
        startup_config = get_app_config()
        configure_logging(startup_config)
        ensure_browser_runtime_available(startup_config)
        from app.gateway.github.webhook_auth import (
            GitHubWebhookAuthMode,
            resolve_github_webhook_auth,
        )
        from app.runtime.deployment import (
            DeploymentProfile,
            describe_native_ingress,
            validate_deployment_profile,
        )

        startup_deployment = getattr(startup_config, "deployment", None)
        startup_profile = getattr(
            startup_deployment,
            "profile",
            DeploymentProfile.local_development,
        )
        webhook_auth = resolve_github_webhook_auth(
            deployment_profile=startup_profile,
        )
        current_verified_sources = frozenset({"github"}) if webhook_auth.mode is GitHubWebhookAuthMode.hmac_sha256_verified else frozenset()
        composition_verified_sources = getattr(
            app.state,
            "verified_native_sources_at_composition",
            current_verified_sources,
        )
        verified_sources = frozenset(composition_verified_sources).intersection(current_verified_sources)
        startup_native_ingress = describe_native_ingress(
            startup_config,
            verified_sources=verified_sources,
        )
        validate_deployment_profile(
            startup_config,
            verified_sources=verified_sources,
        )
        durable_native_ingress_required = startup_profile == DeploymentProfile.durable_production and bool(startup_native_ingress.sources)
        app.state.deployment_profile = startup_profile
        logger.info("Configuration loaded successfully")
        warn_if_auth_disabled_enabled()
    except Exception as e:
        error_msg = f"Failed to load configuration during gateway startup: {e}"
        logger.exception(error_msg)
        raise RuntimeError(error_msg) from e
    config = get_gateway_config()
    logger.info(f"Starting API Gateway on {config.host}:{config.port}")

    from deerflow.runtime.skill_snapshot import cleanup_abandoned_skill_snapshots
    from deerflow.skills.projection import ensure_public_skill_projection

    try:
        removed_skill_snapshots = await asyncio.to_thread(cleanup_abandoned_skill_snapshots)
        if removed_skill_snapshots:
            logger.info(
                "Removed %d abandoned accepted skill snapshot(s)",
                removed_skill_snapshots,
            )
    except Exception:
        logger.warning(
            "Accepted skill snapshot startup cleanup failed",
            exc_info=True,
        )

    public_projection_ready = await asyncio.to_thread(ensure_public_skill_projection, app_config=startup_config)
    if public_projection_ready:
        logger.info("Ensured the public skill projection; user projections repair lazily on sandbox acquire")

    # Agent observability (Monocle). Off by default; enabled with
    # MONOCLE_TRACING. Initialized here at startup — not at import time — so a
    # plain `import deerflow.agents` never installs a process-global tracer.
    # Unlike LangSmith/Langfuse, whose validation failures abort the agent run,
    # a bad Monocle config only logs: the Gateway keeps serving without tracing.
    try:
        setup_monocle_tracing_if_enabled()
    except Exception:  # observability must never break startup
        logger.exception("Monocle tracing setup failed; continuing without it")

    # Rebuild the derived memory retrieval index in the background. Scoped
    # searches remain correct while this runs because DeerMem lazily rebuilds
    # the requested scope when the full warm-up has not completed yet.
    retrieval_warm_task: asyncio.Task[None] | None = None
    try:
        from deerflow.agents.memory import get_memory_manager

        if startup_config.memory.enabled:
            manager = await asyncio.to_thread(get_memory_manager)
            warm_retrieval = getattr(manager, "warm_retrieval", None)
            if callable(warm_retrieval):
                retrieval_warm_task = asyncio.create_task(
                    _warm_memory_retrieval(manager),
                    name="memory-retrieval-warm-up",
                )
        else:
            logger.info("Memory is disabled; skipping retrieval index rebuild")
    except Exception:
        logger.warning("Memory retrieval index rebuild skipped", exc_info=True)

    # Pre-warm tiktoken encoding cache so the first memory-injection request
    # never blocks on the BPE data download (which hits an OpenAI/Azure URL
    # that may be unreachable in restricted networks — see issue #3402).
    # Warm-up runs via the manager's `warm()` tier-3 hook. DeerMem.warm re-checks
    # token_counting=="char" and returns early, so char-mode backends never touch
    # tiktoken (avoids even the 5s probe in network-restricted deployments - see
    # issue #3429). A backend with nothing to warm (e.g. noop) returns None from
    # the base default -- log "skipping" instead of the misleading "warmed
    # successfully" so the log reflects what actually happened.
    try:
        from deerflow.agents.memory import get_memory_manager

        manager = await asyncio.to_thread(get_memory_manager)
        warmed = await asyncio.wait_for(
            asyncio.to_thread(manager.warm),
            timeout=5,
        )
        if warmed is None:
            logger.info("Memory backend %s has nothing to warm; skipping tiktoken warm-up", type(manager).__name__)
        elif warmed:
            logger.info("tiktoken encoding cache warmed successfully")
        else:
            logger.warning("tiktoken encoding cache warm-up failed; token counting will use character-based fallback until tiktoken loads successfully")
    except TimeoutError:
        logger.warning("tiktoken encoding cache warm-up timed out; token counting will use character-based fallback until tiktoken loads successfully")
    except Exception:
        logger.warning("tiktoken warm-up skipped", exc_info=True)

    try:
        removed_upload_staging_files = await asyncio.to_thread(cleanup_stale_upload_staging_files)
        if removed_upload_staging_files:
            logger.info("Removed %d stale upload staging file(s)", removed_upload_staging_files)
    except Exception:
        logger.warning("Upload staging file cleanup skipped", exc_info=True)

    # Initialize LangGraph runtime components (StreamBridge, RunManager, checkpointer, store)
    async with langgraph_runtime(app, startup_config):
        logger.info("LangGraph runtime initialised")

        # Check admin bootstrap state and migrate orphan threads after admin exists.
        # Must run AFTER langgraph_runtime so app.state.store is available for thread migration
        await _ensure_admin_user(app)

        # Start IM channel service if any channels are configured
        try:
            from app.channels.service import start_channel_service
            from app.gateway.services import build_channel_invocation_runtime

            # Closure over `app` (mirrors the scheduler runtime construction
            # below) rather than resolving `app.state.stream_bridge` here
            # directly: `stream_bridge` is a STARTUP_ONLY_FIELDS singleton set
            # once, above, by `langgraph_runtime(app, startup_config)`, so
            # either shape is safe by construction — the closure is just the
            # more defensive/consistent-with-precedent form, and it is what
            # ChannelManager's follow-up-drain watcher (issue #4121 Slice 2)
            # uses to reach the same StreamBridge every other run consumer
            # goes through `get_stream_bridge(request)` for.
            channel_service = await start_channel_service(
                startup_config,
                get_stream_bridge=lambda: getattr(app.state, "stream_bridge", None),
                invocation_runtime=build_channel_invocation_runtime(app),
            )
            logger.info("Channel service started: %s", channel_service.get_status())
        except Exception as exc:
            if durable_native_ingress_required:
                raise RuntimeError("durable native ingress failed to initialize") from exc
            logger.exception("No IM channels configured or channel service failed to start")

        try:
            from app.gateway.services import build_scheduled_invocation_runtime
            from app.scheduler import ScheduledTaskService

            if getattr(app.state, "scheduled_task_repo", None) is not None and getattr(app.state, "scheduled_task_run_repo", None) is not None:
                scheduled_task_service = ScheduledTaskService(
                    task_repo=app.state.scheduled_task_repo,
                    task_run_repo=app.state.scheduled_task_run_repo,
                    invocation_runtime=build_scheduled_invocation_runtime(app),
                    poll_interval_seconds=startup_config.scheduler.poll_interval_seconds,
                    lease_seconds=startup_config.scheduler.lease_seconds,
                    max_concurrent_runs=startup_config.scheduler.max_concurrent_runs,
                )
                app.state.scheduled_task_service = scheduled_task_service
                if startup_config.scheduler.enabled:
                    await scheduled_task_service.start()
        except Exception:
            logger.exception("Failed to initialize scheduled task service")

        try:
            from app.mcp_tasks import McpTaskService
            from deerflow.mcp.tasks import McpTaskDriverRegistry

            if getattr(app.state, "mcp_task_repo", None) is not None:
                mcp_task_drivers = McpTaskDriverRegistry()
                mcp_task_service = McpTaskService(
                    repository=app.state.mcp_task_repo,
                    drivers=mcp_task_drivers,
                    poll_interval_seconds=startup_config.mcp_tasks.poll_interval_seconds,
                    lease_seconds=startup_config.mcp_tasks.lease_seconds,
                    max_concurrent_polls=startup_config.mcp_tasks.max_concurrent_polls,
                )
                app.state.mcp_task_drivers = mcp_task_drivers
                app.state.mcp_task_service = mcp_task_service
                if startup_config.mcp_tasks.enabled:
                    await mcp_task_service.start()
        except Exception:
            logger.exception("Failed to initialize MCP task service")

        from app.channels.service import stop_channel_service
        from app.gateway.shutdown import GracefulShutdownCoordinator, ShutdownBudgets
        from deerflow.community.browser_automation import get_browser_session_manager

        shutdown_config = getattr(getattr(startup_config, "deployment", None), "shutdown", None)

        def shutdown_budget(name: str, default: float) -> float:
            value = getattr(shutdown_config, name, default)
            return float(value) if isinstance(value, (int, float)) else default

        memory_enabled = bool(getattr(startup_config.memory, "enabled", False))
        memory_manager = None
        if memory_enabled:
            try:
                from deerflow.agents.memory import get_memory_manager

                memory_manager = await asyncio.to_thread(get_memory_manager)
            except Exception:
                logger.exception("Memory manager unavailable for graceful shutdown")

        async def begin_admission_drain() -> bool:
            readiness = getattr(app.state, "runtime_readiness", None)
            if readiness is None:
                return False
            return await readiness.begin_draining()

        async def stop_scheduler() -> None:
            services = (
                getattr(app.state, "scheduled_task_service", None),
                getattr(app.state, "mcp_task_service", None),
            )
            stops = [service.stop() for service in services if service is not None]
            if not stops:
                return
            results = await asyncio.gather(*stops, return_exceptions=True)
            failures = [result for result in results if isinstance(result, BaseException)]
            if failures:
                raise RuntimeError("producer shutdown failed") from failures[0]

        async def drain_runs(timeout: float) -> bool:
            manager = getattr(app.state, "run_manager", None)
            if manager is None:
                return True
            result = await manager.shutdown(timeout=timeout)
            return result is not False

        async def stop_retrieval() -> bool:
            if retrieval_warm_task is None or retrieval_warm_task.done():
                return True
            retrieval_warm_task.cancel()
            await asyncio.gather(retrieval_warm_task, return_exceptions=True)
            # asyncio cancellation cannot stop the sync warm_retrieval call
            # already running in the executor. Keep its manager open rather
            # than closing a connection the thread may still be using.
            return False

        async def close_browser() -> None:
            await get_browser_session_manager().close_all_sessions()

        async def close_runtime() -> None:
            close = getattr(app.state, "close_runtime_dependencies", None)
            if close is not None:
                await close()

        coordinator = GracefulShutdownCoordinator(
            budgets=ShutdownBudgets(
                admission_seconds=shutdown_budget("admission_seconds", 2.0),
                channel_seconds=shutdown_budget("channel_seconds", _SHUTDOWN_HOOK_TIMEOUT_SECONDS),
                scheduler_seconds=shutdown_budget("scheduler_seconds", 3.0),
                run_seconds=shutdown_budget("run_seconds", 8.0),
                memory_seconds=float(getattr(startup_config.memory, "shutdown_flush_timeout_seconds", 30.0)),
                dependencies_seconds=shutdown_budget("dependencies_seconds", 5.0),
            ),
            begin_admission_drain=begin_admission_drain,
            stop_channels=stop_channel_service,
            stop_scheduler=stop_scheduler,
            drain_runs=drain_runs,
            flush_memory=(memory_manager.shutdown_flush if memory_manager is not None else lambda _timeout: not memory_enabled),
            close_memory=(getattr(memory_manager, "close", lambda: None) if memory_manager is not None else lambda: None),
            close_browser=close_browser,
            close_oidc=auth.close_oidc_service,
            stop_retrieval=stop_retrieval,
            close_runtime=close_runtime,
        )
        app.state.shutdown_coordinator = coordinator
        try:
            yield
        finally:
            report = await coordinator.shutdown()
            if report.memory_flushed:
                logger.info("Memory queue flush completed during Gateway graceful shutdown")
            else:
                logger.warning("Memory queue flush did not finish during Gateway graceful shutdown")

    logger.info("Shutting down API Gateway")


def create_app() -> FastAPI:
    """Create and configure the FastAPI application.

    Returns:
        Configured FastAPI application instance.
    """
    config = get_gateway_config()
    docs_url = "/docs" if config.enable_docs else None
    redoc_url = "/redoc" if config.enable_docs else None
    openapi_url = "/openapi.json" if config.enable_docs else None

    app = FastAPI(
        title="DeerFlow API Gateway",
        description="""
## DeerFlow API Gateway

API Gateway for DeerFlow - A LangGraph-based AI agent backend with sandbox execution capabilities.

### Features

- **Models Management**: Query and retrieve available AI models
- **MCP Configuration**: Manage Model Context Protocol (MCP) server configurations
- **Memory Management**: Access and manage global memory data for personalized conversations
- **Skills Management**: Query and manage skills and their enabled status
- **Artifacts**: Access thread artifacts and generated files
- **Health Monitoring**: System health check endpoints

### Architecture

LangGraph-compatible requests are routed through nginx to this gateway.
This gateway provides runtime endpoints for agent runs plus custom endpoints for models, MCP configuration, skills, and artifacts.
        """,
        version="0.1.0",
        lifespan=lifespan,
        docs_url=docs_url,
        redoc_url=redoc_url,
        openapi_url=openapi_url,
        openapi_tags=[
            {
                "name": "models",
                "description": "Operations for querying available AI models and their configurations",
            },
            {
                "name": "mcp",
                "description": "Manage Model Context Protocol (MCP) server configurations",
            },
            {
                "name": "memory",
                "description": "Access and manage global memory data for personalized conversations",
            },
            {
                "name": "skills",
                "description": "Manage skills and their configurations",
            },
            {
                "name": "artifacts",
                "description": "Access and download thread artifacts and generated files",
            },
            {
                "name": "uploads",
                "description": "Upload and manage user files for threads",
            },
            {
                "name": "threads",
                "description": "Manage DeerFlow thread-local filesystem data",
            },
            {
                "name": "agents",
                "description": "Create and manage custom agents with per-agent config and prompts",
            },
            {
                "name": "suggestions",
                "description": "Generate follow-up question suggestions for conversations",
            },
            {
                "name": "input-polish",
                "description": "Polish composer draft input before sending",
            },
            {
                "name": "channels",
                "description": "Manage IM channel integrations (Feishu, Slack, Telegram)",
            },
            {
                "name": "assistants-compat",
                "description": "LangGraph Platform-compatible assistants API (stub)",
            },
            {
                "name": "runs",
                "description": "LangGraph Platform-compatible runs lifecycle (create, stream, cancel)",
            },
            {
                "name": "runtime",
                "description": "Versioned durable invocation ensure, observe, and control API",
            },
            {
                "name": "health",
                "description": "Health check and system status endpoints",
            },
        ],
    )
    install_runtime_error_handlers(app)

    # Auth: reject unauthenticated requests to non-public paths (fail-closed safety net)
    app.add_middleware(AuthMiddleware)

    # CSRF: Double Submit Cookie pattern for state-changing requests
    app.add_middleware(CSRFMiddleware)

    # CORS: the unified nginx endpoint is same-origin by default. Split-origin
    # browser clients must opt in with this explicit Gateway allowlist so CORS
    # and CSRF origin checks share the same source of truth. They also need the
    # run id the Gateway returns in a non-safelisted response header; without
    # exposing it the SDK never reports a created run, so a new thread keeps its
    # placeholder route and every action gated on an established thread stays
    # hidden until the page is reloaded.
    cors_origins = sorted(get_configured_cors_origins())
    if cors_origins:
        app.add_middleware(
            CORSMiddleware,
            allow_origins=cors_origins,
            allow_credentials=True,
            allow_methods=["*"],
            allow_headers=["*"],
            expose_headers=list(CORS_EXPOSED_HEADERS),
        )

    # Request trace correlation: when logging.enhance.enabled=true, bind one
    # trace id per Gateway HTTP request and write it to response start headers.
    # `logging` is registered as restart-required (see reload_boundary.py) so we
    # snapshot the flag from the startup AppConfig instead of reading live; a
    # runtime toggle would otherwise leave the log formatter (installed once by
    # configure_logging() at lifespan startup) out of sync with the middleware.
    app.add_middleware(TraceMiddleware, enabled=_resolve_trace_enabled_for_app_construction())

    # Python extensions load once while the Gateway app is constructed. Agent
    # middleware builders consume the same immutable set through the process
    # singleton; app.state exposes it to the Gateway runtime.
    from deerflow.extensions import (
        EMPTY_EXTENSIONS,
        ExtensionLoadError,
        initialize_runtime_diagnostics,
        load_extensions,
        set_loaded_extensions,
    )

    # Resolving the configured plugin list is deliberately outside the
    # fail-open guard below: a config.yaml that exists but cannot be parsed or
    # validated is a configuration failure, not an extension failure. Reporting
    # it as the latter would silently drop a `required: true` extension instead
    # of failing the boot. Only an absent config.yaml is tolerated, mirroring
    # _resolve_trace_enabled_for_app_construction() — create_app() runs at
    # import time, and lifespan still performs strict config loading before
    # serving.
    construction_config = None
    try:
        construction_config = get_app_config()
        configured_plugins = construction_config.plugins
        required_capabilities = construction_config.required_capabilities
        construction_authorization = construction_config.authorization
        construction_deployment = construction_config.deployment
        construction_database_backend = construction_config.database.backend
    except FileNotFoundError:
        logger.debug("config.yaml not found while constructing Gateway app; loading no extensions for this app instance")
        configured_plugins = []
        required_capabilities = []
        from deerflow.config.authorization_config import AuthorizationConfig
        from deerflow.config.deployment_config import DeploymentConfig

        construction_authorization = AuthorizationConfig()
        construction_deployment = DeploymentConfig()
        construction_database_backend = "memory"

    try:
        loaded_extensions, extension_diagnostics = load_extensions(configured_plugins)
    except ExtensionLoadError:
        # `required: true` makes the extension part of the startup contract.
        # Booting without it would silently change configured behaviour.
        raise
    except Exception:
        logger.exception("Extension loading failed; continuing with no extensions")
        loaded_extensions, extension_diagnostics = EMPTY_EXTENSIONS, []
    from deerflow.extensions.capabilities import route_required_capabilities
    from deerflow.extensions.contributors import ContributorHost
    from deerflow.extensions.loader import Diagnostic

    required_capability_routes = route_required_capabilities(required_capabilities)

    contributor_host = ContributorHost(
        loaded_extensions,
        required_capabilities=required_capability_routes.contributors,
    )
    extension_diagnostics.extend(
        Diagnostic(
            level="warning",
            source=item.capability_id,
            message=item.error_class,
            code=item.diagnostic_code,
            error_class=item.error_class,
            correlation_id=item.correlation_id,
            contribution_id=item.contribution_id,
        )
        for item in contributor_host.startup_diagnostics
    )
    from deerflow.extensions.constraints import InvocationConstraintsHost

    invocation_constraints_host = InvocationConstraintsHost(
        loaded_extensions,
        required_capabilities=required_capability_routes.constraints,
    )
    from deerflow_extension_api import (
        INVOCATION_CONSTRAINTS_CAPABILITY_API_VERSION_V2,
        INVOCATION_CONSTRAINTS_REQUIRED_CAPABILITY,
        INVOCATION_CONSTRAINTS_REQUIRED_CAPABILITY_V2,
    )

    constraint_registration = loaded_extensions.invocation_constraints_provider_factory
    constraint_diagnostic_id = (
        INVOCATION_CONSTRAINTS_REQUIRED_CAPABILITY_V2
        if constraint_registration is not None and constraint_registration.capability_api_version == INVOCATION_CONSTRAINTS_CAPABILITY_API_VERSION_V2
        else INVOCATION_CONSTRAINTS_REQUIRED_CAPABILITY
    )
    extension_diagnostics.extend(Diagnostic.warning(constraint_diagnostic_id, message) for message in invocation_constraints_host.startup_diagnostics)
    from deerflow.extensions.mcp import McpInterceptorHost

    mcp_interceptor_host = McpInterceptorHost(
        loaded_extensions,
        required_capabilities=required_capability_routes.mcp,
    )
    extension_diagnostics.extend(Diagnostic.warning(item.capability_id, item.diagnostic_code) for item in mcp_interceptor_host.startup_diagnostics)
    # One application-owned resolver supplies a coherent provider instance to
    # route checks and every durable-run authorization path. Extension-backed
    # factories are startup-only; legacy class-path providers may be replaced
    # atomically when the hot-reloaded authorization config changes.
    from app.gateway.authorization import AuthorizationProviderResolver

    authorization_provider_resolver = AuthorizationProviderResolver(
        loaded_extensions,
        construction_authorization,
    )
    from app.runtime.authorization import validate_invocation_authorization_startup

    validate_invocation_authorization_startup(
        construction_authorization,
        authorization_provider_resolver.snapshot(),
    )
    from deerflow.extensions.capabilities import (
        CapabilityHealthMonitor,
        build_capability_manifest,
    )

    invocation_authorization_required = any(
        (
            construction_authorization.invocation_operations.start_enabled,
            construction_authorization.invocation_operations.observe_enabled,
            construction_authorization.invocation_operations.cancel_enabled,
        )
    )
    mcp_authorization_required = bool(required_capability_routes.mcp)
    authorization_snapshot = authorization_provider_resolver.snapshot()
    initialized_capability_ids = set(contributor_host.initialized_capability_ids)
    initialized_capability_ids.update(invocation_constraints_host.initialized_capability_ids)
    initialized_capability_ids.update(mcp_interceptor_host.initialized_capability_ids)
    registration = authorization_provider_resolver.snapshot().registration
    if registration is not None:
        initialized_capability_ids.add(f"authorization_provider:{registration.contribution_id}")
    capability_manifest = build_capability_manifest(
        loaded_extensions,
        required_capabilities=required_capabilities,
        authorization_required=(invocation_authorization_required or mcp_authorization_required),
        legacy_authorization_initialized=(authorization_snapshot.source_kind == "legacy" and authorization_snapshot.provider is not None),
        initialized_capability_ids=initialized_capability_ids,
    )
    capability_health_monitor = CapabilityHealthMonitor(
        capability_manifest,
        loaded_extensions,
        cache_seconds=(construction_deployment.readiness.capability_cache_seconds),
        timeout_seconds=(construction_deployment.readiness.capability_probe_timeout_seconds),
        stale_seconds=(construction_deployment.readiness.required_health_stale_seconds),
        admission_max_age_seconds=(construction_deployment.readiness.admission_health_max_age_seconds),
    )
    from deerflow.extensions.mcp import configure_mcp_interceptor_runtime

    configure_mcp_interceptor_runtime(
        mcp_interceptor_host,
        capability_health_monitor,
    )
    set_loaded_extensions(loaded_extensions)
    app.state.extensions = loaded_extensions
    app.state.extension_diagnostics = initialize_runtime_diagnostics(extension_diagnostics)
    app.state.authorization_provider_resolver = authorization_provider_resolver
    app.state.invocation_authorization_config = construction_authorization.invocation_operations.model_copy(deep=True)
    app.state.contributor_host = contributor_host
    app.state.invocation_constraints_host = invocation_constraints_host
    app.state.mcp_interceptor_host = mcp_interceptor_host
    app.state.capability_manifest = capability_manifest
    app.state.capability_health_monitor = capability_health_monitor
    from app.runtime.visibility import ConfiguredServiceObservationGrantResolver

    app.state.service_observation_visibility_resolver = (
        ConfiguredServiceObservationGrantResolver(
            lambda: get_app_config().authorization.service_observation_grants,
        )
        if construction_authorization.invocation_operations.observe_enabled
        else None
    )
    from app.gateway.github.webhook_auth import (
        GitHubWebhookAuthMode,
        resolve_github_webhook_auth,
    )
    from app.runtime.deployment import (
        DeploymentProvenance,
        DeploymentQualification,
        GatewayDeploymentReporter,
        describe_native_ingress,
    )

    app.state.deployment_profile = construction_deployment.profile
    composition_webhook_auth = resolve_github_webhook_auth(
        deployment_profile=construction_deployment.profile,
    )
    composition_verified_sources = frozenset({"github"}) if composition_webhook_auth.mode is GitHubWebhookAuthMode.hmac_sha256_verified else frozenset()
    app.state.verified_native_sources_at_composition = composition_verified_sources

    def current_native_ingress():
        webhook_auth = resolve_github_webhook_auth(
            deployment_profile=construction_deployment.profile,
        )
        current_verified_sources = frozenset({"github"}) if webhook_auth.mode is GitHubWebhookAuthMode.hmac_sha256_verified else frozenset()
        verified_sources = composition_verified_sources.intersection(current_verified_sources)
        return describe_native_ingress(
            construction_config,
            verified_sources=verified_sources,
        )

    try:
        deployment_provenance = DeploymentProvenance.from_environment()
    except ValueError:
        logger.warning("Ignoring invalid bounded deployment provenance identifiers")
        deployment_provenance = DeploymentProvenance()
    try:
        deployment_qualification = DeploymentQualification.from_environment()
    except ValueError:
        logger.warning("Ignoring invalid bounded deployment qualification evidence")
        deployment_qualification = DeploymentQualification()
    app.state.deployment_reporter = GatewayDeploymentReporter(
        profile=construction_deployment.profile,
        database_backend=construction_database_backend,
        atomic_lifecycle=False,
        manifest=capability_manifest,
        health_monitor=capability_health_monitor,
        readiness_supplier=lambda: getattr(
            getattr(app.state, "runtime_readiness", None),
            "last_snapshot",
            None,
        ),
        provenance=deployment_provenance,
        qualification=deployment_qualification,
        native_ingress_supplier=current_native_ingress,
    )
    from app.runtime.readiness import RuntimeReadinessCoordinator

    app.state.runtime_readiness = RuntimeReadinessCoordinator(
        health_monitor=capability_health_monitor,
        lifecycle_store=lambda: getattr(app.state, "run_store", None),
        persistence_ready=lambda: bool(
            getattr(
                getattr(app.state, "deployment_reporter", None),
                "admission_profile_ready",
                False,
            )
        ),
        extension_generation=lambda: int(getattr(app.state.capability_manifest, "extension_generation")),
        overall_timeout_seconds=(construction_deployment.readiness.overall_timeout_seconds),
    )

    # Include routers
    # Models API is mounted at /api/models
    app.include_router(models.router)

    # Features API is mounted at /api/features
    app.include_router(features.router)

    # Console API (cross-thread observability) is mounted at /api/console
    app.include_router(console.router)

    # MCP API is mounted at /api/mcp
    app.include_router(mcp.router)

    # Memory API is mounted at /api/memory
    app.include_router(memory.router)

    # Skills API is mounted at /api/skills
    app.include_router(skills.router)

    # First-party integrations API is mounted at /api/integrations
    app.include_router(integrations.router)

    # Artifacts API is mounted at /api/threads/{thread_id}/artifacts
    app.include_router(artifacts.router)

    # Browser API is mounted at /api/threads/{thread_id}/browser
    app.include_router(browser.router)

    # Uploads API is mounted at /api/threads/{thread_id}/uploads
    app.include_router(uploads.router)

    # Thread cleanup API is mounted at /api/threads/{thread_id}
    app.include_router(threads.router)

    # Scheduled tasks API is mounted at /api/scheduled-tasks
    app.include_router(scheduled_tasks.router)

    # Agents API is mounted at /api/agents
    app.include_router(agents.router)

    # Suggestions API is mounted at /api/threads/{thread_id}/suggestions
    app.include_router(suggestions.router)

    # Input polishing API is mounted at /api/input-polish
    app.include_router(input_polish.router)

    # User-facing IM channel connection API is mounted at /api/channels
    app.include_router(channel_connections.router)

    # Channels API is mounted at /api/channels
    app.include_router(channels.router)

    # Assistants compatibility API (LangGraph Platform stub)
    app.include_router(assistants_compat.router)

    # Auth API is mounted at /api/v1/auth
    app.include_router(auth.router)

    # Feedback API is mounted at /api/threads/{thread_id}/runs/{run_id}/feedback
    app.include_router(feedback.router)

    # Thread Runs API (LangGraph Platform-compatible runs lifecycle)
    app.include_router(thread_runs.router)

    # Stateless Runs API (stream/wait without a pre-existing thread)
    app.include_router(runs.router)

    # Versioned durable invocation HTTP API
    app.include_router(runtime_api.router)

    # GitHub webhooks API is mounted at /api/webhooks/github
    # Exempt from auth and CSRF middleware (see auth_middleware._PUBLIC_PATH_PREFIXES
    # and csrf_middleware.should_check_csrf); authenticity is enforced via the
    # X-Hub-Signature-256 HMAC against GITHUB_WEBHOOK_SECRET.
    # Including this router transitively imports app.gateway.github, which
    # registers the GitHub channel's ChannelRunPolicy as an import side-effect.
    #
    # Fail-closed: only mount the route when a webhook secret is configured
    # (or when the explicit DEER_FLOW_ALLOW_UNVERIFIED_GITHUB_WEBHOOKS=1
    # dev opt-in is set). A misconfigured deployment without a secret cannot
    # serve forged deliveries because the URL responds 404 — there is no
    # handler to reach.
    if composition_webhook_auth.route_enabled:
        app.include_router(github_webhooks.router)
        logger.info("GitHub webhooks route mounted at /api/webhooks/github")
    else:
        logger.warning("GitHub webhooks route NOT mounted: GITHUB_WEBHOOK_SECRET unset and DEER_FLOW_ALLOW_UNVERIFIED_GITHUB_WEBHOOKS not set. /api/webhooks/github will respond 404. Configure either env var to enable the route.")

    @app.get("/health", tags=["health"])
    async def health_check() -> dict[str, str]:
        """Health check endpoint.

        Returns:
            Service health status information.
        """
        return {"status": "healthy", "service": "deer-flow-gateway"}

    @app.get("/ready", tags=["health"])
    async def readiness_check() -> JSONResponse:
        """Return a deliberately minimal deployable readiness result."""

        readiness = await app.state.runtime_readiness.readiness()
        ready = readiness.status == "ready"
        return JSONResponse(
            status_code=200 if ready else 503,
            content={"status": "ready" if ready else "not_ready"},
        )

    return app


def _resolve_trace_enabled_for_app_construction() -> bool:
    """Resolve the trace middleware flag without making imports require config.yaml."""
    try:
        return resolve_trace_enabled(get_app_config())
    except FileNotFoundError:
        # Startup lifespan still performs strict config loading before serving.
        logger.debug("config.yaml not found while constructing Gateway app; TraceMiddleware disabled for this app instance")
        return False


# Create app instance for uvicorn
app = create_app()
