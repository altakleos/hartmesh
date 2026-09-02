"""Operational health and readiness for authoritative capabilities."""

from __future__ import annotations

import asyncio
import logging
from datetime import UTC, datetime, timedelta
from uuid import UUID

import httpx
import pytest
from _router_auth_helpers import make_authed_test_app
from deerflow_extension_api import (
    AUTHORIZATION_PROVIDER_CAPABILITY_API_VERSION,
    INVOCATION_CONSTRAINTS_CAPABILITY_API_VERSION,
    ORIGIN_CONTRIBUTOR_CAPABILITY_API_VERSION,
    AuthorizationProviderFactory,
    CapabilityHealthResult,
    InvocationConstraintsProviderFactory,
    OriginContributorFactory,
)
from deerflow_runtime_api import RuntimeCapabilities, record_from_dict
from fastapi.testclient import TestClient
from sqlalchemy import delete, event, text, update
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from app.gateway.auth.models import User
from app.gateway.routers import runtime_api
from app.runtime.deployment import GatewayDeploymentReporter
from deerflow.config.deployment_config import DeploymentConfig
from deerflow.extensions.capabilities import (
    CapabilityHealthMonitor,
    CapabilityHealthSnapshot,
    build_capability_manifest,
)
from deerflow.extensions.registry import ExtensionRegistry
from deerflow.persistence.base import Base
from deerflow.persistence.run.model import (
    RunAdmissionCursorStateRow,
    RunLifecycleCursorStateRow,
    RunLifecycleEventRow,
)
from deerflow.persistence.run.sql import RunRepository
from deerflow.runtime import PostCommitObligationStatus


class _OriginContributor:
    async def contribute(self, request):
        return None


class _AuthorizationProvider:
    name = "fixture"

    def authorize(self, request):
        raise NotImplementedError

    async def aauthorize(self, request):
        raise NotImplementedError

    def filter_resources(self, principal, resource_type, candidates):
        return candidates


class _ConstraintsProvider:
    async def project(self, request):
        raise NotImplementedError


def test_readiness_timing_configuration_is_explicit_and_fail_closed() -> None:
    timing = DeploymentConfig().readiness

    assert timing.capability_cache_seconds == 10.0
    assert timing.admission_health_max_age_seconds == 10.0
    assert timing.required_health_stale_seconds == 30.0
    assert timing.capability_probe_timeout_seconds == 2.0
    assert timing.overall_timeout_seconds == 5.0
    assert timing.required_failure_threshold == 1

    with pytest.raises(ValueError, match="overall_timeout_seconds"):
        DeploymentConfig(
            readiness={
                "capability_probe_timeout_seconds": 3.0,
                "overall_timeout_seconds": 2.0,
            }
        )

    with pytest.raises(ValueError, match="required_failure_threshold"):
        DeploymentConfig(
            readiness={"required_failure_threshold": 2},
        )


@pytest.mark.asyncio
async def test_concurrent_readiness_requests_share_one_health_probe_and_cache() -> None:
    gate = asyncio.Event()
    calls = 0

    async def probe() -> CapabilityHealthResult:
        nonlocal calls
        calls += 1
        await gate.wait()
        return CapabilityHealthResult(status="healthy")

    registry = ExtensionRegistry()
    with registry.attributed_to(
        "secret.module:install",
        package_name="example-policy",
        package_version="1.0.0",
    ):
        registry.origin_contributor(
            OriginContributorFactory(
                contribution_id="policy_origin",
                capability_api_version=ORIGIN_CONTRIBUTOR_CAPABILITY_API_VERSION,
                factory=_OriginContributor,
                kind="origin_contributor",
                health_probe=probe,
            )
        )
    loaded = registry.build(generation=4)
    manifest = build_capability_manifest(
        loaded,
        required_capabilities=("origin_contributor:policy_origin",),
        initialized_capability_ids=("origin_contributor:policy_origin",),
    )
    monitor = CapabilityHealthMonitor(
        manifest,
        loaded,
        clock=lambda: datetime(2026, 8, 7, tzinfo=UTC),
    )

    requests = [asyncio.create_task(monitor.readiness()) for _ in range(8)]
    await asyncio.sleep(0)
    gate.set()
    results = await asyncio.gather(*requests)

    assert calls == 1
    assert [item.status for item in results].count("ready") == 1
    assert [item.status for item in results].count("not_ready") == 7
    assert {item.health[0].diagnostic_code for item in results if item.status == "not_ready"} == {"refresh_in_progress"}
    assert (await monitor.readiness()).status == "ready"
    assert calls == 1, "a fresh 10-second cache must avoid another probe"


@pytest.mark.asyncio
async def test_required_authorization_and_constraints_health_fail_closed_while_optional_does_not() -> None:
    async def unhealthy() -> CapabilityHealthResult:
        return CapabilityHealthResult(
            status="unhealthy",
            diagnostic_code="upstream_unavailable",
        )

    registry = ExtensionRegistry()
    with registry.attributed_to(
        "fixture:install",
        package_name="example-authority",
        package_version="2.0.0",
    ):
        registry.authorization_provider(
            AuthorizationProviderFactory(
                contribution_id="policy",
                capability_api_version=AUTHORIZATION_PROVIDER_CAPABILITY_API_VERSION,
                factory=_AuthorizationProvider,
                kind="authorization_provider",
                health_probe=unhealthy,
            )
        )
        registry.invocation_constraints(
            InvocationConstraintsProviderFactory(
                contribution_id="limits",
                capability_api_version=INVOCATION_CONSTRAINTS_CAPABILITY_API_VERSION,
                factory=_ConstraintsProvider,
                kind="invocation_constraints",
                health_probe=unhealthy,
            )
        )
    loaded = registry.build(generation=2)
    initialized = {
        "authorization_provider:policy",
        "invocation_constraints.v1",
    }

    required_manifest = build_capability_manifest(
        loaded,
        authorization_required=True,
        required_capabilities=("invocation_constraints.v1",),
        initialized_capability_ids=initialized,
    )
    optional_manifest = build_capability_manifest(
        loaded,
        initialized_capability_ids=initialized,
    )

    required = await CapabilityHealthMonitor(required_manifest, loaded).readiness()
    optional = await CapabilityHealthMonitor(optional_manifest, loaded).readiness()

    assert required.status == "not_ready"
    assert {item.capability_id for item in required.health if item.status == "unhealthy"} == {
        "authorization_provider:policy",
        "invocation_constraints.v1",
    }
    assert optional.status == "ready"


@pytest.mark.asyncio
async def test_required_probe_timeout_and_stale_snapshot_are_not_ready() -> None:
    now = datetime(2026, 8, 7, tzinfo=UTC)

    async def never_returns() -> CapabilityHealthResult:
        await asyncio.Event().wait()
        raise AssertionError("unreachable")

    registry = ExtensionRegistry()
    with registry.attributed_to("fixture:install"):
        registry.origin_contributor(
            OriginContributorFactory(
                contribution_id="policy_origin",
                capability_api_version=ORIGIN_CONTRIBUTOR_CAPABILITY_API_VERSION,
                factory=_OriginContributor,
                kind="origin_contributor",
                health_probe=never_returns,
            )
        )
    loaded = registry.build()
    manifest = build_capability_manifest(
        loaded,
        required_capabilities=("origin_contributor:policy_origin",),
        initialized_capability_ids=("origin_contributor:policy_origin",),
    )
    monitor = CapabilityHealthMonitor(
        manifest,
        loaded,
        clock=lambda: now,
        timeout_seconds=0.01,
    )

    timed_out = await monitor.readiness()
    assert timed_out.status == "not_ready"
    assert timed_out.health[0].diagnostic_code == "probe_timeout"

    calls = 0

    async def healthy() -> CapabilityHealthResult:
        nonlocal calls
        calls += 1
        return CapabilityHealthResult(status="healthy")

    registry = ExtensionRegistry()
    with registry.attributed_to("fixture:install"):
        registry.origin_contributor(
            OriginContributorFactory(
                contribution_id="policy_origin",
                capability_api_version=ORIGIN_CONTRIBUTOR_CAPABILITY_API_VERSION,
                factory=_OriginContributor,
                kind="origin_contributor",
                health_probe=healthy,
            )
        )
    loaded = registry.build()
    manifest = build_capability_manifest(
        loaded,
        required_capabilities=("origin_contributor:policy_origin",),
        initialized_capability_ids=("origin_contributor:policy_origin",),
    )
    current = [now]
    monitor = CapabilityHealthMonitor(manifest, loaded, clock=lambda: current[0])
    assert (await monitor.readiness()).status == "ready"
    current[0] += timedelta(seconds=31)

    stale = await monitor.readiness(refresh=False)

    assert stale.status == "not_ready"
    assert stale.health[0].status == "unknown"
    assert stale.health[0].diagnostic_code == "snapshot_stale"
    assert calls == 1


@pytest.mark.asyncio
async def test_admission_health_requires_fresh_success_from_current_generation() -> None:
    now = [datetime(2026, 8, 7, tzinfo=UTC)]
    healthy = [True]

    async def probe() -> CapabilityHealthResult:
        if healthy[0]:
            return CapabilityHealthResult(status="healthy")
        return CapabilityHealthResult(
            status="unhealthy",
            diagnostic_code="authority_unavailable",
        )

    registry = ExtensionRegistry()
    with registry.attributed_to("fixture:install"):
        registry.origin_contributor(
            OriginContributorFactory(
                contribution_id="policy_origin",
                capability_api_version=ORIGIN_CONTRIBUTOR_CAPABILITY_API_VERSION,
                factory=_OriginContributor,
                kind="origin_contributor",
                health_probe=probe,
            )
        )
    loaded = registry.build(generation=7)
    manifest = build_capability_manifest(
        loaded,
        required_capabilities=("origin_contributor:policy_origin",),
        initialized_capability_ids=("origin_contributor:policy_origin",),
    )
    monitor = CapabilityHealthMonitor(
        manifest,
        loaded,
        clock=lambda: now[0],
        cache_seconds=1,
        admission_max_age_seconds=5,
    )

    first = await monitor.admission_readiness(expected_generation=7)
    assert first.status == "ready"
    assert first.health[0].extension_generation == 7
    assert first.health[0].last_healthy_at == now[0]

    generation_mismatch = await monitor.admission_readiness(
        expected_generation=8,
    )
    assert generation_mismatch.status == "not_ready"
    assert generation_mismatch.health[0].diagnostic_code == ("generation_mismatch")

    healthy[0] = False
    now[0] += timedelta(seconds=6)
    unavailable = await monitor.admission_readiness(expected_generation=7)
    assert unavailable.status == "not_ready"
    assert unavailable.health[0].diagnostic_code == "authority_unavailable"
    assert unavailable.health[0].last_healthy_at == datetime(
        2026,
        8,
        7,
        tzinfo=UTC,
    )

    healthy[0] = True
    now[0] += timedelta(seconds=2)
    recovered = await monitor.admission_readiness(expected_generation=7)
    assert recovered.status == "ready"
    assert recovered.health[0].last_healthy_at == now[0]


@pytest.mark.asyncio
async def test_authority_probe_exception_is_fail_closed_and_safely_correlated(
    caplog: pytest.LogCaptureFixture,
) -> None:
    async def probe() -> CapabilityHealthResult:
        raise RuntimeError("provider token=never-publish")

    registry = ExtensionRegistry()
    with registry.attributed_to("fixture:install"):
        registry.origin_contributor(
            OriginContributorFactory(
                contribution_id="policy_origin",
                capability_api_version=ORIGIN_CONTRIBUTOR_CAPABILITY_API_VERSION,
                factory=_OriginContributor,
                kind="origin_contributor",
                health_probe=probe,
            )
        )
    loaded = registry.build(generation=9)
    manifest = build_capability_manifest(
        loaded,
        required_capabilities=("origin_contributor:policy_origin",),
        initialized_capability_ids=("origin_contributor:policy_origin",),
    )

    with caplog.at_level(
        logging.WARNING,
        logger="deerflow.extensions.capabilities",
    ):
        result = await CapabilityHealthMonitor(
            manifest,
            loaded,
        ).admission_readiness(expected_generation=9)

    assert result.status == "not_ready"
    snapshot = result.health[0]
    assert snapshot.diagnostic_code == "probe_failed"
    assert snapshot.correlation_id is not None
    public = {
        "diagnostic_code": snapshot.diagnostic_code,
        "correlation_id": snapshot.correlation_id,
    }
    assert "never-publish" not in str(public)
    matching = [record for record in caplog.records if getattr(record, "correlation_id", None) == snapshot.correlation_id]
    assert len(matching) == 1
    assert matching[0].exception_class == "RuntimeError"
    assert "never-publish" not in caplog.text


@pytest.mark.asyncio
async def test_missing_required_initialization_is_unready_but_optional_failure_is_diagnostic_only() -> None:
    missing_extensions = ExtensionRegistry().build()
    missing = build_capability_manifest(
        missing_extensions,
        required_capabilities=("origin_contributor:required_origin",),
    )
    missing_result = await CapabilityHealthMonitor(
        missing,
        missing_extensions,
    ).readiness()
    missing_authorization = build_capability_manifest(
        missing_extensions,
        authorization_required=True,
    )
    missing_authorization_result = await CapabilityHealthMonitor(
        missing_authorization,
        missing_extensions,
    ).readiness()

    registry = ExtensionRegistry()
    with registry.attributed_to("fixture:install"):
        registry.origin_contributor(
            OriginContributorFactory(
                contribution_id="optional_origin",
                capability_api_version=ORIGIN_CONTRIBUTOR_CAPABILITY_API_VERSION,
                factory=_OriginContributor,
                kind="origin_contributor",
            )
        )
    loaded = registry.build()
    optional_failure = build_capability_manifest(loaded)
    optional_result = await CapabilityHealthMonitor(
        optional_failure,
        loaded,
    ).readiness()

    assert missing_result.status == "not_ready"
    assert missing_result.health[0].diagnostic_code == "not_registered"
    assert missing_authorization_result.status == "not_ready"
    assert missing_authorization_result.health[0].capability_id == ("authorization_provider:missing")
    assert optional_result.status == "ready"
    assert optional_result.health[0].status == "unhealthy"
    assert optional_result.health[0].diagnostic_code == "initialization_failed"


@pytest.mark.asyncio
async def test_lifecycle_counter_corruption_is_reported_not_ready(tmp_path) -> None:
    engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path / 'readiness.db'}")
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    store = RunRepository(factory)
    try:
        await store.initialize_lifecycle()
        assert await store.lifecycle_ready() is True
        async with factory() as session:
            await session.execute(delete(RunLifecycleCursorStateRow))
            await session.commit()

        missing = await store.lifecycle_readiness()
        assert missing.ready is False
        assert missing.reason_code == "lifecycle_cursor_missing"

        await store.initialize_lifecycle()
        await store.put("run-1", thread_id="thread-1")
        async with factory() as session:
            await session.execute(delete(RunLifecycleCursorStateRow))
            await session.commit()

        assert await store.lifecycle_ready() is False
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_lifecycle_readiness_rejects_invalid_pruning_and_event_bounds(
    tmp_path,
) -> None:
    engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path / 'readiness-bounds.db'}")
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    store = RunRepository(factory)
    try:
        await store.initialize_lifecycle()
        await store.put("run-1", thread_id="thread-1")
        assert (await store.lifecycle_readiness()).ready is True

        async with factory() as session:
            await session.execute(text("PRAGMA ignore_check_constraints = ON"))
            await session.execute(
                update(RunLifecycleCursorStateRow).values(
                    pruned_through=2,
                    last_cursor=1,
                )
            )
            await session.commit()
        invalid_pruning = await store.lifecycle_readiness()
        assert invalid_pruning.ready is False
        assert invalid_pruning.reason_code == "lifecycle_pruning_invalid"

        async with factory() as session:
            await session.execute(
                update(RunLifecycleCursorStateRow).values(
                    pruned_through=0,
                    last_cursor=2,
                )
            )
            await session.commit()
        invalid_events = await store.lifecycle_readiness()
        assert invalid_events.ready is False
        assert invalid_events.reason_code == "lifecycle_event_bounds_invalid"

        async with factory() as session:
            await session.execute(
                update(RunLifecycleCursorStateRow).values(
                    pruned_through=0,
                    last_cursor=1,
                )
            )
            await session.commit()
        await store.put("run-2", thread_id="thread-2")
        async with factory() as session:
            await session.execute(update(RunLifecycleEventRow).where(RunLifecycleEventRow.run_id == "run-2").values(cursor=3))
            await session.execute(update(RunLifecycleCursorStateRow).values(last_cursor=3))
            await session.commit()
        invalid_sequence = await store.lifecycle_readiness()
        assert invalid_sequence.ready is False
        assert invalid_sequence.reason_code == "lifecycle_event_sequence_invalid"
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_lifecycle_readiness_rejects_invalid_admission_cursor_authority(
    tmp_path,
) -> None:
    engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path / 'readiness-admission-cursor.db'}")
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    store = RunRepository(factory)
    try:
        await store.initialize_lifecycle()
        await store.put("run-1", thread_id="thread-1")

        async with factory.begin() as session:
            await session.execute(update(RunAdmissionCursorStateRow).values(last_cursor=0))
        stale = await store.lifecycle_readiness()
        assert stale.ready is False
        assert stale.reason_code == "admission_cursor_state_invalid"

        async with factory.begin() as session:
            await session.execute(update(RunAdmissionCursorStateRow).values(last_cursor=1))
        assert (await store.lifecycle_readiness()).ready is True
        await store.put("run-2", thread_id="thread-2")
        second = await store.get("run-2")
        assert second is not None
        assert second["admission_cursor"] == 2

        async with factory.begin() as session:
            await session.execute(delete(RunAdmissionCursorStateRow))
        missing = await store.lifecycle_readiness()
        assert missing.ready is False
        assert missing.reason_code == "admission_cursor_state_invalid"

        await store.initialize_lifecycle()
        assert (await store.lifecycle_readiness()).ready is True
        async with factory.begin() as session:
            await session.execute(text("PRAGMA ignore_check_constraints = ON"))
            session.add(
                RunAdmissionCursorStateRow(
                    singleton_id=2,
                    last_cursor=2,
                )
            )
        duplicate = await store.lifecycle_readiness()
        assert duplicate.ready is False
        assert duplicate.reason_code == "admission_cursor_state_invalid"
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_lifecycle_readiness_uses_only_bounded_edge_queries(tmp_path) -> None:
    engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path / 'readiness-query-bound.db'}")
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)
    store = RunRepository(async_sessionmaker(engine, expire_on_commit=False))
    statements: list[str] = []

    def record_statement(
        _connection,
        _cursor,
        statement,
        _parameters,
        _context,
        _executemany,
    ) -> None:
        statements.append(statement)

    try:
        await store.initialize_lifecycle()
        for index in range(6):
            await store.put(
                f"run-{index}",
                thread_id=f"thread-{index}",
            )
        event.listen(
            engine.sync_engine,
            "before_cursor_execute",
            record_statement,
        )

        assert (await store.lifecycle_readiness()).ready is True

        selects = [statement.upper() for statement in statements if statement.lstrip().upper().startswith("SELECT")]
        assert len(selects) == 4
        assert all(" LIMIT " in statement for statement in selects)
        assert all("COUNT(" not in statement for statement in selects)
    finally:
        event.remove(
            engine.sync_engine,
            "before_cursor_execute",
            record_statement,
        )
        await engine.dispose()


@pytest.mark.asyncio
async def test_lifecycle_readiness_rejects_deleted_interior_event_without_scanning(
    tmp_path,
) -> None:
    engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path / 'readiness-interior-gap.db'}")
    store = RunRepository(async_sessionmaker(engine, expire_on_commit=False))
    try:
        async with engine.begin() as connection:
            await connection.run_sync(Base.metadata.create_all)
        for index in range(6):
            await store.put(f"run-{index}", thread_id=f"thread-{index}")
        assert (await store.lifecycle_readiness()).ready is True

        async with engine.begin() as connection:
            await connection.execute(text("DELETE FROM run_lifecycle_events WHERE cursor = 3"))

        readiness = await store.lifecycle_readiness()
        assert readiness.ready is False
        assert readiness.reason_code == "lifecycle_event_cardinality_invalid"
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_gateway_liveness_is_independent_and_readiness_is_minimal(
    monkeypatch,
) -> None:
    import app.gateway.app as app_module
    import deerflow.extensions as extensions_module
    from deerflow.config.app_config import AppConfig
    from deerflow.config.sandbox_config import SandboxConfig

    config = AppConfig(sandbox=SandboxConfig(use="test"))
    monkeypatch.setattr(app_module, "get_app_config", lambda: config)
    monkeypatch.setattr(
        extensions_module,
        "load_extensions",
        lambda _plugins, **_kwargs: (ExtensionRegistry().build(), []),
    )
    app = app_module.create_app()

    class CorruptLifecycle:
        async def lifecycle_readiness(self):
            from deerflow.runtime.runs.store.base import LifecycleReadiness

            return LifecycleReadiness(
                False,
                "lifecycle_cursor_missing",
            )

    class HealthyLifecycle:
        async def lifecycle_readiness(self):
            from deerflow.runtime.runs.store.base import LifecycleReadiness

            return LifecycleReadiness(True)

    app.state.run_store = CorruptLifecycle()
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport,
        base_url="http://testserver",
    ) as client:
        liveness = await client.get("/health")
        readiness = await client.get("/ready")
        app.state.run_store = HealthyLifecycle()
        recovered = await client.get("/ready")

    assert liveness.status_code == 200
    assert liveness.json() == {
        "status": "healthy",
        "service": "deer-flow-gateway",
        "tenant_identity": {
            "version": 1,
            "public_ref": "tenant-fd1e0d1ead4a5e20",
            "digest": "fd1e0d1ead4a5e206a1ada1acb0a795d78857d325ec031014cb9bb99dff2abb9",
            "prefix_schema_version": 1,
        },
    }
    assert readiness.status_code == 503
    assert readiness.json() == {
        "status": "not_ready",
        "tenant_identity": liveness.json()["tenant_identity"],
        "extension_provenance": readiness.json()["extension_provenance"],
    }
    assert recovered.status_code == 200
    assert recovered.json() == {
        "status": "ready",
        "tenant_identity": liveness.json()["tenant_identity"],
        "extension_provenance": readiness.json()["extension_provenance"],
    }


@pytest.mark.asyncio
async def test_gateway_readiness_reports_optional_contextual_memory_degradation(
    monkeypatch,
) -> None:
    import app.gateway.app as app_module
    import deerflow.extensions as extensions_module
    from deerflow.config.app_config import AppConfig
    from deerflow.config.sandbox_config import SandboxConfig
    from deerflow.runtime.runs.store.base import LifecycleReadiness

    config = AppConfig(sandbox=SandboxConfig(use="test"))
    monkeypatch.setattr(app_module, "get_app_config", lambda: config)
    monkeypatch.setattr(
        extensions_module,
        "load_extensions",
        lambda _plugins, **_kwargs: (ExtensionRegistry().build(), []),
    )
    app = app_module.create_app()

    class HealthyLifecycle:
        async def lifecycle_readiness(self):
            return LifecycleReadiness(True)

    class DegradedContextualMemory:
        def safe_diagnostics(self):
            return {
                "backend": "honcho",
                "dependency_role": "mutable_contextual_memory",
                "durable_dependency": False,
                "operational_status": "degraded",
                "last_error_code": "honcho_memory_recall_failed",
            }

    app.state.run_store = HealthyLifecycle()
    app.state.memory_manager = DegradedContextualMemory()
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport,
        base_url="http://testserver",
    ) as client:
        response = await client.get("/ready")

    assert response.status_code == 200
    assert response.json()["status"] == "ready"
    assert response.json()["contextual_memory"] == {
        "backend": "honcho",
        "dependency_role": "mutable_contextual_memory",
        "durable_dependency": False,
        "operational_status": "degraded",
        "last_error_code": "honcho_memory_recall_failed",
    }


@pytest.mark.asyncio
async def test_gateway_deployment_report_reads_live_post_commit_status(
    monkeypatch,
) -> None:
    import app.gateway.app as app_module
    import deerflow.extensions as extensions_module
    from deerflow.config.app_config import AppConfig
    from deerflow.config.sandbox_config import SandboxConfig

    config = AppConfig(sandbox=SandboxConfig(use="test"))
    monkeypatch.setattr(app_module, "get_app_config", lambda: config)
    monkeypatch.setattr(
        extensions_module,
        "load_extensions",
        lambda _plugins, **_kwargs: (ExtensionRegistry().build(), []),
    )
    app = app_module.create_app()

    class StatusManager:
        @staticmethod
        def post_commit_obligation_status() -> PostCommitObligationStatus:
            return PostCommitObligationStatus(
                pending_admissions=1,
                pending_thread_operation_releases=2,
                pending_quarantines=1,
                resolved_admissions_since_start=3,
                resolved_thread_operation_releases_since_start=4,
            )

    app.state.run_manager = StatusManager()

    report = await app.state.deployment_reporter.deployment_report()

    assert report.to_dict()["post_commit_obligations"] == {
        "version": 1,
        "scope": "process_local",
        "window": "since_start",
        "pending_by_type": {
            "admission": 1,
            "thread_operation_release": 2,
        },
        "quarantined_identities": 1,
        "resolved_since_start_by_type": {
            "admission": 3,
            "thread_operation_release": 4,
        },
    }


@pytest.mark.asyncio
async def test_gateway_readiness_authenticates_required_remote_skill_profile(
    monkeypatch,
) -> None:
    import app.gateway.app as app_module
    import deerflow.extensions as extensions_module
    from deerflow.community.aio_sandbox.remote_backend import RemoteSandboxBackend
    from deerflow.config.app_config import AppConfig
    from deerflow.config.sandbox_config import SandboxConfig
    from deerflow.runtime.runs.store.base import LifecycleReadiness

    config = AppConfig(
        sandbox=SandboxConfig(
            use="deerflow.community.aio_sandbox:AioSandboxProvider",
            provisioner_url="http://provisioner:8002",
            provisioner_service_account_token_file="/projected/token",
            accepted_skill_projection_profile="rwx_verified_copy_v2",
        ),
    )
    monkeypatch.setattr(app_module, "get_app_config", lambda: config)
    monkeypatch.setattr(
        extensions_module,
        "load_extensions",
        lambda _plugins, **_kwargs: (ExtensionRegistry().build(), []),
    )
    results = iter((False, True))
    monkeypatch.setattr(
        RemoteSandboxBackend,
        "accepted_skill_projection_ready",
        lambda _self: next(results),
    )
    app = app_module.create_app()

    class HealthyLifecycle:
        async def lifecycle_readiness(self):
            return LifecycleReadiness(True)

    app.state.run_store = HealthyLifecycle()
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport,
        base_url="http://testserver",
    ) as client:
        unavailable = await client.get("/ready")
        recovered = await client.get("/ready")

    assert unavailable.status_code == 503
    assert recovered.status_code == 200


def test_admin_deployment_report_separates_immutable_manifest_from_mutable_health() -> None:
    manifest = build_capability_manifest(ExtensionRegistry().build(generation=14))
    checked_at = datetime(2026, 8, 7, tzinfo=UTC)

    class ChangingMonitor:
        def __init__(self) -> None:
            self.calls = 0

        async def health(self):
            self.calls += 1
            return (
                CapabilityHealthSnapshot(
                    contribution_id="policy",
                    capability_id="authorization_provider:policy",
                    status="healthy" if self.calls == 1 else "unhealthy",
                    diagnostic_code=("healthy" if self.calls == 1 else "upstream_unavailable"),
                    checked_at=checked_at,
                    expires_at=checked_at + timedelta(seconds=10),
                ),
            )

    def admin() -> User:
        return User(
            id=UUID("00000000-0000-0000-0000-000000000001"),
            email="runtime-admin@example.com",
            password_hash="x",
            system_role="admin",
        )

    app = make_authed_test_app(user_factory=admin)
    app.include_router(runtime_api.router)
    app.dependency_overrides[runtime_api.get_runtime_api] = lambda: type(
        "CapabilitiesAdapter",
        (),
        {"capabilities": staticmethod(RuntimeCapabilities)},
    )()
    app.state.deployment_reporter = GatewayDeploymentReporter(
        profile="local_development",
        database_backend="sqlite",
        atomic_lifecycle=True,
        manifest=manifest,
        health_monitor=ChangingMonitor(),
    )

    with TestClient(app) as client:
        portable = client.get("/api/runtime/v1/capabilities").json()
        first = client.get("/api/runtime/v1/deployment").json()
        second = client.get("/api/runtime/v1/deployment").json()

    assert record_from_dict(portable) == RuntimeCapabilities()
    assert first["extension_manifest"] == second["extension_manifest"]
    assert first["extension_manifest"]["manifest_digest"] == manifest.digest
    assert first["capability_health"]["snapshots"][0]["status"] == "healthy"
    assert second["capability_health"]["snapshots"][0]["status"] == "unhealthy"
