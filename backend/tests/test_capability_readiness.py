"""Operational health and readiness for authoritative capabilities."""

from __future__ import annotations

import asyncio
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
from fastapi.testclient import TestClient
from sqlalchemy import delete
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from app.gateway.auth.models import User
from app.gateway.routers import runtime_api
from deerflow.extensions.capabilities import (
    CapabilityHealthMonitor,
    CapabilityHealthSnapshot,
    build_capability_manifest,
)
from deerflow.extensions.registry import ExtensionRegistry
from deerflow.persistence.base import Base
from deerflow.persistence.run.model import RunLifecycleCursorStateRow
from deerflow.persistence.run.sql import RunRepository


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
        package_name="acme-policy",
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
        package_name="acme-authority",
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
        await store.put("run-1", thread_id="thread-1")
        async with factory() as session:
            await session.execute(delete(RunLifecycleCursorStateRow))
            await session.commit()

        assert await store.lifecycle_ready() is False
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
        lambda _plugins: (ExtensionRegistry().build(), []),
    )
    app = app_module.create_app()

    class CorruptLifecycle:
        async def lifecycle_ready(self) -> bool:
            return False

    app.state.run_store = CorruptLifecycle()
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport,
        base_url="http://testserver",
    ) as client:
        liveness = await client.get("/health")
        readiness = await client.get("/ready")

    assert liveness.status_code == 200
    assert liveness.json() == {
        "status": "healthy",
        "service": "deer-flow-gateway",
    }
    assert readiness.status_code == 503
    assert readiness.json() == {"status": "not_ready"}


def test_admin_capabilities_separates_immutable_manifest_from_mutable_health() -> None:
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
    app.state.capability_manifest = manifest
    app.state.capability_health_monitor = ChangingMonitor()

    with TestClient(app) as client:
        first = client.get("/api/runtime/v1/capabilities").json()
        second = client.get("/api/runtime/v1/capabilities").json()

    assert first["capability_manifest"] == second["capability_manifest"]
    assert first["capability_manifest"]["manifest_digest"] == manifest.digest
    assert first["capability_health"]["snapshots"][0]["status"] == "healthy"
    assert second["capability_health"]["snapshots"][0]["status"] == "unhealthy"
