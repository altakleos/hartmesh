"""Least-privilege service observation grants at the durable runtime seam."""

from __future__ import annotations

import logging
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

import pytest
from _router_auth_helpers import make_authed_test_app
from deerflow_extension_api import EffectiveSubjectV1, InvocationIdentityV1
from deerflow_runtime_api import (
    ContextInvocationsQuery,
    FailureCode,
    InvocationObservation,
    InvocationQuery,
    RuntimeFailure,
    record_from_dict,
)
from fastapi.testclient import TestClient
from pydantic import ValidationError
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from app.gateway.routers import runtime_api
from app.gateway.services import _GatewayDurableRuns
from app.runtime.api import InProcessInvocationRuntime, InvocationRuntimeAPI
from app.runtime.invocation import (
    InternalAuthorizationDecision,
    InternalSourceKind,
    InvocationPrincipal,
    InvocationRuntime,
)
from app.runtime.visibility import ServiceObservationGrant
from deerflow.config.authorization_config import (
    AuthorizationConfig,
    ServiceObservationGrantConfig,
)
from deerflow.persistence.base import Base
from deerflow.persistence.run.sql import RunRepository
from deerflow.runtime import RunManager
from deerflow.runtime.runs.lifecycle_query import LifecycleQuery, LifecycleVisibilityScope
from deerflow.runtime.runs.store.memory import MemoryRunStore


class _AllowObserve:
    def __init__(self) -> None:
        self.observed_run_ids: list[str] = []

    async def authorize_observe(self, record, _principal, *, target_kind="run"):
        self.observed_run_ids.append(record.run_id)
        return InternalAuthorizationDecision.allowed()

    async def authorize_context_observe(self, thread_id, _principal):
        self.observed_run_ids.append(f"context:{thread_id}")
        return InternalAuthorizationDecision.allowed()


class _StaticVisibilityResolver:
    def __init__(self, grant: ServiceObservationGrant | None) -> None:
        self.grant = grant

    async def resolve(self, _principal):
        return self.grant


class _DenyObserve(_AllowObserve):
    async def authorize_observe(self, record, _principal, *, target_kind="run"):
        self.observed_run_ids.append(record.run_id)
        return InternalAuthorizationDecision.denied()

    async def authorize_context_observe(self, thread_id, _principal):
        self.observed_run_ids.append(f"context:{thread_id}")
        return InternalAuthorizationDecision.denied()


def _service_principal(service_id: str = "service-1") -> InvocationPrincipal:
    return InvocationPrincipal(
        identity=InvocationIdentityV1(
            effective_subject=EffectiveSubjectV1(
                kind="service",
                subject_id=service_id,
                role="service",
            )
        )
    )


def _grant(service_id: str = "service-1", **selectors) -> ServiceObservationGrant:
    now = datetime.now(UTC)
    return ServiceObservationGrant(
        service_id=service_id,
        issued_at=now,
        valid_until=now + timedelta(seconds=30),
        **selectors,
    )


def _runtime_for_store(store, *, authorization=None, visibility=None) -> InvocationRuntime:
    class ThreadStore:
        async def get(self, _thread_id, *, user_id=None):
            return None

        async def check_access(self, _thread_id, _user_id, *, require_existing=False):
            return False

    request = SimpleNamespace(
        app=SimpleNamespace(
            state=SimpleNamespace(
                run_manager=RunManager(store=store),
                thread_store=ThreadStore(),
            )
        )
    )
    return InvocationRuntime(
        normalizer=object(),
        runs=_GatewayDurableRuns(request),
        authorization=authorization or _AllowObserve(),
        visibility=visibility,
    )


@pytest.mark.anyio
async def test_granted_service_observes_a_selected_human_owned_run() -> None:
    store = MemoryRunStore()
    await store.put(
        "human-run",
        thread_id="thread-a",
        user_id="human-1",
    )
    request = SimpleNamespace(
        app=SimpleNamespace(
            state=SimpleNamespace(
                run_manager=RunManager(store=store),
            )
        )
    )
    authorization = _AllowObserve()
    now = datetime.now(UTC)
    runtime = InvocationRuntime(
        normalizer=object(),
        runs=_GatewayDurableRuns(request),
        authorization=authorization,
        visibility=_StaticVisibilityResolver(
            ServiceObservationGrant(
                service_id="service-1",
                run_ids=("human-run",),
                issued_at=now,
                valid_until=now + timedelta(seconds=30),
            )
        ),
    )

    result = await InProcessInvocationRuntime(
        runtime,
        authenticated_service_id="service-1",
    ).observe(InvocationQuery(run_id="human-run"))

    assert isinstance(result, InvocationObservation)
    assert result.run_id == "human-run"
    assert result.thread_id == "thread-a"
    assert authorization.observed_run_ids == ["human-run"]


@pytest.mark.anyio
async def test_granted_service_observes_only_the_selected_human_context() -> None:
    store = MemoryRunStore()
    await store.put("run-a", thread_id="thread-a", user_id="human-1")
    await store.put("run-b", thread_id="thread-b", user_id="human-1")

    class ThreadStore:
        async def get(self, _thread_id, *, user_id=None):
            return None

        async def check_access(self, _thread_id, _user_id, *, require_existing=False):
            return False

    request = SimpleNamespace(
        app=SimpleNamespace(
            state=SimpleNamespace(
                run_manager=RunManager(store=store),
                thread_store=ThreadStore(),
            )
        )
    )
    authorization = _AllowObserve()
    now = datetime.now(UTC)
    runtime = InvocationRuntime(
        normalizer=object(),
        runs=_GatewayDurableRuns(request),
        authorization=authorization,
        visibility=_StaticVisibilityResolver(
            ServiceObservationGrant(
                service_id="service-1",
                thread_ids=("thread-a",),
                issued_at=now,
                valid_until=now + timedelta(seconds=30),
            )
        ),
    )
    adapter = InProcessInvocationRuntime(runtime, authenticated_service_id="service-1")

    allowed = await adapter.observe(ContextInvocationsQuery(thread_id="thread-a"))
    hidden = await adapter.observe(ContextInvocationsQuery(thread_id="thread-b"))

    assert isinstance(allowed, InvocationObservation)
    assert tuple(event["run_id"] for event in allowed.events) == ("run-a",)
    assert hidden.code == "not_found_or_invisible"
    assert authorization.observed_run_ids == ["context:thread-a"]


@pytest.mark.anyio
async def test_operator_grant_revocation_blocks_the_next_observation() -> None:
    from app.runtime.visibility import ConfiguredServiceObservationGrantResolver

    configured = AuthorizationConfig(
        enabled=True,
        invocation_operations={"observe_enabled": True},
        service_observation_grants=[
            {
                "service_id": "service-1",
                "thread_ids": ["thread-a"],
            }
        ],
    )
    current: tuple[ServiceObservationGrantConfig, ...] = configured.service_observation_grants
    resolver = ConfiguredServiceObservationGrantResolver(lambda: current)
    store = MemoryRunStore()
    await store.put("run-a", thread_id="thread-a", user_id="human-1")

    class ThreadStore:
        async def get(self, _thread_id, *, user_id=None):
            return None

        async def check_access(self, _thread_id, _user_id, *, require_existing=False):
            return False

    request = SimpleNamespace(
        app=SimpleNamespace(
            state=SimpleNamespace(
                run_manager=RunManager(store=store),
                thread_store=ThreadStore(),
            )
        )
    )
    runtime = InvocationRuntime(
        normalizer=object(),
        runs=_GatewayDurableRuns(request),
        authorization=_AllowObserve(),
        visibility=resolver,
    )
    adapter = InProcessInvocationRuntime(runtime, authenticated_service_id="service-1")

    allowed = await adapter.observe(ContextInvocationsQuery(thread_id="thread-a"))
    current = ()
    revoked = await adapter.observe(ContextInvocationsQuery(thread_id="thread-a"))

    assert isinstance(allowed, InvocationObservation)
    assert revoked.code == "not_found_or_invisible"


@pytest.mark.anyio
async def test_gateway_service_runtime_uses_the_application_visibility_resolver() -> None:
    from app.gateway.services import build_service_invocation_runtime
    from app.runtime.visibility import ConfiguredServiceObservationGrantResolver

    config = AuthorizationConfig(
        enabled=True,
        invocation_operations={"observe_enabled": True},
        service_observation_grants=[
            {"service_id": "service-1", "run_ids": ["human-run"]},
        ],
    )
    store = MemoryRunStore()
    await store.put("human-run", thread_id="thread-a", user_id="human-1")

    class ThreadStore:
        async def get(self, _thread_id, *, user_id=None):
            return None

        async def check_access(self, _thread_id, _user_id, *, require_existing=False):
            return False

    app = SimpleNamespace(
        state=SimpleNamespace(
            run_manager=RunManager(store=store),
            thread_store=ThreadStore(),
            runtime_readiness=None,
            service_observation_visibility_resolver=(
                ConfiguredServiceObservationGrantResolver(
                    lambda: config.service_observation_grants,
                )
            ),
        )
    )
    adapter = InProcessInvocationRuntime(
        build_service_invocation_runtime(
            app,
            authenticated_service_id="service-1",
        ),
        authenticated_service_id="service-1",
    )

    result = await adapter.observe(InvocationQuery(run_id="human-run"))

    assert isinstance(result, InvocationObservation)
    assert result.run_id == "human-run"


@pytest.mark.anyio
async def test_ordinary_service_cannot_observe_another_owner_or_trigger_policy() -> None:
    store = MemoryRunStore()
    await store.put("human-run", thread_id="thread-a", user_id="human-1")
    authorization = _AllowObserve()
    adapter = InProcessInvocationRuntime(
        _runtime_for_store(store, authorization=authorization),
        authenticated_service_id="service-1",
    )

    existing = await adapter.observe(InvocationQuery(run_id="human-run"))
    missing = await adapter.observe(InvocationQuery(run_id="missing-run"))

    assert existing == missing
    assert isinstance(existing, RuntimeFailure)
    assert existing.code is FailureCode.not_found_or_invisible
    assert authorization.observed_run_ids == []


@pytest.mark.anyio
@pytest.mark.parametrize("query", [InvocationQuery(run_id="human-run"), ContextInvocationsQuery(thread_id="thread-a")])
async def test_current_authorization_denial_overrides_a_valid_visibility_grant(query) -> None:
    store = MemoryRunStore()
    await store.put("human-run", thread_id="thread-a", user_id="human-1")
    authorization = _DenyObserve()
    adapter = InProcessInvocationRuntime(
        _runtime_for_store(
            store,
            authorization=authorization,
            visibility=_StaticVisibilityResolver(_grant(thread_ids=("thread-a",))),
        ),
        authenticated_service_id="service-1",
    )

    result = await adapter.observe(query)

    assert isinstance(result, RuntimeFailure)
    assert result.code is FailureCode.denied


@pytest.mark.anyio
@pytest.mark.parametrize("resolver_kind", ["unavailable", "malformed", "stale", "wrong-service"])
async def test_invalid_visibility_resolution_fails_closed(resolver_kind: str) -> None:
    store = MemoryRunStore()
    await store.put("human-run", thread_id="thread-a", user_id="human-1")
    now = datetime.now(UTC)

    class Resolver:
        async def resolve(self, _principal):
            if resolver_kind == "unavailable":
                raise RuntimeError("private resolver failure")
            if resolver_kind == "malformed":
                return {"run_ids": ["human-run"]}
            if resolver_kind == "stale":
                return ServiceObservationGrant(
                    service_id="service-1",
                    run_ids=("human-run",),
                    issued_at=now - timedelta(minutes=2),
                    valid_until=now - timedelta(minutes=1),
                )
            return _grant("service-2", run_ids=("human-run",))

    adapter = InProcessInvocationRuntime(
        _runtime_for_store(store, visibility=Resolver()),
        authenticated_service_id="service-1",
    )

    result = await adapter.observe(InvocationQuery(run_id="human-run"))

    assert isinstance(result, RuntimeFailure)
    assert result.code is FailureCode.indeterminate


@pytest.mark.anyio
async def test_visibility_resolver_exception_has_safe_correlated_diagnostic(caplog) -> None:
    store = MemoryRunStore()
    await store.put("human-run", thread_id="thread-a", user_id="human-1")

    class Resolver:
        async def resolve(self, _principal):
            raise RuntimeError("credential=do-not-log")

    adapter = InProcessInvocationRuntime(
        _runtime_for_store(store, visibility=Resolver()),
        authenticated_service_id="service-1",
    )

    with caplog.at_level(logging.WARNING, logger="app.runtime.visibility"):
        result = await adapter.observe(InvocationQuery(run_id="human-run"))

    assert isinstance(result, RuntimeFailure)
    assert result.code is FailureCode.indeterminate
    record = next(item for item in caplog.records if item.message == "Service observation visibility resolution failed")
    assert record.observer_service_id == "service-1"
    assert record.diagnostic_code == "visibility_resolution_failed"
    assert record.error_class == "RuntimeError"
    assert len(record.correlation_id) == 32
    assert "do-not-log" not in caplog.text


def test_service_observation_config_rejects_unbounded_or_duplicate_scope() -> None:
    with pytest.raises(ValidationError, match="128 aggregate selectors"):
        AuthorizationConfig(
            enabled=True,
            invocation_operations={"observe_enabled": True},
            service_observation_grants=[
                {
                    "service_id": "service-1",
                    "run_ids": [f"run-{index}" for index in range(129)],
                }
            ],
        )
    now = datetime.now(UTC)
    with pytest.raises(ValueError, match="lifetime exceeds"):
        ServiceObservationGrant(
            service_id="service-1",
            run_ids=("run-a",),
            issued_at=now,
            valid_until=now + timedelta(seconds=31),
        )
    with pytest.raises(ValidationError, match="duplicate service_id"):
        AuthorizationConfig(
            enabled=True,
            invocation_operations={"observe_enabled": True},
            service_observation_grants=[
                {"service_id": "service-1", "run_ids": ["run-a"]},
                {"service_id": "service-1", "run_ids": ["run-b"]},
            ],
        )


def test_service_observation_grant_requires_current_observe_authorization() -> None:
    with pytest.raises(ValidationError, match="observe authorization"):
        AuthorizationConfig(
            service_observation_grants=[
                {"service_id": "service-1", "run_ids": ["run-a"]},
            ]
        )


@pytest.mark.parametrize(
    "field",
    ["role", "owner_ids", "visibility_prevalidated", "source_facts"],
)
def test_portable_observation_query_cannot_carry_trusted_visibility_fields(field: str) -> None:
    payload = InvocationQuery(run_id="run-a").to_dict()
    payload[field] = True if field == "visibility_prevalidated" else "forged"

    with pytest.raises(ValueError, match="unknown fields"):
        InvocationQuery.from_dict(payload)


def test_internal_visibility_scope_cannot_be_reused_for_another_context() -> None:
    scope = LifecycleVisibilityScope(
        thread_id="thread-a",
        allow_context=True,
    )

    with pytest.raises(ValueError, match="exact context"):
        LifecycleQuery(
            thread_id="thread-b",
            visibility_scope=scope,
        )


@pytest.mark.anyio
async def test_role_or_internal_flag_cannot_forge_service_visibility() -> None:
    store = MemoryRunStore()
    await store.put("human-run", thread_id="thread-a", user_id="human-1")

    class CountingResolver:
        calls = 0

        async def resolve(self, _principal):
            self.calls += 1
            return _grant(run_ids=("human-run",))

    resolver = CountingResolver()
    runtime = _runtime_for_store(store, visibility=resolver)
    forged = InvocationPrincipal(
        user_id="service-1",
        role="service",
        is_internal=True,
    )

    result = await runtime.observe_run("human-run", forged)

    assert result.value == "not_found_or_invisible"
    assert resolver.calls == 0


@pytest.mark.anyio
async def test_context_pagination_stays_inside_the_finite_owner_scope() -> None:
    store = MemoryRunStore()
    await store.put("allowed-1", thread_id="thread-a", user_id="human-1")
    await store.put("hidden", thread_id="thread-a", user_id="human-2")
    await store.put("allowed-2", thread_id="thread-a", user_id="human-1")
    adapter = InProcessInvocationRuntime(
        _runtime_for_store(
            store,
            visibility=_StaticVisibilityResolver(_grant(owner_ids=("human-1",))),
        ),
        authenticated_service_id="service-1",
    )

    first = await adapter.observe(ContextInvocationsQuery(thread_id="thread-a", limit=1))
    second = await adapter.observe(
        ContextInvocationsQuery(
            thread_id="thread-a",
            cursor=first.next_cursor,
            limit=1,
        )
    )

    assert isinstance(first, InvocationObservation)
    assert isinstance(second, InvocationObservation)
    assert tuple(event["run_id"] for event in first.events) == ("allowed-1",)
    assert tuple(event["run_id"] for event in second.events) == ("allowed-2",)
    assert all(event["run_id"] != "hidden" for event in (*first.events, *second.events))


@pytest.mark.anyio
@pytest.mark.parametrize("store_kind", ["memory", "sql"])
async def test_store_filters_source_kind_before_bounded_page_materialization(
    store_kind: str,
    tmp_path,
) -> None:
    engine = None
    if store_kind == "memory":
        store = MemoryRunStore()
    else:
        engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path / 'visibility.db'}")
        async with engine.begin() as connection:
            await connection.run_sync(Base.metadata.create_all)
        store = RunRepository(async_sessionmaker(engine, expire_on_commit=False))
    try:
        await store.put(
            "channel-run",
            thread_id="thread-a",
            user_id="human-1",
            status="success",
            origin_json={"version": 1, "source_kind": "native_channel", "references": {}},
        )
        await store.put(
            "http-run",
            thread_id="thread-a",
            user_id="human-2",
            status="success",
            origin_json={"version": 1, "source_kind": "http", "references": {}},
        )

        scope = LifecycleVisibilityScope(
            thread_id="thread-a",
            source_kinds=("native_channel",),
        )
        page = await store.query_lifecycle(
            LifecycleQuery(
                thread_id="thread-a",
                visibility_scope=scope,
                limit=1,
            )
        )

        assert tuple(event["run_id"] for event in page.events) == ("channel-run",)
        assert await store.context_visible_in_scope("thread-a", scope) is True
        assert (
            await store.context_visible_in_scope(
                "thread-a",
                LifecycleVisibilityScope(
                    thread_id="thread-a",
                    source_kinds=("scheduled_task",),
                ),
            )
            is False
        )
    finally:
        if engine is not None:
            await engine.dispose()


@pytest.mark.anyio
async def test_granted_observation_audit_is_bounded_and_contains_no_target(caplog) -> None:
    store = MemoryRunStore()
    await store.put("private-run", thread_id="private-thread", user_id="human-1")
    adapter = InProcessInvocationRuntime(
        _runtime_for_store(
            store,
            visibility=_StaticVisibilityResolver(_grant(run_ids=("private-run",))),
        ),
        authenticated_service_id="service-1",
    )

    with caplog.at_level(logging.INFO, logger="app.runtime.visibility"):
        result = await adapter.observe(InvocationQuery(run_id="private-run"))

    assert isinstance(result, InvocationObservation)
    record = next(item for item in caplog.records if item.message == "Scoped service invocation observation evaluated")
    assert record.observer_service_id == "service-1"
    assert len(record.visibility_evidence_digest) == 64
    assert record.authorization_outcome == "allowed"
    assert len(record.correlation_id) == 32
    assert "private-run" not in record.getMessage()
    assert "private-thread" not in record.getMessage()


@pytest.mark.anyio
async def test_http_and_in_process_adapters_return_identical_granted_observations() -> None:
    store = MemoryRunStore()
    await store.put("human-run", thread_id="thread-a", user_id="human-1")
    runtime = _runtime_for_store(
        store,
        visibility=_StaticVisibilityResolver(_grant(run_ids=("human-run",))),
    )
    in_process = InProcessInvocationRuntime(runtime, authenticated_service_id="service-1")

    app = make_authed_test_app()
    app.include_router(runtime_api.router)
    app.dependency_overrides[runtime_api.get_runtime_api] = lambda: InvocationRuntimeAPI(
        runtime,
        principal=_service_principal(),
        source_kind=InternalSourceKind.service,
        trusted_service_id="service-1",
    )
    with TestClient(app) as client:
        response = client.get("/api/runtime/v1/invocations/human-run")

    expected = await in_process.observe(InvocationQuery(run_id="human-run"))
    actual = record_from_dict(response.json())
    assert response.status_code == 200
    assert actual == expected
