"""HTTP transport tests for ``deerflow.runtime/v1``."""

from __future__ import annotations

from uuid import UUID

import pytest
from _router_auth_helpers import make_authed_test_app
from deerflow_runtime_api import (
    CancelInvocationRequest,
    ContextInvocationsQuery,
    ControlDisposition,
    EnsureDisposition,
    FailureCode,
    InvocationControlReceipt,
    InvocationEnsureReceipt,
    InvocationObservation,
    InvocationQuery,
    RuntimeCapabilities,
    RuntimeFailure,
)
from fastapi.testclient import TestClient

from app.gateway.auth.models import User
from app.gateway.routers import runtime_api


def _admin_user() -> User:
    return User(
        id=UUID("00000000-0000-0000-0000-000000000001"),
        email="runtime-admin@example.com",
        password_hash="x",
        system_role="admin",
    )


def test_admin_can_read_exact_runtime_capabilities() -> None:
    app = make_authed_test_app(user_factory=_admin_user)
    app.include_router(runtime_api.router)
    app.dependency_overrides[runtime_api.get_runtime_api] = lambda: _CapabilitiesAdapter()

    with TestClient(app) as client:
        response = client.get("/api/runtime/v1/capabilities")

    assert response.status_code == 200
    assert response.json() == {
        "api_version": "deerflow.runtime/v1",
        "kind": "runtime.capabilities",
        "ensure": True,
        "observe_invocation": True,
        "observe_context": True,
        "controls": ["cancel"],
        "context_export": False,
        "context_retirement": False,
    }


@pytest.mark.parametrize(
    ("operation", "method", "path", "payload"),
    [
        ("capabilities", "get", "/api/runtime/v1/capabilities", None),
        ("ensure", "post", "/api/runtime/v1/invocations/ensure", "ensure"),
        ("observe", "get", "/api/runtime/v1/invocations/run-1", None),
        ("observe", "get", "/api/runtime/v1/contexts/thread-1/invocations", None),
        (
            "control",
            "post",
            "/api/runtime/v1/invocations/run-1/control",
            "control",
        ),
    ],
)
def test_every_runtime_port_operation_uses_one_redacted_failure_envelope(
    operation,
    method,
    path,
    payload,
    caplog,
) -> None:
    marker = "provider-secret-runtime-http-marker"

    class ThrowingPort:
        def capabilities(self):
            if operation == "capabilities":
                raise RuntimeError(marker)
            return RuntimeCapabilities()

        async def ensure(self, _request):
            if operation == "ensure":
                raise RuntimeError(marker)
            raise AssertionError("unexpected ensure call")

        async def observe(self, _request):
            if operation == "observe":
                raise RuntimeError(marker)
            raise AssertionError("unexpected observe call")

        async def control(self, _request):
            if operation == "control":
                raise RuntimeError(marker)
            raise AssertionError("unexpected control call")

    app = _app_with_adapter(ThrowingPort(), user_factory=_admin_user)
    with caplog.at_level("ERROR", logger="app.runtime.api"):
        with TestClient(app, raise_server_exceptions=False) as client:
            response = client.request(
                method,
                path,
                json=(None if payload is None else (_ensure_payload() if payload == "ensure" else _cancel_payload())),
            )

    assert response.status_code == 503
    assert response.headers["content-type"].startswith("application/json")
    body = response.json()
    assert body == {
        "api_version": "deerflow.runtime/v1",
        "kind": "runtime.error",
        "code": "indeterminate",
        "details": {"correlation_id": body["details"]["correlation_id"]},
    }
    assert len(body["details"]["correlation_id"]) == 32
    assert marker not in response.text
    assert marker not in caplog.text
    record = next(record for record in caplog.records if getattr(record, "runtime_operation", None) == operation)
    assert record.correlation_id == body["details"]["correlation_id"]
    assert record.exception_class == "RuntimeError"


class _CapabilitiesAdapter:
    def capabilities(self) -> RuntimeCapabilities:
        return RuntimeCapabilities()


def _ensure_payload() -> dict:
    return {
        "api_version": "deerflow.runtime/v1",
        "kind": "invocation.ensure",
        "external_key": "delivery-1",
        "thread_id": "thread-1",
        "agent_hint": None,
        "input": {
            "api_version": "deerflow.runtime/v1",
            "kind": "invocation.input.graph",
            "value": {"messages": []},
        },
        "options": {
            "api_version": "deerflow.runtime/v1",
            "kind": "invocation.options",
            "model_name": None,
            "thinking_enabled": None,
            "multitask_strategy": "reject",
            "checkpoint_id": None,
            "interrupt_before": None,
            "interrupt_after": None,
        },
    }


def _app_with_adapter(adapter, *, user_factory=None):
    app = make_authed_test_app(user_factory=user_factory)
    app.include_router(runtime_api.router)
    app.dependency_overrides[runtime_api.get_runtime_api] = lambda: adapter
    return app


def test_ensure_created_maps_to_201_with_the_exact_receipt() -> None:
    class Adapter:
        async def ensure(self, request):
            assert request.external_key == "delivery-1"
            return InvocationEnsureReceipt(
                disposition="created",
                run_id="run-1",
                thread_id="thread-1",
                status="pending",
                state_version=1,
            )

    app = make_authed_test_app()
    app.include_router(runtime_api.router)
    app.dependency_overrides[runtime_api.get_runtime_api] = lambda: Adapter()

    with TestClient(app) as client:
        response = client.post(
            "/api/runtime/v1/invocations/ensure",
            json=_ensure_payload(),
        )

    assert response.status_code == 201
    assert response.json() == {
        "api_version": "deerflow.runtime/v1",
        "kind": "invocation.ensure.receipt",
        "disposition": "created",
        "run_id": "run-1",
        "thread_id": "thread-1",
        "status": "pending",
        "state_version": 1,
    }


def test_invocation_observation_maps_cursor_and_read_fence() -> None:
    class Adapter:
        async def observe(self, query):
            assert query == InvocationQuery(
                run_id="run-1",
                cursor="lc1.MQ",
                limit=25,
            )
            return InvocationObservation(
                run_id="run-1",
                thread_id="thread-1",
                status="running",
                state_version=2,
                snapshots=(
                    {
                        "run_id": "run-1",
                        "thread_id": "thread-1",
                        "status": "running",
                        "state_version": 2,
                    },
                ),
                events=(),
                next_cursor="lc1.Mg",
                minimum_available_cursor="lc1.MA",
                read_fence_cursor="lc1.Mg",
            )

    app = make_authed_test_app()
    app.include_router(runtime_api.router)
    app.dependency_overrides[runtime_api.get_runtime_api] = lambda: Adapter()

    with TestClient(app) as client:
        response = client.get(
            "/api/runtime/v1/invocations/run-1",
            params={"cursor": "lc1.MQ", "limit": 25},
        )

    assert response.status_code == 200
    assert response.json()["read_fence_cursor"] == "lc1.Mg"
    assert response.json()["state_version"] == 2


def test_context_observation_defaults_to_a_100_event_page() -> None:
    class Adapter:
        async def observe(self, query):
            assert query == ContextInvocationsQuery(thread_id="thread-1", limit=100)
            return InvocationObservation(
                run_id=None,
                thread_id="thread-1",
                status=None,
                state_version=None,
                snapshots=(),
                events=(),
                next_cursor="lc1.Nw",
                minimum_available_cursor="lc1.MA",
                read_fence_cursor="lc1.Nw",
            )

    app = make_authed_test_app()
    app.include_router(runtime_api.router)
    app.dependency_overrides[runtime_api.get_runtime_api] = lambda: Adapter()

    with TestClient(app) as client:
        response = client.get("/api/runtime/v1/contexts/thread-1/invocations")

    assert response.status_code == 200
    assert response.json()["thread_id"] == "thread-1"
    assert response.json()["next_cursor"] == "lc1.Nw"


def _cancel_payload(*, run_id: str = "run-1") -> dict:
    return {
        "api_version": "deerflow.runtime/v1",
        "kind": "invocation.cancel",
        "run_id": run_id,
        "expected_state_version": 2,
        "action": "interrupt",
    }


def test_cancel_requested_maps_to_202_and_preserves_the_fenced_state() -> None:
    class Adapter:
        async def control(self, command):
            assert command == CancelInvocationRequest(
                run_id="run-1",
                expected_state_version=2,
            )
            return InvocationControlReceipt(
                disposition="requested",
                run_id="run-1",
                thread_id="thread-1",
                status="running",
                state_version=3,
            )

    app = make_authed_test_app()
    app.include_router(runtime_api.router)
    app.dependency_overrides[runtime_api.get_runtime_api] = lambda: Adapter()

    with TestClient(app) as client:
        response = client.post(
            "/api/runtime/v1/invocations/run-1/control",
            json=_cancel_payload(),
        )

    assert response.status_code == 202
    assert response.json()["disposition"] == "requested"
    assert response.json()["state_version"] == 3


def test_gateway_mounts_runtime_routes_without_replacing_legacy_runs() -> None:
    from app.gateway.app import create_app

    paths = {route.path for route in create_app().routes}

    assert "/api/runtime/v1/capabilities" in paths
    assert "/api/runtime/v1/deployment" in paths
    assert "/api/runtime/v1/invocations/ensure" in paths
    assert "/api/runtime/v1/invocations/{run_id}" in paths
    assert "/api/runtime/v1/contexts/{thread_id}/invocations" in paths
    assert "/api/runtime/v1/invocations/{run_id}/control" in paths
    assert "/api/runs/stream" in paths
    assert "/api/threads/{thread_id}/runs" in paths


def test_capabilities_requires_an_authenticated_administrator() -> None:
    app = make_authed_test_app()
    app.include_router(runtime_api.router)

    with TestClient(app) as client:
        response = client.get("/api/runtime/v1/capabilities")

    assert response.status_code == 403
    assert response.json() == {
        "api_version": "deerflow.runtime/v1",
        "kind": "runtime.error",
        "code": "denied",
    }


def test_unauthenticated_runtime_requests_keep_the_runtime_error_envelope(
    monkeypatch,
) -> None:
    from fastapi import FastAPI

    from app.gateway import auth_middleware
    from app.gateway.auth_middleware import AuthMiddleware

    monkeypatch.setattr(auth_middleware, "is_auth_disabled", lambda: False)
    app = FastAPI()
    app.add_middleware(AuthMiddleware)
    app.include_router(runtime_api.router)

    with TestClient(app) as client:
        response = client.get("/api/runtime/v1/invocations/run-1")

    assert response.status_code == 401
    assert response.json() == {
        "api_version": "deerflow.runtime/v1",
        "kind": "runtime.error",
        "code": "denied",
    }


def test_runtime_csrf_failures_keep_the_runtime_error_envelope(monkeypatch) -> None:
    from fastapi import FastAPI

    from app.gateway import csrf_middleware
    from app.gateway.csrf_middleware import CSRFMiddleware

    monkeypatch.setattr(csrf_middleware, "is_auth_disabled", lambda: False)
    app = FastAPI()
    app.add_middleware(CSRFMiddleware)
    app.include_router(runtime_api.router)

    with TestClient(app) as client:
        response = client.post(
            "/api/runtime/v1/invocations/ensure",
            json=_ensure_payload(),
        )

    assert response.status_code == 403
    assert response.json() == {
        "api_version": "deerflow.runtime/v1",
        "kind": "runtime.error",
        "code": "denied",
    }


def test_runtime_path_validation_keeps_the_runtime_error_envelope() -> None:
    from app.gateway.runtime_http import install_runtime_error_handlers

    class Adapter:
        async def observe(self, _query):
            raise AssertionError("invalid thread ids must not reach the runtime")

    app = _app_with_adapter(Adapter())
    install_runtime_error_handlers(app)

    with TestClient(app) as client:
        response = client.get("/api/runtime/v1/contexts/not.valid/invocations")

    assert response.status_code == 422
    assert response.json() == {
        "api_version": "deerflow.runtime/v1",
        "kind": "runtime.error",
        "code": "invalid_request",
    }


@pytest.mark.parametrize(
    ("result", "status_code", "kind", "code"),
    [
        (
            InvocationEnsureReceipt(
                disposition=EnsureDisposition.known,
                run_id="run-1",
                thread_id="thread-1",
                status="success",
                state_version=4,
            ),
            200,
            "invocation.ensure.receipt",
            None,
        ),
        (InvocationEnsureReceipt(disposition=EnsureDisposition.conflict), 409, "runtime.error", "conflict"),
        (InvocationEnsureReceipt(disposition=EnsureDisposition.denied), 403, "runtime.error", "denied"),
        (InvocationEnsureReceipt(disposition=EnsureDisposition.indeterminate), 503, "runtime.error", "indeterminate"),
        (InvocationEnsureReceipt(disposition=EnsureDisposition.thread_busy), 409, "runtime.error", "thread_busy"),
    ],
)
def test_ensure_maps_every_remaining_disposition(result, status_code, kind, code) -> None:
    class Adapter:
        async def ensure(self, _request):
            return result

    with TestClient(_app_with_adapter(Adapter())) as client:
        response = client.post("/api/runtime/v1/invocations/ensure", json=_ensure_payload())

    assert response.status_code == status_code
    assert response.json()["kind"] == kind
    if code is not None:
        assert response.json() == {
            "api_version": "deerflow.runtime/v1",
            "kind": "runtime.error",
            "code": code,
        }


@pytest.mark.parametrize(
    "mutate",
    [
        lambda body: body.update(api_version="deerflow.runtime/v2"),
        lambda body: body.update(kind="invocation.query"),
        lambda body: body.update(external_scope="caller-controlled"),
        lambda body: body.pop("external_key"),
        lambda body: body.update(external_key=""),
        lambda body: body["input"].update(kind="invocation.input.unknown"),
        lambda body: body["options"].update(unexpected=True),
    ],
)
def test_ensure_rejects_wrong_versions_kinds_fields_and_inputs(mutate) -> None:
    class Adapter:
        async def ensure(self, _request):
            raise AssertionError("invalid requests must not reach admission")

    payload = _ensure_payload()
    mutate(payload)
    with TestClient(_app_with_adapter(Adapter())) as client:
        response = client.post("/api/runtime/v1/invocations/ensure", json=payload)

    assert response.status_code == 422
    assert response.json() == {
        "api_version": "deerflow.runtime/v1",
        "kind": "runtime.error",
        "code": "invalid_request",
    }


@pytest.mark.parametrize(
    ("failure", "status_code", "details"),
    [
        (RuntimeFailure(FailureCode.not_found_or_invisible, {"version": 1}), 404, None),
        (RuntimeFailure(FailureCode.denied, {"version": 1}), 403, None),
        (RuntimeFailure(FailureCode.indeterminate, {"version": 1}), 503, None),
        (RuntimeFailure(FailureCode.invalid_request, {"version": 1}), 422, None),
        (
            RuntimeFailure(
                FailureCode.cursor_gap,
                {"version": 1, "minimum_available_cursor": "lc1.NQ"},
            ),
            410,
            {"minimum_available_cursor": "lc1.NQ"},
        ),
        (
            RuntimeFailure(
                FailureCode.cursor_ahead,
                {"version": 1, "read_fence_cursor": "lc1.OA"},
            ),
            422,
            {"read_fence_cursor": "lc1.OA"},
        ),
    ],
)
def test_observe_uses_one_error_envelope_for_policy_visibility_and_cursor_failures(
    failure,
    status_code,
    details,
) -> None:
    class Adapter:
        async def observe(self, _query):
            return failure

    with TestClient(_app_with_adapter(Adapter())) as client:
        response = client.get("/api/runtime/v1/invocations/run-1")

    expected = {
        "api_version": "deerflow.runtime/v1",
        "kind": "runtime.error",
        "code": failure.code.value,
    }
    if details is not None:
        expected["details"] = details
    assert response.status_code == status_code
    assert response.json() == expected
    assert "detail" not in response.json()


@pytest.mark.parametrize(
    "query",
    [
        "limit=0",
        "limit=501",
        "limit=1.5",
        "limit=01",
        "cursor=",
        "unknown=value",
        "limit=10&limit=11",
    ],
)
def test_context_paging_rejects_values_outside_the_exact_http_contract(query: str) -> None:
    class Adapter:
        async def observe(self, _query):
            raise AssertionError("invalid paging must not reach the runtime")

    with TestClient(_app_with_adapter(Adapter())) as client:
        response = client.get(f"/api/runtime/v1/contexts/thread-1/invocations?{query}")

    assert response.status_code == 422
    assert response.json()["code"] == "invalid_request"


@pytest.mark.parametrize(
    ("disposition", "status_code", "visible"),
    [
        (ControlDisposition.already_requested, 200, True),
        (ControlDisposition.already_terminal, 200, True),
        (ControlDisposition.stale, 409, False),
        (ControlDisposition.not_found_or_invisible, 404, False),
        (ControlDisposition.denied, 403, False),
        (ControlDisposition.indeterminate, 503, False),
    ],
)
def test_control_maps_every_remaining_race_and_policy_disposition(
    disposition,
    status_code,
    visible,
) -> None:
    class Adapter:
        async def control(self, _command):
            if visible or disposition is ControlDisposition.stale:
                return InvocationControlReceipt(
                    disposition=disposition,
                    run_id="run-1",
                    thread_id="thread-1",
                    status=("running" if disposition in {ControlDisposition.already_requested, ControlDisposition.stale} else "success"),
                    state_version=3,
                )
            return InvocationControlReceipt(disposition=disposition)

    with TestClient(_app_with_adapter(Adapter())) as client:
        response = client.post(
            "/api/runtime/v1/invocations/run-1/control",
            json=_cancel_payload(),
        )

    assert response.status_code == status_code
    if visible:
        assert response.json()["kind"] == "invocation.control.receipt"
        assert response.json()["state_version"] == 3
    else:
        assert response.json() == {
            "api_version": "deerflow.runtime/v1",
            "kind": "runtime.error",
            "code": disposition.value,
        }


def test_control_rejects_a_path_body_run_id_mismatch_before_mutation() -> None:
    class Adapter:
        async def control(self, _command):
            raise AssertionError("mismatched commands must not mutate")

    with TestClient(_app_with_adapter(Adapter())) as client:
        response = client.post(
            "/api/runtime/v1/invocations/run-1/control",
            json=_cancel_payload(run_id="run-2"),
        )

    assert response.status_code == 422
    assert response.json()["code"] == "invalid_request"


def test_http_adapter_binds_the_current_gateway_principal_and_http_source(monkeypatch) -> None:
    from app.gateway.auth_disabled import AUTH_SOURCE_SESSION
    from app.runtime.invocation import (
        InternalSourceKind,
        InvocationAuthorizationOutcome,
        NotFoundOrInvisible,
    )

    captured = {}

    class Runtime:
        async def launch(self, intent):
            captured["source_kind"] = intent.source_kind
            captured["trusted_service_id"] = intent.trusted_service_id
            return InvocationAuthorizationOutcome.denied

        async def observe_invocation_lifecycle(self, query):
            captured["principal"] = query.principal
            return NotFoundOrInvisible.not_found_or_invisible

        async def cancel_run(self, command):
            captured["cancel_principal"] = command.principal
            return NotFoundOrInvisible.not_found_or_invisible

    user = User(
        id=UUID("00000000-0000-0000-0000-000000000042"),
        email="runtime-user@example.com",
        password_hash="x",
        system_role="user",
    )
    monkeypatch.setattr(runtime_api, "build_invocation_runtime", lambda _request: Runtime())
    app = make_authed_test_app(user_factory=lambda: user)

    @app.middleware("http")
    async def _stamp_auth_source(request, call_next):
        request.state.auth_source = AUTH_SOURCE_SESSION
        return await call_next(request)

    app.include_router(runtime_api.router)

    with TestClient(app) as client:
        denied = client.post(
            "/api/runtime/v1/invocations/ensure",
            json=_ensure_payload(),
        )
        hidden = client.get("/api/runtime/v1/invocations/run-unknown")
        hidden_cancel = client.post(
            "/api/runtime/v1/invocations/run-unknown/control",
            json=_cancel_payload(run_id="run-unknown"),
        )

    assert denied.status_code == 403
    assert hidden.status_code == 404
    assert hidden_cancel.status_code == 404
    assert captured["source_kind"] is InternalSourceKind.http
    assert captured["trusted_service_id"] is None
    assert captured["principal"].user_id == str(user.id)
    assert captured["principal"].role == "user"
    assert captured["cancel_principal"].user_id == str(user.id)


def test_runtime_invocation_lookup_does_not_disclose_another_owners_run() -> None:
    import asyncio

    from app.gateway.auth_disabled import AUTH_SOURCE_SESSION
    from deerflow.runtime import RunManager
    from deerflow.runtime.runs.store.memory import MemoryRunStore

    owner = User(
        id=UUID("00000000-0000-0000-0000-000000000051"),
        email="runtime-owner@example.com",
        password_hash="x",
    )
    intruder = User(
        id=UUID("00000000-0000-0000-0000-000000000052"),
        email="runtime-intruder@example.com",
        password_hash="x",
    )
    store = MemoryRunStore()
    asyncio.run(
        store.put(
            "private-run",
            thread_id="private-thread",
            user_id=str(owner.id),
        )
    )
    app = make_authed_test_app(user_factory=lambda: intruder)

    @app.middleware("http")
    async def _stamp_auth_source(request, call_next):
        request.state.auth_source = AUTH_SOURCE_SESSION
        return await call_next(request)

    app.state.run_manager = RunManager(store=store)
    app.include_router(runtime_api.router)

    with TestClient(app) as client:
        response = client.get("/api/runtime/v1/invocations/private-run")

    assert response.status_code == 404
    assert response.json() == {
        "api_version": "deerflow.runtime/v1",
        "kind": "runtime.error",
        "code": "not_found_or_invisible",
    }


def test_runtime_context_lookup_does_not_disclose_another_owners_thread() -> None:
    import asyncio

    from langgraph.store.memory import InMemoryStore

    from app.gateway.auth_disabled import AUTH_SOURCE_SESSION
    from deerflow.persistence.thread_meta.memory import MemoryThreadMetaStore
    from deerflow.runtime import RunManager
    from deerflow.runtime.runs.store.memory import MemoryRunStore

    owner = User(
        id=UUID("00000000-0000-0000-0000-000000000061"),
        email="context-owner@example.com",
        password_hash="x",
    )
    intruder = User(
        id=UUID("00000000-0000-0000-0000-000000000062"),
        email="context-intruder@example.com",
        password_hash="x",
    )
    thread_store = MemoryThreadMetaStore(InMemoryStore())
    asyncio.run(thread_store.create("private-thread", user_id=str(owner.id)))
    app = make_authed_test_app(user_factory=lambda: intruder)

    @app.middleware("http")
    async def _stamp_auth_source(request, call_next):
        request.state.auth_source = AUTH_SOURCE_SESSION
        return await call_next(request)

    app.state.thread_store = thread_store
    app.state.run_manager = RunManager(store=MemoryRunStore())
    app.include_router(runtime_api.router)

    with TestClient(app) as client:
        invisible = client.get("/api/runtime/v1/contexts/private-thread/invocations")
        unknown = client.get("/api/runtime/v1/contexts/unknown-thread/invocations")

    assert invisible.status_code == unknown.status_code == 404
    assert (
        invisible.json()
        == unknown.json()
        == {
            "api_version": "deerflow.runtime/v1",
            "kind": "runtime.error",
            "code": "not_found_or_invisible",
        }
    )
