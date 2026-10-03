"""``POST /api/threads/{id}/workspace/prewarm``: the sandbox built while the person types.

The route is thin on purpose: it names the thread, takes the request's own
user identity, and hands the provider's prewarm to the background.
What these tests pin is the contract the client relies on -- it always answers
quickly, it never surfaces a build failure, it says honestly when it built
nothing -- and the identity the build is scoped to.
"""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from _router_auth_helpers import make_authed_test_app
from fastapi import FastAPI, Request
from fastapi.testclient import TestClient

from app.gateway.authz import AuthContext, resolve_route_permissions
from app.gateway.routers import thread_workspace
from deerflow.config.authorization_config import AuthorizationConfig, AuthorizationProviderConfig
from deerflow.runtime.user_context import reset_current_user, set_current_user


@pytest.fixture(autouse=True)
def _default_config(monkeypatch):
    config = SimpleNamespace(authorization=AuthorizationConfig())
    monkeypatch.setattr(thread_workspace, "get_app_config", lambda: config)


class _PrewarmingProvider:
    def __init__(self, *, fail: bool = False, park: bool = True) -> None:
        self.calls: list[tuple[str, str]] = []
        self.fail = fail
        self.park = park

    async def prewarm_async(self, thread_id: str, *, user_id: str) -> str | None:
        self.calls.append((thread_id, user_id))
        if self.fail:
            raise RuntimeError("daemon refused")
        return f"sandbox-{thread_id}" if self.park else None


class _PlainProvider:
    """A provider that cannot prewarm at all."""


def _client(monkeypatch, provider: object, *, user_id: str | None = "user-7") -> TestClient:
    app = make_authed_test_app()
    app.include_router(thread_workspace.router)
    monkeypatch.setattr(thread_workspace, "get_sandbox_provider", lambda: provider)
    monkeypatch.setattr(thread_workspace, "get_effective_user_id", lambda: user_id)
    return TestClient(app)


def test_schedules_the_build_for_the_requests_own_identity(monkeypatch):
    provider = _PrewarmingProvider()

    response = _client(monkeypatch, provider).post("/api/threads/thread-open/workspace/prewarm")

    assert response.status_code == 202
    assert response.json() == {"thread_id": "thread-open", "scheduled": True, "reason": None}
    # TestClient runs background tasks before returning, so the call is visible.
    assert provider.calls == [("thread-open", "user-7")]


def test_a_provider_without_the_capability_is_not_an_error(monkeypatch):
    response = _client(monkeypatch, _PlainProvider()).post("/api/threads/thread-open/workspace/prewarm")

    assert response.status_code == 202
    assert response.json() == {"thread_id": "thread-open", "scheduled": False, "reason": "unsupported"}


def test_an_unavailable_provider_is_not_an_error(monkeypatch):
    def _no_provider():
        raise RuntimeError("sandbox not configured")

    app = make_authed_test_app()
    app.include_router(thread_workspace.router)
    monkeypatch.setattr(thread_workspace, "get_sandbox_provider", _no_provider)

    response = TestClient(app).post("/api/threads/thread-open/workspace/prewarm")

    assert response.status_code == 202
    assert response.json()["scheduled"] is False
    assert response.json()["reason"] == "unavailable"


def test_a_failed_build_never_reaches_the_client(monkeypatch):
    provider = _PrewarmingProvider(fail=True)

    response = _client(monkeypatch, provider).post("/api/threads/thread-fail/workspace/prewarm")

    assert response.status_code == 202
    assert response.json()["scheduled"] is True
    assert provider.calls == [("thread-fail", "user-7")]


def test_a_build_that_parks_nothing_is_still_a_normal_outcome(monkeypatch):
    provider = _PrewarmingProvider(park=False)

    response = _client(monkeypatch, provider).post("/api/threads/thread-busy/workspace/prewarm")

    assert response.status_code == 202
    assert provider.calls == [("thread-busy", "user-7")]


def test_no_identity_means_no_build(monkeypatch):
    provider = _PrewarmingProvider()

    response = _client(monkeypatch, provider, user_id=None).post("/api/threads/thread-anon/workspace/prewarm")

    assert response.status_code == 202
    assert response.json()["reason"] == "anonymous"
    assert provider.calls == []


def test_an_invalid_thread_id_is_refused_before_anything_runs(monkeypatch):
    provider = _PrewarmingProvider()

    response = _client(monkeypatch, provider).post("/api/threads/not%2Fa%2Fthread/workspace/prewarm")

    assert response.status_code in (404, 422)
    assert provider.calls == []


@pytest.mark.parametrize("internal", [False, True])
@pytest.mark.parametrize("sandbox_allowed", [False, True])
def test_real_rbac_gates_prewarm_before_provider_lookup(monkeypatch, internal, sandbox_allowed):
    config = SimpleNamespace(
        authorization=AuthorizationConfig(
            enabled=True,
            provider=AuthorizationProviderConfig(
                use="deerflow.authz.rbac:RbacAuthorizationProvider",
                config={"roles": {"user": {"threads": {"allow": "*"}, "sandbox": {"allow": "*" if sandbox_allowed else []}}}},
            ),
        )
    )
    monkeypatch.setattr("deerflow.config.app_config.get_app_config", lambda: config)
    monkeypatch.setattr("deerflow.config.get_app_config", lambda: config)
    monkeypatch.setattr(thread_workspace, "get_app_config", lambda: config)
    provider = _PrewarmingProvider()
    lookups = []

    def get_provider():
        lookups.append(True)
        return provider

    monkeypatch.setattr(thread_workspace, "get_sandbox_provider", get_provider)
    app = FastAPI()
    app.state.thread_store = SimpleNamespace(check_access=AsyncMock(return_value=True))

    @app.middleware("http")
    async def authenticate(request: Request, call_next):
        user = SimpleNamespace(id="user-7", system_role="internal" if internal else "user")
        request.state.auth_source = "internal" if internal else "session"
        request.state.user = user
        permissions = await resolve_route_permissions(user, is_internal=internal)
        assert "threads:write" in permissions
        request.state.auth = AuthContext(user=user, permissions=permissions)
        token = set_current_user(user)
        try:
            return await call_next(request)
        finally:
            reset_current_user(token)

    app.include_router(thread_workspace.router)
    response = TestClient(app).post("/api/threads/new-thread/workspace/prewarm")
    assert response.status_code == 202
    assert response.json()["scheduled"] is sandbox_allowed
    assert lookups == ([True] if sandbox_allowed else [])
    assert provider.calls == ([("new-thread", "user-7")] if sandbox_allowed else [])
    if not sandbox_allowed:
        assert response.json()["reason"] == "forbidden"


def test_foreign_thread_is_refused_before_prewarm(monkeypatch):
    provider = _PrewarmingProvider()
    app = make_authed_test_app(owner_check_passes=False)
    app.include_router(thread_workspace.router)
    monkeypatch.setattr(thread_workspace, "get_sandbox_provider", lambda: provider)
    response = TestClient(app).post("/api/threads/foreign-thread/workspace/prewarm")
    assert response.status_code == 404
    assert provider.calls == []


def test_config_lookup_failure_cannot_bypass_known_sandbox_denial(monkeypatch):
    def unavailable():
        raise RuntimeError("configuration unavailable")

    monkeypatch.setattr("deerflow.config.app_config.get_app_config", unavailable)
    monkeypatch.setattr("deerflow.config.get_app_config", unavailable)
    monkeypatch.setattr(thread_workspace, "get_app_config", unavailable)
    monkeypatch.setattr("app.gateway.authz._get_route_authorization_config", lambda: AuthorizationConfig(enabled=True, fail_closed=True))
    provider = _PrewarmingProvider()
    response = _client(monkeypatch, provider).post("/api/threads/new-thread/workspace/prewarm")
    assert response.status_code == 202
    assert response.json()["scheduled"] is False
    assert response.json()["reason"] == "unavailable"
    assert provider.calls == []


@pytest.mark.parametrize("provider_path", ["deerflow.authz.rbac:RbacAuthorizationProvider", "missing_authz_provider:Unavailable"])
def test_prewarm_uses_one_config_snapshot_and_fails_closed_on_provider_error(monkeypatch, provider_path):
    config = SimpleNamespace(
        authorization=AuthorizationConfig(
            enabled=True,
            provider=AuthorizationProviderConfig(use=provider_path, config={"roles": {"user": {"sandbox": {"allow": []}}}}),
        )
    )
    monkeypatch.setattr(thread_workspace, "get_app_config", lambda: config)
    # A second config read may race a reload or fall back after an error. It
    # must not replace the already resolved sandbox-denying snapshot.
    monkeypatch.setattr("app.gateway.authz._get_route_authorization_config", lambda: AuthorizationConfig())
    provider = _PrewarmingProvider()
    response = _client(monkeypatch, provider).post("/api/threads/new-thread/workspace/prewarm")
    assert response.status_code == 202
    assert response.json()["reason"] == "forbidden"
    assert provider.calls == []
