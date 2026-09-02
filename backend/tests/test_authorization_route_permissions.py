"""Route-level authorization tests for the Gateway permission decorators."""

from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest
from deerflow_extension_api import (
    ActingServiceV1,
    EffectiveSubjectV1,
    InvocationIdentityV1,
)
from fastapi import FastAPI, HTTPException, Request
from fastapi.testclient import TestClient

from app.gateway.auth.models import User
from app.gateway.auth_middleware import AuthMiddleware
from app.gateway.authorization import AuthorizationProviderResolver
from app.gateway.authz import (
    Permissions,
    _authenticate,
    require_permission,
    resolve_route_permissions,
)
from app.gateway.routers import runs, scheduled_tasks
from deerflow.authz.provider import AuthzDecision, AuthzReason
from deerflow.authz.rbac import RbacAuthorizationProvider
from deerflow.config.authorization_config import AuthorizationConfig, AuthorizationProviderConfig
from deerflow.extensions.registry import ExtensionRegistry


class _RecordingProvider:
    name = "recording"

    def __init__(
        self,
        *,
        denied: set[str] | None = None,
        errors: set[str] | None = None,
    ) -> None:
        self.denied = denied or set()
        self.errors = errors or set()
        self.requests = []

    def authorize(self, request):
        raise AssertionError("route authorization must use the async provider API")

    async def aauthorize(self, request):
        self.requests.append(request)
        if request.target in self.errors:
            raise RuntimeError(f"provider failed for {request.target}")
        allowed = request.target not in self.denied
        return AuthzDecision(
            allow=allowed,
            reasons=[AuthzReason(code="authz.allowed" if allowed else "authz.denied")],
        )

    def filter_resources(self, principal, resource_type, candidates):
        raise AssertionError("route authorization must preserve per-action requests")


def _user(**overrides):
    values = {
        "id": "user-123",
        "system_role": "user",
        "oauth_provider": "github",
        "oauth_id": "oauth-456",
    }
    values.update(overrides)
    return SimpleNamespace(**values)


class _FixedResolver:
    def __init__(self, provider) -> None:
        self.provider = provider

    def resolve(self, config):
        return SimpleNamespace(provider=self.provider)


def _enable_authorization(monkeypatch, provider, *, fail_closed: bool = True):
    config = AuthorizationConfig(
        enabled=True,
        fail_closed=fail_closed,
        default_role="user",
    )
    monkeypatch.setattr("app.gateway.authz._get_route_authorization_config", lambda: config)
    return _FixedResolver(provider)


@pytest.mark.asyncio
async def test_route_permissions_disabled_preserves_all_permissions(monkeypatch):
    config = AuthorizationConfig(enabled=False)
    monkeypatch.setattr("app.gateway.authz._get_route_authorization_config", lambda: config)
    resolver = _FixedResolver(None)
    resolver.resolve = AsyncMock(side_effect=AssertionError("disabled authorization must not resolve a provider"))

    permissions = await resolve_route_permissions(_user(), is_internal=False, resolver=resolver)

    assert permissions == [
        Permissions.THREADS_READ,
        Permissions.THREADS_WRITE,
        Permissions.THREADS_DELETE,
        Permissions.RUNS_CREATE,
        Permissions.RUNS_READ,
        Permissions.RUNS_CANCEL,
    ]
    resolver.resolve.assert_not_called()


@pytest.mark.asyncio
async def test_route_permissions_use_async_provider_and_trusted_principal(monkeypatch):
    provider = _RecordingProvider(denied={Permissions.THREADS_DELETE, Permissions.RUNS_CANCEL})
    resolver = _enable_authorization(monkeypatch, provider)

    permissions = await resolve_route_permissions(_user(), is_internal=True, resolver=resolver)

    assert permissions == [
        Permissions.THREADS_READ,
        Permissions.THREADS_WRITE,
        Permissions.RUNS_CREATE,
        Permissions.RUNS_READ,
    ]
    assert [(request.resource, request.action, request.target) for request in provider.requests] == [
        ("route", "read", Permissions.THREADS_READ),
        ("route", "write", Permissions.THREADS_WRITE),
        ("route", "delete", Permissions.THREADS_DELETE),
        ("route", "create", Permissions.RUNS_CREATE),
        ("route", "read", Permissions.RUNS_READ),
        ("route", "cancel", Permissions.RUNS_CANCEL),
    ]
    principal = provider.requests[0].principal
    assert principal.user_id == "user-123"
    assert principal.role == "user"
    assert principal.oauth_provider == "github"
    assert principal.oauth_id == "oauth-456"
    assert principal.is_internal is False


@pytest.mark.asyncio
async def test_route_permissions_preserve_split_human_and_acting_service(
    monkeypatch,
):
    provider = _RecordingProvider()
    resolver = _enable_authorization(monkeypatch, provider)
    identity = InvocationIdentityV1(
        effective_subject=EffectiveSubjectV1(
            kind="human",
            subject_id="owner-1",
            role="member",
        ),
        acting_service=ActingServiceV1(service_id="gateway-internal"),
    )

    await resolve_route_permissions(
        _user(system_role="internal"),
        is_internal=True,
        resolver=resolver,
        identity=identity,
    )

    assert all(request.principal.identity is identity for request in provider.requests)
    assert all(request.principal.user_id == "owner-1" for request in provider.requests)
    assert all(request.principal.is_internal is False for request in provider.requests)


@pytest.mark.asyncio
async def test_route_permissions_fail_closed_denies_only_the_failed_permission(monkeypatch):
    provider = _RecordingProvider(errors={Permissions.RUNS_CANCEL})
    resolver = _enable_authorization(monkeypatch, provider, fail_closed=True)

    permissions = await resolve_route_permissions(_user(), is_internal=False, resolver=resolver)

    assert permissions == [
        Permissions.THREADS_READ,
        Permissions.THREADS_WRITE,
        Permissions.THREADS_DELETE,
        Permissions.RUNS_CREATE,
        Permissions.RUNS_READ,
    ]


@pytest.mark.asyncio
async def test_route_permission_provider_failure_diagnostic_redacts_exception_text(
    monkeypatch,
    caplog,
):
    marker = "credential=route-provider-secret-marker"

    class _MaliciousProvider(_RecordingProvider):
        async def aauthorize(self, request):
            raise RuntimeError(marker)

    resolver = _enable_authorization(monkeypatch, _MaliciousProvider(), fail_closed=True)

    with caplog.at_level("WARNING", logger="app.gateway.authz"):
        permissions = await resolve_route_permissions(
            _user(),
            is_internal=False,
            resolver=resolver,
        )

    assert permissions == []
    assert marker not in caplog.text
    assert "authorization_decision_failed" in caplog.text
    assert any(getattr(record, "correlation_id", None) for record in caplog.records)


@pytest.mark.asyncio
async def test_route_permissions_fail_open_allows_the_failed_permission(monkeypatch):
    provider = _RecordingProvider(errors={Permissions.RUNS_CANCEL})
    resolver = _enable_authorization(monkeypatch, provider, fail_closed=False)

    permissions = await resolve_route_permissions(_user(), is_internal=False, resolver=resolver)

    assert permissions == [
        Permissions.THREADS_READ,
        Permissions.THREADS_WRITE,
        Permissions.THREADS_DELETE,
        Permissions.RUNS_CREATE,
        Permissions.RUNS_READ,
        Permissions.RUNS_CANCEL,
    ]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("fail_closed", "expected"),
    [
        (True, []),
        (
            False,
            [
                Permissions.THREADS_READ,
                Permissions.THREADS_WRITE,
                Permissions.THREADS_DELETE,
                Permissions.RUNS_CREATE,
                Permissions.RUNS_READ,
                Permissions.RUNS_CANCEL,
            ],
        ),
    ],
)
async def test_route_permissions_apply_failure_mode_to_provider_resolution(
    monkeypatch,
    caplog,
    fail_closed,
    expected,
):
    config = AuthorizationConfig(
        enabled=True,
        fail_closed=fail_closed,
        default_role="user",
    )
    monkeypatch.setattr("app.gateway.authz._get_route_authorization_config", lambda: config)

    class _FailingResolver:
        def resolve(self, config):
            raise ValueError("credential=resolver-secret-marker")

    with caplog.at_level("WARNING", logger="app.gateway.authz"):
        assert await resolve_route_permissions(_user(), is_internal=False, resolver=_FailingResolver()) == expected
    assert "resolver-secret-marker" not in caplog.text
    assert "authorization_provider_resolution_failed" in caplog.text


@pytest.mark.asyncio
async def test_route_permissions_use_builtin_rbac_route_policy(monkeypatch):
    provider = RbacAuthorizationProvider(
        roles={
            "user": {
                "routes": {
                    "allow": [Permissions.THREADS_READ, Permissions.RUNS_READ],
                }
            }
        }
    )
    resolver = _enable_authorization(monkeypatch, provider)

    permissions = await resolve_route_permissions(_user(), is_internal=False, resolver=resolver)

    assert permissions == [Permissions.THREADS_READ, Permissions.RUNS_READ]


@pytest.mark.asyncio
async def test_authenticate_uses_route_permission_resolution(monkeypatch):
    user = User(email="route-authz@example.com", password_hash="hash")
    permission_resolver = AsyncMock(return_value=[Permissions.THREADS_READ])
    monkeypatch.setattr("app.gateway.deps.get_optional_user_from_request", AsyncMock(return_value=user))
    monkeypatch.setattr("app.gateway.authz.resolve_route_permissions", permission_resolver)
    request = SimpleNamespace(
        state=SimpleNamespace(),
        app=SimpleNamespace(state=SimpleNamespace(authorization_provider_resolver=None)),
    )

    auth_context = await _authenticate(request)

    assert auth_context.user is user
    assert auth_context.permissions == [Permissions.THREADS_READ]
    permission_resolver.assert_awaited_once_with(
        user,
        is_internal=False,
        resolver=None,
        request=request,
    )


def _make_middleware_app() -> FastAPI:
    app = FastAPI()
    app.add_middleware(AuthMiddleware)

    @app.get("/api/threads")
    @require_permission("threads", "read")
    async def read_threads(request: Request):
        return {"ok": True}

    @app.delete("/api/threads")
    @require_permission("threads", "delete")
    async def delete_threads(request: Request):
        return {"ok": True}

    return app


def test_auth_middleware_stamps_provider_derived_permissions(monkeypatch):
    monkeypatch.setenv("DEER_FLOW_AUTH_DISABLED", "1")
    permission_resolver = AsyncMock(return_value=[Permissions.THREADS_READ])
    monkeypatch.setattr("app.gateway.auth_middleware.resolve_route_permissions", permission_resolver)

    with TestClient(_make_middleware_app()) as client:
        assert client.get("/api/threads").status_code == 200
        assert client.delete("/api/threads").status_code == 403

    assert permission_resolver.await_count == 2
    for call in permission_resolver.await_args_list:
        assert call.kwargs["is_internal"] is False
        assert call.kwargs["resolver"] is None
        assert call.kwargs["request"] is not None


def test_auth_middleware_marks_internal_route_principal(monkeypatch):
    from app.gateway.internal_auth import create_internal_auth_headers

    permission_resolver = AsyncMock(return_value=[Permissions.THREADS_READ])
    monkeypatch.setattr("app.gateway.auth_middleware.resolve_route_permissions", permission_resolver)

    with TestClient(_make_middleware_app()) as client:
        response = client.get("/api/threads", headers=create_internal_auth_headers())

    assert response.status_code == 200
    permission_resolver.assert_awaited_once()
    assert permission_resolver.await_args.kwargs["is_internal"] is True
    assert permission_resolver.await_args.kwargs["resolver"] is None
    assert permission_resolver.await_args.kwargs["request"] is not None


_STATELESS_RUN_PATHS = ("/api/runs/stream", "/api/runs/wait")


def _enable_auth_disabled_for_route_test(monkeypatch) -> None:
    monkeypatch.setenv("DEER_FLOW_AUTH_DISABLED", "1")
    monkeypatch.delenv("DEER_FLOW_ENV", raising=False)
    monkeypatch.delenv("ENVIRONMENT", raising=False)


def _make_stateless_runs_app() -> FastAPI:
    app = FastAPI()
    app.add_middleware(AuthMiddleware)
    app.include_router(runs.router)
    app.state.stream_bridge = MagicMock()
    app.state.run_manager = MagicMock()
    return app


@pytest.mark.parametrize("path", _STATELESS_RUN_PATHS)
def test_stateless_run_creation_requires_runs_create(monkeypatch, path):
    _enable_auth_disabled_for_route_test(monkeypatch)
    monkeypatch.setattr(
        "app.gateway.auth_middleware.resolve_route_permissions",
        AsyncMock(return_value=[Permissions.RUNS_READ]),
    )
    start_run = AsyncMock(side_effect=HTTPException(status_code=418, detail="run creation reached"))
    monkeypatch.setattr(runs, "start_run", start_run)

    with TestClient(_make_stateless_runs_app()) as client:
        response = client.post(path, json={})

    assert response.status_code == 403
    assert response.json() == {"detail": "Permission denied: runs:create"}
    start_run.assert_not_awaited()


@pytest.mark.parametrize("path", _STATELESS_RUN_PATHS)
def test_stateless_run_creation_allows_runs_create(monkeypatch, path):
    _enable_auth_disabled_for_route_test(monkeypatch)
    monkeypatch.setattr(
        "app.gateway.auth_middleware.resolve_route_permissions",
        AsyncMock(return_value=[Permissions.RUNS_CREATE]),
    )
    start_run = AsyncMock(side_effect=HTTPException(status_code=418, detail="run creation reached"))
    monkeypatch.setattr(runs, "start_run", start_run)

    with TestClient(_make_stateless_runs_app()) as client:
        response = client.post(path, json={})

    assert response.status_code == 418
    assert response.json() == {"detail": "run creation reached"}
    start_run.assert_awaited_once()


_SCHEDULED_RUN_CREATION_REQUESTS = (
    (
        "POST",
        "/api/scheduled-tasks",
        {
            "title": "Daily summary",
            "prompt": "Summarize the latest activity",
            "schedule_type": "cron",
            "schedule_spec": {"cron": "0 9 * * *"},
            "timezone": "UTC",
        },
    ),
    ("PATCH", "/api/scheduled-tasks/task-1", {"title": "Updated summary"}),
    ("POST", "/api/scheduled-tasks/task-1/resume", None),
    ("POST", "/api/scheduled-tasks/task-1/trigger", None),
)


def _make_scheduled_tasks_app() -> FastAPI:
    app = FastAPI()
    app.add_middleware(AuthMiddleware)
    app.include_router(scheduled_tasks.router)
    return app


@pytest.mark.parametrize(("method", "path", "payload"), _SCHEDULED_RUN_CREATION_REQUESTS)
@pytest.mark.parametrize(
    ("permissions", "denied_permission"),
    [
        ([Permissions.THREADS_WRITE], Permissions.RUNS_CREATE),
        ([Permissions.RUNS_CREATE], Permissions.THREADS_WRITE),
    ],
)
def test_scheduled_run_creation_requires_thread_write_and_runs_create(monkeypatch, method, path, payload, permissions, denied_permission):
    _enable_auth_disabled_for_route_test(monkeypatch)
    monkeypatch.setattr(
        "app.gateway.auth_middleware.resolve_route_permissions",
        AsyncMock(return_value=permissions),
    )

    with TestClient(_make_scheduled_tasks_app()) as client:
        response = client.request(method, path, json=payload)

    assert response.status_code == 403
    assert response.json() == {"detail": f"Permission denied: {denied_permission}"}


# ── Provider cache tests ────────────────────────────────────────────────


class TestRouteProviderResolver:
    """Verify provider identity across legacy authorization generations."""

    @staticmethod
    def _resolver(config):
        return AuthorizationProviderResolver(
            ExtensionRegistry().build(generation=1),
            config,
        )

    def test_same_config_returns_same_provider(self):
        """Calling twice with the same config object returns the same instance."""
        config = AuthorizationConfig(
            enabled=True,
            provider=AuthorizationProviderConfig(
                use="deerflow.authz.rbac:RbacAuthorizationProvider",
                config={"roles": {"user": {"routes": {"allow": "*"}}}},
            ),
        )

        resolver = self._resolver(config)
        p1 = resolver.resolve(config).provider
        p2 = resolver.resolve(config).provider
        assert p1 is not None
        assert p2 is p1

    def test_changed_config_returns_new_provider(self):
        """A config with different content triggers re-resolution."""
        config1 = AuthorizationConfig(
            enabled=True,
            provider=AuthorizationProviderConfig(
                use="deerflow.authz.rbac:RbacAuthorizationProvider",
                config={"roles": {"user": {"routes": {"allow": "*"}}}},
            ),
        )
        config2 = AuthorizationConfig(
            enabled=True,
            provider=AuthorizationProviderConfig(
                use="deerflow.authz.rbac:RbacAuthorizationProvider",
                config={"roles": {"user": {"routes": {"allow": []}}}},
            ),
        )

        resolver = self._resolver(config1)
        p1 = resolver.resolve(config1).provider
        p2 = resolver.resolve(config2).provider
        assert p1 is not None
        assert p2 is not None
        assert p1 is not p2

    def test_same_content_different_object_reuses_provider(self):
        """Same content in a new object (e.g. hot-reload with no changes) reuses provider."""
        config1 = AuthorizationConfig(
            enabled=True,
            provider=AuthorizationProviderConfig(
                use="deerflow.authz.rbac:RbacAuthorizationProvider",
                config={"roles": {"user": {"routes": {"allow": "*"}}}},
            ),
        )
        # Same content, different object
        config2 = AuthorizationConfig(
            enabled=True,
            provider=AuthorizationProviderConfig(
                use="deerflow.authz.rbac:RbacAuthorizationProvider",
                config={"roles": {"user": {"routes": {"allow": "*"}}}},
            ),
        )

        resolver = self._resolver(config1)
        p1 = resolver.resolve(config1).provider
        p2 = resolver.resolve(config2).provider
        assert p1 is p2
