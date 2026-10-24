"""Background delivery distinguishes unreadable authority from a real denial."""

import asyncio
import threading
from types import SimpleNamespace

import pytest

from app.gateway import authz
from deerflow.authz.provider import AuthzDecision
from deerflow.authz.runtime import AuthorizationProviderSpec
from deerflow.config.authorization_config import AuthorizationConfig, AuthorizationProviderConfig

pytestmark = pytest.mark.asyncio


@pytest.fixture(autouse=True)
def isolated_cache(monkeypatch):
    monkeypatch.setattr(authz, "_route_provider_cache", {})
    monkeypatch.setattr(authz, "_route_provider_config_id", None)
    monkeypatch.setattr(authz, "_route_provider_config_sig", None)


def user():
    return SimpleNamespace(id="owner", system_role="user", oauth_provider=None, oauth_id=None)


class Provider:
    name = "strict-test"

    def __init__(self, *, fail=False):
        self.loop = asyncio.get_running_loop()
        self.fail = fail
        self.requests = []

    def authorize(self, request):
        raise AssertionError("async evaluation required")

    async def aauthorize(self, request):
        assert asyncio.get_running_loop() is self.loop
        self.requests.append(request)
        if self.fail:
            raise RuntimeError("unavailable provider")
        return AuthzDecision(allow=request.target != "runs:create")

    def filter_resources(self, principal, resource_type, candidates):
        raise AssertionError("route evaluation required")


def config(*, fail_closed=False):
    return AuthorizationConfig(enabled=True, fail_closed=fail_closed, provider=AuthorizationProviderConfig(use="offline:Provider"))


@pytest.mark.parametrize("fail_closed", [False, True])
async def test_strict_provider_error_is_unavailable_even_when_http_fail_open(monkeypatch, fail_closed):
    provider = Provider(fail=True)
    monkeypatch.setattr(authz, "_aget_cached_route_provider", lambda _: asyncio.sleep(0, result=provider), raising=False)
    with pytest.raises(authz.RouteAuthorityUnavailable):
        await authz.resolve_route_permissions(user(), is_internal=False, prepared_config=config(fail_closed=fail_closed), strict=True)


async def test_strict_disabled_authorization_is_a_real_all_permissions_decision(monkeypatch):
    def forbidden(_):
        raise AssertionError("disabled authorization has no provider")

    monkeypatch.setattr(authz, "_aget_cached_route_provider", forbidden, raising=False)
    result = await authz.resolve_route_permissions(user(), is_internal=False, prepared_config=AuthorizationConfig(), strict=True)
    assert "threads:read" in result and "runs:create" in result


@pytest.mark.parametrize("malformed", ["false", 1, None])
async def test_strict_decision_requires_exact_boolean_instead_of_truthiness(monkeypatch, malformed):
    provider = Provider()

    async def invalid(request):
        return AuthzDecision(allow=malformed)

    provider.aauthorize = invalid
    monkeypatch.setattr(authz, "_aget_cached_route_provider", lambda _: asyncio.sleep(0, result=provider))
    with pytest.raises(authz.RouteAuthorityUnavailable):
        await authz.resolve_route_permissions(user(), is_internal=False, prepared_config=config(), strict=True)


async def test_strict_config_missing_is_unavailable_instead_of_disabled(monkeypatch):
    def absent():
        raise FileNotFoundError("no configuration")

    monkeypatch.setattr("deerflow.config.app_config.get_app_config", absent)
    with pytest.raises(authz.RouteAuthorityUnavailable):
        await authz.resolve_route_permissions(user(), is_internal=False, strict=True)


async def test_cold_discovery_off_loop_construction_and_evaluation_on_original_loop(monkeypatch):
    loop = asyncio.get_running_loop()
    loop_thread = threading.get_ident()
    discovered = []

    def discover(c):
        discovered.append(threading.get_ident())
        return AuthorizationProviderSpec(c.provider.use, Provider, {})

    monkeypatch.setattr(authz, "resolve_authorization_provider_spec", discover)
    c = config()
    first = await authz.resolve_route_permissions(user(), is_internal=False, prepared_config=c, strict=True)
    second = await authz.resolve_route_permissions(user(), is_internal=False, prepared_config=c, strict=True)
    assert first == second and "threads:read" in first and "runs:create" not in first
    assert len(discovered) == 1 and discovered[0] != loop_thread
    cached = authz._route_provider_cache["provider"]
    assert cached.loop is loop and all(not r.principal.is_internal for r in cached.requests)


async def test_prepared_account_mode_does_not_reload_files(monkeypatch):
    from app.gateway.auth.mode import ACCOUNT_INERT, account_refusal

    monkeypatch.setattr("app.gateway.auth.mode.sign_on_only", lambda: (_ for _ in ()).throw(AssertionError("unexpected config read")))
    assert account_refusal(user(), sign_on_only_mode=False) is None
    assert account_refusal(user(), sign_on_only_mode=True) == ACCOUNT_INERT
