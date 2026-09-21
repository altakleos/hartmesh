"""Sign-on-only mode: ``auth.local.enabled: false`` closes every local-password door.

One fact selects the mode, and everything here derives from it: the four
local routes refuse, the first-admin bootstrap is gone whatever the admin
count, ``setup-status`` publishes nothing about bootstrap, a local-password
account's session and personal access token are inert, the ``reset_admin``
command refuses, and ``DEER_FLOW_AUTH_DISABLED`` refuses the start. The
identity-provider flow itself is covered end to end in
``test_sign_on_only_e2e.py`` against a provider the tests start.
"""

from __future__ import annotations

import asyncio
import os
from types import SimpleNamespace
from uuid import uuid4

import pytest
from fastapi.testclient import TestClient

os.environ.setdefault("AUTH_JWT_SECRET", "test-secret-key-sign-on-only-min-32")

from app.gateway.auth.config import AuthConfig, set_auth_config
from deerflow.config.app_config import AppConfig
from deerflow.config.auth_config import AuthAppConfig, LocalAuthConfig, OIDCAuthConfig, OIDCProviderConfig
from deerflow.config.sandbox_config import SandboxConfig

_TEST_SECRET = "test-secret-key-sign-on-only-min-32"
_PROVIDER = OIDCProviderConfig(display_name="Company", issuer="https://id.example.com/realms/co", client_id="hartmesh", client_secret="FAKE-CREDENTIAL-SENTINEL-NOT-A-KEY")


_SANDBOX = SandboxConfig(use="deerflow.sandbox.local:LocalSandboxProvider")


def _sign_on_only_config() -> AppConfig:
    return AppConfig(sandbox=_SANDBOX, auth=AuthAppConfig(local=LocalAuthConfig(enabled=False), oidc=OIDCAuthConfig(enabled=True, providers={"sso": _PROVIDER})))


def _local_config() -> AppConfig:
    return AppConfig(sandbox=_SANDBOX, auth=AuthAppConfig())


# ── The fact ─────────────────────────────────────────────────────────────


def test_local_passwords_are_on_unless_switched_off() -> None:
    assert LocalAuthConfig().enabled is True
    assert AuthAppConfig().local.enabled is True


def test_switching_local_passwords_off_needs_a_way_in() -> None:
    with pytest.raises(ValueError, match="auth.local.enabled is false"):
        AuthAppConfig(local=LocalAuthConfig(enabled=False))
    with pytest.raises(ValueError, match="auth.local.enabled is false"):
        AuthAppConfig(local=LocalAuthConfig(enabled=False), oidc=OIDCAuthConfig(enabled=False, providers={"sso": _PROVIDER}))
    with pytest.raises(ValueError, match="auth.local.enabled is false"):
        AuthAppConfig(local=LocalAuthConfig(enabled=False), oidc=OIDCAuthConfig(enabled=True, providers={}))
    assert _sign_on_only_config().auth.local.enabled is False


def test_the_mode_is_read_from_the_live_config_and_absent_config_means_local(monkeypatch: pytest.MonkeyPatch) -> None:
    from app.gateway.auth.mode import AUTH_MODE_LOCAL, AUTH_MODE_SIGN_ON_ONLY, auth_mode, sign_on_only

    monkeypatch.setattr("deerflow.config.app_config.get_app_config", _sign_on_only_config)
    assert sign_on_only() is True
    assert auth_mode() == AUTH_MODE_SIGN_ON_ONLY
    monkeypatch.setattr("deerflow.config.app_config.get_app_config", _local_config)
    assert sign_on_only() is False
    assert auth_mode() == AUTH_MODE_LOCAL

    def _missing() -> AppConfig:
        raise FileNotFoundError("no config.yaml")

    monkeypatch.setattr("deerflow.config.app_config.get_app_config", _missing)
    assert sign_on_only() is False, "a bare app without a config file is today's local-password deployment"


# ── The doors ────────────────────────────────────────────────────────────


@pytest.fixture
def users_db(tmp_path, monkeypatch: pytest.MonkeyPatch):
    """A fresh users table, the auth secret, and no cached provider."""
    from app.gateway import deps
    from app.gateway.routers.auth import _SETUP_STATUS_CACHE, _SETUP_STATUS_INFLIGHT
    from deerflow.persistence.engine import close_engine, init_engine

    set_auth_config(AuthConfig(jwt_secret=_TEST_SECRET))
    asyncio.run(init_engine("sqlite", url=f"sqlite+aiosqlite:///{tmp_path}/users.db", sqlite_dir=str(tmp_path)))
    deps._cached_local_provider = None
    deps._cached_repo = None
    _SETUP_STATUS_CACHE.clear()
    _SETUP_STATUS_INFLIGHT.clear()
    monkeypatch.setenv("DEER_FLOW_AUTH_DISABLED", "")
    try:
        yield
    finally:
        deps._cached_local_provider = None
        deps._cached_repo = None
        _SETUP_STATUS_CACHE.clear()
        _SETUP_STATUS_INFLIGHT.clear()
        asyncio.run(close_engine())


def _client(monkeypatch: pytest.MonkeyPatch, config: AppConfig) -> TestClient:
    from app.gateway.app import create_app

    monkeypatch.setattr("deerflow.config.app_config.get_app_config", lambda: config)
    set_auth_config(AuthConfig(jwt_secret=_TEST_SECRET))
    # Not a context manager: the lifespan wants a config.yaml, and the auth
    # routes only need the users table users_db set up.
    return TestClient(create_app())


def _csrf(client: TestClient) -> dict[str, str]:
    """The double-submit pair a session-bearing POST must carry."""
    from app.gateway.csrf_middleware import CSRF_COOKIE_NAME, CSRF_HEADER_NAME, generate_csrf_token

    token = generate_csrf_token()
    client.cookies.set(CSRF_COOKIE_NAME, token)
    return {CSRF_HEADER_NAME: token}


_ADMIN = {"email": "owner@example.com", "password": "Str0ng!Pass99"}
_LOCAL_DOORS = (
    ("post", "/api/v1/auth/initialize", {"json": _ADMIN}),
    ("post", "/api/v1/auth/register", {"json": {"email": "new@example.com", "password": "Tr0ub4dor3a"}}),
    ("post", "/api/v1/auth/login/local", {"data": {"username": _ADMIN["email"], "password": _ADMIN["password"]}}),
)


def _assert_refused(response, *, status: int = 403) -> None:
    assert response.status_code == status, response.text
    assert response.json()["detail"]["code"] == "sign_on_required"
    assert "FAKE-CREDENTIAL" not in response.text


def test_every_local_door_refuses_with_zero_admins(users_db, monkeypatch: pytest.MonkeyPatch) -> None:
    client = _client(monkeypatch, _sign_on_only_config())
    for method, path, kwargs in _LOCAL_DOORS:
        _assert_refused(getattr(client, method)(path, **kwargs))
    # Nothing was created by the refused initialize: still no cookie, still no account.
    assert "access_token" not in client.cookies
    from app.gateway.deps import get_local_provider

    assert asyncio.run(get_local_provider().count_users()) == 0


def test_the_local_doors_stay_shut_when_the_database_holds_a_local_admin(users_db, monkeypatch: pytest.MonkeyPatch) -> None:
    """What a restore of a pre-switch database produces: local accounts, inert."""
    local = _client(monkeypatch, _local_config())
    assert local.post("/api/v1/auth/initialize", json=_ADMIN).status_code == 201
    admin_cookie = local.cookies["access_token"]

    client = _client(monkeypatch, _sign_on_only_config())
    for method, path, kwargs in _LOCAL_DOORS:
        _assert_refused(getattr(client, method)(path, **kwargs))
    # The session minted before the switch is refused, and the password change
    # that would complete a setup flow is refused before it reads the account.
    client.cookies.set("access_token", admin_cookie)
    _assert_refused(client.get("/api/v1/auth/me"), status=401)
    _assert_refused(client.post("/api/v1/auth/change-password", json={"current_password": _ADMIN["password"], "new_password": "An0ther!Pass99"}, headers=_csrf(client)), status=401)


def test_a_personal_access_token_of_a_local_account_is_refused(users_db, monkeypatch: pytest.MonkeyPatch) -> None:
    from deerflow.persistence.engine import get_session_factory
    from deerflow.persistence.personal_access_tokens import PersonalAccessTokenRepository
    from deerflow.runtime.tenant_identity import TenantIdentityV1

    repo = PersonalAccessTokenRepository(get_session_factory(), tenant=TenantIdentityV1.from_canonical_id("tenant-a").to_persisted_reference())
    local = _client(monkeypatch, _local_config())
    local.app.state.pat_repo = repo
    assert local.post("/api/v1/auth/initialize", json=_ADMIN).status_code == 201
    minted = local.post("/api/v1/auth/pats", json={"name": "automation", "scopes": ["threads:read"]}, headers=_csrf(local))
    assert minted.status_code == 201, minted.text
    token = minted.json()["token"]
    local.cookies.clear()
    # A PAT-admissible route. Without the lifespan the route itself answers
    # 503 (no thread store), which is the proof the token got past
    # authentication: the auth middleware answers 401 before any route runs.
    accepted = local.get("/api/threads/whoami/goal", headers={"Authorization": f"Bearer {token}"})
    assert accepted.status_code == 503, accepted.text
    from app.gateway.auth.pat import authenticate_pat

    user, _scopes, _record = asyncio.run(authenticate_pat(local.app, f"Bearer {token}"))
    assert user.email == _ADMIN["email"]

    client = _client(monkeypatch, _sign_on_only_config())
    client.app.state.pat_repo = repo
    refused = client.get("/api/threads/whoami/goal", headers={"Authorization": f"Bearer {token}"})
    assert refused.status_code == 401
    assert refused.json() == {"detail": "Invalid token"}, "the same answer every dead token gets"
    from fastapi import HTTPException

    with pytest.raises(HTTPException) as direct:
        asyncio.run(authenticate_pat(client.app, f"Bearer {token}"))
    assert direct.value.status_code == 401


def test_setup_status_says_only_that_the_deployment_is_sign_on_only(users_db, monkeypatch: pytest.MonkeyPatch) -> None:
    constant = {"needs_setup": False, "registration_enabled": False, "sign_on_only": True}
    client = _client(monkeypatch, _sign_on_only_config())
    with_zero_admins = client.get("/api/v1/auth/setup-status").json()

    local = _client(monkeypatch, _local_config())
    assert local.post("/api/v1/auth/register", json={"email": "staff@example.com", "password": "Tr0ub4dor3a"}).status_code == 201
    client = _client(monkeypatch, _sign_on_only_config())
    with_accounts_no_admin = client.get("/api/v1/auth/setup-status").json()
    for method, path, kwargs in _LOCAL_DOORS:
        _assert_refused(getattr(client, method)(path, **kwargs))

    assert _client(monkeypatch, _local_config()).post("/api/v1/auth/initialize", json=_ADMIN).status_code == 201
    client = _client(monkeypatch, _sign_on_only_config())
    with_a_local_admin = client.get("/api/v1/auth/setup-status").json()

    assert with_zero_admins == with_accounts_no_admin == with_a_local_admin == constant


def test_local_mode_setup_status_is_unchanged_but_names_its_mode(users_db, monkeypatch: pytest.MonkeyPatch) -> None:
    client = _client(monkeypatch, _local_config())
    assert client.get("/api/v1/auth/setup-status").json() == {"needs_setup": True, "registration_enabled": True, "sign_on_only": False}


def test_a_provider_account_keeps_its_session_in_sign_on_only_mode(users_db, monkeypatch: pytest.MonkeyPatch) -> None:
    """The inert-account rule is about the account's shape, not the switch date."""
    from app.gateway.auth import create_access_token
    from app.gateway.deps import get_local_provider

    user = asyncio.run(get_local_provider().create_oauth_user(email="pat@example.com", oauth_provider="sso", oauth_id="sub-1", oauth_issuer=_PROVIDER.issuer))
    client = _client(monkeypatch, _sign_on_only_config())
    client.cookies.set("access_token", create_access_token(str(user.id), token_version=user.token_version))
    me = client.get("/api/v1/auth/me")
    assert me.status_code == 200, me.text
    assert me.json()["oauth_provider"] == "sso"
    # Their password change is refused for the reason it always was: no password.
    assert client.post("/api/v1/auth/change-password", json={"current_password": "x", "new_password": "An0ther!Pass99"}, headers=_csrf(client)).status_code in {400, 401}


def test_every_other_path_that_resolves_a_user_applies_the_same_rule(users_db, monkeypatch: pytest.MonkeyPatch) -> None:
    """The browser WebSocket and the LangGraph auth hook resolve a cookie outside the middleware."""
    from app.gateway.auth import create_access_token
    from app.gateway.deps import get_local_provider
    from app.gateway.routers.browser import _authenticate_ws

    local = _client(monkeypatch, _local_config())
    assert local.post("/api/v1/auth/initialize", json=_ADMIN).status_code == 201
    admin_cookie = local.cookies["access_token"]
    provider_user = asyncio.run(get_local_provider().create_oauth_user(email="pat@example.com", oauth_provider="sso", oauth_id="sub-1", oauth_issuer=_PROVIDER.issuer))
    provider_cookie = create_access_token(str(provider_user.id), token_version=provider_user.token_version)

    monkeypatch.setattr("deerflow.config.app_config.get_app_config", _sign_on_only_config)
    assert asyncio.run(_authenticate_ws(SimpleNamespace(cookies={"access_token": admin_cookie}))) is None
    assert asyncio.run(_authenticate_ws(SimpleNamespace(cookies={"access_token": provider_cookie}))).email == "pat@example.com"
    monkeypatch.setattr("deerflow.config.app_config.get_app_config", _local_config)
    assert asyncio.run(_authenticate_ws(SimpleNamespace(cookies={"access_token": admin_cookie}))).email == _ADMIN["email"]


def test_an_internal_service_may_not_act_as_an_inert_account(users_db, monkeypatch: pytest.MonkeyPatch) -> None:
    """An IM connection bound under a local session keeps nothing running after the switch."""
    from app.gateway.deps import get_local_provider
    from app.gateway.internal_auth import create_internal_auth_headers

    local = _client(monkeypatch, _local_config())
    assert local.post("/api/v1/auth/initialize", json=_ADMIN).status_code == 201
    admin_id = local.get("/api/v1/auth/me").json()["id"]
    provider_user = asyncio.run(get_local_provider().create_oauth_user(email="pat@example.com", oauth_provider="sso", oauth_id="sub-1", oauth_issuer=_PROVIDER.issuer))

    client = _client(monkeypatch, _sign_on_only_config())
    # A PAT-admissible GET: 503 (no thread store without the lifespan) is the
    # proof the call got past authentication; 401 is the refusal.
    route = f"/api/threads/{uuid4()}/state"
    refused = client.get(route, headers=create_internal_auth_headers(owner_user_id=admin_id))
    assert refused.status_code == 401 and refused.json()["detail"]["code"] == "sign_on_required"
    assert client.get(route, headers=create_internal_auth_headers(owner_user_id=str(provider_user.id))).status_code == 503
    assert client.get(route, headers=create_internal_auth_headers(owner_user_id="telegram-chat-1234")).status_code == 503, "an unbound channel's own id is not an account"
    assert client.get(route, headers=create_internal_auth_headers()).status_code == 503
    assert _client(monkeypatch, _local_config()).get(route, headers=create_internal_auth_headers(owner_user_id=admin_id)).status_code == 503, "local mode is unchanged"


def test_the_first_start_pins_provider_accounts_linked_before_the_issuer_was_recorded(users_db, monkeypatch: pytest.MonkeyPatch) -> None:
    from app.gateway.deps import _pin_provider_accounts_to_their_issuer, get_local_provider

    provider = get_local_provider()
    legacy = asyncio.run(provider.create_oauth_user(email="old@example.com", oauth_provider="sso", oauth_id="sub-old"))
    other = asyncio.run(provider.create_oauth_user(email="other@example.com", oauth_provider="github", oauth_id="sub-gh"))
    pinned = asyncio.run(provider.create_oauth_user(email="new@example.com", oauth_provider="sso", oauth_id="sub-new", oauth_issuer="https://elsewhere.example.com"))
    assert legacy.oauth_issuer is None
    assert asyncio.run(_pin_provider_accounts_to_their_issuer(_sign_on_only_config())) == 1
    assert asyncio.run(provider.get_user(str(legacy.id))).oauth_issuer == _PROVIDER.issuer
    assert asyncio.run(provider.get_user(str(other.id))).oauth_issuer is None, "another provider name is not this provider's to pin"
    assert asyncio.run(provider.get_user(str(pinned.id))).oauth_issuer == "https://elsewhere.example.com", "a recorded issuer is never rewritten"
    assert asyncio.run(_pin_provider_accounts_to_their_issuer(_sign_on_only_config())) == 0, "idempotent"
    assert asyncio.run(_pin_provider_accounts_to_their_issuer(_local_config())) == 0, "nothing to pin without a provider"


# ── The command and the start ────────────────────────────────────────────


def test_reset_admin_refuses_before_it_opens_the_database(monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]) -> None:
    from app.gateway.auth import reset_admin

    monkeypatch.setattr("deerflow.config.get_app_config", _sign_on_only_config)

    async def _must_not_run(*_args, **_kwargs):
        raise AssertionError("the database must not be opened")

    monkeypatch.setattr("deerflow.persistence.engine.init_engine_from_config", _must_not_run)
    assert asyncio.run(reset_admin._run(None)) == 1
    err = capsys.readouterr().err
    assert "sign-on only" in err and "auth.local.enabled" in err and "admin_emails" in err


def test_auth_disabled_refuses_the_start_whatever_the_environment_says(monkeypatch: pytest.MonkeyPatch) -> None:
    from app.gateway.deps import _enforce_auth_settings

    monkeypatch.setenv("DEER_FLOW_AUTH_DISABLED", "1")
    monkeypatch.setenv("DEER_FLOW_ENV", "production")
    with pytest.raises(RuntimeError, match="DEER_FLOW_AUTH_DISABLED"):
        _enforce_auth_settings(_sign_on_only_config())
    monkeypatch.delenv("DEER_FLOW_ENV")
    with pytest.raises(RuntimeError, match="DEER_FLOW_AUTH_DISABLED"):
        _enforce_auth_settings(_sign_on_only_config())
    # Local mode keeps today's behaviour: the switch is honoured (and warned about) later.
    _enforce_auth_settings(_local_config())
    monkeypatch.setenv("DEER_FLOW_AUTH_DISABLED", "")
    _enforce_auth_settings(_sign_on_only_config())


@pytest.mark.parametrize("bad", ["0", "31", "7.5", "seven", "-1", " "])
def test_the_session_lifetime_key_is_whole_days_within_the_product_bound(monkeypatch: pytest.MonkeyPatch, bad: str) -> None:
    from app.gateway.auth.config import token_expiry_days_from_environment
    from app.gateway.deps import _enforce_auth_settings

    monkeypatch.setenv("AUTH_TOKEN_EXPIRY_DAYS", bad)
    if bad.strip():
        with pytest.raises(ValueError, match="AUTH_TOKEN_EXPIRY_DAYS"):
            token_expiry_days_from_environment()
        with pytest.raises(RuntimeError, match="AUTH_TOKEN_EXPIRY_DAYS"):
            _enforce_auth_settings(_local_config())
    else:
        assert token_expiry_days_from_environment() is None


def test_the_session_lifetime_key_reaches_the_auth_config(monkeypatch: pytest.MonkeyPatch) -> None:
    from app.gateway.auth import config as auth_config_module

    monkeypatch.setenv("AUTH_JWT_SECRET", _TEST_SECRET)
    monkeypatch.setenv("AUTH_TOKEN_EXPIRY_DAYS", "14")
    monkeypatch.setattr(auth_config_module, "_auth_config", None)
    assert auth_config_module.get_auth_config().token_expiry_days == 14
    monkeypatch.delenv("AUTH_TOKEN_EXPIRY_DAYS")
    monkeypatch.setattr(auth_config_module, "_auth_config", None)
    assert auth_config_module.get_auth_config().token_expiry_days == 7


# ── The published port ───────────────────────────────────────────────────


def test_nginx_drops_the_internal_caller_headers_in_every_proxied_location() -> None:
    """The published port is nginx; whatever a client sends in these two headers dies there."""
    from pathlib import Path

    root = Path(__file__).resolve().parents[2]
    for conf in (root / "docker" / "nginx" / "nginx.conf", root / "deploy" / "compose" / "nginx" / "nginx.conf"):
        text = conf.read_text(encoding="utf-8")
        blocks = text.split("location ")[1:]
        proxied = [block for block in blocks if "proxy_pass http://$gateway_upstream" in block or "proxy_pass http://$provisioner_upstream" in block]
        assert len(proxied) >= 15, conf
        for block in proxied:
            head = block.split("{", 1)[0].strip()
            assert 'proxy_set_header X-DeerFlow-Internal-Token "";' in block, (conf.name, head)
            assert 'proxy_set_header X-DeerFlow-Owner-User-Id "";' in block, (conf.name, head)


# ── Issuer binding ───────────────────────────────────────────────────────


class _Users:
    """A users table with the four methods provisioning uses."""

    def __init__(self, *users) -> None:
        self.users = list(users)
        self.updated: list = []
        self.created: list = []

    async def get_user_by_oauth(self, provider: str, oauth_id: str):
        return next((u for u in self.users if u.oauth_provider == provider and u.oauth_id == oauth_id), None)

    async def get_user_by_email(self, email: str):
        return next((u for u in self.users if u.email == email), None)

    async def is_identity_disabled(self, issuer: str, subject: str) -> bool:
        return False

    async def update_user(self, user):
        self.updated.append(user)
        return user

    async def record_sign_in(self, user):
        self.updated.append(user)

    async def create_oauth_user(self, **kwargs):
        user = SimpleNamespace(id=uuid4(), needs_setup=False, token_version=0, password_hash=None, **kwargs)
        self.created.append(user)
        self.users.append(user)
        return user


def _identity(sub: str = "sub-1", email: str = "pat@example.com"):
    from app.gateway.auth.oidc import OIDCIdentity

    return OIDCIdentity(provider="sso", subject=sub, email=email, email_verified=True, name="Pat", claims={})


def _linked(issuer: str | None, **extra):
    return SimpleNamespace(id=uuid4(), email="pat@example.com", oauth_provider="sso", oauth_id="sub-1", oauth_issuer=issuer, system_role="user", needs_setup=False, token_version=0, password_hash=None, **extra)


@pytest.mark.anyio
async def test_a_new_account_records_the_issuer_it_was_created_under() -> None:
    from app.gateway.auth.user_provisioning import get_or_provision_oidc_user

    users = _Users()
    result = await get_or_provision_oidc_user("sso", _PROVIDER, _identity(), users)
    assert result["created"] is True
    assert users.created[0].oauth_issuer == _PROVIDER.issuer


@pytest.mark.anyio
async def test_an_account_linked_before_the_issuer_was_recorded_adopts_it_on_its_next_sign_in() -> None:
    from app.gateway.auth.user_provisioning import get_or_provision_oidc_user

    legacy = _linked(None)
    users = _Users(legacy)
    result = await get_or_provision_oidc_user("sso", _PROVIDER, _identity(), users)
    assert result["user"] is legacy and result["created"] is False
    assert legacy.oauth_issuer == _PROVIDER.issuer
    assert users.updated == [legacy]


@pytest.mark.anyio
async def test_the_same_subject_from_another_issuer_is_refused_the_account() -> None:
    from fastapi import HTTPException

    from app.gateway.auth.user_provisioning import get_or_provision_oidc_user

    bound = _linked("https://old-id.example.com/realms/co")
    users = _Users(bound)
    with pytest.raises(HTTPException) as refused:
        await get_or_provision_oidc_user("sso", _PROVIDER, _identity(), users)
    assert refused.value.status_code == 403
    assert users.created == [] and users.updated == []
    assert bound.oauth_issuer == "https://old-id.example.com/realms/co"


@pytest.mark.anyio
async def test_a_trailing_slash_is_not_a_different_issuer() -> None:
    from app.gateway.auth.user_provisioning import get_or_provision_oidc_user

    bound = _linked(_PROVIDER.issuer + "/")
    result = await get_or_provision_oidc_user("sso", _PROVIDER, _identity(), bound_users := _Users(bound))
    assert result["user"] is bound
    assert bound.oauth_issuer == _PROVIDER.issuer + "/", "an equivalent issuer is left as it was recorded"
    assert bound_users.updated == [bound] and bound.last_sign_in_at is not None, "the one write is the sign-in stamp"
