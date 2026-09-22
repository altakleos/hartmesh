"""An administrator adds a local account, and a pending setup reaches nothing but setup.

With self-registration closed, ``POST /api/v1/auth/users`` (an
administrator's interactive session) and ``python -m app.gateway.auth.add_user``
(the deployer, inside the deployment) are how an ordinary local account comes
into existence: one operation behind both, role ``user``, a one-time password
returned once. Until the person chooses their own password every session
of the account is refused everywhere but the setup routes, and choosing one
makes the one-time password open nothing. The readiness signal names whether
registration is open. The served Gateway, the command as a subprocess and an
ordinary chat turn by the new person are in ``test_local_add_user_e2e.py``.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
from types import SimpleNamespace

import pytest
from fastapi.testclient import TestClient

os.environ.setdefault("AUTH_JWT_SECRET", "test-secret-key-local-add-user-min-32")

from app.gateway.auth.config import AuthConfig, set_auth_config
from deerflow.config.app_config import AppConfig
from deerflow.config.auth_config import AuthAppConfig, LocalAuthConfig, OIDCAuthConfig, OIDCProviderConfig
from deerflow.config.sandbox_config import SandboxConfig

_TEST_SECRET = "test-secret-key-local-add-user-min-32"
_SANDBOX = SandboxConfig(use="deerflow.sandbox.local:LocalSandboxProvider")
_PROVIDER = OIDCProviderConfig(display_name="Company", issuer="https://id.example.com/realms/co", client_id="hartmesh", client_secret="FAKE-CREDENTIAL-SENTINEL-NOT-A-KEY")
_ADMIN = {"email": "owner@example.com", "password": "Str0ng!Pass99"}
_PERSON = "pat@example.com"
_CHOSEN = "Pat's-0wn-Passw0rd"
# A PAT-admissible route that needs the lifespan's thread store: without it the
# route answers 503, which proves a credential got past authentication (the
# middleware answers 401/403 before any route runs).
_ORDINARY_ROUTE = "/api/threads/whoami/goal"


def _local(*, open_: bool) -> AppConfig:
    return AppConfig(sandbox=_SANDBOX, auth=AuthAppConfig(local=LocalAuthConfig(allow_registration=open_)))


def _sign_on_only() -> AppConfig:
    return AppConfig(sandbox=_SANDBOX, auth=AuthAppConfig(local=LocalAuthConfig(enabled=False), oidc=OIDCAuthConfig(enabled=True, providers={"sso": _PROVIDER})))


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
    from app.gateway.routers.auth import _SETUP_STATUS_CACHE

    monkeypatch.setattr("deerflow.config.app_config.get_app_config", lambda: config)
    set_auth_config(AuthConfig(jwt_secret=_TEST_SECRET))
    _SETUP_STATUS_CACHE.clear()
    return TestClient(create_app())


def _csrf(client: TestClient) -> dict[str, str]:
    from app.gateway.csrf_middleware import CSRF_COOKIE_NAME, CSRF_HEADER_NAME, generate_csrf_token

    token = generate_csrf_token()
    client.cookies.set(CSRF_COOKIE_NAME, token)
    return {CSRF_HEADER_NAME: token}


def _admin(monkeypatch: pytest.MonkeyPatch, config: AppConfig) -> TestClient:
    client = _client(monkeypatch, config)
    created = client.post("/api/v1/auth/initialize", json=_ADMIN)
    assert created.status_code == 201, created.text
    return client


def _add(client: TestClient, email: str = _PERSON):
    return client.post("/api/v1/auth/users", json={"email": email}, headers=_csrf(client))


def _login(client: TestClient, email: str, password: str):
    return client.post("/api/v1/auth/login/local", data={"username": email, "password": password})


def _row(email: str):
    from app.gateway.deps import get_local_provider

    return asyncio.run(get_local_provider().get_user_by_email(email))


# ── The readiness signal ─────────────────────────────────────────────────


@pytest.mark.parametrize(
    ("config", "mode", "registration"),
    [(_local(open_=True), "local", "open"), (_local(open_=False), "local", "closed"), (_sign_on_only(), "sign_on_only", "closed")],
    ids=["local-open", "local-closed", "sign-on-only"],
)
def test_health_names_the_mode_and_whether_registration_is_open(users_db, monkeypatch: pytest.MonkeyPatch, config: AppConfig, mode: str, registration: str) -> None:
    client = _client(monkeypatch, config)
    health = client.get("/health").json()
    assert (health["auth_mode"], health["registration"]) == (mode, registration)
    # The door and the login page's source agree with the signal.
    if mode == "local":
        assert client.get("/api/v1/auth/setup-status").json()["registration_enabled"] is (registration == "open")
        door = client.post("/api/v1/auth/register", json={"email": "visitor@example.com", "password": "Tr0ub4dor3a"})
        assert door.status_code == (201 if registration == "open" else 403), door.text
        if registration == "closed":
            assert door.json()["detail"]["code"] == "registration_disabled"


def test_the_registration_state_is_read_live_and_a_bare_app_is_open(monkeypatch: pytest.MonkeyPatch) -> None:
    from app.gateway.auth.mode import registration_state

    monkeypatch.setattr("deerflow.config.app_config.get_app_config", lambda: _local(open_=False))
    assert registration_state() == "closed"
    monkeypatch.setattr("deerflow.config.app_config.get_app_config", lambda: _local(open_=True))
    assert registration_state() == "open"
    monkeypatch.setattr("deerflow.config.app_config.get_app_config", _sign_on_only)
    assert registration_state() == "closed", "sign-on only: nobody creates a local account, whatever allow_registration says"

    def _missing() -> AppConfig:
        raise FileNotFoundError("no config.yaml")

    monkeypatch.setattr("deerflow.config.app_config.get_app_config", _missing)
    assert registration_state() == "open", "the same fallback /register has always had"


# ── Adding a person in the product ───────────────────────────────────────


@pytest.mark.parametrize("open_", [False, True], ids=["registration-closed", "registration-open"])
def test_an_administrator_adds_a_person_and_stays_themselves(users_db, monkeypatch: pytest.MonkeyPatch, open_: bool) -> None:
    admin = _admin(monkeypatch, _local(open_=open_))
    before = admin.get("/api/v1/auth/me").json()
    session_before = admin.cookies["access_token"]

    added = _add(admin)
    assert added.status_code == 201, added.text
    body = added.json()
    assert set(body) == {"id", "email", "system_role", "needs_setup", "one_time_password"}
    assert (body["email"], body["system_role"], body["needs_setup"]) == (_PERSON, "user", True)
    assert len(body["one_time_password"]) >= 20
    assert added.headers["cache-control"] == "no-store"
    assert not any("access_token" in cookie for cookie in added.headers.get_list("set-cookie")), "no session is issued for the new account"

    assert admin.cookies["access_token"] == session_before
    assert admin.get("/api/v1/auth/me").json() == before, "the administrator is the same account before and after"
    stored = _row(_PERSON)
    assert stored.system_role == "user" and stored.needs_setup is True and stored.oauth_provider is None
    from app.gateway.auth.password import verify_password

    assert body["one_time_password"] not in (stored.password_hash or "") and verify_password(body["one_time_password"], stored.password_hash), "only a hash of it is kept"


def test_an_existing_address_is_refused_with_the_answer_register_gives(users_db, monkeypatch: pytest.MonkeyPatch) -> None:
    admin = _admin(monkeypatch, _local(open_=True))
    assert _client(monkeypatch, _local(open_=True)).post("/api/v1/auth/register", json={"email": "staff@example.com", "password": "Tr0ub4dor3a"}).status_code == 201
    register_twice = _client(monkeypatch, _local(open_=True)).post("/api/v1/auth/register", json={"email": "staff@example.com", "password": "Tr0ub4dor3a"})
    for email in ("staff@example.com", "Staff@Example.com", _ADMIN["email"]):
        refused = _add(admin, email)
        assert refused.status_code == register_twice.status_code == 400, refused.text
        assert refused.json()["detail"] == register_twice.json()["detail"] == {"code": "email_already_exists", "message": "Email already registered"}
    assert _row("staff@example.com").needs_setup is False, "the existing account is untouched"


def test_a_user_a_token_and_nobody_are_refused(users_db, monkeypatch: pytest.MonkeyPatch) -> None:
    from deerflow.persistence.engine import get_session_factory
    from deerflow.persistence.personal_access_tokens import PersonalAccessTokenRepository
    from deerflow.runtime.tenant_identity import TenantIdentityV1

    repo = PersonalAccessTokenRepository(get_session_factory(), tenant=TenantIdentityV1.from_canonical_id("tenant-a").to_persisted_reference())
    admin = _admin(monkeypatch, _local(open_=True))
    admin.app.state.pat_repo = repo
    minted = admin.post("/api/v1/auth/pats", json={"name": "automation", "scopes": ["threads:read", "threads:write"]}, headers=_csrf(admin))
    assert minted.status_code == 201, minted.text

    as_token = _client(monkeypatch, _local(open_=True))
    as_token.app.state.pat_repo = repo
    refused = as_token.post("/api/v1/auth/users", json={"email": _PERSON}, headers={"Authorization": f"Bearer {minted.json()['token']}"})
    # No PAT scope covers the route, so the middleware refuses a token before
    # the route's own session-only guard is reached.
    assert refused.status_code == 403 and refused.json()["detail"] == "PAT credentials are not permitted on this route", refused.text
    from app.gateway.auth.pat import required_pat_scope
    from app.gateway.routers.auth import add_user, require_session_source, router

    assert required_pat_scope("POST", "/api/v1/auth/users") is None
    route = next(route for route in router.routes if getattr(route, "endpoint", None) is add_user)
    assert require_session_source in [dependency.call for dependency in route.dependant.dependencies], "and the route is session-only on its own, as the lockout routes are"

    staff = _client(monkeypatch, _local(open_=True))
    assert staff.post("/api/v1/auth/register", json={"email": "staff@example.com", "password": "Tr0ub4dor3a"}).status_code == 201
    refused = _add(staff)
    assert refused.status_code == 403 and refused.json()["detail"] == "Admin role required to add people", refused.text

    nobody = _client(monkeypatch, _local(open_=True))
    assert _add(nobody).status_code == 401
    assert _row(_PERSON) is None


def test_sign_on_only_refuses_even_an_administrator(users_db, monkeypatch: pytest.MonkeyPatch) -> None:
    from app.gateway.auth import create_access_token
    from app.gateway.deps import get_local_provider

    owner = asyncio.run(get_local_provider().create_oauth_user(email="owner@example.com", oauth_provider="sso", oauth_id="sub-owner", system_role="admin", oauth_issuer=_PROVIDER.issuer))
    client = _client(monkeypatch, _sign_on_only())
    client.cookies.set("access_token", create_access_token(str(owner.id), token_version=owner.token_version))
    assert client.get("/api/v1/auth/me").json()["system_role"] == "admin"
    refused = _add(client)
    assert refused.status_code == 403 and refused.json()["detail"]["code"] == "sign_on_required", refused.text
    assert _row(_PERSON) is None


# ── The first credential ─────────────────────────────────────────────────


def test_the_one_time_password_opens_only_setup_and_dies_when_the_person_chooses_theirs(users_db, monkeypatch: pytest.MonkeyPatch) -> None:
    admin = _admin(monkeypatch, _local(open_=False))
    one_time = _add(admin).json()["one_time_password"]

    person = _client(monkeypatch, _local(open_=False))
    login = _login(person, _PERSON, one_time)
    assert login.status_code == 200 and login.json()["needs_setup"] is True, login.text
    me = person.get("/api/v1/auth/me")
    assert me.status_code == 200 and me.json()["needs_setup"] is True
    for method, path in (("get", _ORDINARY_ROUTE), ("get", "/api/v1/auth/pats"), ("get", "/api/models")):
        refused = getattr(person, method)(path)
        assert refused.status_code == 403 and refused.json()["detail"]["code"] == "setup_required", (path, refused.text)
    refused = person.post("/api/v1/auth/pats", json={"name": "x", "scopes": ["threads:read"]}, headers=_csrf(person))
    assert refused.status_code == 403 and refused.json()["detail"]["code"] == "setup_required"

    # A second sign-in before setup is again a setup-only session (see
    # local_accounts: nothing could issue a replacement for a lost one).
    again = _client(monkeypatch, _local(open_=False))
    assert _login(again, _PERSON, one_time).json()["needs_setup"] is True
    assert again.get(_ORDINARY_ROUTE).status_code == 403

    done = person.post("/api/v1/auth/change-password", json={"current_password": one_time, "new_password": _CHOSEN, "new_email": _PERSON}, headers=_csrf(person))
    assert done.status_code == 200, done.text
    assert person.get("/api/v1/auth/me").json()["needs_setup"] is False
    assert person.get(_ORDINARY_ROUTE).status_code == 503, "past authentication: the route itself answers"
    assert again.get("/api/v1/auth/me").status_code == 401, "the other setup-only session ended with the password change"

    stale = _login(_client(monkeypatch, _local(open_=False)), _PERSON, one_time)
    assert stale.status_code == 401 and stale.json()["detail"]["code"] == "invalid_credentials", "the one-time password opens nothing any more"
    fresh = _login(_client(monkeypatch, _local(open_=False)), _PERSON, _CHOSEN)
    assert fresh.status_code == 200 and fresh.json()["needs_setup"] is False


def test_a_pending_setup_confines_every_session_path_and_no_other_credential(users_db, monkeypatch: pytest.MonkeyPatch) -> None:
    """The browser WebSocket and the LangGraph auth hook refuse the session; an internal caller's owner header does not refuse the owner."""
    from app.gateway.auth import create_access_token
    from app.gateway.auth.mode import owner_is_refused
    from app.gateway.langgraph_auth import authenticate
    from app.gateway.routers.browser import _authenticate_ws

    admin = _admin(monkeypatch, _local(open_=False))
    _add(admin)
    user = _row(_PERSON)
    cookie = create_access_token(str(user.id), token_version=user.token_version)
    assert asyncio.run(_authenticate_ws(SimpleNamespace(cookies={"access_token": cookie}))) is None
    with pytest.raises(Exception) as hook:
        asyncio.run(authenticate(SimpleNamespace(cookies={"access_token": cookie}, headers={}, method="GET", url=SimpleNamespace(path="/api/threads"))))
    assert getattr(hook.value, "status_code", None) == 401
    # An added account holds no channel or schedule an internal caller could
    # act for; the header rule stays the disabled/inert one.
    assert asyncio.run(owner_is_refused(str(user.id))) is None


def test_a_reset_leaves_the_accounts_tokens_working_and_confines_only_its_sessions(users_db, monkeypatch: pytest.MonkeyPatch) -> None:
    """``reset_admin`` sets ``needs_setup`` too: the tokens it never exposed keep the automation running."""
    from app.gateway.deps import get_local_provider
    from deerflow.persistence.engine import get_session_factory
    from deerflow.persistence.personal_access_tokens import PersonalAccessTokenRepository
    from deerflow.runtime.tenant_identity import TenantIdentityV1

    repo = PersonalAccessTokenRepository(get_session_factory(), tenant=TenantIdentityV1.from_canonical_id("tenant-a").to_persisted_reference())
    admin = _admin(monkeypatch, _local(open_=False))
    admin.app.state.pat_repo = repo
    minted = admin.post("/api/v1/auth/pats", json={"name": "automation", "scopes": ["threads:read", "threads:write"]}, headers=_csrf(admin))
    assert minted.status_code == 201, minted.text

    owner = _row(_ADMIN["email"])
    owner.needs_setup = True  # what reset_admin writes, beside a new password and token_version
    asyncio.run(get_local_provider().update_user(owner))

    session = admin.get(_ORDINARY_ROUTE)
    assert session.status_code == 403 and session.json()["detail"]["code"] == "setup_required", session.text
    as_token = _client(monkeypatch, _local(open_=False))
    as_token.app.state.pat_repo = repo
    worked = as_token.get(_ORDINARY_ROUTE, headers={"Authorization": f"Bearer {minted.json()['token']}"})
    assert worked.status_code == 503, "past authentication: the route itself answers"


@pytest.mark.parametrize(("owner", "refused"), [({"needs_setup": True}, False), ({"needs_setup": True, "disabled_at": "2026-09-22T00:00:00+00:00"}, True)])
def test_a_scheduled_task_of_a_reset_owner_still_runs(monkeypatch: pytest.MonkeyPatch, owner: dict, refused: bool) -> None:
    """Internal launches keep the disabled/inert rule only: a pending setup confines sessions, not the owner's schedules or channels."""
    from app.gateway import services
    from app.gateway.auth_disabled import AUTH_SOURCE_INTERNAL
    from app.runtime.invocation import InternalLaunchIntent, InternalSourceKind

    async def resolve_owner(_request, _owner_user_id):
        return SimpleNamespace(id="owner-1", system_role="admin", oauth_provider=None, oauth_id=None, **owner)

    monkeypatch.setattr(services, "resolve_trusted_internal_owner_for_attribution", resolve_owner)
    monkeypatch.setattr("deerflow.config.app_config.get_app_config", lambda: _local(open_=False))
    request = SimpleNamespace(state=SimpleNamespace(user=SimpleNamespace(id="internal", system_role="internal"), auth_source=AUTH_SOURCE_INTERNAL))
    intent = InternalLaunchIntent(thread_id="thread-1", source_kind=InternalSourceKind.scheduled_task, owner_user_id="owner-1", trusted_task_id="task-1", task_run_id="occurrence-1")
    launch = services._principal_projection_for_intent(request, intent, owner_user_id="owner-1")
    if refused:
        with pytest.raises(ValueError, match="account is disabled"):
            asyncio.run(launch)
    else:
        assert asyncio.run(launch).identity.effective_subject.subject_id == "owner-1"


def test_the_added_account_does_not_show_its_password_when_printed(users_db) -> None:
    from app.gateway.auth.local_accounts import add_local_account
    from app.gateway.deps import get_local_provider

    added = asyncio.run(add_local_account(get_local_provider(), _PERSON))
    assert added.one_time_password not in repr(added) and added.one_time_password not in str(added)


def test_the_one_time_password_is_never_logged(users_db, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture) -> None:
    caplog.set_level(logging.DEBUG)
    admin = _admin(monkeypatch, _local(open_=False))
    one_time = _add(admin).json()["one_time_password"]
    person = _client(monkeypatch, _local(open_=False))
    _login(person, _PERSON, one_time)
    person.post("/api/v1/auth/change-password", json={"current_password": one_time, "new_password": _CHOSEN, "new_email": _PERSON}, headers=_csrf(person))
    _login(_client(monkeypatch, _local(open_=False)), _PERSON, one_time)
    assert any("Local account added: pat@example.com" in record.getMessage() for record in caplog.records)
    assert one_time not in caplog.text


# ── Adding a person from inside the deployment ───────────────────────────


@pytest.fixture
def deployment(tmp_path, monkeypatch: pytest.MonkeyPatch):
    """A config the command loads, on a sqlite file the command opens and closes itself."""
    from app.gateway import deps
    from deerflow.config.database_config import DatabaseConfig

    set_auth_config(AuthConfig(jwt_secret=_TEST_SECRET))
    deps._cached_local_provider = None
    deps._cached_repo = None

    def _use(config: AppConfig) -> AppConfig:
        config = config.model_copy(update={"database": DatabaseConfig(backend="sqlite", sqlite_dir=str(tmp_path))})
        monkeypatch.setattr("deerflow.config.app_config.get_app_config", lambda: config)
        monkeypatch.setattr("deerflow.config.get_app_config", lambda: config)
        return config

    try:
        yield _use
    finally:
        deps._cached_local_provider = None
        deps._cached_repo = None


def _command(capsys: pytest.CaptureFixture[str], *args: str) -> tuple[int, dict, str]:
    from app.gateway.auth import add_user

    code = add_user.main(list(args))
    captured = capsys.readouterr()
    lines = [line for line in captured.out.splitlines() if line.strip()]
    assert len(lines) == 1, captured.out
    return code, json.loads(lines[0]), captured.err


def test_the_command_adds_a_person_and_refuses_the_same_address_twice(deployment, capsys: pytest.CaptureFixture[str], caplog: pytest.LogCaptureFixture) -> None:
    caplog.set_level(logging.DEBUG)
    config = deployment(_local(open_=False))
    code, document, err = _command(capsys, "--email", "Pat@Example.com")
    assert code == 0, document
    assert set(document) == {"command", "id", "email", "system_role", "needs_setup", "one_time_password"}
    assert (document["command"], document["email"], document["system_role"], document["needs_setup"]) == ("add-user", "pat@example.com", "user", True)
    one_time = document["one_time_password"]
    assert one_time not in err and one_time not in caplog.text

    code, again, _ = _command(capsys, "--email", "pat@example.com")
    assert code == 1 and again == {"command": "add-user", "error": "email_already_exists", "message": "Email already registered"}

    # The account the command made is one the product opens -- for setup only.
    from deerflow.persistence.engine import close_engine, init_engine_from_config

    asyncio.run(init_engine_from_config(config.database))
    try:
        stored = _row("pat@example.com")
        assert stored.system_role == "user" and stored.needs_setup is True
        from app.gateway.auth.password import verify_password

        assert verify_password(one_time, stored.password_hash)
    finally:
        asyncio.run(close_engine())


def test_the_command_refuses_in_sign_on_only_mode_before_it_opens_the_database(deployment, capsys: pytest.CaptureFixture[str], monkeypatch: pytest.MonkeyPatch) -> None:
    deployment(_sign_on_only())

    async def _must_not_open(*_: object, **__: object) -> None:
        raise AssertionError("the database was opened")

    monkeypatch.setattr("deerflow.persistence.engine.init_engine_from_config", _must_not_open)
    code, document, _ = _command(capsys, "--email", "pat@example.com")
    assert code == 1 and document["error"] == "sign_on_required" and "identity provider" in document["message"], document


def test_the_command_refuses_what_is_not_an_address_without_echoing_it(deployment, capsys: pytest.CaptureFixture[str]) -> None:
    deployment(_local(open_=False))
    code, document, _ = _command(capsys, "--email", "not an address FAKE-CREDENTIAL-SENTINEL-NOT-A-KEY")
    assert code == 1 and document == {"command": "add-user", "error": "email_invalid", "message": "--email must be one email address"}


@pytest.mark.parametrize("argv", [[], ["-h"], ["--help"], ["--email"], ["--emial", "FAKE-CREDENTIAL-SENTINEL-NOT-A-KEY"], ["--email", "pat@example.com", "FAKE-CREDENTIAL-SENTINEL-NOT-A-KEY"]])
def test_a_command_line_it_cannot_read_is_one_document_too(deployment, capsys: pytest.CaptureFixture[str], argv: list[str]) -> None:
    deployment(_local(open_=False))
    code, document, err = _command(capsys, *argv)
    assert code == 1 and document == {"command": "add-user", "error": "usage", "message": "usage: python -m app.gateway.auth.add_user --email ADDRESS"}
    assert "FAKE-CREDENTIAL" not in err


def test_the_command_answers_with_one_document_whatever_failed(deployment, capsys: pytest.CaptureFixture[str], monkeypatch: pytest.MonkeyPatch) -> None:
    deployment(_local(open_=False))

    async def _broken(*_: object, **__: object) -> None:
        raise RuntimeError("postgresql://deerflow:FAKE-CREDENTIAL-SENTINEL-NOT-A-KEY@postgres unreachable")

    monkeypatch.setattr("deerflow.persistence.engine.init_engine_from_config", _broken)
    code, document, _ = _command(capsys, "--email", "pat@example.com")
    assert code == 1 and document == {"command": "add-user", "error": "failed", "message": "RuntimeError"}, "the type only: a driver's message can carry its DSN"
