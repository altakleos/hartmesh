"""Sign-on-only mode through the real Gateway, against a provider the tests start.

``test_sign_on_only.py`` proves each door on the bare app. This drives the
served Gateway -- lifespan, migrations, middleware, the OIDC flow with state,
PKCE and nonce, the run pipeline -- against ``_oidc_test_provider``, a generic
OpenID Connect provider on loopback with no vendor behaviour, and asserts
what a tenant would see: a first sign-in lands in the product with the role
the administrators' list gives it, a signed-in ``user`` uses a tool and the
sandbox in an ordinary turn and is refused what the product reserves for
administrators, an account never crosses issuers, a database from before the
switch keeps its local accounts inert, the readiness signal names the mode,
and the client secret appears nowhere.

The published port (nginx) is not part of this: the header-stripping it adds
is pinned on the configuration file in ``test_sign_on_only.py``.
"""

from __future__ import annotations

import contextlib
import logging
import sqlite3
import uuid
from collections.abc import Iterator
from pathlib import Path
from urllib.parse import parse_qs, urlparse

import httpx
import pytest
import test_turn_phase_gateway_stream_e2e as e2e
from _oidc_test_provider import OIDCTestProvider

pytestmark = pytest.mark.xdist_group("sign-on-only-gateway")

CLIENT_ID = "hartmesh-tenant"
CLIENT_SECRET = "FAKE-CREDENTIAL-SENTINEL-NOT-A-KEY"
SECRET_ENV = "SSO_CLIENT_SECRET"
OWNER = "owner@example.com"
STAFF = "pat@example.com"


def _config(provider: OIDCTestProvider, *, issuer: str = "a", admin_emails: tuple[str, ...] = (OWNER,), local: bool = False, second_provider: bool = True) -> str:
    """The Gateway's config.yaml: the probe model, host bash in the local sandbox, and the sign-in mode."""
    admins = "".join(f"\n        - {email}" for email in admin_emails)
    providers = f"""
      sso:
        display_name: Company
        issuer: {provider.issuer_url(issuer)}
        client_id: {CLIENT_ID}
        client_secret: ${SECRET_ENV}
        admin_emails:{admins or " []"}"""
    if second_provider:
        providers += f"""
      sso-basic:
        display_name: Company (basic)
        issuer: {provider.issuer_url("a")}
        client_id: {CLIENT_ID}
        client_secret: ${SECRET_ENV}
        token_endpoint_auth_method: client_secret_basic"""
    auth = (
        "auth:\n  local:\n    enabled: true\n"
        if local
        else f"""auth:
  local:
    enabled: false
  oidc:
    enabled: true
    providers:{providers}
"""
    )
    return f"""\
log_level: info
models:
  - name: turn-phase-probe
    display_name: Turn Phase Probe
    use: _turn_phase_probe_model:ProbeStreamingChatModel
    model: probe
sandbox:
  use: deerflow.sandbox.local:LocalSandboxProvider
  allow_host_bash: true
agents_api:
  enabled: true
database:
  backend: sqlite
tool_groups:
  - name: bash
tools:
  - name: bash
    group: bash
    use: deerflow.sandbox.tools:bash_tool
{auth}"""


class _Journal(logging.Handler):
    """Every record the Gateway emits while served, for the secret-hygiene check."""

    def __init__(self) -> None:
        super().__init__(level=logging.DEBUG)
        self.lines: list[str] = []

    def emit(self, record: logging.LogRecord) -> None:
        with contextlib.suppress(Exception):
            self.lines.append(self.format(record))


@pytest.fixture(scope="module")
def provider() -> Iterator[OIDCTestProvider]:
    started = OIDCTestProvider(("a", "b"), client_id=CLIENT_ID, client_secret=CLIENT_SECRET).start()
    try:
        yield started
    finally:
        started.stop()


@pytest.fixture(scope="module")
def journal() -> Iterator[_Journal]:
    handler = _Journal()
    handler.setFormatter(logging.Formatter("%(name)s %(levelname)s %(message)s"))
    logging.getLogger().addHandler(handler)
    try:
        yield handler
    finally:
        logging.getLogger().removeHandler(handler)


@pytest.fixture(scope="class")
def gateway(provider: OIDCTestProvider, journal: _Journal, tmp_path_factory: pytest.TempPathFactory) -> Iterator[e2e._Gateway]:
    """One served Gateway for the class below; class-scoped because the process singletons it owns cannot be shared with a second served Gateway."""
    env = pytest.MonkeyPatch()
    env.setenv(SECRET_ENV, CLIENT_SECRET)
    env.setenv("DEER_FLOW_AUTH_DISABLED", "")
    try:
        with e2e.serve_gateway(tmp_path_factory.mktemp("sign-on-only"), config_yaml=_config(provider)) as served:
            yield served
    finally:
        env.undo()


# ── Driving the flow the way a browser does ──────────────────────────────


def _begin(client: httpx.Client, base: str, provider_id: str = "sso") -> str:
    """Leg one: the Gateway sends the browser to the provider. Returns the authorization URL."""
    started = client.get(f"{base}/api/v1/auth/oauth/{provider_id}", params={"next": "/workspace"})
    assert started.status_code == 302, started.text
    assert f"df_oidc_state_{provider_id}" in client.cookies, "the signed state cookie travels with the browser"
    return started.headers["location"]


def _sign_in(client: httpx.Client, base: str, provider: OIDCTestProvider, *, subject: str, email: str, provider_id: str = "sso", email_verified: bool = True) -> httpx.Response:
    """All three legs; returns the Gateway's answer to the callback."""
    authorization_url = _begin(client, base, provider_id)
    query = parse_qs(urlparse(authorization_url).query)
    assert query["response_type"] == ["code"] and query["code_challenge_method"] == ["S256"] and query.get("nonce")
    at_provider = httpx.get(provider.sign_in_url(authorization_url, subject=subject, email=email, email_verified=email_verified), follow_redirects=False)
    assert at_provider.status_code == 302, at_provider.text
    back = at_provider.headers["location"]
    assert back.startswith(f"{base}/api/v1/auth/callback/{provider_id}?"), back
    return client.get(back)


def _client(base: str) -> httpx.Client:
    return httpx.Client(base_url=base, follow_redirects=False, timeout=30.0)


def _csrf(client: httpx.Client) -> dict[str, str]:
    token = client.cookies.get("csrf_token")
    assert token, "sign-in must set the CSRF cookie"
    return {"X-CSRF-Token": token}


def _create_thread(client: httpx.Client, base: str) -> str:
    thread_id = str(uuid.uuid4())
    created = client.post(f"{base}/api/threads", json={"thread_id": thread_id, "metadata": {}}, headers=_csrf(client))
    assert created.status_code == 200, created.text
    return thread_id


def _db(gateway: e2e._Gateway) -> Path:
    return gateway.tmp_home / "deer-flow-home" / "db" / "deerflow.db"


def _account_row(gateway: e2e._Gateway, email: str) -> tuple | None:
    with sqlite3.connect(_db(gateway)) as connection:
        return connection.execute("SELECT system_role, oauth_provider, oauth_id, oauth_issuer, password_hash FROM users WHERE email = ?", (email,)).fetchone()


def _rewrite_config(gateway: e2e._Gateway, text: str) -> None:
    (gateway.tmp_home / "config.yaml").write_text(text, encoding="utf-8")


class TestServedGateway:
    """Every test here drives the one class-scoped Gateway; the module-level tests below serve their own."""

    # ── Evidence 6: a full sign-in ───────────────────────────────────────────

    def test_a_full_sign_in_creates_the_account_and_lands_in_the_product(self, gateway: e2e._Gateway, provider: OIDCTestProvider) -> None:
        base = gateway.loopback_url
        with _client(base) as client:
            landed = _sign_in(client, base, provider, subject="sub-owner", email=OWNER)
            assert landed.status_code == 302, landed.text
            assert landed.headers["location"] == "/auth/callback?next=/workspace"
            assert "access_token" in client.cookies and "csrf_token" in client.cookies
            assert "df_oidc_state_sso" not in client.cookies, "the state cookie is spent"

            me = client.get(f"{base}/api/v1/auth/me")
            assert me.status_code == 200, me.text
            assert me.json()["email"] == OWNER and me.json()["system_role"] == "admin" and me.json()["oauth_provider"] == "sso"

        exchange = provider.issuers["a"].exchanges[-1]
        assert exchange == {"method": "client_secret_post", "client_id": CLIENT_ID, "pkce_verified": True, "ok": True, "nonce": exchange["nonce"]}
        assert exchange["nonce"], "the nonce the Gateway generated went into the ID token, and the Gateway accepted that token"
        assert _account_row(gateway, OWNER) == ("admin", "sso", "sub-owner", provider.issuer_url("a"), None)

        with _client(base) as client:
            landed = _sign_in(client, base, provider, subject="sub-staff", email=STAFF)
            assert landed.status_code == 302 and landed.headers["location"].startswith("/auth/callback")
            assert client.get(f"{base}/api/v1/auth/me").json()["system_role"] == "user"

    def test_state_pkce_and_nonce_are_each_verified(self, gateway: e2e._Gateway, provider: OIDCTestProvider) -> None:
        base = gateway.loopback_url
        # A callback whose state is not the one in the signed cookie.
        with _client(base) as client:
            authorization_url = _begin(client, base)
            at_provider = httpx.get(provider.sign_in_url(authorization_url, subject="sub-x", email="x@example.com"), follow_redirects=False)
            back = urlparse(at_provider.headers["location"])
            forged = f"{base}{back.path}?code={parse_qs(back.query)['code'][0]}&state=not-the-state"
            refused = client.get(forged)
            assert refused.status_code == 403 and "state" in refused.text.lower()
            assert "access_token" not in client.cookies
        # A provider that answers with a token carrying the wrong nonce.
        provider.issuers["a"].wrong_nonce = True
        try:
            with _client(base) as client:
                landed = _sign_in(client, base, provider, subject="sub-y", email="y@example.com")
                assert landed.status_code == 302 and landed.headers["location"] == "/login?error=sso_failed"
                assert "access_token" not in client.cookies
        finally:
            provider.issuers["a"].wrong_nonce = False
        assert _account_row(gateway, "y@example.com") is None
        # The PKCE verifier is what the provider checked before it issued anything:
        # the fake refuses a mismatched verifier, and every successful exchange
        # above recorded that the check ran.
        assert all(exchange["pkce_verified"] for exchange in provider.issuers["a"].exchanges if exchange["ok"])

    def test_the_second_provider_uses_basic_client_authentication_and_keeps_its_own_accounts(self, gateway: e2e._Gateway, provider: OIDCTestProvider) -> None:
        base = gateway.loopback_url
        with _client(base) as client:
            landed = _sign_in(client, base, provider, subject="sub-basic", email="basic@example.com", provider_id="sso-basic")
            assert landed.status_code == 302 and landed.headers["location"].startswith("/auth/callback"), landed.text
            assert client.get(f"{base}/api/v1/auth/me").json()["oauth_provider"] == "sso-basic"
        assert provider.issuers["a"].exchanges[-1]["method"] == "client_secret_basic"
        # Two providers are two account namespaces: the owner's subject presented
        # through the other provider is a different (provider, subject) key, and
        # the address it carries already belongs to the owner's account. (The
        # owner signs in first here rather than relying on an earlier test:
        # the shards split a class across processes.)
        with _client(base) as client:
            assert _sign_in(client, base, provider, subject="sub-owner", email=OWNER).headers["location"].startswith("/auth/callback")
        with _client(base) as client:
            landed = _sign_in(client, base, provider, subject="sub-owner", email=OWNER, provider_id="sso-basic")
            assert landed.headers["location"] == "/login?error=sso_account_exists"
        assert _account_row(gateway, OWNER)[1:3] == ("sso", "sub-owner")

    def test_an_address_no_account_can_hold_is_refused_in_its_own_words(self, gateway: e2e._Gateway, provider: OIDCTestProvider, journal: _Journal) -> None:
        """A first sign-in for an address the record will not hold: its own code, and no account claimed to be in the way."""
        base = gateway.loopback_url
        unusable = "someone@example.invalid"
        with _client(base) as client:
            landed = _sign_in(client, base, provider, subject="sub-unusable", email=unusable)
            assert landed.status_code == 302 and landed.headers["location"] == "/login?error=sso_email_unusable", landed.text
            assert "access_token" not in client.cookies
        assert _account_row(gateway, unusable) is None, "nothing was created"
        refusals = [line for line in journal.lines if "sub-unusable" in line]
        # The provisioning line, by its own words: the callback's line carries
        # the subject and the issuer too, so matching only those would pass
        # against the refusal this change replaced.
        assert any("is not one an account can hold" in line and provider.issuer_url("a") in line for line in refusals), refusals
        assert not any("already exists" in line for line in refusals), refusals

    # ── Evidence 2, 4, 5: the doors through the served Gateway ───────────────

    def test_the_local_doors_are_closed_and_setup_status_is_constant(self, gateway: e2e._Gateway) -> None:
        base = gateway.loopback_url
        with _client(base) as client:
            initialize = client.post(f"{base}/api/v1/auth/initialize", json={"email": "stranger@example.com", "password": "Str0ng!Pass99"})
            register = client.post(f"{base}/api/v1/auth/register", json={"email": "stranger@example.com", "password": "Str0ng!Pass99"})
            login = client.post(f"{base}/api/v1/auth/login/local", data={"username": OWNER, "password": "Str0ng!Pass99"})
            for door in (initialize, register, login):
                assert door.status_code == 403, door.text
                assert door.json()["detail"]["code"] == "sign_on_required"
            assert "access_token" not in client.cookies
            assert client.get(f"{base}/api/v1/auth/setup-status").json() == {"needs_setup": False, "registration_enabled": False, "sign_on_only": True}
            assert client.get(f"{base}/api/v1/auth/providers").json() == {"providers": [{"id": "sso", "display_name": "Company", "type": "oidc"}, {"id": "sso-basic", "display_name": "Company (basic)", "type": "oidc"}]}
            health = client.get(f"{base}/health").json()
            assert health["auth_mode"] == "sign_on_only", health
        assert _account_row(gateway, "stranger@example.com") is None

    def test_the_internal_owner_header_alone_is_nobody_and_the_internal_service_still_works(self, gateway: e2e._Gateway) -> None:
        """Without the per-process token the owner header is a stranger's header; with it, an internal service acts as the owner as before.

        The token never leaves the process: its one holder is the channel
        manager inside the Gateway, which this test process is, so the
        headers it would send are the headers ``create_internal_auth_headers``
        mints here. (Through the published port, nginx blanks both headers
        before the Gateway sees them: ``test_sign_on_only.py``.)
        """
        from app.gateway.csrf_middleware import generate_csrf_token
        from app.gateway.internal_auth import create_internal_auth_headers

        base = gateway.loopback_url
        with _client(base) as client:
            refused = client.get(f"{base}/api/threads/{uuid.uuid4()}/state", headers={"X-DeerFlow-Owner-User-Id": "sub-owner", "X-DeerFlow-Internal-Token": "guessed"})
            assert refused.status_code == 401, refused.text
            # The channel manager mints its own CSRF pair beside the internal headers (app/channels/manager.py).
            pair = generate_csrf_token()
            served = client.post(
                f"{base}/api/threads/search",
                json={},
                headers={**create_internal_auth_headers(owner_user_id="channel-owner"), "X-CSRF-Token": pair, "Cookie": f"csrf_token={pair}"},
            )
            assert served.status_code == 200, served.text
            assert served.json() == [], "the owner the header names has no threads; the call is served, not refused"

    # ── Evidence 7: ordinary use as a user, and what is reserved for admins ──

    def test_a_user_runs_a_tool_in_the_sandbox_and_is_refused_what_admins_get(self, gateway: e2e._Gateway, provider: OIDCTestProvider) -> None:
        base = gateway.loopback_url
        with _client(base) as staff:
            _sign_in(staff, base, provider, subject="sub-staff", email=STAFF)
            assert staff.get(f"{base}/api/v1/auth/me").json()["system_role"] == "user"
            thread_id = _create_thread(staff, base)
            observed = e2e._observe_stream(staff, base, thread_id, _csrf(staff)["X-CSRF-Token"], "probe:bash echo sign-on-only-turn-ok", timeout=120.0, recursion_limit=100)
            run = staff.get(f"{base}/api/threads/{thread_id}/runs/{observed.run_id}").json()
            history = staff.post(f"{base}/api/threads/{thread_id}/history", json={"limit": 30}, headers=_csrf(staff)).json()
            refused = staff.get(f"{base}/api/v1/auth/lockouts")
        assert run["status"] == "success", run
        assert observed.text_frames >= 1
        messages = [message for state in history for message in (state.get("values") or {}).get("messages", [])]
        tool_results = [message for message in messages if message.get("type") == "tool"]
        assert tool_results and "sign-on-only-turn-ok" in str(tool_results[0].get("content")), tool_results
        assert refused.status_code == 403, refused.text

        with _client(base) as owner:
            _sign_in(owner, base, provider, subject="sub-owner", email=OWNER)
            allowed = owner.get(f"{base}/api/v1/auth/lockouts")
        assert allowed.status_code == 200, allowed.text
        assert allowed.json() == {"lockouts": []}

    # ── Evidence 8: no account crosses issuers ───────────────────────────────

    def test_no_account_crosses_issuers(self, gateway: e2e._Gateway, provider: OIDCTestProvider) -> None:
        base = gateway.loopback_url
        email = "mover@example.com"
        with _client(base) as client:
            assert _sign_in(client, base, provider, subject="sub-mover", email=email).headers["location"].startswith("/auth/callback")
        assert _account_row(gateway, email)[3] == provider.issuer_url("a")

        # The same provider name pointed at another issuer: the same subject there is refused the account.
        _rewrite_config(gateway, _config(provider, issuer="b"))
        try:
            with _client(base) as client:
                landed = _sign_in(client, base, provider, subject="sub-mover", email=email)
                assert landed.headers["location"] == "/login?error=sso_not_allowed", landed.headers
                assert "access_token" not in client.cookies
            assert _account_row(gateway, email)[3] == provider.issuer_url("a"), "the row is left as it was"

            # An account linked before the issuer was recorded (NULL) keeps signing
            # in, and is pinned to the issuer that vouched for it this time.
            with sqlite3.connect(_db(gateway)) as connection:
                connection.execute("UPDATE users SET oauth_issuer = NULL WHERE email = ?", (email,))
            with _client(base) as client:
                landed = _sign_in(client, base, provider, subject="sub-mover", email=email)
                assert landed.headers["location"].startswith("/auth/callback"), landed.headers
                assert client.get(f"{base}/api/v1/auth/me").json()["email"] == email
            assert _account_row(gateway, email)[3] == provider.issuer_url("b")
        finally:
            _rewrite_config(gateway, _config(provider))

        # Back on the original issuer, the row now pinned to b is refused there.
        with _client(base) as client:
            assert _sign_in(client, base, provider, subject="sub-mover", email=email).headers["location"] == "/login?error=sso_not_allowed"

    # ── Evidence 10: the secret ──────────────────────────────────────────────

    def test_zz_the_client_secret_appears_in_no_log_line(self, gateway: e2e._Gateway, journal: _Journal, tmp_path_factory: pytest.TempPathFactory) -> None:
        """Runs last in the class: every line every Gateway served in this process journalled so far, and every config file one wrote."""
        assert journal.lines, "the Gateway logged something while it was driven"
        leaked = [line for line in journal.lines if CLIENT_SECRET in line]
        assert leaked == []
        for config in tmp_path_factory.getbasetemp().glob("sign-on-only*/config.yaml"):
            assert CLIENT_SECRET not in config.read_text(encoding="utf-8"), f"{config} must carry the reference, not the value"


# ── Evidence 6 (last clause): no administrators' list, no administrator ──


def test_a_deployment_without_the_administrators_list_has_no_administrator(provider: OIDCTestProvider, tmp_path_factory: pytest.TempPathFactory) -> None:
    env = pytest.MonkeyPatch()
    env.setenv(SECRET_ENV, CLIENT_SECRET)
    env.setenv("DEER_FLOW_AUTH_DISABLED", "")
    try:
        with e2e.serve_gateway(tmp_path_factory.mktemp("sign-on-only-no-admins"), config_yaml=_config(provider, admin_emails=(), second_provider=False)) as served:
            base = served.loopback_url
            with _client(base) as client:
                _sign_in(client, base, provider, subject="sub-owner", email=OWNER)
                assert client.get(f"{base}/api/v1/auth/me").json()["system_role"] == "user"
                assert client.get(f"{base}/api/v1/auth/lockouts").status_code == 403
                assert client.get(f"{base}/api/v1/auth/setup-status").json() == {"needs_setup": False, "registration_enabled": False, "sign_on_only": True}
            with sqlite3.connect(_db(served)) as connection:
                assert connection.execute("SELECT count(*) FROM users WHERE system_role = 'admin'").fetchone() == (0,)
    finally:
        env.undo()


# ── Evidence 2 (restore) and 3 (the start) ───────────────────────────────


def test_a_database_from_before_the_switch_keeps_its_local_accounts_inert(provider: OIDCTestProvider, tmp_path_factory: pytest.TempPathFactory) -> None:
    home = tmp_path_factory.mktemp("sign-on-only-switch")
    env = pytest.MonkeyPatch()
    env.setenv(SECRET_ENV, CLIENT_SECRET)
    env.setenv("DEER_FLOW_AUTH_DISABLED", "")
    try:
        # Local-password mode first: an admin and a token minted before the switch.
        with e2e.serve_gateway(home, config_yaml=_config(provider, local=True)) as served:
            base = served.loopback_url
            with _client(base) as client:
                assert client.get(f"{base}/health").json()["auth_mode"] == "local"
                created = client.post(f"{base}/api/v1/auth/initialize", json={"email": OWNER, "password": "Str0ng!Pass99"})
                assert created.status_code == 201, created.text
                session_cookie = client.cookies["access_token"]
                minted = client.post(f"{base}/api/v1/auth/pats", json={"name": "automation", "scopes": ["threads:read"]}, headers=_csrf(client))
                assert minted.status_code == 201, minted.text
                token = minted.json()["token"]
                thread_id = _create_thread(client, base)
                client.cookies.clear()
                assert client.get(f"{base}/api/threads/{thread_id}/state", headers={"Authorization": f"Bearer {token}"}).status_code == 200

        # The switch, twice: the second start is the restart.
        for start in range(2):
            with e2e.serve_gateway(home, config_yaml=_config(provider)) as served:
                base = served.loopback_url
                with _client(base) as client:
                    assert client.get(f"{base}/health").json()["auth_mode"] == "sign_on_only"
                    assert client.get(f"{base}/api/v1/auth/setup-status").json() == {"needs_setup": False, "registration_enabled": False, "sign_on_only": True}, start
                    login = client.post(f"{base}/api/v1/auth/login/local", data={"username": OWNER, "password": "Str0ng!Pass99"})
                    assert login.status_code == 403 and login.json()["detail"]["code"] == "sign_on_required"
                    assert client.get(f"{base}/api/threads/{thread_id}/state", headers={"Authorization": f"Bearer {token}"}).status_code == 401
                    client.cookies.set("access_token", session_cookie)
                    me = client.get(f"{base}/api/v1/auth/me")
                    assert me.status_code == 401 and me.json()["detail"]["code"] == "sign_on_required", me.text
                    change = client.post(f"{base}/api/v1/auth/change-password", json={"current_password": "Str0ng!Pass99", "new_password": "An0ther!Pass99"}, headers={"X-CSRF-Token": "x"})
                    assert change.status_code in {401, 403}, change.text
                    # The owner signs in through the provider as themself: a new
                    # account, because the address is held by the inert local one.
                    landed = _sign_in(client, base, provider, subject="sub-owner", email=OWNER)
                    assert landed.headers["location"] == "/login?error=sso_account_exists"
            assert _account_row(served, OWNER)[0] == "admin" and _account_row(served, OWNER)[4] is not None, "the local row is kept, inert; clearing it is the deployer's job"
    finally:
        env.undo()


def test_auth_disabled_refuses_the_start(provider: OIDCTestProvider, tmp_path_factory: pytest.TempPathFactory, caplog: pytest.LogCaptureFixture) -> None:
    env = pytest.MonkeyPatch()
    env.setenv(SECRET_ENV, CLIENT_SECRET)
    env.setenv("DEER_FLOW_AUTH_DISABLED", "1")
    env.setenv("DEER_FLOW_ENV", "development")
    try:
        with caplog.at_level(logging.ERROR), pytest.raises(RuntimeError, match="exited before it started"), e2e.serve_gateway(tmp_path_factory.mktemp("sign-on-only-auth-disabled"), config_yaml=_config(provider)):
            pass
    finally:
        env.undo()
    assert "DEER_FLOW_AUTH_DISABLED=1 is set on a sign-on-only deployment" in caplog.text
