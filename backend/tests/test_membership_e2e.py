"""Membership follows the claim, through the real Gateway, against a provider the tests start.

``test_membership_claim.py`` proves the pieces; this drives the served
Gateway -- lifespan, migrations, middleware, the OIDC flow, the run
pipeline, the scheduler -- against ``_oidc_test_provider`` with claims the
tests control, and runs the deployer's command as the deployer does: a
subprocess inside the deployment, reading one JSON document back. What a
tenant would see: a token that carries the admitting value signs in and
one that does not is refused with no account, at the first sign-in and at
every later one; each claim shape admits from either source; the role
follows the claim in both directions and an open session in another browser
does not keep the old one; a turned-off account is refused on every path
that acts for it, before and after a restart and on a copied database; an
enable revives nothing; ending sessions leaves tokens alone; nothing over
HTTP turns an account off or on.
"""

from __future__ import annotations

import contextlib
import json
import logging
import os
import shutil
import sqlite3
import subprocess
import sys
import threading
import time
import uuid
from collections.abc import Iterator
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlparse

import httpx
import pytest
import test_turn_phase_gateway_stream_e2e as e2e
from _oidc_test_provider import OIDCTestProvider

pytestmark = pytest.mark.xdist_group("membership-gateway")

CLIENT_ID = "hartmesh-tenant"
CLIENT_SECRET = "FAKE-CREDENTIAL-SENTINEL-NOT-A-KEY"
SECRET_ENV = "SSO_CLIENT_SECRET"
CLAIM = "urn:zitadel:iam:org:project:roles"
OWNER = "owner@example.com"
BACKEND = Path(__file__).resolve().parents[1]


def _config(provider: OIDCTestProvider, home: Path, *, claim: bool = True, roles: bool = True, admin_emails: tuple[str, ...] = ()) -> str:
    """The Gateway's config.yaml: the probe model, host bash in the local sandbox, sign-on only, the scheduler polling every second, and the membership rule.

    The database directory is in the file (the served harness would set it in
    process anyway) because the deployer's command runs as a separate process
    and must open the same file the Gateway does.
    """
    admins = "".join(f"\n        - {email}" for email in admin_emails)
    membership = ""
    if claim:
        membership += f"""
        access_claim: "{CLAIM}"
        access_values: [admin, member]"""
    if roles:
        membership += """
        access_roles: {admin: admin, member: user}"""
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
  sqlite_dir: {home / "deer-flow-home" / "db"}
scheduler:
  enabled: true
  poll_interval_seconds: 1
  min_once_delay_seconds: 1
tool_groups:
  - name: bash
tools:
  - name: bash
    group: bash
    use: deerflow.sandbox.tools:bash_tool
auth:
  local:
    enabled: false
  oidc:
    enabled: true
    providers:
      sso:
        display_name: Company
        issuer: {provider.issuer_url("a")}
        client_id: {CLIENT_ID}
        client_secret: ${SECRET_ENV}
        admin_emails:{admins or " []"}{membership}
"""


class _Journal(logging.Handler):
    def __init__(self) -> None:
        super().__init__(level=logging.DEBUG)
        self.lines: list[str] = []

    def emit(self, record: logging.LogRecord) -> None:
        with contextlib.suppress(Exception):
            self.lines.append(self.format(record))


@pytest.fixture(scope="module")
def provider() -> Iterator[OIDCTestProvider]:
    started = OIDCTestProvider(("a",), client_id=CLIENT_ID, client_secret=CLIENT_SECRET).start()
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


@contextlib.contextmanager
def _served(home: Path, provider: OIDCTestProvider, **config: Any) -> Iterator[e2e._Gateway]:
    env = pytest.MonkeyPatch()
    env.setenv(SECRET_ENV, CLIENT_SECRET)
    env.setenv("DEER_FLOW_AUTH_DISABLED", "")
    try:
        with e2e.serve_gateway(home, config_yaml=_config(provider, home, **config)) as served:
            yield served
    finally:
        env.undo()


@pytest.fixture(scope="class")
def gateway(provider: OIDCTestProvider, journal: _Journal, tmp_path_factory: pytest.TempPathFactory) -> Iterator[e2e._Gateway]:
    with _served(tmp_path_factory.mktemp("membership"), provider) as served:
        yield served


# ── Driving the flow the way a browser does, and the command the way the deployer does ──


def _client(base: str) -> httpx.Client:
    return httpx.Client(base_url=base, follow_redirects=False, timeout=30.0)


def _sign_in(client: httpx.Client, base: str, provider: OIDCTestProvider, *, subject: str, email: str, claims: dict[str, Any] | None = None, userinfo_claims: dict[str, Any] | None = None) -> httpx.Response:
    started = client.get(f"{base}/api/v1/auth/oauth/sso", params={"next": "/workspace"})
    assert started.status_code == 302, started.text
    authorization_url = started.headers["location"]
    at_provider = httpx.get(provider.sign_in_url(authorization_url, subject=subject, email=email, claims=claims, userinfo_claims=userinfo_claims), follow_redirects=False)
    assert at_provider.status_code == 302, at_provider.text
    back = at_provider.headers["location"]
    assert back.startswith(f"{base}/api/v1/auth/callback/sso?"), back
    return client.get(back)


def _landed(response: httpx.Response) -> bool:
    return response.status_code == 302 and response.headers["location"].startswith("/auth/callback")


def _error(response: httpx.Response) -> str | None:
    if response.status_code != 302:
        return None
    query = parse_qs(urlparse(response.headers["location"]).query)
    return query.get("error", [None])[0]


def _csrf(client: httpx.Client) -> dict[str, str]:
    token = client.cookies.get("csrf_token")
    assert token, "sign-in must set the CSRF cookie"
    return {"X-CSRF-Token": token}


def _me(client: httpx.Client, base: str) -> httpx.Response:
    return client.get(f"{base}/api/v1/auth/me")


def _db(gateway: e2e._Gateway) -> Path:
    return gateway.tmp_home / "deer-flow-home" / "db" / "deerflow.db"


def _row(gateway: e2e._Gateway, email: str) -> tuple | None:
    with sqlite3.connect(_db(gateway)) as connection:
        return connection.execute("SELECT system_role, oauth_id, oauth_issuer, last_sign_in_at FROM users WHERE email = ?", (email,)).fetchone()


def _accounts(gateway: e2e._Gateway, *args: str) -> tuple[int, dict[str, Any]]:
    """Run the deployer's command as a subprocess against the served Gateway's home; one JSON document back."""
    env = {**os.environ, "PYTHONPATH": f"{BACKEND}{os.pathsep}{BACKEND / 'tests'}"}
    completed = subprocess.run([sys.executable, "-m", "app.gateway.auth.accounts", *args], cwd=BACKEND, env=env, capture_output=True, text=True, timeout=120, check=False)
    lines = [line for line in completed.stdout.splitlines() if line.strip()]
    assert len(lines) == 1, f"one JSON document on stdout, got: {completed.stdout!r} / {completed.stderr[-800:]!r}"
    document = json.loads(lines[0])
    assert (completed.returncode != 0) == ("error" in document), (completed.returncode, document)
    return completed.returncode, document


def _create_thread(client: httpx.Client, base: str) -> str:
    thread_id = str(uuid.uuid4())
    created = client.post(f"{base}/api/threads", json={"thread_id": thread_id, "metadata": {}}, headers=_csrf(client))
    assert created.status_code == 200, created.text
    return thread_id


def _mint_pat(client: httpx.Client, base: str) -> str:
    minted = client.post(f"{base}/api/v1/auth/pats", json={"name": "automation", "scopes": ["threads:read"]}, headers=_csrf(client))
    assert minted.status_code == 201, minted.text
    return minted.json()["token"]


def _pat_works(base: str, token: str, thread_id: str) -> bool:
    with httpx.Client(base_url=base, timeout=30.0) as bare:
        return bare.get(f"{base}/api/threads/{thread_id}/state", headers={"Authorization": f"Bearer {token}"}).status_code == 200


def _wait(predicate, *, timeout: float, what: str) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return
        time.sleep(0.25)
    raise AssertionError(f"timed out waiting for {what}")


class TestMembership:
    """Every test here drives the one class-scoped Gateway (claim + roles, no administrators' list)."""

    # ── Evidence 1: admission ────────────────────────────────────────────

    def test_an_admitting_token_signs_in_and_one_without_is_refused_first_time_and_every_time(self, gateway: e2e._Gateway, provider: OIDCTestProvider, journal: _Journal) -> None:
        base = gateway.loopback_url
        with _client(base) as client:
            landed = _sign_in(client, base, provider, subject="sub-member", email="member@example.com", claims={CLAIM: {"member": {"org": "1"}}})
            assert _landed(landed), landed.headers
            assert _me(client, base).json()["system_role"] == "user"
        assert _row(gateway, "member@example.com")[0] == "user" and _row(gateway, "member@example.com")[3], "created, as user, with the sign-in stamped"

        with _client(base) as client:
            refused = _sign_in(client, base, provider, subject="sub-stranger", email="stranger@example.com", claims={CLAIM: ["other-project"]})
            assert _error(refused) == "sso_no_access", refused.headers
            assert "access_token" not in client.cookies
        assert _row(gateway, "stranger@example.com") is None, "no account is created"

        # The same token shape for an account that is already linked: refused before it is returned.
        with _client(base) as client:
            refused = _sign_in(client, base, provider, subject="sub-member", email="member@example.com", claims={CLAIM: ["other-project"]})
            assert _error(refused) == "sso_no_access"
            assert "access_token" not in client.cookies
        lines = [line for line in journal.lines if "no access for subject sub-member" in line]
        assert lines and provider.issuer_url("a") in lines[-1] and CLIENT_SECRET not in lines[-1], lines
        assert not any("eyJ" in line for line in lines), "never a token"

    # ── Evidence 2: the shapes, from either source ───────────────────────

    @pytest.mark.parametrize("shape", [["member"], "member", {"member": {}}], ids=["list", "string", "object"])
    @pytest.mark.parametrize("source", ["claims", "userinfo_claims"], ids=["id_token", "userinfo"])
    def test_each_claim_shape_admits_from_the_id_token_and_from_userinfo(self, gateway: e2e._Gateway, provider: OIDCTestProvider, shape, source: str) -> None:
        base = gateway.loopback_url
        subject = f"sub-shape-{source}-{type(shape).__name__}"
        with _client(base) as client:
            landed = _sign_in(client, base, provider, subject=subject, email=f"{subject}@example.com", **{source: {CLAIM: shape}})
            assert _landed(landed), landed.headers
            assert _me(client, base).json()["email"] == f"{subject}@example.com"

    @pytest.mark.parametrize("claims", [{}, {CLAIM: []}, {CLAIM: 42}], ids=["missing", "empty", "wrong-type"])
    def test_a_missing_empty_or_wrongly_typed_claim_refuses(self, gateway: e2e._Gateway, provider: OIDCTestProvider, claims: dict) -> None:
        base = gateway.loopback_url
        with _client(base) as client:
            refused = _sign_in(client, base, provider, subject="sub-badclaim", email="badclaim@example.com", claims=claims)
            assert _error(refused) == "sso_no_access", refused.headers
        assert _row(gateway, "badclaim@example.com") is None

    # ── Evidence 3: the role, both ways, and the other browser ───────────

    def test_the_role_follows_the_claim_both_ways_and_an_open_session_elsewhere_does_not_keep_it(self, gateway: e2e._Gateway, provider: OIDCTestProvider) -> None:
        base = gateway.loopback_url
        with _client(base) as first_browser, _client(base) as second_browser:
            assert _landed(_sign_in(first_browser, base, provider, subject="sub-flip", email="flip@example.com", claims={CLAIM: ["member"]}))
            assert _me(first_browser, base).json()["system_role"] == "user"
            assert first_browser.get(f"{base}/api/v1/auth/lockouts").status_code == 403

            assert _landed(_sign_in(first_browser, base, provider, subject="sub-flip", email="flip@example.com", claims={CLAIM: ["member", "admin"]}))
            assert _me(first_browser, base).json()["system_role"] == "admin", "promoted at the next sign-in; admin wins among several values"
            assert first_browser.get(f"{base}/api/v1/auth/lockouts").status_code == 200

            # Demoted from the second browser; the first browser's open session is no administrator any more.
            assert _landed(_sign_in(second_browser, base, provider, subject="sub-flip", email="flip@example.com", claims={CLAIM: ["member"]}))
            assert _me(second_browser, base).json()["system_role"] == "user"
            assert _me(first_browser, base).json()["system_role"] == "user"
            assert first_browser.get(f"{base}/api/v1/auth/lockouts").status_code == 403
        assert _row(gateway, "flip@example.com")[0] == "user"

    # ── Evidence 5: disable, with a session, a stream and a token in hand ─

    def test_disable_stops_every_path_that_acts_for_the_account(self, gateway: e2e._Gateway, provider: OIDCTestProvider, journal: _Journal) -> None:
        from app.gateway.csrf_middleware import generate_csrf_token
        from app.gateway.internal_auth import create_internal_auth_headers

        base = gateway.loopback_url
        issuer = provider.issuer_url("a")
        with _client(base) as client:
            assert _landed(_sign_in(client, base, provider, subject="sub-off", email="off@example.com", claims={CLAIM: ["member"]}))
            user_id = _me(client, base).json()["id"]
            thread_id = _create_thread(client, base)
            token = _mint_pat(client, base)
            assert _pat_works(base, token, thread_id)
            # A schedule due in a moment, owned by the account.
            run_at = (datetime.now(UTC) + timedelta(seconds=25)).isoformat()
            created = client.post(f"{base}/api/scheduled-tasks", json={"title": "held", "prompt": "probe:bash echo held", "schedule_type": "once", "schedule_spec": {"run_at": run_at}, "timezone": "UTC"}, headers=_csrf(client))
            assert created.status_code == 200, created.text
            task_id = created.json()["id"]

            # A stream open: the run's bash call sleeps, so the disable lands mid-run.
            stream: dict[str, Any] = {}

            def _run_stream() -> None:
                try:
                    stream["observed"] = e2e._observe_stream(client, base, thread_id, _csrf(client)["X-CSRF-Token"], "probe:bash sleep 6; echo stream-ran-to-its-end", timeout=120.0, recursion_limit=100)
                except BaseException as exc:  # noqa: BLE001 - reported below
                    stream["error"] = exc

            worker = threading.Thread(target=_run_stream, name="membership-open-stream", daemon=True)
            worker.start()

            def _run_started() -> bool:
                if "error" in stream:
                    return True
                with sqlite3.connect(_db(gateway)) as connection:
                    return connection.execute("SELECT count(*) FROM runs WHERE thread_id = ?", (thread_id,)).fetchone() != (0,)

            _wait(_run_started, timeout=30.0, what="the stream to open and its run to start")
            assert "error" not in stream, stream.get("error")

            def _run_status() -> tuple | None:
                with sqlite3.connect(_db(gateway)) as connection:
                    return connection.execute("SELECT status FROM runs WHERE thread_id = ?", (thread_id,)).fetchone()

            assert _run_status() == ("running",), "the run is in flight when the command runs"
            code, document = _accounts(gateway, "disable", "--issuer", issuer, "--subject", "sub-off")
            assert code == 0, document
            assert _run_status() == ("running",), "the command did not interrupt the run"
            t_disabled = time.monotonic()
            assert document["verdict"] == "disabled" and document["sessions_ended"] is True and document["tokens_revoked"] == 1 and document["schedules_held"] == 1, document
            assert document["account"]["email"] == "off@example.com" and document["account"]["disabled"] is True and document["account"]["subject"] == "sub-off"

            # The session: refused at its next request. Disable ended the sessions
            # (token_version) on top of the derived refusal, so the cookie dies on
            # the version check first; an internal caller for the owner sees the
            # derived refusal below.
            refused = _me(client, base)
            assert refused.status_code == 401 and refused.json()["detail"]["code"] in {"token_invalid", "account_disabled"}, refused.text
            # The token: refused, with the same answer as any dead token.
            assert not _pat_works(base, token, thread_id)
            # The stream: a run already executing finishes, and the stream ends with it; nothing new starts.
            worker.join(timeout=120)
            assert not worker.is_alive() and "error" not in stream, stream.get("error")
            observed = stream["observed"]
            assert observed.t_end is not None and observed.events[-1] == "end", "the stream ran to its end frame"
            assert observed.t_end > t_disabled, "and ended after the command had returned"
            assert "stream-ran-to-its-end" in "".join(str(payload) for _, payload in observed.frames)
            with httpx.Client(base_url=base, timeout=30.0) as internal:
                pair = generate_csrf_token()
                # An internal caller acting for the owner (a channel bound to the account) is refused too --
                headers = {**create_internal_auth_headers(owner_user_id=user_id), "X-CSRF-Token": pair, "Cookie": f"csrf_token={pair}"}
                as_owner = internal.post(f"{base}/api/threads/search", json={}, headers=headers)
                assert as_owner.status_code == 401 and as_owner.json()["detail"]["code"] == "account_disabled", as_owner.text
                # -- while an internal caller for nobody in particular still works, so the run's terminal state can be read.
                nobody = {**create_internal_auth_headers(owner_user_id="channel-nobody"), "X-CSRF-Token": pair, "Cookie": f"csrf_token={pair}"}
                assert internal.post(f"{base}/api/threads/search", json={}, headers=nobody).status_code == 200
            with sqlite3.connect(_db(gateway)) as connection:
                status = connection.execute("SELECT status FROM runs WHERE run_id = ?", (observed.run_id,)).fetchone()
            assert status == ("success",), status
            assert observed.text_frames >= 1

            # A fresh sign-in, although the claim admits: refused, and the journal names issuer and subject.
            with _client(base) as again:
                refused_sign_in = _sign_in(again, base, provider, subject="sub-off", email="off@example.com", claims={CLAIM: ["admin"]})
                assert _error(refused_sign_in) == "sso_access_off", refused_sign_in.headers
                assert "access_token" not in again.cookies
            lines = [line for line in journal.lines if "access turned off for subject sub-off" in line]
            assert lines and issuer in lines[-1] and CLIENT_SECRET not in lines[-1] and "eyJ" not in lines[-1], "issuer and subject, never a token"

            # The due schedule: not started. The scheduler records the refusal on the task and no run row appears for it.
            def _held() -> bool:
                with sqlite3.connect(_db(gateway)) as connection:
                    row = connection.execute("SELECT last_error, last_run_id FROM scheduled_tasks WHERE id = ?", (task_id,)).fetchone()
                return row is not None and row[0] is not None

            _wait(_held, timeout=90.0, what="the scheduler to reach the due task")
            with sqlite3.connect(_db(gateway)) as connection:
                last_error, last_run_id = connection.execute("SELECT last_error, last_run_id FROM scheduled_tasks WHERE id = ?", (task_id,)).fetchone()
                launched = connection.execute("SELECT count(*) FROM scheduled_task_runs WHERE task_id = ? AND run_id IS NOT NULL", (task_id,)).fetchone()
            assert "disabled" in last_error and last_run_id is None, (last_error, last_run_id)
            assert launched == (0,)
            assert any("Internal launch refused: owner" in line and "disabled" in line for line in journal.lines)

    # ── Evidence 6: turned off before the first sign-in ──────────────────

    def test_a_subject_turned_off_before_its_first_sign_in_cannot_create_an_account(self, gateway: e2e._Gateway, provider: OIDCTestProvider) -> None:
        base = gateway.loopback_url
        code, document = _accounts(gateway, "disable", "--issuer", provider.issuer_url("a") + "/", "--subject", "sub-early")
        assert code == 0 and document["verdict"] == "disabled" and document["account"] is None and "no account existed" in document["note"], document
        with _client(base) as client:
            refused = _sign_in(client, base, provider, subject="sub-early", email="early@example.com", claims={CLAIM: ["admin"]})
            assert _error(refused) == "sso_access_off"
        assert _row(gateway, "early@example.com") is None
        code, again = _accounts(gateway, "disable", "--issuer", provider.issuer_url("a"), "--subject", "sub-early")
        assert code == 0 and again["verdict"] == "already_disabled"
        # The failure contract through the command line: one document, non-zero exit.
        code, failed = _accounts(gateway, "end-sessions", "--issuer", provider.issuer_url("a"), "--subject", "sub-early")
        assert code == 1 and failed == {"command": "end-sessions", "error": failed["error"]} and "no account exists" in failed["error"]

    # ── Evidence 8: enable revives nothing ───────────────────────────────

    def test_enable_lets_the_person_in_again_while_the_old_session_and_token_stay_dead(self, gateway: e2e._Gateway, provider: OIDCTestProvider) -> None:
        base = gateway.loopback_url
        issuer = provider.issuer_url("a")
        with _client(base) as before:
            assert _landed(_sign_in(before, base, provider, subject="sub-back", email="back@example.com", claims={CLAIM: ["member"]}))
            thread_id = _create_thread(before, base)
            token = _mint_pat(before, base)
            old_cookie = before.cookies["access_token"]
        assert _accounts(gateway, "disable", "--email", "back@example.com")[1]["verdict"] == "disabled"
        code, document = _accounts(gateway, "enable", "--issuer", issuer, "--subject", "sub-back")
        assert code == 0 and document["verdict"] == "enabled" and document["account"]["disabled"] is False, document
        with _client(base) as after:
            assert _landed(_sign_in(after, base, provider, subject="sub-back", email="back@example.com", claims={CLAIM: ["member"]}))
            assert _me(after, base).status_code == 200
            after.cookies.clear()
            after.cookies.set("access_token", old_cookie)
            assert _me(after, base).status_code == 401, "the session from before the disable stays dead"
        assert not _pat_works(base, token, thread_id), "the token revoked by the disable stays revoked"
        assert _accounts(gateway, "enable", "--issuer", issuer, "--subject", "sub-back")[1]["verdict"] == "already_enabled"

    # ── Evidence 9: end sessions ─────────────────────────────────────────

    def test_end_sessions_signs_a_demoted_administrator_out_and_leaves_the_token_alone(self, gateway: e2e._Gateway, provider: OIDCTestProvider) -> None:
        base = gateway.loopback_url
        with _client(base) as browser:
            assert _landed(_sign_in(browser, base, provider, subject="sub-demote", email="demote@example.com", claims={CLAIM: ["admin"]}))
            assert _me(browser, base).json()["system_role"] == "admin"
            thread_id = _create_thread(browser, base)
            token = _mint_pat(browser, base)
            # The provider now says member; the open session would stay an administrator until it expires.
            code, document = _accounts(gateway, "end-sessions", "--issuer", provider.issuer_url("a"), "--subject", "sub-demote")
            assert code == 0 and document["verdict"] == "sessions_ended" and document["tokens_revoked"] == 0, document
            refused = _me(browser, base)
            assert refused.status_code == 401, refused.text
            assert _pat_works(base, token, thread_id), "the token is untouched"
            assert _landed(_sign_in(browser, base, provider, subject="sub-demote", email="demote@example.com", claims={CLAIM: ["member"]}))
            assert _me(browser, base).json()["system_role"] == "user", "the next sign-in re-read the claim"

    # ── Evidence 10: the list ────────────────────────────────────────────

    def test_list_shows_every_account_with_the_turned_off_ones_marked(self, gateway: e2e._Gateway, provider: OIDCTestProvider) -> None:
        base = gateway.loopback_url
        issuer = provider.issuer_url("a")
        with _client(base) as client:
            assert _landed(_sign_in(client, base, provider, subject="sub-listed", email="listed@example.com", claims={CLAIM: ["admin"]}))
        assert _accounts(gateway, "disable", "--issuer", issuer, "--subject", "sub-listed")[1]["verdict"] == "disabled"
        assert _accounts(gateway, "disable", "--issuer", issuer, "--subject", "sub-never")[1]["account"] is None
        code, document = _accounts(gateway, "list")
        assert code == 0
        by_email = {entry["email"]: entry for entry in document["accounts"]}
        listed = by_email["listed@example.com"]
        assert listed == {**listed, "issuer": issuer, "subject": "sub-listed", "role": "admin", "provider": "sso", "disabled": True}
        assert listed["disabled_at"] and listed["last_sign_in_at"]
        assert all(entry["disabled"] is False for email, entry in by_email.items() if email not in {"listed@example.com", "off@example.com", "early@example.com"} and entry["subject"] != "sub-early"), by_email
        assert {"issuer": issuer, "subject": "sub-never"} == {key: value for key, value in next(entry for entry in document["disabled_without_account"] if entry["subject"] == "sub-never").items() if key != "disabled_at"}

    # ── Evidence 11: no route ────────────────────────────────────────────

    def test_nothing_over_http_turns_an_account_off_or_on_or_ends_another_accounts_sessions(self, gateway: e2e._Gateway, provider: OIDCTestProvider) -> None:
        base = gateway.loopback_url
        with _client(base) as admin:
            assert _landed(_sign_in(admin, base, provider, subject="sub-root", email="root@example.com", claims={CLAIM: ["admin"]}))
            assert _me(admin, base).json()["system_role"] == "admin"
            paths = list(admin.get(f"{base}/openapi.json").json()["paths"])
            suspects = [path for path in paths if any(word in path.lower() for word in ("disable", "enable", "end-sessions", "end_sessions", "accounts", "sessions", "release"))]
            assert suspects == [], suspects
            guesses = (
                ("POST", "/api/v1/auth/accounts/disable"),
                ("POST", "/api/v1/auth/accounts/sub-root/disable"),
                ("POST", "/api/v1/auth/accounts/sub-root/enable"),
                ("POST", "/api/v1/auth/accounts/release-email"),
                ("POST", "/api/v1/auth/accounts/sub-root/release-email"),
                ("DELETE", "/api/v1/auth/accounts/sub-root/email"),
                ("DELETE", "/api/v1/auth/sessions"),
                ("POST", "/api/v1/auth/users/sub-root/sessions/end"),
            )
            for method, path in guesses:
                answer = admin.request(method, f"{base}{path}", headers=_csrf(admin))
                assert answer.status_code in {404, 405}, (method, path, answer.status_code)

    def test_a_recreated_person_signs_in_once_the_deployer_releases_the_address(self, gateway: e2e._Gateway, provider: OIDCTestProvider) -> None:
        """The provider's own remedy for a mis-sent invitation: delete the person, invite them again.

        The new subject carries the same address, which the old account still
        holds. Through the served Gateway, with the deployer's command run as
        a subprocess the way a deployer runs it.
        """
        base = gateway.loopback_url
        issuer = provider.issuer_url("a")
        address = "recreated@example.com"

        with _client(base) as first:
            assert _landed(_sign_in(first, base, provider, subject="sub-first", email=address, claims={CLAIM: ["member"]}))
        assert _accounts(gateway, "disable", "--issuer", issuer, "--subject", "sub-first")[1]["verdict"] == "disabled"

        # The same person, a new subject, the same address: refused, and told why.
        with _client(base) as again:
            landed = _sign_in(again, base, provider, subject="sub-second", email=address, claims={CLAIM: ["member"]})
            assert landed.headers["location"] == "/login?error=sso_email_taken", landed.text
            assert "access_token" not in again.cookies

        # Releasing is for an account nobody can use; while it is on, refused.
        assert _accounts(gateway, "enable", "--issuer", issuer, "--subject", "sub-first")[1]["verdict"] == "enabled"
        code, refused = _accounts(gateway, "release-email", "--issuer", issuer, "--subject", "sub-first")
        assert code == 1 and "not turned off" in refused["error"], refused
        assert _accounts(gateway, "disable", "--issuer", issuer, "--subject", "sub-first")[1]["verdict"] == "disabled"

        code, released = _accounts(gateway, "release-email", "--issuer", issuer, "--subject", "sub-first")
        assert code == 0 and released["verdict"] == "released" and released["released"] == address, released
        assert _accounts(gateway, "release-email", "--issuer", issuer, "--subject", "sub-first")[1]["verdict"] == "already_released"

        with _client(base) as again:
            assert _landed(_sign_in(again, base, provider, subject="sub-second", email=address, claims={CLAIM: ["member"]}))
            assert _me(again, base).json()["email"] == address

        listed = {entry["subject"]: entry for entry in _accounts(gateway, "list")[1]["accounts"]}
        assert listed["sub-first"]["released"] is True and listed["sub-first"]["released_from"] == address
        assert listed["sub-first"]["disabled"] is True, "the old account is still there, still off"
        assert listed["sub-second"]["email"] == address and listed["sub-second"]["released"] is False

    def test_an_address_the_provider_changes_follows_the_person(self, gateway: e2e._Gateway, provider: OIDCTestProvider) -> None:
        """One subject, a corrected address, through the real flow."""
        base = gateway.loopback_url
        with _client(base) as client:
            assert _landed(_sign_in(client, base, provider, subject="sub-moves", email="before@example.com", claims={CLAIM: ["member"]}))
            assert _me(client, base).json()["email"] == "before@example.com"
        with _client(base) as client:
            assert _landed(_sign_in(client, base, provider, subject="sub-moves", email="after@example.com", claims={CLAIM: ["member"]}))
            assert _me(client, base).json()["email"] == "after@example.com"
        listed = {entry["subject"]: entry for entry in _accounts(gateway, "list")[1]["accounts"]}
        assert listed["sub-moves"]["email"] == "after@example.com"

    def test_zz_the_client_secret_appears_in_no_log_line(self, gateway: e2e._Gateway, journal: _Journal) -> None:
        assert journal.lines and [line for line in journal.lines if CLIENT_SECRET in line] == []


# ── Evidence 4: one start refused, one not ───────────────────────────────


def test_the_administrators_list_beside_the_mapping_refuses_the_start(provider: OIDCTestProvider, tmp_path_factory: pytest.TempPathFactory) -> None:
    # The refusal is the configuration's: it fires the moment the file is loaded, before anything is served.
    with pytest.raises(Exception, match="access_roles and admin_emails are both set"), _served(tmp_path_factory.mktemp("membership-refused"), provider, admin_emails=(OWNER,)):
        pass


def test_admission_without_the_mapping_takes_roles_from_the_administrators_list(provider: OIDCTestProvider, tmp_path_factory: pytest.TempPathFactory) -> None:
    with _served(tmp_path_factory.mktemp("membership-no-mapping"), provider, roles=False, admin_emails=(OWNER,)) as served:
        base = served.loopback_url
        with _client(base) as owner, _client(base) as staff, _client(base) as stranger:
            assert _landed(_sign_in(owner, base, provider, subject="sub-owner", email=OWNER, claims={CLAIM: ["member"]}))
            assert _me(owner, base).json()["system_role"] == "admin", "from the list, whatever the claim value"
            assert _landed(_sign_in(staff, base, provider, subject="sub-staff", email="staff@example.com", claims={CLAIM: ["admin"]}))
            assert _me(staff, base).json()["system_role"] == "user", "the claim admits; without the mapping it carries no role"
            assert _error(_sign_in(stranger, base, provider, subject="sub-stranger", email="stranger@example.com", claims={CLAIM: ["other"]})) == "sso_no_access"


# ── Evidence 7: durability ───────────────────────────────────────────────


def test_the_refusal_survives_a_restart_and_a_copied_database(provider: OIDCTestProvider, tmp_path_factory: pytest.TempPathFactory) -> None:
    home = tmp_path_factory.mktemp("membership-durable")
    issuer = provider.issuer_url("a")
    with _served(home, provider) as served:
        base = served.loopback_url
        with _client(base) as client:
            assert _landed(_sign_in(client, base, provider, subject="sub-durable", email="durable@example.com", claims={CLAIM: ["member"]}))
            thread_id = _create_thread(client, base)
            token = _mint_pat(client, base)
        assert _accounts(served, "disable", "--issuer", issuer, "--subject", "sub-durable")[1]["verdict"] == "disabled"
        # A consistent copy of the live database (the file plus what its write-ahead
        # log holds), as a backup taken while the Gateway runs must be.
        db_copy = tmp_path_factory.mktemp("membership-copy-db") / "deerflow.db"
        with sqlite3.connect(_db(served)) as live, sqlite3.connect(db_copy) as copy:
            live.backup(copy)

    # The restart.
    with _served(home, provider) as served:
        base = served.loopback_url
        with _client(base) as client:
            assert _error(_sign_in(client, base, provider, subject="sub-durable", email="durable@example.com", claims={CLAIM: ["member"]})) == "sso_access_off"
        assert not _pat_works(base, token, thread_id)
        listed = _accounts(served, "list")[1]
        assert next(entry for entry in listed["accounts"] if entry["email"] == "durable@example.com")["disabled"] is True

    # A database copied while the account was off, restored under a new home.
    restored = tmp_path_factory.mktemp("membership-restored")
    (restored / "deer-flow-home" / "db").mkdir(parents=True)
    shutil.copyfile(db_copy, restored / "deer-flow-home" / "db" / "deerflow.db")
    with _served(restored, provider) as served:
        base = served.loopback_url
        with _client(base) as client:
            assert _error(_sign_in(client, base, provider, subject="sub-durable", email="durable@example.com", claims={CLAIM: ["member"]})) == "sso_access_off"
        assert _accounts(served, "enable", "--issuer", issuer, "--subject", "sub-durable")[1]["verdict"] == "enabled"
        with _client(base) as client:
            assert _landed(_sign_in(client, base, provider, subject="sub-durable", email="durable@example.com", claims={CLAIM: ["member"]}))
