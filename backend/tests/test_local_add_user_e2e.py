"""Adding a person with registration closed, through the real Gateway.

``test_local_add_user.py`` proves the pieces on the bare app. This drives the
served Gateway -- lifespan, migrations, middleware, the run pipeline -- and
runs the deployer's command the way the deployer does, as a subprocess inside
the deployment reading one JSON document back. What a tenant would see: with
registration closed a visitor cannot sign up, the readiness signal and the
start line say so, an administrator adds a person and stays signed in as
themselves, the person signs in once with the one-time password, chooses
their own and runs an ordinary chat turn with a tool in the sandbox, and the
one-time password opens nothing afterwards; the command does the same from a
shell; and in sign-on-only mode neither surface adds anyone. The one-time
passwords appear nowhere in the journal or the command's stderr.
"""

from __future__ import annotations

import contextlib
import json
import logging
import os
import subprocess
import sys
import uuid
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import httpx
import pytest
import test_turn_phase_gateway_stream_e2e as e2e
from _oidc_test_provider import OIDCTestProvider

pytestmark = pytest.mark.xdist_group("local-add-user-gateway")

BACKEND = Path(__file__).resolve().parents[1]
OWNER = {"email": "owner@example.com", "password": "Str0ng!Pass99"}
PERSON = "pat@example.com"
CHOSEN = "Pat's-0wn-Passw0rd"
CLIENT_ID = "hartmesh-tenant"
CLIENT_SECRET = "FAKE-CREDENTIAL-SENTINEL-NOT-A-KEY"
SECRET_ENV = "SSO_CLIENT_SECRET"


def _config(home: Path, auth: str) -> str:
    """The probe model, host bash in the local sandbox, the database directory in the file (the command is a separate process), and the sign-in block."""
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
tool_groups:
  - name: bash
tools:
  - name: bash
    group: bash
    use: deerflow.sandbox.tools:bash_tool
{auth}"""


def _local(open_: bool) -> str:
    return f"auth:\n  local:\n    enabled: true\n    allow_registration: {str(open_).lower()}\n"


def _sign_on_only(provider: OIDCTestProvider) -> str:
    return f"""auth:
  local:
    enabled: false
    allow_registration: false
  oidc:
    enabled: true
    providers:
      sso:
        display_name: Company
        issuer: {provider.issuer_url("a")}
        client_id: {CLIENT_ID}
        client_secret: ${SECRET_ENV}
        admin_emails:
          - {OWNER["email"]}
"""


class _Journal(logging.Handler):
    def __init__(self) -> None:
        super().__init__(level=logging.DEBUG)
        self.lines: list[str] = []

    def emit(self, record: logging.LogRecord) -> None:
        with contextlib.suppress(Exception):
            self.lines.append(self.format(record))


@pytest.fixture(scope="module")
def journal() -> Iterator[_Journal]:
    handler = _Journal()
    handler.setFormatter(logging.Formatter("%(name)s %(levelname)s %(message)s"))
    root = logging.getLogger()
    previous = root.level
    root.addHandler(handler)
    root.setLevel(logging.DEBUG)
    try:
        yield handler
    finally:
        root.removeHandler(handler)
        root.setLevel(previous)


@contextlib.contextmanager
def _served(home: Path, auth: str) -> Iterator[e2e._Gateway]:
    env = pytest.MonkeyPatch()
    env.setenv(SECRET_ENV, CLIENT_SECRET)
    env.setenv("DEER_FLOW_AUTH_DISABLED", "")
    try:
        with e2e.serve_gateway(home, config_yaml=_config(home, auth)) as served:
            yield served
    finally:
        env.undo()


def _client(base: str) -> httpx.Client:
    return httpx.Client(base_url=base, follow_redirects=False, timeout=30.0)


def _csrf(client: httpx.Client) -> dict[str, str]:
    token = client.cookies.get("csrf_token")
    assert token, "a signed-in client carries the CSRF cookie"
    return {"X-CSRF-Token": token}


def _login(client: httpx.Client, email: str, password: str) -> httpx.Response:
    return client.post("/api/v1/auth/login/local", data={"username": email, "password": password})


def _add_user_command(*args: str) -> tuple[int, dict[str, Any], str]:
    """The deployer's command as a subprocess against the served Gateway's config; one JSON document back."""
    env = {**os.environ, "PYTHONPATH": f"{BACKEND}{os.pathsep}{BACKEND / 'tests'}"}
    completed = subprocess.run([sys.executable, "-m", "app.gateway.auth.add_user", *args], cwd=BACKEND, env=env, capture_output=True, text=True, timeout=120, check=False)
    lines = [line for line in completed.stdout.splitlines() if line.strip()]
    assert len(lines) == 1, f"one JSON document on stdout, got: {completed.stdout!r} / {completed.stderr[-800:]!r}"
    return completed.returncode, json.loads(lines[0]), completed.stderr


def _complete_setup(base: str, email: str, one_time: str) -> httpx.Client:
    """What the /setup page does after a first sign-in: the one-time password, then the person's own."""
    person = _client(base)
    login = _login(person, email, one_time)
    assert login.status_code == 200 and login.json()["needs_setup"] is True, login.text
    done = person.post("/api/v1/auth/change-password", json={"current_password": one_time, "new_password": CHOSEN, "new_email": email}, headers=_csrf(person))
    assert done.status_code == 200, done.text
    return person


def test_with_registration_closed_an_administrator_adds_a_person_who_signs_in_once_and_works(journal: _Journal, tmp_path_factory: pytest.TempPathFactory) -> None:
    passwords: list[str] = []
    with _served(tmp_path_factory.mktemp("add-user-closed"), _local(open_=False)) as gateway:
        base = gateway.loopback_url
        assert any("auth mode: local (local passwords on; registration closed)" in line for line in journal.lines)
        with _client(base) as visitor:
            health = visitor.get("/health").json()
            assert (health["auth_mode"], health["registration"]) == ("local", "closed"), health
            signup = visitor.post("/api/v1/auth/register", json={"email": "stranger@example.com", "password": "Tr0ub4dor3a"})
            assert signup.status_code == 403 and signup.json()["detail"]["code"] == "registration_disabled", signup.text
            assert visitor.get("/api/v1/auth/setup-status").json() == {"needs_setup": True, "registration_enabled": False, "sign_on_only": False}

        with _client(base) as owner:
            assert owner.post("/api/v1/auth/initialize", json=OWNER).status_code == 201
            me_before = owner.get("/api/v1/auth/me").json()
            session_before = owner.cookies.get("access_token")
            added = owner.post("/api/v1/auth/users", json={"email": PERSON}, headers=_csrf(owner))
            assert added.status_code == 201, added.text
            assert "access_token" not in added.headers.get("set-cookie", "") and owner.cookies.get("access_token") == session_before, "no cookie for the administrator"
            one_time = added.json()["one_time_password"]
            passwords.append(one_time)
            assert added.json()["system_role"] == "user" and added.headers["cache-control"] == "no-store"
            assert owner.get("/api/v1/auth/me").json() == me_before, "the administrator is still themselves"
            duplicate = owner.post("/api/v1/auth/users", json={"email": PERSON}, headers=_csrf(owner))
            assert duplicate.status_code == 400 and duplicate.json()["detail"]["code"] == "email_already_exists"
            minted = owner.post("/api/v1/auth/pats", json={"name": "automation", "scopes": ["threads:read", "threads:write"]}, headers=_csrf(owner))
            assert minted.status_code == 201, minted.text
            token = minted.json()["token"]

        with _client(base) as by_token:
            refused = by_token.post("/api/v1/auth/users", json={"email": "other@example.com"}, headers={"Authorization": f"Bearer {token}"})
            assert refused.status_code == 403, refused.text

        # Before setup: the session reaches setup and nothing else.
        with _client(base) as early:
            assert _login(early, PERSON, one_time).json()["needs_setup"] is True
            blocked = early.post("/api/threads", json={"thread_id": str(uuid.uuid4()), "metadata": {}}, headers=_csrf(early))
            assert blocked.status_code == 403 and blocked.json()["detail"]["code"] == "setup_required", blocked.text

        with contextlib.closing(_complete_setup(base, PERSON, one_time)) as person:
            assert person.get("/api/v1/auth/me").json()["needs_setup"] is False
            thread_id = str(uuid.uuid4())
            assert person.post("/api/threads", json={"thread_id": thread_id, "metadata": {}}, headers=_csrf(person)).status_code == 200
            observed = e2e._observe_stream(person, base, thread_id, _csrf(person)["X-CSRF-Token"], "probe:bash echo added-person-turn-ok", timeout=120.0, recursion_limit=100)
            run = person.get(f"/api/threads/{thread_id}/runs/{observed.run_id}").json()
            history = person.post(f"/api/threads/{thread_id}/history", json={"limit": 30}, headers=_csrf(person)).json()
            refused = person.post("/api/v1/auth/users", json={"email": "other@example.com"}, headers=_csrf(person))
        assert run["status"] == "success", run
        tool_results = [message for state in history for message in (state.get("values") or {}).get("messages", []) if message.get("type") == "tool"]
        assert tool_results and "added-person-turn-ok" in str(tool_results[0].get("content")), tool_results
        assert refused.status_code == 403, "a user cannot add people"

        with _client(base) as replay:
            stale = _login(replay, PERSON, one_time)
            assert stale.status_code == 401 and stale.json()["detail"]["code"] == "invalid_credentials", "once the person chose their own, the one-time password opens nothing"

        # The deployer's surface: the same operation from a shell.
        code, document, err = _add_user_command("--email", "sam@example.com")
        assert code == 0 and document["system_role"] == "user" and document["needs_setup"] is True, document
        passwords.append(document["one_time_password"])
        assert document["one_time_password"] not in err
        code, again, _ = _add_user_command("--email", "sam@example.com")
        assert code == 1 and again == {"command": "add-user", "error": "email_already_exists", "message": "Email already registered"}
        with contextlib.closing(_complete_setup(base, "sam@example.com", document["one_time_password"])) as sam:
            assert sam.get("/api/v1/auth/me").json()["email"] == "sam@example.com"

    assert all(password not in line for password in passwords for line in journal.lines), "no one-time password is ever logged"


def test_with_registration_open_the_door_and_the_signal_say_open(journal: _Journal, tmp_path_factory: pytest.TempPathFactory) -> None:
    with _served(tmp_path_factory.mktemp("add-user-open"), _local(open_=True)) as gateway:
        with _client(gateway.loopback_url) as visitor:
            assert visitor.get("/health").json()["registration"] == "open"
            assert visitor.get("/api/v1/auth/setup-status").json()["registration_enabled"] is True
            assert visitor.post("/api/v1/auth/register", json={"email": "visitor@example.com", "password": "Tr0ub4dor3a"}).status_code == 201
    assert any("auth mode: local (local passwords on; registration open)" in line for line in journal.lines)


def test_sign_on_only_adds_nobody_on_either_surface(journal: _Journal, tmp_path_factory: pytest.TempPathFactory) -> None:
    provider = OIDCTestProvider(("a",), client_id=CLIENT_ID, client_secret=CLIENT_SECRET).start()
    try:
        with _served(tmp_path_factory.mktemp("add-user-sign-on"), _sign_on_only(provider)) as gateway:
            base = gateway.loopback_url
            # Registration last: the earlier line's prefix still matches.
            assert any("auth mode: sign_on_only (local passwords off; provider sso (" in line and line.endswith("; registration closed)") for line in journal.lines), journal.lines
            with _client(base) as owner:
                health = owner.get("/health").json()
                assert (health["auth_mode"], health["registration"]) == ("sign_on_only", "closed"), health
                started = owner.get("/api/v1/auth/oauth/sso", params={"next": "/workspace"})
                at_provider = httpx.get(provider.sign_in_url(started.headers["location"], subject="sub-owner", email=OWNER["email"]), follow_redirects=False)
                landed = owner.get(at_provider.headers["location"])
                assert landed.status_code == 302 and landed.headers["location"].startswith("/auth/callback"), landed.text
                assert owner.get("/api/v1/auth/me").json()["system_role"] == "admin"
                refused = owner.post("/api/v1/auth/users", json={"email": PERSON}, headers=_csrf(owner))
                assert refused.status_code == 403 and refused.json()["detail"]["code"] == "sign_on_required", refused.text
            code, document, _ = _add_user_command("--email", PERSON)
            assert code == 1 and document["error"] == "sign_on_required", document
    finally:
        provider.stop()
