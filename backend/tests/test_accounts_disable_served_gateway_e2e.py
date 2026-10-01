"""Turning an account off, end to end, on a served Gateway with no durable layer.

One real Gateway in sign-on-only mode, one real identity provider, one person
signed in through it with a run in flight, and the deployer's command run as
its own process against the same database, the way it runs inside the
deployment:

    python -m app.gateway.auth.accounts disable --issuer ... --subject ...

The command holds the database and nothing else. What stops the run is the
cancellation request the run's own cancel route persists, read by the worker
that owns the run when it renews the run's lease
(``run_ownership.heartbeat_enabled``, which the compose profile sets). These
tests pin that whole path: the refusal recorded, the session refused at its
next request, the run found and cancelled and confirmed terminal, the stream
ended, a new sign-in refused, and ``enable`` letting the person back in.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
import threading
import time
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest

os.environ.setdefault("AUTH_JWT_SECRET", "test-secret-key-for-accounts-e2e-min-32-chars")

import test_sign_on_only_e2e as sign_on
import test_turn_phase_gateway_stream_e2e as e2e
from _oidc_test_provider import OIDCTestProvider

pytestmark = pytest.mark.xdist_group("sign-on-only-gateway")

BACKEND = Path(__file__).resolve().parents[1]
STAFF_SUBJECT = "sub-pat"

# The lease is renewed every third of it, and that renewal is where the worker
# reads the cancellation: five seconds is the shortest the config accepts.
_OWNERSHIP = """\
run_ownership:
  heartbeat_enabled: true
  lease_seconds: 5
"""


@pytest.fixture(scope="module")
def provider() -> Iterator[OIDCTestProvider]:
    started = OIDCTestProvider(("a", "b"), client_id=sign_on.CLIENT_ID, client_secret=sign_on.CLIENT_SECRET).start()
    try:
        yield started
    finally:
        started.stop()


@pytest.fixture(scope="module")
def gateway(provider: OIDCTestProvider, tmp_path_factory: pytest.TempPathFactory) -> Iterator[e2e._Gateway]:
    env = pytest.MonkeyPatch()
    env.setenv(sign_on.SECRET_ENV, sign_on.CLIENT_SECRET)
    env.setenv("DEER_FLOW_AUTH_DISABLED", "")
    try:
        home = tmp_path_factory.mktemp("accounts-disable")
        # The database's place is written into the config, as a deployment's
        # is: the command is its own process and reads it from there.
        config = sign_on._config(provider, second_provider=False).replace("database:\n  backend: sqlite\n", f"database:\n  backend: sqlite\n  sqlite_dir: {home / 'deer-flow-home' / 'db'}\n")
        assert "sqlite_dir" in config
        with e2e.serve_gateway(home, config_yaml=config + _OWNERSHIP) as served:
            yield served
    finally:
        env.undo()


def _accounts(gateway: e2e._Gateway, *args: str) -> tuple[int, dict[str, Any]]:
    """The deployer's command as its own process against the served Gateway's config and database."""
    env = {
        **os.environ,
        "PYTHONPATH": os.pathsep.join(str(path) for path in (BACKEND, BACKEND / "tests", BACKEND / "packages" / "harness", BACKEND / "packages" / "runtime-api", BACKEND / "packages" / "extension-api")),
        "DEER_FLOW_HOME": str(gateway.tmp_home / "deer-flow-home"),
        "DEER_FLOW_CONFIG_PATH": str(gateway.tmp_home / "config.yaml"),
        "DEER_FLOW_EXTENSIONS_CONFIG_PATH": str(gateway.tmp_home / "extensions_config.json"),
    }
    completed = subprocess.run([sys.executable, "-m", "app.gateway.auth.accounts", *args], cwd=BACKEND, env=env, capture_output=True, text=True, timeout=180, check=False)
    lines = [line for line in completed.stdout.splitlines() if line.strip()]
    assert len(lines) == 1, f"one JSON document on stdout, got: {completed.stdout!r} / {completed.stderr[-1200:]!r}"
    return completed.returncode, json.loads(lines[0])


def _run_status(client, base: str, thread_id: str, run_id: str) -> str | None:
    answer = client.get(f"{base}/api/threads/{thread_id}/runs/{run_id}")
    return answer.json().get("status") if answer.status_code == 200 else None


def test_disable_ends_the_session_and_the_run_in_flight_and_enable_lets_the_person_back(gateway: e2e._Gateway, provider: OIDCTestProvider) -> None:
    base = gateway.loopback_url
    issuer = provider.issuer_url("a")

    with sign_on._client(base) as owner, sign_on._client(base) as staff:
        assert sign_on._sign_in(owner, base, provider, subject="sub-owner", email=sign_on.OWNER).status_code == 302
        assert sign_on._sign_in(staff, base, provider, subject=STAFF_SUBJECT, email=sign_on.STAFF).status_code == 302
        assert staff.get(f"{base}/api/v1/auth/me").json()["system_role"] == "user"
        thread_id = sign_on._create_thread(staff, base)

        # A run that would hold its model call open for 30 s: in flight when
        # the account is turned off.
        seen: dict[str, Any] = {}
        started = threading.Event()

        def on_frame(observation) -> None:
            seen["run_id"] = observation.run_id
            started.set()

        def drive() -> None:
            try:
                seen["observation"] = e2e._observe_stream(staff, base, thread_id, sign_on._csrf(staff)["X-CSRF-Token"], "probe:hang", on_frame=on_frame, timeout=120.0)
            except Exception as exc:  # noqa: BLE001 - the stream ending however it ends is the result
                seen["stream_error"] = exc

        stream = threading.Thread(target=drive, name="staff-run", daemon=True)
        stream.start()
        assert started.wait(timeout=30), "the run never started streaming"
        run_id = seen["run_id"]
        assert _run_status(owner, base, thread_id, run_id) in {"pending", "running"} or _run_status(staff, base, thread_id, run_id) in {"pending", "running"}

        t0 = time.monotonic()
        code, document = _accounts(gateway, "disable", "--issuer", issuer, "--subject", STAFF_SUBJECT, "--wait-seconds", "60")
        elapsed = time.monotonic() - t0

        assert code == 0, document
        assert document["verdict"] == "disabled"
        assert document["sessions_ended"] is True, document
        assert document["runs_found"] == 1 and document["runs_cancelled"] == 1, document
        assert document["runs_unconfirmed"] == [] and document["surfaces_unconfirmed"] == [], document
        assert document["surfaces"]["running_work"]["action"] == "ended"
        assert document["surfaces"]["sessions"]["action"] == "ended"
        assert document["surfaces"]["sign_in"]["action"] == "refused_at_next_use"
        assert elapsed < 30, f"the worker reads the cancellation at its next lease renewal; it took {elapsed:.1f}s"

        # The stream ended with the run, well before the model call would have.
        stream.join(timeout=30)
        assert not stream.is_alive(), "the stream outlived the run"

        # The session is refused at its next request: the disable ended it
        # (its token version moved on), which is read before the refusal itself.
        refused = staff.get(f"{base}/api/v1/auth/me")
        assert refused.status_code == 401, refused.text
        assert refused.json()["detail"]["code"] in {"token_invalid", "account_disabled"}

        # A new sign-in is refused before an account is returned.
        with sign_on._client(base) as again:
            landed = sign_on._sign_in(again, base, provider, subject=STAFF_SUBJECT, email=sign_on.STAFF)
            assert landed.status_code == 302 and "access_token" not in again.cookies, landed.headers.get("location")

        # The owner still works, and the list names the person as turned off.
        assert owner.get(f"{base}/api/v1/auth/me").status_code == 200
        code, listing = _accounts(gateway, "list")
        assert code == 0
        by_email = {account["email"]: account for account in listing["accounts"]}
        assert by_email[sign_on.STAFF]["disabled"] is True and by_email[sign_on.OWNER]["disabled"] is False

        # A second disable changes nothing and says so.
        code, again_document = _accounts(gateway, "disable", "--issuer", issuer, "--subject", STAFF_SUBJECT, "--wait-seconds", "10")
        assert code == 0 and again_document["verdict"] == "already_disabled" and again_document["runs_found"] == 0

        # Enable withdraws the refusal; the ended session stays ended, and a new sign-in works.
        code, enabled = _accounts(gateway, "enable", "--issuer", issuer, "--subject", STAFF_SUBJECT)
        assert code == 0 and enabled["verdict"] == "enabled", enabled
        assert staff.get(f"{base}/api/v1/auth/me").status_code == 401, "the session the disable ended stays ended"
        with sign_on._client(base) as back:
            assert sign_on._sign_in(back, base, provider, subject=STAFF_SUBJECT, email=sign_on.STAFF).status_code == 302
            assert back.get(f"{base}/api/v1/auth/me").json()["email"] == sign_on.STAFF
