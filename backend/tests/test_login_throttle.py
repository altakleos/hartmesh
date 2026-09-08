"""The login lockout is keyed on the account, not the client address.

Every numbered section below is one of the six pieces of evidence the change
had to carry:

1. N failures lock one account, and a *second* account from the same source
   address still logs in. That second login is the whole point.
2. Failures spread across many accounts from one source trip the looser guard.
3. The admin unlock works, requires the admin role, and needs no restart.
4. A locked account and an account that does not exist are indistinguishable,
   in the response body and in timing.
5. With the backing store unavailable the login path fails closed, which is
   what the change claims it does.
6. Nothing regressed for a single user on default settings.
"""

from __future__ import annotations

import asyncio
import statistics
import time
from typing import Any

import pytest
from fastapi import FastAPI, HTTPException, Response
from fastapi.security import OAuth2PasswordRequestForm
from starlette.requests import Request
from starlette.testclient import TestClient

from app.gateway.auth import login_throttle
from app.gateway.auth.login_throttle import (
    AttemptRecord,
    LoginThrottleUnavailable,
    MemoryLoginThrottleStore,
    RedisLoginThrottleStore,
    ThrottlePolicy,
    account_key,
    apply_failure,
    evaluate_record,
    normalize_account,
)
from app.gateway.routers import auth as auth_router
from deerflow.config.app_config import AppConfig, reset_app_config, set_app_config
from deerflow.config.auth_config import AuthAppConfig, LocalAuthConfig
from deerflow.config.sandbox_config import SandboxConfig

pytestmark = pytest.mark.asyncio

OFFICE_IP = "203.0.113.10"
ALICE = "alice@example.com"
BOB = "bob@example.com"
PASSWORD = "correct-horse-battery"


# ── Harness ───────────────────────────────────────────────────────────────


class _Provider:
    """A local auth provider over a fixed set of accounts.

    ``authenticate`` mirrors the real provider's shape, including the
    equal-cost verification an unknown address now pays.
    """

    def __init__(self, users: dict[str, str]) -> None:
        self._users = users
        self.calls: list[str] = []

    async def authenticate(self, credentials: dict) -> Any:
        email = normalize_account(credentials.get("email"))
        self.calls.append(email)
        if self._users.get(email) == credentials.get("password"):
            return _User(email)
        from app.gateway.auth.password import equalize_password_timing

        await equalize_password_timing()
        return None


class _User:
    def __init__(self, email: str) -> None:
        self.id = f"id-{email}"
        self.email = email
        self.token_version = 0
        self.needs_setup = False
        self.system_role = "user"


def _request(source: str = OFFICE_IP) -> Request:
    return Request(
        {
            "type": "http",
            "method": "POST",
            "path": "/api/v1/auth/login/local",
            "headers": [],
            "query_string": b"",
            "client": (source, 44000),
            "server": ("testserver", 80),
            "app": _BareApp(),
        }
    )


class _BareApp:
    """Stands in for ``request.app`` — the router only reads app.state."""

    class _State:
        redis_tenant_namespace = None

    state = _State()


def _form(username: str, password: str) -> OAuth2PasswordRequestForm:
    return OAuth2PasswordRequestForm(username=username, password=password)


async def _login(username: str, password: str, *, source: str = OFFICE_IP):
    return await auth_router.login_local(_request(source), Response(), _form(username, password), remember_me=True)


def _set_config(**local: Any) -> None:
    set_app_config(AppConfig(sandbox=SandboxConfig(use="test"), auth=AuthAppConfig(local=LocalAuthConfig(**local))))


@pytest.fixture(autouse=True)
def _isolated_throttle(monkeypatch):
    """A fresh memory store and a known provider for every test."""
    monkeypatch.delenv("AUTH_TRUSTED_PROXIES", raising=False)
    store = MemoryLoginThrottleStore()
    login_throttle.set_login_throttle_store(store, signature=("memory",))
    monkeypatch.setattr(auth_router, "get_local_provider", lambda: _Provider({ALICE: PASSWORD, BOB: PASSWORD}))
    monkeypatch.setattr(auth_router, "create_access_token", lambda *a, **k: "token")
    monkeypatch.setattr(auth_router, "_set_session_cookie", lambda *a, **k: None)
    yield store
    login_throttle.set_login_throttle_store(None)
    reset_app_config()


# ── 1. The per-account lock, and the colleague who still logs in ──────────


async def test_account_locks_and_a_colleague_on_the_same_address_still_logs_in():
    """Five failures lock Alice. Bob, from the same office address, logs in.

    This is the defect in one assertion: the office shares one egress address,
    so under the per-address counter Bob was locked out by Alice's typing —
    and with no AUTH_TRUSTED_PROXIES set, so was every remote colleague,
    because every request carried the reverse proxy's address.
    """
    _set_config()
    for _ in range(5):
        with pytest.raises(HTTPException) as failure:
            await _login(ALICE, "wrong")
        assert failure.value.status_code == 401

    with pytest.raises(HTTPException) as locked:
        await _login(ALICE, PASSWORD)  # even the right password is refused now
    assert locked.value.status_code == 401

    result = await _login(BOB, PASSWORD, source=OFFICE_IP)
    assert result.expires_in > 0


async def test_lockout_follows_the_account_across_source_addresses():
    """The lock is the account's, so it is not escaped by changing address."""
    _set_config()
    for index in range(5):
        with pytest.raises(HTTPException):
            await _login(ALICE, "wrong", source=f"198.51.100.{index}")
    with pytest.raises(HTTPException):
        await _login(ALICE, PASSWORD, source="198.51.100.200")


async def test_account_key_is_case_insensitive_and_never_stores_the_address():
    """Email lookup is case-insensitive, so the counter must be too."""
    _set_config()
    for _ in range(5):
        with pytest.raises(HTTPException):
            await _login("ALICE@example.com", "wrong")
    with pytest.raises(HTTPException):
        await _login(ALICE, PASSWORD)
    assert account_key(ALICE) == account_key(" Alice@Example.com ")
    assert ALICE not in account_key(ALICE)


# ── 2. The looser per-source spray guard ──────────────────────────────────


async def test_spraying_many_accounts_from_one_source_trips_the_source_guard():
    """Distinct-account spraying is what the per-account lock cannot see.

    Sized for one office behind one NAT address: the default limit of 50
    distinct accounts takes more accounts than a five-to-twenty person company
    has staff, so no legitimate morning reaches it. Here the limit is lowered
    to 4 to keep the test fast; the arithmetic is the same.
    """
    _set_config(source_max_distinct_accounts=4, source_max_failures=1000)
    for index in range(4):
        with pytest.raises(HTTPException) as failure:
            await _login(f"user{index}@example.com", "wrong")
        assert failure.value.status_code == 401

    with pytest.raises(HTTPException) as sprayed:
        await _login(BOB, PASSWORD)
    assert sprayed.value.status_code == 429


async def test_volume_from_one_source_trips_the_source_guard_too():
    """Sheer volume against one account is bounded even after it locks."""
    _set_config(account_max_attempts=2, source_max_distinct_accounts=1000, source_max_failures=6)
    for _ in range(6):
        with pytest.raises(HTTPException) as failure:
            await _login(ALICE, "wrong")
        assert failure.value.status_code == 401
    with pytest.raises(HTTPException) as sprayed:
        await _login(BOB, PASSWORD)
    assert sprayed.value.status_code == 429


async def test_a_whole_office_typing_badly_never_reaches_the_source_guard():
    """Twenty staff, four failures each, on the defaults: no source lock.

    Eighty failures across twenty accounts is a bad Monday morning with a tool
    nobody's password manager has learned yet. Each account is well inside its
    own five-failure budget, and the source guard's defaults (50 distinct
    accounts, 300 failures per 15 minutes) are far above it.
    """
    _set_config()
    for index in range(20):
        for _ in range(4):
            with pytest.raises(HTTPException) as failure:
                await _login(f"staff{index}@example.com", "wrong")
            assert failure.value.status_code == 401
    result = await _login(BOB, PASSWORD)
    assert result.expires_in > 0


# ── 3. The admin unlock ───────────────────────────────────────────────────


def _admin_app(store, *, role: str) -> TestClient:
    """Mount the two lockout routes with a fixed caller identity."""
    app = FastAPI()

    @app.middleware("http")
    async def _identity(request: Request, call_next):
        from app.gateway.auth_disabled import AUTH_SOURCE_SESSION

        request.state.user = _AdminUser(role)
        request.state.auth_source = AUTH_SOURCE_SESSION
        return await call_next(request)

    app.include_router(auth_router.router)
    app.state.redis_tenant_namespace = None
    return TestClient(app)


class _AdminUser:
    def __init__(self, role: str) -> None:
        self.id = "admin-1"
        self.email = f"{role}@example.com"
        self.system_role = role


async def test_admin_unlock_releases_an_account_with_no_restart(_isolated_throttle):
    """An admin clears the lock and the account logs in immediately after."""
    _set_config()
    for _ in range(5):
        with pytest.raises(HTTPException):
            await _login(ALICE, "wrong")
    with pytest.raises(HTTPException):
        await _login(ALICE, PASSWORD)

    client = _admin_app(_isolated_throttle, role="admin")
    listed = client.get("/api/v1/auth/lockouts")
    assert listed.status_code == 200
    labels = [row["label"] for row in listed.json()["lockouts"]]
    assert ALICE in labels

    cleared = client.post("/api/v1/auth/lockouts/clear", json={"account": ALICE})
    assert cleared.status_code == 200, cleared.text
    assert cleared.json() == {"cleared": True, "kind": "account", "label": ALICE}

    result = await _login(ALICE, PASSWORD)  # same process, no restart
    assert result.expires_in > 0
    assert client.get("/api/v1/auth/lockouts").json()["lockouts"] == []


async def test_unprivileged_caller_cannot_read_or_clear_lockouts(_isolated_throttle):
    """The unlock is an admin control; a regular user is refused both halves."""
    _set_config()
    for _ in range(5):
        with pytest.raises(HTTPException):
            await _login(ALICE, "wrong")

    client = _admin_app(_isolated_throttle, role="user")
    assert client.get("/api/v1/auth/lockouts").status_code == 403
    assert client.post("/api/v1/auth/lockouts/clear", json={"account": ALICE}).status_code == 403

    with pytest.raises(HTTPException):
        await _login(ALICE, PASSWORD)  # still locked


async def test_admin_unlock_clears_a_source_lock_and_rejects_ambiguous_requests(_isolated_throttle):
    _set_config(source_max_distinct_accounts=3, source_max_failures=1000)
    for index in range(3):
        with pytest.raises(HTTPException):
            await _login(f"user{index}@example.com", "wrong")
    with pytest.raises(HTTPException) as sprayed:
        await _login(BOB, PASSWORD)
    assert sprayed.value.status_code == 429

    client = _admin_app(_isolated_throttle, role="admin")
    kinds = {row["kind"] for row in client.get("/api/v1/auth/lockouts").json()["lockouts"]}
    assert "source" in kinds
    assert client.post("/api/v1/auth/lockouts/clear", json={}).status_code == 400
    assert client.post("/api/v1/auth/lockouts/clear", json={"account": ALICE, "source": OFFICE_IP}).status_code == 400
    assert client.post("/api/v1/auth/lockouts/clear", json={"source": OFFICE_IP}).status_code == 200

    result = await _login(BOB, PASSWORD)
    assert result.expires_in > 0


async def test_lockout_routes_are_not_public():
    """The middleware must require a session before the route's own checks.

    ``require_admin_user`` reads ``request.state.user``, which only exists
    once the auth middleware has authenticated the caller; a public path
    would reach the route with no user at all.
    """
    from app.gateway.auth_middleware import _is_public

    assert _is_public("/api/v1/auth/lockouts") is False
    assert _is_public("/api/v1/auth/lockouts/clear") is False
    assert _is_public("/api/v1/auth/login/local") is True  # unchanged


# ── 4. No account enumeration ─────────────────────────────────────────────


async def test_locked_account_and_unknown_account_are_indistinguishable():
    """Same status, same body: the lock is not an existence oracle.

    A 429 for a locked account would tell an unauthenticated caller which
    addresses are real, so a locked account answers exactly as a wrong
    password does. The per-source guard still answers 429, because it says
    nothing about any account.
    """
    _set_config()
    for _ in range(5):
        with pytest.raises(HTTPException):
            await _login(ALICE, "wrong")

    with pytest.raises(HTTPException) as locked:
        await _login(ALICE, "guess")
    with pytest.raises(HTTPException) as unknown:
        await _login("nobody@example.com", "guess")

    assert locked.value.status_code == unknown.value.status_code == 401
    assert locked.value.detail == unknown.value.detail
    assert locked.value.detail["code"] == "invalid_credentials"


async def test_locked_account_pays_the_same_verification_cost_as_an_unknown_one(monkeypatch):
    """Timing: every miss spends exactly one password verification.

    bcrypt is deliberately slow, so an early return is measurable over a WAN.
    Counting the verifications is the deterministic form of that check — the
    wall-clock comparison below is the same claim, sampled.
    """
    _set_config()
    calls: list[str] = []
    from app.gateway.auth import password as password_module

    real = password_module.verify_password_async

    async def _counting(plain: str, hashed: str) -> bool:
        calls.append(hashed[:12])
        return await real(plain, hashed)

    monkeypatch.setattr(password_module, "verify_password_async", _counting)
    monkeypatch.setattr(auth_router, "get_local_provider", lambda: _RealShapeProvider({ALICE: PASSWORD}))

    for _ in range(5):
        with pytest.raises(HTTPException):
            await _login(ALICE, "wrong")
    baseline = len(calls)

    with pytest.raises(HTTPException):
        await _login(ALICE, "guess")  # locked
    locked_cost = len(calls) - baseline

    with pytest.raises(HTTPException):
        await _login("nobody@example.com", "guess")  # unknown
    unknown_cost = len(calls) - baseline - locked_cost

    assert locked_cost == unknown_cost == 1


class _RealShapeProvider:
    """Wraps the production provider over an in-memory user table."""

    def __init__(self, users: dict[str, str]) -> None:
        from app.gateway.auth.password import hash_password

        self._hashes = {email: hash_password(password) for email, password in users.items()}

    async def authenticate(self, credentials: dict) -> Any:
        from app.gateway.auth import password as password_module

        email = normalize_account(credentials.get("email"))
        stored = self._hashes.get(email)
        if stored is None:
            await password_module.equalize_password_timing()
            return None
        if not await password_module.verify_password_async(credentials.get("password", ""), stored):
            return None
        return _User(email)


async def test_locked_and_unknown_response_times_are_comparable(monkeypatch):
    """The sampled form of the same claim: medians within a bcrypt of each other."""
    _set_config()
    monkeypatch.setattr(auth_router, "get_local_provider", lambda: _RealShapeProvider({ALICE: PASSWORD}))
    for _ in range(5):
        with pytest.raises(HTTPException):
            await _login(ALICE, "wrong")

    async def _sample(username: str) -> float:
        started = time.perf_counter()
        with pytest.raises(HTTPException):
            await _login(username, "guess")
        return time.perf_counter() - started

    locked = statistics.median([await _sample(ALICE) for _ in range(5)])
    unknown = statistics.median([await _sample("nobody@example.com") for _ in range(5)])
    slower, faster = max(locked, unknown), min(locked, unknown)
    # Both pay one bcrypt; the ratio bound is loose enough for a shared CI box
    # and still fails the early-return this test exists to prevent (which was
    # two orders of magnitude faster, not 3x).
    assert slower < faster * 3, (locked, unknown)


# ── 5. The store's failure mode is the one the change claims ──────────────


class _BrokenStore:
    """Every operation fails, as a store whose backend is unreachable does."""

    async def account_locked(self, account: str, policy: ThrottlePolicy) -> bool:
        raise LoginThrottleUnavailable("down")

    async def record_account_failure(self, account: str, policy: ThrottlePolicy) -> bool:
        raise LoginThrottleUnavailable("down")

    async def clear_account(self, account: str) -> bool:
        raise LoginThrottleUnavailable("down")

    async def source_locked(self, source: str, policy: ThrottlePolicy) -> bool:
        raise LoginThrottleUnavailable("down")

    async def record_source_failure(self, source: str, account: str, policy: ThrottlePolicy) -> bool:
        raise LoginThrottleUnavailable("down")

    async def clear_source(self, source: str) -> bool:
        raise LoginThrottleUnavailable("down")

    async def lockouts(self, policy: ThrottlePolicy) -> list:
        raise LoginThrottleUnavailable("down")

    async def close(self) -> None:
        return None


async def test_store_outage_fails_the_login_path_closed():
    """No counter, no login — including for correct credentials.

    Failing open would hand an attacker unlimited guesses during exactly the
    window nobody is watching. The 503 says nothing about any account, so it
    is not an enumeration channel either.
    """
    _set_config()
    login_throttle.set_login_throttle_store(_BrokenStore(), signature=("memory",))
    for username, password in ((ALICE, PASSWORD), (ALICE, "wrong"), ("nobody@example.com", "guess")):
        with pytest.raises(HTTPException) as unavailable:
            await _login(username, password)
        assert unavailable.value.status_code == 503, username
        assert "temporarily unavailable" in unavailable.value.detail


async def test_store_outage_after_a_correct_password_still_issues_the_session():
    """A store that fails only on the way out must not fail a good login."""
    _set_config()

    class _FailsOnClear(MemoryLoginThrottleStore):
        async def clear_account(self, account: str) -> bool:
            raise LoginThrottleUnavailable("down")

    login_throttle.set_login_throttle_store(_FailsOnClear(), signature=("memory",))
    result = await _login(ALICE, PASSWORD)
    assert result.expires_in > 0


async def test_redis_store_translates_backend_errors_into_unavailable():
    """The redis backend surfaces an outage as the fail-closed signal."""

    class _DeadRedis:
        async def hgetall(self, key: str):
            raise OSError("connection refused")

        async def incr(self, key: str):
            raise OSError("connection refused")

        async def delete(self, *keys: str):
            raise OSError("connection refused")

        async def scan(self, **kwargs):
            raise OSError("connection refused")

    store = RedisLoginThrottleStore("redis://unused", key_prefix="t:auth", client=_DeadRedis())
    policy = ThrottlePolicy.from_local_config(LocalAuthConfig())
    with pytest.raises(LoginThrottleUnavailable):
        await store.account_locked(ALICE, policy)
    with pytest.raises(LoginThrottleUnavailable):
        await store.record_source_failure(OFFICE_IP, ALICE, policy)
    with pytest.raises(LoginThrottleUnavailable):
        await store.lockouts(policy)


# ── 6. Nothing regressed for a single user ────────────────────────────────


async def test_single_user_normal_login_failure_lockout_and_expiry(monkeypatch):
    """The whole default-settings lifecycle for one person on one machine."""
    _set_config()
    assert (await _login(ALICE, PASSWORD)).expires_in > 0  # normal login

    with pytest.raises(HTTPException) as failure:
        await _login(ALICE, "wrong")  # normal failure
    assert failure.value.status_code == 401
    assert (await _login(ALICE, PASSWORD)).expires_in > 0  # success clears it

    for _ in range(5):
        with pytest.raises(HTTPException):
            await _login(ALICE, "wrong")
    with pytest.raises(HTTPException):
        await _login(ALICE, PASSWORD)  # normal lockout

    locked_at = time.time()
    monkeypatch.setattr(login_throttle.time, "time", lambda: locked_at + 301.0)
    assert (await _login(ALICE, PASSWORD)).expires_in > 0  # normal expiry (300s)


async def test_defaults_are_the_historical_numbers():
    """5 failures / 300 seconds, unchanged — only the key changed."""
    local = LocalAuthConfig()
    assert (local.effective_account_max_attempts, local.effective_account_lockout_seconds) == (5, 300.0)
    assert local.lockout_store == "memory"


async def test_deprecated_keys_still_apply_and_new_keys_win():
    """An operator's existing numbers keep working; the new names take priority."""
    assert LocalAuthConfig(max_login_attempts=3).effective_account_max_attempts == 3
    assert LocalAuthConfig(lockout_seconds=30.0).effective_account_lockout_seconds == 30.0
    assert LocalAuthConfig(max_login_attempts=3, account_max_attempts=7).effective_account_max_attempts == 7


async def test_deprecated_keys_warn_when_an_operator_sets_them(caplog):
    """The rename is not silent: the dimension changed from address to account."""
    with caplog.at_level("WARNING"):
        LocalAuthConfig(max_login_attempts=50)
    rendered = [record.getMessage() for record in caplog.records]
    assert any("max_login_attempts" in message and "ACCOUNT" in message for message in rendered), rendered
    caplog.clear()
    with caplog.at_level("WARNING"):
        LocalAuthConfig()
    assert not caplog.records


async def test_throttle_knobs_reject_degenerate_values():
    """One failed attempt must never lock an account."""
    import pydantic

    for kwargs in (
        {"account_max_attempts": 1},
        {"account_lockout_seconds": 0},
        {"account_lockout_seconds": float("inf")},
        {"source_max_distinct_accounts": 1},
        {"source_max_failures": 1},
        {"source_window_seconds": 0},
        {"lockout_store": "postgres"},
    ):
        with pytest.raises(pydantic.ValidationError):
            LocalAuthConfig(**kwargs)


# ── Live policy semantics, carried over from the per-address throttle ─────


async def test_raising_the_threshold_unlocks_without_a_restart():
    """The hot-reload escape hatch survives the re-keying."""
    _set_config(account_max_attempts=2)
    for _ in range(2):
        with pytest.raises(HTTPException):
            await _login(ALICE, "wrong")
    with pytest.raises(HTTPException):
        await _login(ALICE, PASSWORD)
    _set_config(account_max_attempts=5)
    assert (await _login(ALICE, PASSWORD)).expires_in > 0


async def test_evaluate_record_directional_duration_semantics():
    """Lower releases early, raise extends, a served sentence never returns."""
    record = AttemptRecord(fail_count=5, locked_at=1000.0, locked_duration=60.0, label=ALICE)
    assert evaluate_record(record, 5, 60.0, 1002.0).locked is True
    assert evaluate_record(record, 5, 1.0, 1002.0).locked is False  # lowered: released
    assert evaluate_record(record, 5, 600.0, 1050.0).locked is True  # raised inside the sentence: extended
    assert evaluate_record(record, 5, 600.0, 1100.0).expired is True  # raised after it was served: not resurrected
    served = AttemptRecord(fail_count=5, locked_at=1000.0, locked_duration=1.0, label=ALICE)
    assert evaluate_record(served, 5, 600.0, 1002.0).expired is True  # not resurrected
    assert evaluate_record(record, 9, 60.0, 1002.0).locked is False  # threshold raised
    assert evaluate_record(None, 5, 60.0, 1002.0).locked is False


async def test_apply_failure_never_locks_on_the_first_failure():
    record, locked = apply_failure(None, label=ALICE, max_attempts=2, lockout_seconds=60.0, now=1000.0)
    assert (record.fail_count, locked) == (1, False)
    record, locked = apply_failure(record, label=ALICE, max_attempts=2, lockout_seconds=60.0, now=1000.0)
    assert (record.fail_count, locked, record.is_locked) == (2, True, True)
    _, locked_again = apply_failure(record, label=ALICE, max_attempts=2, lockout_seconds=60.0, now=1001.0)
    assert locked_again is False, "an already-locked account does not re-announce"


async def test_concurrent_checks_of_an_expired_lock_are_race_free():
    """Policy resolution yields the loop; concurrent expiry must not KeyError."""
    store = MemoryLoginThrottleStore()
    policy = ThrottlePolicy.from_local_config(LocalAuthConfig())
    store._accounts[account_key(ALICE)] = AttemptRecord(fail_count=5, locked_at=1.0, locked_duration=1.0, label=ALICE)
    results = await asyncio.gather(*[store.account_locked(ALICE, policy) for _ in range(8)], return_exceptions=True)
    assert results == [False] * 8


async def test_tightening_the_threshold_preserves_accumulated_failures():
    """Lowering the limit mid-count must not hand out a fresh budget.

    Four failures under a limit of 5, then the operator tightens to 2: the
    count survives, the next failure locks, and a correct password still
    clears everything — no retroactive lockout of someone who fat-fingered it.
    """
    _set_config(account_max_attempts=5)
    for _ in range(4):
        with pytest.raises(HTTPException):
            await _login(ALICE, "wrong")

    _set_config(account_max_attempts=2)
    with pytest.raises(HTTPException) as failure:
        await _login(ALICE, "wrong")  # 4 + 1 >= 2: locks on the very next failure
    assert failure.value.status_code == 401
    with pytest.raises(HTTPException):
        await _login(ALICE, PASSWORD)  # locked

    _set_config(account_max_attempts=9)  # raise it back: 5 < 9, released
    assert (await _login(ALICE, PASSWORD)).expires_in > 0


async def test_capacity_eviction_sweeps_served_sentences_before_live_counters(monkeypatch):
    """The sweep expires records by their own committed sentence.

    A record locked under an older, lower threshold has a count below the live
    maximum; gating expiry on the current threshold would keep it resident
    while the capacity fallback evicted live counters first (they sort
    earliest), handing an active offender a fresh budget.
    """
    store = MemoryLoginThrottleStore()
    monkeypatch.setattr(login_throttle, "MAX_TRACKED_KEYS", 2)
    monkeypatch.setattr(login_throttle.time, "time", lambda: 100.0)
    policy = ThrottlePolicy.from_local_config(LocalAuthConfig(account_max_attempts=3, account_lockout_seconds=60.0))

    store._accounts["live-counter"] = AttemptRecord(fail_count=1, locked_at=0.0, locked_duration=0.0, label="live")
    store._accounts["expired-lock"] = AttemptRecord(fail_count=2, locked_at=10.0, locked_duration=1.0, label="served")

    await store.record_account_failure("fresh@example.com", policy)

    assert "expired-lock" not in store._accounts  # served sentence swept
    assert store._accounts["live-counter"].fail_count == 1  # live counter survives
    assert store._accounts[account_key("fresh@example.com")].fail_count == 1


# ── The redis backend, against a fake client ──────────────────────────────


class _FakeRedis:
    """Minimal async redis stand-in: hashes, counters, sets, scan, expire."""

    def __init__(self) -> None:
        self.hashes: dict[str, dict[str, str]] = {}
        self.values: dict[str, int] = {}
        self.sets: dict[str, set[str]] = {}
        self.expires: dict[str, int] = {}

    async def hgetall(self, key: str) -> dict[str, str]:
        return dict(self.hashes.get(key, {}))

    async def hset(self, key: str, mapping: dict) -> int:
        self.hashes.setdefault(key, {}).update({k: str(v) for k, v in mapping.items()})
        return len(mapping)

    async def expire(self, key: str, seconds: int) -> bool:
        self.expires[key] = seconds
        return True

    async def delete(self, *keys: str) -> int:
        removed = 0
        for key in keys:
            removed += self.hashes.pop(key, None) is not None
            removed += self.values.pop(key, None) is not None
            removed += self.sets.pop(key, None) is not None
        return removed

    async def incr(self, key: str) -> int:
        self.values[key] = self.values.get(key, 0) + 1
        return self.values[key]

    async def sadd(self, key: str, member: str) -> int:
        before = len(self.sets.setdefault(key, set()))
        self.sets[key].add(member)
        return len(self.sets[key]) - before

    async def scard(self, key: str) -> int:
        return len(self.sets.get(key, set()))

    async def scan(self, cursor: int = 0, match: str | None = None, count: int = 500) -> tuple[int, list[str]]:
        import fnmatch

        keys = sorted(self.hashes)
        if match is not None:
            keys = [key for key in keys if fnmatch.fnmatchcase(key, match)]
        return 0, keys


async def test_redis_backend_locks_lists_and_clears():
    """The shared backend does what the memory one does, on one client."""
    client = _FakeRedis()
    store = RedisLoginThrottleStore("redis://unused", key_prefix="hm:v1:tenant-x:redis:auth:login-throttle:v1", client=client)
    policy = ThrottlePolicy.from_local_config(LocalAuthConfig())

    assert await store.account_locked(ALICE, policy) is False
    for _ in range(5):
        await store.record_account_failure(ALICE, policy)
    assert await store.account_locked(ALICE, policy) is True
    assert await store.account_locked(BOB, policy) is False  # a colleague is unaffected

    views = await store.lockouts(policy)
    assert [(view.kind, view.label) for view in views] == [("account", ALICE)]
    assert all(key.startswith("hm:v1:tenant-x:redis:auth:login-throttle:v1") for key in client.hashes)
    assert ALICE not in "".join(client.hashes)  # the key is a digest, not the address

    assert await store.clear_account(ALICE) is True
    assert await store.account_locked(ALICE, policy) is False


async def test_redis_backend_source_guard_counts_distinct_accounts():
    client = _FakeRedis()
    store = RedisLoginThrottleStore("redis://unused", key_prefix="t:auth", client=client)
    policy = ThrottlePolicy.from_local_config(LocalAuthConfig(source_max_distinct_accounts=3, source_max_failures=1000))

    for index in range(2):
        assert await store.record_source_failure(OFFICE_IP, f"user{index}@example.com", policy) is False
    assert await store.record_source_failure(OFFICE_IP, "user2@example.com", policy) is True
    assert await store.source_locked(OFFICE_IP, policy) is True
    assert await store.clear_source(OFFICE_IP) is True
    assert await store.source_locked(OFFICE_IP, policy) is False


async def test_redis_keys_are_tenant_scoped():
    """Two tenants on one Redis cannot collide, like every key family here."""
    from deerflow.runtime.tenant_identity import TenantIdentityV1, TenantSubsystem

    namespaces = [TenantIdentityV1.from_canonical_id(name).namespace(TenantSubsystem.REDIS) for name in ("tenant-a", "tenant-b")]
    prefixes = [login_throttle.redis_key_prefix(namespace) for namespace in namespaces]
    assert prefixes[0] != prefixes[1]
    assert all(prefix.endswith(":auth:login-throttle:v1") for prefix in prefixes)
    assert login_throttle.redis_key_prefix(None) == login_throttle.UNSCOPED_KEY_PREFIX
