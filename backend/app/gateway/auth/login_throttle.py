"""Login failure counters, keyed on the account and guarded per source.

Two limits, deliberately very different in size:

* **Per account** — ``auth.local.account_max_attempts`` failures against one
  submitted email address lock *that address* for
  ``auth.local.account_lockout_seconds``. This is the control the threat
  actually wants: it is the account being guessed, not the building. It was
  previously keyed on the client address, which meant one office behind one
  NAT address locked itself out five failures into a Monday morning — and with
  no ``AUTH_TRUSTED_PROXIES`` set, one *reverse proxy address* stood for every
  user, remote staff included.
* **Per source** — a much looser guard against untargeted spraying, which the
  per-account limit cannot see: one address may fail against at most
  ``source_max_distinct_accounts`` distinct accounts, and make at most
  ``source_max_failures`` failures in total, within ``source_window_seconds``.
  Tripping either locks the address for ``source_lockout_seconds``.

  The window is **fixed, not sliding**: it opens on that address's first
  counted failure and closes ``source_window_seconds`` later, after which the
  next failure opens a new one. Nothing decays inside an open window, and the
  redis backend expresses exactly this by carrying a TTL on the counter from
  its first increment.

  Both defaults sit above what one small office is expected to produce, but
  neither is a promise it cannot get there: retries against an already-locked
  account still count (nothing in the response tells a person to stop), and
  mistyped addresses count as distinct accounts. Deployments that know their
  shape should size ``source_max_failures`` against it — the tenant Compose
  profile does, in ``deploy/compose/config.yaml``. Neither limit makes an
  address immune to a malicious user who shares it.

  Raising a limit does **not** release an address already locked: unlike the
  per-account lock, which is re-derived against the live threshold on every
  check, the source lock is a written sentence that runs out on
  ``source_lockout_seconds`` or is cleared by an administrator.

Both counters live behind :class:`LoginThrottleStore`. The memory backend is
per process (the historical behaviour, and the safe default for a single-user
deployment); the redis backend shares them across workers and replicas and
survives a restart, which is what makes an admin unlock outlive a rollout.

**A backing-store outage fails the login path closed**: the store raises
:class:`LoginThrottleUnavailable` and the router answers 503. Failing open
would hand an attacker unlimited guesses during exactly the window nobody is
watching, and on this deployment Redis is already load-bearing for streaming,
so a Redis outage is not a state the product works in anyway.

Account keys are hashed before they reach the store, so no key carries an
email address; the address is kept as a field on the record because the admin
lockout list has to name the account an operator is being asked about.
"""

from __future__ import annotations

import hashlib
import logging
import math
import time
from collections.abc import Iterable
from dataclasses import dataclass, replace
from typing import Any, Literal, Protocol

logger = logging.getLogger(__name__)

REDIS_INSTALL = "redis is required for auth.local.lockout_store: redis. Install it with: uv sync --extra redis"

#: Bound on tracked keys for the memory backend (the redis backend bounds
#: itself with per-key TTLs instead).
MAX_TRACKED_KEYS = 10000

#: Ceiling on redis SCAN iterations when listing lockouts for an admin.
_MAX_SCAN_ITERATIONS = 200

KeyKind = Literal["account", "source"]


class LoginThrottleUnavailable(RuntimeError):
    """The backing store could not be reached; the login path fails closed."""


@dataclass(frozen=True)
class ThrottlePolicy:
    """The live policy for one login attempt.

    Resolved per call from ``auth.local`` (like the pre-existing throttle) so a
    config reload applies to the next login without a Gateway restart.
    """

    account_max_attempts: int
    account_lockout_seconds: float
    source_window_seconds: float
    source_max_distinct_accounts: int
    source_max_failures: int
    source_lockout_seconds: float

    @classmethod
    def from_local_config(cls, local: Any) -> ThrottlePolicy:
        return cls(
            account_max_attempts=local.effective_account_max_attempts,
            account_lockout_seconds=local.effective_account_lockout_seconds,
            source_window_seconds=local.source_window_seconds,
            source_max_distinct_accounts=local.source_max_distinct_accounts,
            source_max_failures=local.source_max_failures,
            source_lockout_seconds=local.source_lockout_seconds,
        )


@dataclass(frozen=True)
class AttemptRecord:
    """One key's failure counter and, once locked, its sentence.

    ``locked_duration`` is the duration the lock was last evaluated under, not
    the one it was created under: a lowered ``account_lockout_seconds``
    releases an active lock early, a raised one extends a still-running lock,
    and a sentence already served under a shorter policy is never resurrected.
    That directional behaviour predates this module and is kept intact.
    """

    fail_count: int
    locked_at: float
    locked_duration: float
    #: The account address or source address this record belongs to, kept so
    #: the admin lockout list can name it. Never part of the store key.
    label: str = ""

    @property
    def is_locked(self) -> bool:
        return self.locked_at > 0.0


@dataclass(frozen=True)
class LockoutView:
    """One currently-locked key, as an administrator sees it."""

    kind: KeyKind
    label: str
    failures: int
    locked_at: float
    expires_at: float


def normalize_account(raw: str | None) -> str:
    """Fold an account identifier to its lookup form.

    Email lookup is case-insensitive in the user repository, so the counter
    must be too: otherwise ``Alice@example.com`` and ``alice@example.com``
    would each get their own budget against one account.
    """
    return (raw or "").strip().lower()


def account_key(account: str) -> str:
    """Hash an account identifier into a store key.

    Emails are personal data and unbounded in length; the store key is a
    digest so neither property reaches Redis key space. The plaintext travels
    in the record's ``label`` field, which the admin list reads back.
    """
    return hashlib.sha256(normalize_account(account).encode("utf-8")).hexdigest()[:32]


def source_key(source: str) -> str:
    """Hash a client address into a store key (same reasoning as accounts)."""
    return hashlib.sha256((source or "unknown").encode("utf-8")).hexdigest()[:32]


# ── Pure policy application, shared by every backend ──────────────────────


@dataclass(frozen=True)
class LockDecision:
    """What a check concluded about one record."""

    locked: bool
    #: A record to write back (a committed duration change), or None.
    commit: AttemptRecord | None = None
    #: True when the record has served its sentence and should be deleted.
    expired: bool = False
    expires_at: float = 0.0


def evaluate_record(record: AttemptRecord | None, max_attempts: int, lockout_seconds: float, now: float) -> LockDecision:
    """Decide whether ``record`` is locked right now under the live policy.

    Ported verbatim in behaviour from the per-address throttle this replaces,
    so the hot-reload semantics its tests pinned survive the re-keying.
    """
    if record is None:
        return LockDecision(locked=False)
    if record.fail_count < max_attempts:
        # Under the *current* threshold — the operator raised it mid-count.
        return LockDecision(locked=False)
    if not record.is_locked:
        # Over the threshold but no lock ever started under the threshold
        # these failures accumulated under. Keep the record: the next failure
        # starts the lock, a success clears it; dropping it here would hand
        # the key a fresh budget under a stricter policy.
        return LockDecision(locked=False)
    if now >= record.locked_at + record.locked_duration:
        # The sentence in force at the last evaluation is served. A later
        # duration increase must not resurrect it.
        return LockDecision(locked=False, expired=True)
    if now < record.locked_at + lockout_seconds:
        commit = None
        if lockout_seconds != record.locked_duration:
            commit = replace(record, locked_duration=lockout_seconds)
        return LockDecision(locked=True, commit=commit, expires_at=record.locked_at + lockout_seconds)
    # Original sentence still running, but the current (lowered) duration has
    # already elapsed — release early.
    return LockDecision(locked=False, expired=True)


def apply_failure(record: AttemptRecord | None, *, label: str, max_attempts: int, lockout_seconds: float, now: float) -> tuple[AttemptRecord, bool]:
    """Return the record after one more failure, and whether it just locked.

    ``max_attempts`` is at least 2 by config validation, so the first failure
    against a clean key never locks it: one typo must not lock an account.
    """
    count = 1 if record is None else record.fail_count + 1
    was_locked = record is not None and record.is_locked
    label = label or (record.label if record is not None else "")
    if count >= max_attempts:
        return AttemptRecord(fail_count=count, locked_at=now, locked_duration=lockout_seconds, label=label), not was_locked
    return AttemptRecord(fail_count=count, locked_at=0.0, locked_duration=0.0, label=label), False


# ── Store protocol ────────────────────────────────────────────────────────


class LoginThrottleStore(Protocol):
    """Where failure counters live. Every method may raise LoginThrottleUnavailable."""

    async def account_locked(self, account: str, policy: ThrottlePolicy) -> bool: ...

    async def record_account_failure(self, account: str, policy: ThrottlePolicy) -> bool:
        """Count one failure; return True when this failure created the lock."""

    async def clear_account(self, account: str) -> bool: ...

    async def source_locked(self, source: str, policy: ThrottlePolicy) -> bool: ...

    async def record_source_failure(self, source: str, account: str, policy: ThrottlePolicy) -> bool:
        """Count one failure against the source; True when it created the lock."""

    async def clear_source(self, source: str) -> bool: ...

    async def lockouts(self, policy: ThrottlePolicy) -> list[LockoutView]: ...

    async def close(self) -> None: ...


# ── Memory backend ────────────────────────────────────────────────────────


class MemoryLoginThrottleStore:
    """Per-process counters. The default, and the historical behaviour.

    Not shared between workers or replicas, and lost on restart — which is why
    an admin unlock against this backend clears only the process it reached.
    A deployment that wants the unlock to survive a rollout sets
    ``auth.local.lockout_store: redis``.
    """

    def __init__(self) -> None:
        self._accounts: dict[str, AttemptRecord] = {}
        self._sources: dict[str, AttemptRecord] = {}
        # source key → (window_started_at, failure count, distinct account keys)
        self._source_windows: dict[str, tuple[float, int, set[str]]] = {}

    # -- accounts --

    async def account_locked(self, account: str, policy: ThrottlePolicy) -> bool:
        return self._locked(self._accounts, account_key(account), policy.account_max_attempts, policy.account_lockout_seconds)

    async def record_account_failure(self, account: str, policy: ThrottlePolicy) -> bool:
        return self._record(
            self._accounts,
            account_key(account),
            label=normalize_account(account),
            max_attempts=policy.account_max_attempts,
            lockout_seconds=policy.account_lockout_seconds,
        )

    async def clear_account(self, account: str) -> bool:
        return self._accounts.pop(account_key(account), None) is not None

    # -- sources --

    async def source_locked(self, source: str, policy: ThrottlePolicy) -> bool:
        key = source_key(source)
        record = self._sources.get(key)
        decision = evaluate_record(record, 1, policy.source_lockout_seconds, time.time())
        if decision.expired:
            self._sources.pop(key, None)
            self._source_windows.pop(key, None)
            return False
        if decision.commit is not None:
            self._sources[key] = decision.commit
        return decision.locked

    async def record_source_failure(self, source: str, account: str, policy: ThrottlePolicy) -> bool:
        key = source_key(source)
        now = time.time()
        started, failures, accounts = self._source_windows.get(key, (now, 0, set()))
        if now - started >= policy.source_window_seconds:
            started, failures, accounts = now, 0, set()
        failures += 1
        accounts = accounts | {account_key(account)}
        self._evict_windows_if_needed(now, policy.source_window_seconds)
        self._source_windows[key] = (started, failures, accounts)
        if failures < policy.source_max_failures and len(accounts) < policy.source_max_distinct_accounts:
            return False
        already = self._sources.get(key)
        self._evict_records_if_needed(self._sources, now)
        self._sources[key] = AttemptRecord(fail_count=failures, locked_at=now, locked_duration=policy.source_lockout_seconds, label=source)
        return already is None or not already.is_locked

    async def clear_source(self, source: str) -> bool:
        key = source_key(source)
        self._source_windows.pop(key, None)
        return self._sources.pop(key, None) is not None

    # -- admin view --

    async def lockouts(self, policy: ThrottlePolicy) -> list[LockoutView]:
        now = time.time()
        views: list[LockoutView] = []
        for kind, table, max_attempts, duration in (
            ("account", self._accounts, policy.account_max_attempts, policy.account_lockout_seconds),
            ("source", self._sources, 1, policy.source_lockout_seconds),
        ):
            for key, record in list(table.items()):
                decision = evaluate_record(record, max_attempts, duration, now)
                if decision.expired:
                    if table.get(key) == record:
                        del table[key]
                    continue
                if not decision.locked:
                    continue
                views.append(
                    LockoutView(
                        kind=kind,  # type: ignore[arg-type]
                        label=record.label,
                        failures=record.fail_count,
                        locked_at=record.locked_at,
                        expires_at=decision.expires_at,
                    )
                )
        return sorted(views, key=lambda view: (view.kind, view.label))

    async def close(self) -> None:
        return None

    # -- internals --

    def _locked(self, table: dict[str, AttemptRecord], key: str, max_attempts: int, lockout_seconds: float) -> bool:
        record = table.get(key)
        if record is None:
            return False
        decision = evaluate_record(record, max_attempts, lockout_seconds, time.time())
        if decision.expired:
            if table.get(key) == record:
                del table[key]
            return False
        if decision.commit is not None and table.get(key) == record:
            table[key] = decision.commit
        return decision.locked

    def _record(self, table: dict[str, AttemptRecord], key: str, *, label: str, max_attempts: int, lockout_seconds: float) -> bool:
        now = time.time()
        self._evict_records_if_needed(table, now)
        record, locked_now = apply_failure(table.get(key), label=label, max_attempts=max_attempts, lockout_seconds=lockout_seconds, now=now)
        table[key] = record
        return locked_now

    @staticmethod
    def _evict_records_if_needed(table: dict[str, AttemptRecord], now: float) -> None:
        """Bound a counter table: served sentences first, then cheapest to lose.

        Expiry is a property of each record's own committed sentence, not of
        the live threshold: a record locked under an older, lower threshold
        must still be swept once its sentence is served. Gating on the current
        threshold would retain served records while the capacity fallback
        evicted live counters, handing active offenders fresh budgets.
        """
        if len(table) < MAX_TRACKED_KEYS:
            return
        for key, record in list(table.items()):
            if record.is_locked and now >= record.locked_at + record.locked_duration:
                del table[key]
        if len(table) < MAX_TRACKED_KEYS:
            return
        by_expiry = sorted(table.items(), key=lambda item: item[1].locked_at + item[1].locked_duration)
        for key, _ in by_expiry[: len(by_expiry) // 2]:
            del table[key]

    def _evict_windows_if_needed(self, now: float, window_seconds: float) -> None:
        """Bound the source windows: closed windows first, then oldest."""
        table = self._source_windows
        if len(table) < MAX_TRACKED_KEYS:
            return
        for key, (started, _failures, _accounts) in list(table.items()):
            if now - started >= window_seconds:
                del table[key]
        if len(table) < MAX_TRACKED_KEYS:
            return
        by_age = sorted(table.items(), key=lambda item: item[1][0])
        for key, _ in by_age[: len(by_age) // 2]:
            del table[key]


# ── Redis backend ─────────────────────────────────────────────────────────


def _redis_errors() -> tuple[type[BaseException], ...]:
    try:
        from redis.exceptions import RedisError
    except ImportError as exc:  # pragma: no cover - guarded at construction
        raise ImportError(REDIS_INSTALL) from exc
    return (RedisError, OSError)


class RedisLoginThrottleStore:
    """Counters shared across workers and replicas, with per-key TTLs.

    Concurrency is last-writer-wins on the lock fields, deliberately: this is a
    throttle, not a ledger, and the worst case of a lost update is one failure
    counted twice or not at all. Every operation is wrapped so a Redis outage
    surfaces as :class:`LoginThrottleUnavailable` and the login path fails
    closed rather than silently reverting to unlimited guessing.
    """

    def __init__(self, redis_url: str, *, key_prefix: str, client: Any | None = None) -> None:
        self._prefix = key_prefix.rstrip(":")
        self._client = client if client is not None else self._create_client(redis_url)
        self._owns_client = client is None

    @staticmethod
    def _create_client(redis_url: str) -> Any:
        try:
            import redis.asyncio as redis_async
        except ImportError as exc:
            raise ImportError(REDIS_INSTALL) from exc
        return redis_async.from_url(redis_url, decode_responses=True)

    # -- key space --

    def _account(self, account: str) -> str:
        return f"{self._prefix}:acct:{account_key(account)}"

    def _source(self, source: str) -> str:
        return f"{self._prefix}:src:{source_key(source)}"

    def _window(self, source: str) -> str:
        return f"{self._prefix}:srcwin:{source_key(source)}"

    def _accounts_seen(self, source: str) -> str:
        return f"{self._prefix}:srcacct:{source_key(source)}"

    # -- accounts --

    async def account_locked(self, account: str, policy: ThrottlePolicy) -> bool:
        return await self._locked(self._account(account), policy.account_max_attempts, policy.account_lockout_seconds)

    async def record_account_failure(self, account: str, policy: ThrottlePolicy) -> bool:
        return await self._record(
            self._account(account),
            label=normalize_account(account),
            max_attempts=policy.account_max_attempts,
            lockout_seconds=policy.account_lockout_seconds,
        )

    async def clear_account(self, account: str) -> bool:
        async with _redis_guard():
            return bool(await self._client.delete(self._account(account)))

    # -- sources --

    async def source_locked(self, source: str, policy: ThrottlePolicy) -> bool:
        return await self._locked(self._source(source), 1, policy.source_lockout_seconds)

    async def record_source_failure(self, source: str, account: str, policy: ThrottlePolicy) -> bool:
        window_key = self._window(source)
        seen_key = self._accounts_seen(source)
        window = int(math.ceil(policy.source_window_seconds))
        async with _redis_guard():
            failures = int(await self._client.incr(window_key))
            if failures == 1:
                await self._client.expire(window_key, window)
            await self._client.sadd(seen_key, account_key(account))
            if failures == 1:
                await self._client.expire(seen_key, window)
            distinct = int(await self._client.scard(seen_key))
        if failures < policy.source_max_failures and distinct < policy.source_max_distinct_accounts:
            return False
        now = time.time()
        record = AttemptRecord(fail_count=failures, locked_at=now, locked_duration=policy.source_lockout_seconds, label=source)
        async with _redis_guard():
            existing = await self._read(self._source(source))
            await self._write(self._source(source), record, ttl=policy.source_lockout_seconds)
        return existing is None or not existing.is_locked

    async def clear_source(self, source: str) -> bool:
        async with _redis_guard():
            removed = bool(await self._client.delete(self._source(source)))
            await self._client.delete(self._window(source))
            await self._client.delete(self._accounts_seen(source))
            return removed

    # -- admin view --

    async def lockouts(self, policy: ThrottlePolicy) -> list[LockoutView]:
        now = time.time()
        views: list[LockoutView] = []
        for kind, pattern, max_attempts, duration in (
            ("account", f"{self._prefix}:acct:*", policy.account_max_attempts, policy.account_lockout_seconds),
            ("source", f"{self._prefix}:src:*", 1, policy.source_lockout_seconds),
        ):
            async with _redis_guard():
                keys = await self._scan(pattern)
                for key in keys:
                    record = await self._read(key)
                    if record is None:
                        continue
                    decision = evaluate_record(record, max_attempts, duration, now)
                    if decision.expired:
                        await self._client.delete(key)
                        continue
                    if not decision.locked:
                        continue
                    views.append(
                        LockoutView(
                            kind=kind,  # type: ignore[arg-type]
                            label=record.label,
                            failures=record.fail_count,
                            locked_at=record.locked_at,
                            expires_at=decision.expires_at,
                        )
                    )
        return sorted(views, key=lambda view: (view.kind, view.label))

    async def close(self) -> None:
        if not self._owns_client:
            return
        close = getattr(self._client, "aclose", None) or getattr(self._client, "close", None)
        if close is None:
            return
        try:
            await close()
        except Exception:  # pragma: no cover - shutdown best effort
            logger.debug("Failed to close the login throttle redis client", exc_info=True)

    # -- internals --

    async def _scan(self, pattern: str) -> list[str]:
        cursor = 0
        found: list[str] = []
        for _ in range(_MAX_SCAN_ITERATIONS):
            cursor, keys = await self._client.scan(cursor=cursor, match=pattern, count=500)
            found.extend(keys)
            if cursor == 0:
                break
        return found

    async def _read(self, key: str) -> AttemptRecord | None:
        raw = await self._client.hgetall(key)
        if not raw:
            return None
        try:
            return AttemptRecord(
                fail_count=int(raw.get("fail_count", 0)),
                locked_at=float(raw.get("locked_at", 0.0)),
                locked_duration=float(raw.get("locked_duration", 0.0)),
                label=str(raw.get("label", "")),
            )
        except (TypeError, ValueError):
            # A malformed record is a counter, not a credential: drop it and
            # start counting again rather than failing the login path.
            logger.warning("Discarding a malformed login throttle record")
            await self._client.delete(key)
            return None

    async def _write(self, key: str, record: AttemptRecord, *, ttl: float) -> None:
        await self._client.hset(
            key,
            mapping={
                "fail_count": record.fail_count,
                "locked_at": record.locked_at,
                "locked_duration": record.locked_duration,
                "label": record.label,
            },
        )
        await self._client.expire(key, max(1, int(math.ceil(ttl))))

    async def _locked(self, key: str, max_attempts: int, lockout_seconds: float) -> bool:
        async with _redis_guard():
            record = await self._read(key)
            if record is None:
                return False
            decision = evaluate_record(record, max_attempts, lockout_seconds, time.time())
            if decision.expired:
                await self._client.delete(key)
                return False
            if decision.commit is not None:
                await self._write(key, decision.commit, ttl=lockout_seconds)
            return decision.locked

    async def _record(self, key: str, *, label: str, max_attempts: int, lockout_seconds: float) -> bool:
        async with _redis_guard():
            now = time.time()
            record, locked_now = apply_failure(await self._read(key), label=label, max_attempts=max_attempts, lockout_seconds=lockout_seconds, now=now)
            # A counting (not yet locked) record still needs a TTL, or a single
            # stray failure would sit in Redis forever.
            ttl = lockout_seconds if record.is_locked else max(lockout_seconds, 3600.0)
            await self._write(key, record, ttl=ttl)
            return locked_now


class _redis_guard:
    """Translate every redis failure into LoginThrottleUnavailable."""

    async def __aenter__(self) -> _redis_guard:
        return self

    async def __aexit__(self, exc_type, exc, tb) -> bool:
        if exc is None:
            return False
        if isinstance(exc, LoginThrottleUnavailable):
            return False
        if isinstance(exc, _redis_errors()):
            raise LoginThrottleUnavailable("the login throttle store is unavailable") from exc
        return False


# ── Store selection ───────────────────────────────────────────────────────

_store: LoginThrottleStore | None = None
_store_signature: tuple[Any, ...] | None = None


def resolve_redis_url(local: Any) -> str:
    import os

    return local.lockout_store_redis_url or os.getenv("DEER_FLOW_LOGIN_THROTTLE_REDIS_URL") or os.getenv("DEER_FLOW_STREAM_BRIDGE_REDIS_URL") or os.getenv("REDIS_URL") or "redis://localhost:6379/0"


#: Used when no Gateway tenant namespace is available (unit tests and bare-app
#: contexts). A constructed Gateway always projects the prefix from its frozen
#: tenant identity, like every other Redis key family here, so two tenants on
#: one Redis cannot collide.
UNSCOPED_KEY_PREFIX = "deerflow:auth:login-throttle:v1"


def redis_key_prefix(tenant_namespace: Any | None) -> str:
    """Tenant-scoped key prefix for this key family."""
    if tenant_namespace is None:
        return UNSCOPED_KEY_PREFIX
    return f"{str(tenant_namespace.key_prefix).rstrip(':')}:auth:login-throttle:v1"


def get_login_throttle_store(local: Any, *, tenant_namespace: Any | None = None) -> LoginThrottleStore:
    """Return the process-wide store for the given ``auth.local`` config.

    Rebuilt when the backend selection changes, so an operator switching
    ``lockout_store`` does not need a restart to be taken at their word.
    """
    global _store, _store_signature
    if local.lockout_store == "redis":
        signature: tuple[Any, ...] = ("redis", resolve_redis_url(local), redis_key_prefix(tenant_namespace))
    else:
        signature = ("memory",)
    if _store is not None and _store_signature == signature:
        return _store
    if signature[0] == "redis":
        _store = RedisLoginThrottleStore(signature[1], key_prefix=signature[2])
    else:
        _store = MemoryLoginThrottleStore()
    _store_signature = signature
    return _store


def set_login_throttle_store(store: LoginThrottleStore | None, *, signature: tuple[Any, ...] | None = None) -> None:
    """Install a store explicitly (tests, and the shutdown path)."""
    global _store, _store_signature
    _store = store
    _store_signature = signature if store is not None else None


async def close_login_throttle_store() -> None:
    """Release the store's client at Gateway shutdown."""
    global _store, _store_signature
    store, _store, _store_signature = _store, None, None
    if store is not None:
        await store.close()


def views_as_payload(views: Iterable[LockoutView]) -> list[dict[str, Any]]:
    return [
        {
            "kind": view.kind,
            "label": view.label,
            "failures": view.failures,
            "locked_at": view.locked_at,
            "expires_at": view.expires_at,
        }
        for view in views
    ]
