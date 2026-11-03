"""In-memory run registry with optional persistent RunStore backing."""

from __future__ import annotations

import asyncio
import copy
import inspect
import logging
import socket
import sqlite3
import uuid
from collections.abc import AsyncIterator, Awaitable, Callable, Mapping
from contextlib import asynccontextmanager, contextmanager
from copy import deepcopy
from dataclasses import dataclass, field, replace
from datetime import UTC, datetime, timedelta
from enum import StrEnum
from typing import TYPE_CHECKING, Any

from sqlalchemy.exc import IntegrityError as SAIntegrityError

from deerflow.runtime.events.terminal import RecoveryTerminalPublisher
from deerflow.runtime.user_context import AUTO, _AutoSentinel, get_current_user, resolve_user_id
from deerflow.utils.file_io import await_drained
from deerflow.utils.time import is_lease_expired
from deerflow.utils.time import now_iso as _now_iso

from .capture_io import CaptureIOCleanupError, CaptureIOClosed, CaptureIOOwner, CaptureIOUnsupported, capture_io_scope, current_capture_io_owner, run_capture_io
from .inventory_finalization import InventoryCleanupOutcome, InventoryFinalizationAttempt, _current_inventory_authority, _inventory_authority, _new_attempt
from .output import (
    OUTPUT_CAPTURE_DISCONNECT_KEY,
    OUTPUT_CAPTURE_KEY,
    OUTPUT_EVIDENCE_KEY,
    OutputEvidenceError,
    OutputEvidenceScopeError,
    OutputReceipt,
    RunOutputSnapshot,
    UnavailableOutputReceipt,
    capture_from_kwargs,
    capture_intent_to_dict,
    receipt_from_kwargs,
    validate_receipt_status,
)
from .output_inventory import INVENTORY_EVIDENCE_KEY, EventInventoryEvidence, build_unavailable_inventory_evidence, inventory_evidence_from_kwargs, validate_inventory_pair
from .schemas import EVIDENCE_ORIGINS, DisconnectMode, RunStatus, ThreadOperationKind
from .store.base import (
    EditReplayVisibility,
    RunIdempotencyConflict,
    RunStore,
    StatusFinalization,
    canonical_run_created_at,
    normalize_run_created_at_iso,
    run_is_before_cursor,
    run_sort_key,
)
from .terminal import TERMINAL_STATUSES, TerminalRecoveryPolicy, diagnostic, exception_classification, failure_chain, persist_terminal

if TYPE_CHECKING:
    from deerflow.config.run_ownership_config import RunOwnershipConfig
    from deerflow.runtime.events.store.base import RunEventStore

logger = logging.getLogger(__name__)

ORPHAN_RECOVERY_STOP_REASON = "orphan_recovered"
GATEWAY_STOP_REASON = "gateway_stopped"
STARTUP_ORPHAN_RECOVERY_ERROR = "Gateway restarted before this run reached a durable final state."
LEASE_ORPHAN_RECOVERY_ERROR = "Run lease expired — owning worker is unreachable."

_RETRYABLE_SQLITE_MESSAGES = (
    "database is locked",
    "database table is locked",
    "database is busy",
)

_RETRYABLE_SQLITE_ERROR_CODES = {
    sqlite3.SQLITE_BUSY,
    sqlite3.SQLITE_LOCKED,
}

# Driver-native unique-constraint signals. These are stable across driver and
# SQLAlchemy versions — message text is not (SQLite says "UNIQUE constraint
# failed", Postgres says "duplicate key value violates unique constraint").
_UNIQUE_PGCODE = "23505"
_SQLITE_UNIQUE_ERRORCODE = sqlite3.SQLITE_CONSTRAINT_UNIQUE


def _generate_worker_id() -> str:
    """Generate a unique worker identifier: ``hostname:hex_uuid``."""
    return f"{socket.gethostname()}:{uuid.uuid4().hex}"


def _resolve_record_user_id(user_id: str | None) -> str | None:
    """Fill an omitted run owner from the ambient user, as the SQL store does.

    The SQL store stamps ``user_id=None`` with the request user, so a local
    record left at ``None`` disagrees with its own durable row: owner-scoped
    reads skip it and idempotent reuse rejects it as another user's run.
    Resolving here gives every store the same owner. Without a user in context
    the owner stays ``None``.
    """
    if user_id is not None:
        return user_id
    user = get_current_user()
    return str(user.id) if user is not None else None


def _new_run_kwargs(kwargs: dict | None, *, on_disconnect: DisconnectMode) -> dict:
    """A new admission may request capture, but cannot supply its result."""
    values = dict(kwargs or {})
    values.pop(OUTPUT_EVIDENCE_KEY, None)
    values.pop(INVENTORY_EVIDENCE_KEY, None)
    values.pop("output_inventory_profile", None)
    values.pop("inventory_profile", None)
    values.pop(OUTPUT_CAPTURE_DISCONNECT_KEY, None)
    intent = capture_from_kwargs(values)
    if intent is not None:
        values[OUTPUT_CAPTURE_KEY] = capture_intent_to_dict(intent)
        values[OUTPUT_CAPTURE_DISCONNECT_KEY] = DisconnectMode(on_disconnect).value
    return values


def _cursor_part(value: str | None) -> str | None:
    """Treat missing/blank cursor fields as absent so a one-sided empty string fails."""
    if value is None:
        return None
    stripped = value.strip()
    return stripped or None


def _is_unique_violation(exc: BaseException) -> bool:
    """Return True when *exc* (or its cause chain) is a unique-constraint violation.

    SQLAlchemy wraps the driver's IntegrityError; the wrapped driver exception is
    reachable via ``exc.orig`` (and ``__cause__`` / ``__context__``). Prefer
    driver-native signals — psycopg ``pgcode`` / ``sqlcode`` = "23505" and
    sqlite3 ``sqlite_errorcode`` = ``SQLITE_CONSTRAINT_UNIQUE`` — over message
    matching, then fall back to message substrings for cases where the driver
    exception isn't reachable through the chain.

    Message text drifts across drivers and locales (SQLite raises
    ``UNIQUE constraint failed: <table>.<index>``; Postgres raises
    ``duplicate key value violates unique constraint``), so the code/attribute
    checks are the load-bearing path.
    """
    pending: list[BaseException] = [exc]
    seen: set[int] = set()
    while pending:
        current = pending.pop()
        if id(current) in seen:
            continue
        seen.add(id(current))

        if getattr(current, "pgcode", None) == _UNIQUE_PGCODE:
            return True
        if getattr(current, "sqlcode", None) == _UNIQUE_PGCODE:
            return True
        if getattr(current, "sqlstate", None) == _UNIQUE_PGCODE:
            return True
        if getattr(current, "sqlite_errorcode", None) == _SQLITE_UNIQUE_ERRORCODE:
            return True

        # Message fallbacks are belt-and-suspenders for drivers whose
        # native code attribute isn't reachable through the chain. Gate on
        # an IntegrityError-typed node so an unrelated application
        # exception whose ``str()`` happens to contain "duplicate key" /
        # "unique" + "violat" (CHECK constraint message, validation error,
        # arbitrary subsystem string) cannot be misclassified as a unique
        # violation and silently surface as HTTP 409 instead of 500.
        if isinstance(current, (SAIntegrityError, sqlite3.IntegrityError)):
            message = str(current).lower()
            if "unique constraint failed" in message:
                return True
            if "unique" in message and "violat" in message:
                return True
            if "duplicate key" in message:
                return True

        for attr in ("orig", "__cause__", "__context__"):
            inner = getattr(current, attr, None)
            if isinstance(inner, BaseException):
                pending.append(inner)
    return False


def _is_retryable_persistence_error(exc: BaseException) -> bool:
    """Return True for transient SQLite persistence failures.

    SQLite lock contention normally surfaces through either sqlite3 exceptions
    or SQLAlchemy wrappers.  The short bounded retry here protects run status
    finalization from transient writer pressure without hiding permanent
    failures forever.
    """

    pending: list[BaseException] = [exc]
    seen: set[int] = set()
    while pending:
        current = pending.pop()
        if id(current) in seen:
            continue
        seen.add(id(current))

        message = str(current).lower()
        if any(fragment in message for fragment in _RETRYABLE_SQLITE_MESSAGES):
            return True
        if isinstance(current, (sqlite3.OperationalError, sqlite3.DatabaseError)):
            error_code = getattr(current, "sqlite_errorcode", None)
            if error_code in _RETRYABLE_SQLITE_ERROR_CODES:
                return True
        for chained in (getattr(current, "orig", None), current.__cause__, current.__context__):
            if isinstance(chained, BaseException):
                pending.append(chained)
    return False


@dataclass(frozen=True)
class PersistenceRetryPolicy:
    """Bounded retry policy for short run-store writes."""

    max_attempts: int = 5
    initial_delay: float = 0.05
    max_delay: float = 1.0
    backoff_factor: float = 2.0


class RunStartupPhase(StrEnum):
    """Host-only execution decision; never hydrated from a public/store row."""

    unknown = "unknown"
    unstarted = "unstarted"
    started = "started"
    closed = "closed"


class _AdmissionStage(StrEnum):
    constructed = "constructed"
    registered = "registered"
    reused = "reused"


@dataclass(frozen=True)
class PrestartOutcome:
    status: RunStatus
    reason: str
    error: str | None = None
    stop_reason: str | None = None
    confirmed: bool = False


class RunIdempotencyUnsupported(RuntimeError):
    """The configured store cannot safely persist the requested idempotency fields."""


class RunIdempotencyKeyMismatch(RuntimeError):
    """The idempotency key belongs to a run of another thread or user: a different request."""


def _store_accepts_idempotency_parameter(store: RunStore, parameter_name: str) -> bool:
    """Require an explicit keyword parameter or an opt-in owned by the method."""
    callable_ = store.create_thread_operation_atomic
    if getattr(callable_, "__func__", None) is RunStore.create_thread_operation_atomic:
        return False
    try:
        parameters = inspect.signature(callable_).parameters
    except (TypeError, ValueError):
        return False
    parameter = parameters.get(parameter_name)
    if parameter is not None and parameter.kind in (inspect.Parameter.POSITIONAL_OR_KEYWORD, inspect.Parameter.KEYWORD_ONLY):
        return True
    if not any(parameter.kind is inspect.Parameter.VAR_KEYWORD for parameter in parameters.values()):
        return False
    implementation_owner = next(
        (candidate for candidate in type(store).__mro__ if "create_thread_operation_atomic" in candidate.__dict__),
        None,
    )
    return bool(implementation_owner and implementation_owner.__dict__.get("supports_idempotency_request_kwargs", False))


def _store_supports_terminal_completion(store: RunStore) -> bool:
    """Require explicit strict keywords or an opt-in on the method's owner."""
    callable_ = store.update_run_completion
    if getattr(callable_, "__func__", None) is RunStore.update_run_completion:
        return False
    try:
        parameters = inspect.signature(callable_).parameters
    except (TypeError, ValueError):
        return False
    required = [parameters.get(name) for name in ("require_terminal", "owner_worker_id")]
    if all(parameter is not None and parameter.kind in (inspect.Parameter.POSITIONAL_OR_KEYWORD, inspect.Parameter.KEYWORD_ONLY) for parameter in required):
        return True
    if any(parameter is not None and parameter.kind is inspect.Parameter.POSITIONAL_ONLY for parameter in required):
        return False
    if not any(parameter.kind is inspect.Parameter.VAR_KEYWORD for parameter in parameters.values()):
        return False
    implementation_owner = next(
        (candidate for candidate in type(store).__mro__ if "update_run_completion" in candidate.__dict__),
        None,
    )
    return bool(implementation_owner and implementation_owner.__dict__.get("supports_terminal_completion_kwargs", False))


@dataclass(frozen=True)
class TerminalCommitProof:
    """Process-local proof of an acknowledged terminal transition."""

    status: RunStatus
    error: str | None
    stop_reason: str | None
    goal_verdict: dict[str, Any] | None


@dataclass
class RunRecord:
    """Mutable record for a single run."""

    run_id: str
    thread_id: str
    assistant_id: str | None
    status: RunStatus
    on_disconnect: DisconnectMode
    operation_kind: ThreadOperationKind = ThreadOperationKind.run
    multitask_strategy: str = "reject"
    metadata: dict = field(default_factory=dict)
    kwargs: dict = field(default_factory=dict)
    user_id: str | None = None
    created_at: str = ""
    updated_at: str = ""
    task: asyncio.Task | None = field(default=None, repr=False)
    # The worker's own finalization task: created by ``task``, awaited to
    # completion and never detached, so it holds the same host authority.
    finalization_task: asyncio.Task | None = field(default=None, repr=False)
    # Serializes startup if an admitted run is ever handed to more than one worker path.
    start_lock: asyncio.Lock = field(default_factory=asyncio.Lock, repr=False)
    abort_event: asyncio.Event = field(default_factory=asyncio.Event, repr=False)
    abort_action: str = "interrupt"
    error: str | None = None
    model_name: str | None = None
    store_only: bool = False
    total_input_tokens: int = 0
    total_output_tokens: int = 0
    total_tokens: int = 0
    llm_call_count: int = 0
    lead_agent_tokens: int = 0
    subagent_tokens: int = 0
    middleware_tokens: int = 0
    # Per-model token breakdown
    token_usage_by_model: dict[str, dict[str, int]] = field(default_factory=dict)
    message_count: int = 0
    last_ai_message: str | None = None
    first_human_message: str | None = None
    finalizing: bool = False
    owner_worker_id: str | None = None
    lease_expires_at: str | None = None
    # Process-local fencing signal. Once set, this worker must not perform
    # further durable run/thread finalization because its lease ownership is
    # either known to be lost or could not be confirmed before expiry.
    ownership_lost: bool = False
    terminal_lock: asyncio.Lock = field(default_factory=asyncio.Lock, repr=False)
    terminal_confirmed: bool = False
    terminal_write_unconfirmed: bool = False
    stop_reason: str | None = None
    goal_verdict: dict[str, Any] | None = None
    human_input_request: dict[str, Any] | None = None
    terminal_status_persisted: bool = True
    # Process-local finalization barrier for a worker-installed scheduled goal.
    scheduled_goal_cleanup_pending: bool = False
    # Process-local renewal barrier: the worker stages its terminal status in
    # memory and commits it only at the end of finalization, so the durable row
    # stays active and its lease must keep renewing until that commit.
    terminal_commit_pending: bool = False
    # An interrupted ACK does not authorize completion of an unfinished rollback.
    rollback_finalization_pending: bool = False
    terminal_commit_proof: TerminalCommitProof | None = field(default=None, repr=False)
    terminal_status_revision: int = field(default=0, repr=False)
    idempotency_key: str | None = None
    idempotency_request: dict[str, Any] | None = None
    # True only on the caller that recovered an existing idempotent admission;
    # that caller must not attach a second worker to the durable run.
    idempotency_reused: bool = False
    startup_phase: RunStartupPhase = field(default=RunStartupPhase.unknown, repr=False)
    prestart_outcome: PrestartOutcome | None = field(default=None, repr=False)
    evidence_origin: str = "unknown"
    evidence_agent_id: str | None = None


class RunStartOutcome(StrEnum):
    """Result of the pending-to-running startup barrier."""

    started = "started"
    cancelled = "cancelled"


@dataclass
class _CaptureLifetime:
    owner: CaptureIOOwner
    record: RunRecord | None = None
    controls: dict[asyncio.Task, int] = field(default_factory=dict)
    finished: bool = False
    inventory_attempts: list[InventoryFinalizationAttempt] = field(default_factory=list)


@dataclass(frozen=True)
class _ReservationScope:
    run_id: str
    user_id: str
    thread_id: str
    owner_worker_id: str
    change_seq: int


class _CheckpointReservationRelease:
    """One exact release continuation; never admits a new data-plane call."""

    def __init__(self, store: RunStore):
        self.store = store
        self.scope: _ReservationScope | None = None
        self.ready = asyncio.Event()
        self.uncertain = False
        self.task: asyncio.Task | None = None

    def retire(self) -> None:
        # The owner starts drain only after admitted calls have settled.
        pass

    async def drain(self) -> None:
        await self.ready.wait()
        if self.uncertain:
            raise CaptureIOCleanupError() from None
        if self.scope is None:
            return
        if self.task is None:
            scope = self.scope

            async def release():
                try:
                    result = await self.store.release_owned_checkpoint_reservation(scope.run_id, user_id=scope.user_id, thread_id=scope.thread_id, owner_worker_id=scope.owner_worker_id, expected_change_seq=scope.change_seq)
                except BaseException:
                    raise CaptureIOCleanupError() from None
                if result is not True:
                    raise CaptureIOCleanupError() from None

            self.task = asyncio.create_task(release(), name="capture-checkpoint-reservation-release")
        await await_drained(self.task)


class RunStartupError(RuntimeError):
    """Raised when durable startup cannot be resolved safely."""


OrphanRecoveryCallback = Callable[[list[RunRecord]], Awaitable[None]]


class RunManager:
    """In-memory run registry with optional persistent RunStore backing.

    All mutations are protected by an asyncio lock. When a ``store`` is
    provided, serializable metadata is also persisted to the store so
    that run history survives process restarts.
    """

    def __init__(
        self,
        store: RunStore | None = None,
        *,
        persistence_retry_policy: PersistenceRetryPolicy | None = None,
        terminal_recovery_policy: TerminalRecoveryPolicy | None = None,
        worker_id: str | None = None,
        run_ownership_config: RunOwnershipConfig | None = None,
        event_store: RunEventStore | None = None,
        recovery_terminal_publisher: RecoveryTerminalPublisher | None = None,
        on_orphans_recovered: OrphanRecoveryCallback | None = None,
    ) -> None:
        self._runs: dict[str, RunRecord] = {}
        # Secondary index: thread_id -> insertion-ordered run_id set (a dict is
        # used as an ordered set), maintained in lockstep with ``_runs`` so
        # per-thread queries avoid O(total in-memory runs) full scans while
        # preserving ``_runs`` iteration order (see ``_thread_records_locked``).
        self._runs_by_thread: dict[str, dict[str, None]] = {}
        self._lock = asyncio.Lock()
        self._store = store
        self._persistence_retry_policy = persistence_retry_policy or PersistenceRetryPolicy()
        self._terminal_recovery_policy = terminal_recovery_policy or TerminalRecoveryPolicy()
        self._worker_id = worker_id or _generate_worker_id()
        self._run_ownership_config = run_ownership_config
        self._event_store = event_store
        self._recovery_terminal_publisher = recovery_terminal_publisher
        if self._recovery_terminal_publisher is None and event_store is not None and store is not None:
            self._recovery_terminal_publisher = RecoveryTerminalPublisher(event_store, store)
        self._on_orphans_recovered = on_orphans_recovered
        self._heartbeat_task: asyncio.Task | None = None
        self._heartbeat_stop: asyncio.Event | None = None
        self._orphan_recovery_task: asyncio.Task[None] | None = None
        self._terminal_attempts: dict[str, set[asyncio.Future[bool]]] = {}
        self._lease_checks: dict[str, asyncio.Task[None]] = {}
        self._lease_operations: dict[str, asyncio.Task] = {}
        self._lease_deadlines: dict[str, asyncio.TimerHandle] = {}
        # Runtime capabilities stay outside RunRecord and its projections.
        self._capture_lifetimes: dict[CaptureIOOwner, _CaptureLifetime] = {}
        self._capture_records: dict[int, tuple[RunRecord, _CaptureLifetime]] = {}
        self._capture_admissions_sealed = False
        self._capture_shutdown_task: asyncio.Task | None = None

    def seal_capture_io_admissions(self) -> None:
        """Close new provisional lifetimes without waiting for the SQL lock."""
        self._capture_admissions_sealed = True

    def _new_capture_lifetime(self) -> _CaptureLifetime:
        if self._capture_admissions_sealed:
            raise CaptureIOClosed() from None
        lifetime = _CaptureLifetime(CaptureIOOwner())
        self._capture_lifetimes[lifetime.owner] = lifetime
        return lifetime

    def _capture_lifetime(self, record: RunRecord) -> _CaptureLifetime | None:
        saved = self._capture_records.get(id(record))
        return saved[1] if saved is not None and saved[0] is record else None

    def _associate_capture(self, lifetime: _CaptureLifetime, record: RunRecord) -> None:
        saved = self._capture_records.get(id(record))
        if saved is not None and (saved[0] is not record or saved[1] is not lifetime):
            raise CaptureIOUnsupported() from None
        lifetime.record = record
        self._capture_records[id(record)] = (record, lifetime)

    def get_capture_io_owner(self, record: RunRecord) -> CaptureIOOwner | None:
        """Return only this original record's existing host association."""
        if not isinstance(record, RunRecord):
            raise CaptureIOUnsupported() from None
        lifetime = self._capture_lifetime(record)
        if lifetime is not None:
            return lifetime.owner
        if capture_from_kwargs(record.kwargs) is not None:
            raise CaptureIOUnsupported() from None
        return None

    @contextmanager
    def _capture_control(self, record: RunRecord | None = None, *, lifetime: _CaptureLifetime | None = None):
        if lifetime is None and record is not None:
            lifetime = self._capture_lifetime(record)
            if lifetime is None and self._captures_output(record):
                raise CaptureIOUnsupported() from None
        if lifetime is None:
            yield
            return
        lifetime.owner.check_open()
        task = asyncio.current_task()
        if task is None:
            raise CaptureIOUnsupported() from None
        lifetime.controls[task] = lifetime.controls.get(task, 0) + 1
        try:
            with capture_io_scope(lifetime.owner):
                yield
        finally:
            count = lifetime.controls[task] - 1
            if count:
                lifetime.controls[task] = count
            else:
                lifetime.controls.pop(task)

    async def _finish_capture_lifetime(self, lifetime: _CaptureLifetime, *, discard: bool = False) -> None:
        lifetime.owner.retire()
        await lifetime.owner.drain()
        lifetime.finished = True
        if discard or lifetime.record is None:
            if lifetime.record is not None:
                saved = self._capture_records.get(id(lifetime.record))
                if saved is not None and saved[1] is lifetime:
                    self._capture_records.pop(id(lifetime.record))
            self._capture_lifetimes.pop(lifetime.owner, None)
        else:
            self._discard_evicted_capture(lifetime)

    def _discard_evicted_capture(self, lifetime: _CaptureLifetime) -> None:
        """Release a completed confirmed association only after normal eviction."""
        record = lifetime.record
        if record is None or self._runs.get(record.run_id) is record or not lifetime.finished or not record.terminal_confirmed or record.ownership_lost or record.terminal_write_unconfirmed or record.status.value not in TERMINAL_STATUSES:
            return
        try:
            saved = receipt_from_kwargs(record.kwargs)
            if saved is None or (saved.owner_user_id, saved.thread_id, saved.run_id) != (record.user_id, record.thread_id, record.run_id):
                return
            validate_receipt_status(saved, record.status)
        except OutputEvidenceError:
            return
        association = self._capture_records.get(id(record))
        if association is not None and association[0] is record and association[1] is lifetime:
            self._capture_records.pop(id(record))
            self._capture_lifetimes.pop(lifetime.owner, None)

    async def finish_capture_io(self, record: RunRecord) -> None:
        """Retire after the worker; retain the original closed association."""
        lifetime = self._capture_lifetime(record)
        if lifetime is not None:
            await await_drained(self._finish_capture_lifetime(lifetime))

    @asynccontextmanager
    async def _maintenance_control(self):
        """Track finite host maintenance without adopting any peer record."""
        if current_capture_io_owner() is not None or not self.supports_output_receipts:
            yield
            return
        lifetime = self._new_capture_lifetime()
        try:
            with self._capture_control(lifetime=lifetime):
                yield
        finally:
            await await_drained(self._finish_capture_lifetime(lifetime, discard=True))

    async def _capture_retry_sleep(self, seconds: float) -> None:
        owner = current_capture_io_owner()
        if owner is None:
            await asyncio.sleep(seconds)
            return
        owner.check_open()
        sleeping = asyncio.create_task(asyncio.sleep(seconds))
        retired = asyncio.create_task(owner.wait_retired())
        try:
            await asyncio.wait({sleeping, retired}, return_when=asyncio.FIRST_COMPLETED)
            owner.check_open()
        finally:
            sleeping.cancel()
            retired.cancel()
            await asyncio.gather(sleeping, retired, return_exceptions=True)

    def _index_run_locked(self, record: RunRecord) -> None:
        """Register *record* in the thread index. Caller must hold ``self._lock``."""
        self._runs_by_thread.setdefault(record.thread_id, {})[record.run_id] = None

    def _unindex_run_locked(self, run_id: str, thread_id: str) -> None:
        """Drop *run_id* from the thread index. Caller must hold ``self._lock``."""
        self._clear_lease_deadline(run_id)
        bucket = self._runs_by_thread.get(thread_id)
        if bucket is not None:
            bucket.pop(run_id, None)
            if not bucket:
                self._runs_by_thread.pop(thread_id, None)

    def _thread_records_locked(self, thread_id: str) -> list[RunRecord]:
        """Return live in-memory records for *thread_id*. Caller must hold ``self._lock``.

        Uses the ``_runs_by_thread`` index for O(runs-in-thread) lookup instead of
        scanning every in-memory run. Correctness rests on the index and ``_runs``
        being mutated in lockstep under ``self._lock`` (no ``await`` between the two
        writes), so any holder of the lock sees them agree. The ``self._runs.get``
        filter is defense-in-depth, not reconciliation: it drops a stale id still in
        the index but already gone from ``_runs``, yet it cannot recover a run that is
        in ``_runs`` but missing from the index (such a run would be silently
        omitted). It guards only that one direction, should a future refactor ever
        break the lockstep invariant.
        """
        run_ids = self._runs_by_thread.get(thread_id)
        if not run_ids:
            return []
        return [record for run_id in run_ids if (record := self._runs.get(run_id)) is not None]

    def task_run_ids(self, metadata_key: str, value: str) -> frozenset[str]:
        """Read active local tasks synchronously on this manager's owning loop."""
        return frozenset(run_id for run_id, record in self._runs.items() if record.task is not None and not record.task.done() and metadata_key in record.metadata and record.metadata[metadata_key] == value)

    def task_runs(self, metadata_key: str) -> dict[str, dict]:
        """Snapshot active local tasks in admission order, without store IO."""
        return {
            run_id: {"value": record.metadata[metadata_key], "thread_id": record.thread_id, "user_id": record.user_id, "created_at": record.created_at, "metadata": dict(record.metadata)}
            for run_id, record in self._runs.items()
            if record.task is not None and not record.task.done() and metadata_key in record.metadata
        }

    @staticmethod
    def _store_put_payload(record: RunRecord, *, error: str | None = None, stop_reason: str | None = None) -> dict[str, Any]:
        payload = {
            "thread_id": record.thread_id,
            "assistant_id": record.assistant_id,
            "status": record.status.value,
            "operation_kind": record.operation_kind.value,
            "multitask_strategy": record.multitask_strategy,
            "metadata": record.metadata or {},
            "kwargs": record.kwargs or {},
            "error": error if error is not None else record.error,
            "created_at": record.created_at,
            "model_name": record.model_name,
            "owner_worker_id": record.owner_worker_id,
            "lease_expires_at": record.lease_expires_at,
            "idempotency_key": record.idempotency_key,
            "goal_verdict": record.goal_verdict,
        }
        if record.evidence_origin != "unknown":
            payload["evidence_origin"] = record.evidence_origin
        if record.evidence_agent_id is not None:
            payload["evidence_agent_id"] = record.evidence_agent_id
        if record.idempotency_request is not None:
            payload["idempotency_request"] = record.idempotency_request
        if record.user_id is not None:
            payload["user_id"] = record.user_id
        if record.stop_reason is not None:
            payload["stop_reason"] = record.stop_reason
        return payload

    @staticmethod
    def _terminal_row_matches_payload(
        row: dict[str, Any],
        payload: dict[str, Any],
    ) -> bool:
        return all(
            row.get(key) == payload.get(key)
            for key in (
                "thread_id",
                "user_id",
                "owner_worker_id",
                "status",
                "error",
                "stop_reason",
                "goal_verdict",
            )
        )

    async def _call_store_with_retry(
        self,
        operation_name: str,
        run_id: str,
        operation: Callable[[], Awaitable[Any]],
    ) -> Any:
        """Run a short store operation with bounded retries for SQLite pressure."""
        policy = self._persistence_retry_policy
        attempt = 1
        delay = policy.initial_delay
        while True:
            try:
                return await run_capture_io(operation)
            except CaptureIOClosed:
                raise
            except Exception as exc:
                retryable = _is_retryable_persistence_error(exc)
                if attempt >= policy.max_attempts or not retryable:
                    raise
                logger.warning(
                    "Transient persistence failure during %s for run %s (attempt %d/%d); retrying",
                    operation_name,
                    run_id,
                    attempt,
                    policy.max_attempts,
                )
                if delay > 0:
                    await self._capture_retry_sleep(delay)
                delay = min(policy.max_delay, delay * policy.backoff_factor if delay else policy.initial_delay)
                attempt += 1

    async def _persist_snapshot_to_store(self, run_id: str, payload: dict[str, Any]) -> bool:
        """Best-effort persist a previously captured run snapshot."""
        if self._store is None:
            return True
        try:
            await self._call_store_with_retry(
                "put",
                run_id,
                lambda: self._store.put(run_id, **payload),
            )
            return True
        except CaptureIOClosed:
            raise
        except Exception:
            logger.warning("Failed to persist run %s to store", run_id, exc_info=True)
            return False

    async def _persist_new_run_to_store(self, record: RunRecord) -> None:
        """Persist a newly created run record to the backing store.

        Initial run creation is part of the run visibility boundary: callers
        should not observe a run in memory unless its backing store row exists.
        Unlike follow-up status/model updates, failures are propagated so the
        caller can treat creation as failed. Rollback is the caller's
        responsibility after inserting the record into ``_runs``.
        """
        if self._store is None:
            return
        await self._call_store_with_retry(
            "put",
            record.run_id,
            lambda: self._store.put(record.run_id, **self._store_put_payload(record)),
        )

    async def _persist_to_store(self, record: RunRecord, *, error: str | None = None) -> bool:
        """Best-effort persist run record to backing store."""
        return await self._persist_snapshot_to_store(
            record.run_id,
            self._store_put_payload(record, error=error),
        )

    @staticmethod
    def _terminal_outcome(record: RunRecord, status: RunStatus | None = None, *, error: str | None = None, stop_reason: str | None = None, goal_verdict: dict[str, Any] | None = None) -> TerminalCommitProof:
        return TerminalCommitProof(
            status or record.status,
            error if error is not None else record.error,
            stop_reason if stop_reason is not None else record.stop_reason,
            copy.deepcopy(goal_verdict if goal_verdict is not None else record.goal_verdict),
        )

    def _has_terminal_ack(self, record: RunRecord, outcome: TerminalCommitProof | None = None) -> bool:
        return not record.ownership_lost and (outcome or self._terminal_outcome(record)) == record.terminal_commit_proof

    def _has_final_terminal_ack(self, record: RunRecord) -> bool:
        return not record.rollback_finalization_pending and self._has_terminal_ack(record)

    def terminal_event_status(self, record: RunRecord) -> str | None:
        """Return an attributable final outcome for this local worker only."""
        if self._runs.get(record.run_id) is not record or record.store_only or record.owner_worker_id != self._worker_id or record.status in (RunStatus.pending, RunStatus.running) or not self._has_final_terminal_ack(record):
            return None
        return record.status.value

    def can_release_terminal_barrier(self, record: RunRecord) -> bool:
        """Keep durable ownership barriers through rollback's final error ACK."""
        return not self.heartbeat_enabled or self._store is None or record.ownership_lost or self._has_final_terminal_ack(record)

    def _begin_terminal_attempt(self, record: RunRecord) -> asyncio.Future[bool]:
        future = asyncio.get_running_loop().create_future()
        self._terminal_attempts.setdefault(record.run_id, set()).add(future)
        record.terminal_commit_pending = True
        return future

    def _end_terminal_attempt(self, record: RunRecord, future: asyncio.Future[bool], committed: bool) -> None:
        future.set_result(committed)
        attempts = self._terminal_attempts[record.run_id]
        attempts.remove(future)
        if not attempts:
            self._terminal_attempts.pop(record.run_id)

    def _acknowledge_terminal(self, record: RunRecord, outcome: TerminalCommitProof, staged_revision: int) -> None:
        if self._runs.get(record.run_id) is not record or record.ownership_lost:
            return
        # Publish the acknowledged outcome without an await. With a backing
        # store, an older interrupted ACK cannot supersede a newer error.
        # Storeless transitions are synchronous and retain local mutability.
        previous = record.terminal_commit_proof
        if self._store is None or previous is None or previous.status == RunStatus.interrupted or outcome.status != RunStatus.interrupted:
            record.terminal_commit_proof = outcome
        authoritative = record.terminal_commit_proof
        if authoritative.status != RunStatus.interrupted or record.terminal_status_revision == staged_revision:
            record.status = authoritative.status
            record.error = authoritative.error
            record.stop_reason = authoritative.stop_reason
            record.goal_verdict = copy.deepcopy(authoritative.goal_verdict)
        if authoritative.status == RunStatus.error:
            record.rollback_finalization_pending = False
        record.terminal_status_persisted = self._has_final_terminal_ack(record)
        if record.terminal_status_persisted:
            record.terminal_commit_pending = False
            self._clear_lease_deadline(record.run_id)
        record.updated_at = _now_iso()

    async def _terminal_operation(self, record: RunRecord, outcome: TerminalCommitProof, operation: Callable[[], Awaitable[Any]], *, cas: bool = False) -> Any:
        future = self._begin_terminal_attempt(record)
        staged_revision = record.terminal_status_revision
        committed = False
        try:
            result = await operation()
            committed = result.finalized is True if cas else result is True
            if committed:
                self._acknowledge_terminal(record, outcome, staged_revision)
            return result
        finally:
            self._end_terminal_attempt(record, future, committed)

    async def _atomic_replacement(self, candidates: list[RunRecord], action: str, operation: Callable[[], Awaitable[Any]]) -> Any:
        """Attribute returned claims while the caller holds the admission lock."""
        attempts = [(r, r.terminal_commit_pending, self._begin_terminal_attempt(r), r.terminal_status_revision) for r in candidates]
        claimed: dict[str, dict] = {}
        try:
            result = await operation()
            claimed = {row["run_id"]: row for row in result[1]}
            for record, _, _, revision in attempts:
                row = claimed.get(record.run_id)
                if row is None or row.get("status") != "interrupted" or row.get("owner_worker_id") != self._worker_id or row.get("thread_id") != record.thread_id:
                    continue
                if record.ownership_lost:
                    continue
                # The error writer may have ACKed while this older admission
                # was awaiting its response. Do not restart cancellation after
                # that final outcome, even though this claim also committed.
                if record.terminal_commit_proof is not None and record.terminal_commit_proof.status != RunStatus.interrupted:
                    continue
                task_active = record.task is not None and not record.task.done()
                record.rollback_finalization_pending = action == "rollback" and task_active
                outcome = TerminalCommitProof(RunStatus.interrupted, row.get("error"), row.get("stop_reason"), copy.deepcopy(row.get("goal_verdict")))
                self._acknowledge_terminal(record, outcome, revision)
                record.abort_action = action
                record.abort_event.set()
                if task_active and not record.finalizing:
                    record.task.cancel()
                record.finalizing = task_active
            return result
        finally:
            for record, was_pending, future, revision in attempts:
                row = claimed.get(record.run_id)
                committed = row is not None and row.get("status") == "interrupted" and row.get("owner_worker_id") == self._worker_id and row.get("thread_id") == record.thread_id
                self._end_terminal_attempt(record, future, committed)
                if not committed and not self._terminal_attempts.get(record.run_id) and record.terminal_status_revision == revision and not record.ownership_lost:
                    record.terminal_commit_pending = was_pending

    async def _fence_unconfirmed_terminal(self, record: RunRecord) -> None:
        deadline = self._parse_lease_deadline(record.lease_expires_at)
        remaining = max(0.0, (deadline - datetime.now(UTC)).total_seconds()) if deadline else 0.0
        if await self._release_confirmed_terminal_barrier(record, timeout=remaining):
            return
        await self._mark_ownership_lost(record, reason="This worker's terminal write was rejected or could not be confirmed.", require_active=False)

    async def _persist_status(self, record: RunRecord, status: RunStatus, *, error: str | None = None, stop_reason: str | None = None) -> bool:
        """Best-effort persist a status transition to the backing store."""
        if record.ownership_lost:
            logger.warning(
                "Skipped status update to %s for run %s after lease ownership was lost",
                status.value,
                record.run_id,
            )
            return False
        terminal = status not in (RunStatus.pending, RunStatus.running)
        outcome = self._terminal_outcome(record, status, error=error, stop_reason=stop_reason)
        staged_revision = record.terminal_status_revision
        if self._store is None:
            if terminal and not record.store_only:
                self._acknowledge_terminal(record, outcome, staged_revision)
            return True
        if status.value in TERMINAL_STATUSES and getattr(self._store, "supports_terminal_recovery", False) is True:
            result = await persist_terminal(self, record, status, error=error, stop_reason=stop_reason)
            return result.finalized
        if self.heartbeat_enabled and not record.store_only:
            if terminal and self._has_terminal_ack(record, outcome):
                return True

            async def operation():
                return await self._call_store_with_retry(
                    "update_status",
                    record.run_id,
                    lambda: self._store.update_status(
                        record.run_id,
                        status.value,
                        error=outcome.error,
                        stop_reason=outcome.stop_reason,
                        owner_worker_id=self._worker_id,
                        **({"goal_verdict": copy.deepcopy(outcome.goal_verdict)} if outcome.goal_verdict is not None else {}),
                    ),
                )

            try:
                updated = await self._terminal_operation(record, outcome, operation) if terminal else await operation()
                if updated is True and not record.ownership_lost:
                    return True
            except asyncio.CancelledError:
                if terminal:
                    await self._fence_unconfirmed_terminal(record)
                raise
            except Exception:
                logger.warning("Unable to confirm owned status write for run %s", record.run_id, exc_info=True)
            if terminal:
                await self._fence_unconfirmed_terminal(record)
            return False
        row_recovery_payload = self._store_put_payload(record, error=error, stop_reason=stop_reason)
        row_recovery_payload["status"] = status.value
        try:
            updated = await self._call_store_with_retry(
                "update_status",
                record.run_id,
                lambda: self._store.update_status(record.run_id, status.value, error=error, stop_reason=stop_reason, **({"goal_verdict": record.goal_verdict} if record.goal_verdict is not None else {})),
            )
            # Preserve legacy single-worker recovery below, but only an explicit
            # successful write can authorize a new terminal event. A matching row
            # or a legacy None return does not attribute the commit to us.
            if terminal and updated is True and not record.store_only:
                self._acknowledge_terminal(record, outcome, staged_revision)
            if updated is False:
                # ``update_status`` is now guarded by ``status IN ('pending','running')``.
                # False can mean either:
                #   (a) the row was never persisted (initial ``put()`` failed) → recreate.
                #   (b) the row is terminal — either a peer takeover (``error``)
                #       or a local cancel/completion race (``interrupted`` /
                #       ``success``). The log severity branches on which.
                existing = await run_capture_io(lambda: self._store.get(record.run_id, user_id=record.user_id))
                if existing is not None:
                    existing_status = existing.get("status")
                    if existing_status == status.value and self._terminal_row_matches_payload(
                        existing,
                        row_recovery_payload,
                    ):
                        logger.info(
                            "Run %s status update to %s was already persisted",
                            record.run_id,
                            status.value,
                        )
                        return True
                    if existing_status == "error":
                        logger.warning(
                            "Run %s status update to %s skipped: store row already at error (peer takeover)",
                            record.run_id,
                            status.value,
                        )
                        if self.heartbeat_enabled and not record.store_only:
                            await self._mark_ownership_lost(
                                record,
                                reason="A peer terminalized the run before this worker could persist its outcome.",
                                require_active=False,
                            )
                    else:
                        logger.info(
                            "Run %s status update to %s skipped: store row already at %s (local cancel/completion race)",
                            record.run_id,
                            status.value,
                            existing_status,
                        )
                    return False
                return await self._persist_snapshot_to_store(record.run_id, row_recovery_payload)
            return True
        except CaptureIOClosed:
            raise
        except Exception as exc:
            diagnostic(record, status, outcome="unknown", exception_class=exception_classification(exc))
            return False

    @staticmethod
    def _record_from_store(row: dict[str, Any]) -> RunRecord:
        """Build a read-only runtime record from a serialized store row.

        The result is a detached ``store_only`` snapshot. Never register it in
        ``_runs``: only the owning worker's task lifecycle updates and removes
        local records, so a registered snapshot would never leave.

        Missing status/disconnect values retain the ordinary pending/cancel
        defaults. SQL capture admissions persist their server-owned disconnect
        policy in kwargs because that SQL schema has no on_disconnect column.
        """
        kwargs = row.get("kwargs") or {}
        disconnect = row.get("on_disconnect") or DisconnectMode.cancel.value
        if isinstance(kwargs, Mapping) and OUTPUT_CAPTURE_KEY in kwargs and OUTPUT_CAPTURE_DISCONNECT_KEY in kwargs:
            capture_from_kwargs(kwargs)
            disconnect = kwargs[OUTPUT_CAPTURE_DISCONNECT_KEY]
            if type(disconnect) is not str:
                raise OutputEvidenceError()
        return RunRecord(
            run_id=row["run_id"],
            thread_id=row["thread_id"],
            assistant_id=row.get("assistant_id"),
            status=RunStatus(row.get("status") or RunStatus.pending.value),
            on_disconnect=DisconnectMode(disconnect),
            operation_kind=ThreadOperationKind(row.get("operation_kind") or ThreadOperationKind.run.value),
            multitask_strategy=row.get("multitask_strategy") or "reject",
            metadata=row.get("metadata") or {},
            kwargs=row.get("kwargs") or {},
            created_at=row.get("created_at") or "",
            updated_at=row.get("updated_at") or "",
            user_id=row.get("user_id"),
            error=row.get("error"),
            model_name=row.get("model_name"),
            store_only=True,
            total_input_tokens=row.get("total_input_tokens") or 0,
            total_output_tokens=row.get("total_output_tokens") or 0,
            total_tokens=row.get("total_tokens") or 0,
            llm_call_count=row.get("llm_call_count") or 0,
            lead_agent_tokens=row.get("lead_agent_tokens") or 0,
            subagent_tokens=row.get("subagent_tokens") or 0,
            middleware_tokens=row.get("middleware_tokens") or 0,
            token_usage_by_model=row.get("token_usage_by_model") or {},
            message_count=row.get("message_count") or 0,
            last_ai_message=row.get("last_ai_message"),
            first_human_message=row.get("first_human_message"),
            owner_worker_id=row.get("owner_worker_id"),
            evidence_origin=row.get("evidence_origin") or "unknown",
            evidence_agent_id=row.get("evidence_agent_id"),
            lease_expires_at=row.get("lease_expires_at"),
            stop_reason=row.get("stop_reason"),
            goal_verdict=row.get("goal_verdict"),
            idempotency_key=row.get("idempotency_key"),
            idempotency_request=row.get("idempotency_request"),
        )

    async def update_run_completion(self, run_id: str, **kwargs) -> None:
        """Persist token usage and completion data to the backing store."""
        kwargs = {key: value for key, value in kwargs.items() if key not in {"startup_phase", "prestart_outcome"}}
        row_recovery_payload: dict[str, Any] | None = None
        record: RunRecord | None = None
        async with self._lock:
            record = self._runs.get(run_id)
            if record is not None and record.ownership_lost:
                logger.warning("Skipped completion persistence for run %s after lease ownership was lost", run_id)
                return
            if self.heartbeat_enabled and self._store is not None:
                if record is None or record.store_only or not self._has_final_terminal_ack(record) or kwargs.get("status") != record.status.value:
                    logger.warning("Skipped completion data without an attributable terminal ack for run %s", run_id)
                    return
                if any(kwargs.get(key) is not None and kwargs[key] != getattr(record, key) for key in ("error", "stop_reason", "goal_verdict")):
                    logger.warning("Skipped completion data that changes the acknowledged outcome for run %s", run_id)
                    return
            if record is not None:
                if record.goal_verdict is not None:
                    # This fallback can be the first successful terminal write
                    # after a transient status-store failure. Carry the verdict
                    # in that same update rather than leaving a success gap.
                    kwargs.setdefault("goal_verdict", copy.deepcopy(record.goal_verdict))
                for key, value in kwargs.items():
                    if key == "status":
                        continue
                    if hasattr(record, key) and value is not None:
                        setattr(record, key, value)
                record.updated_at = _now_iso()
                row_recovery_payload = self._store_put_payload(record, error=kwargs.get("error"))
        if self._store is None:
            return
        if getattr(self._store, "supports_terminal_recovery", False) is True:
            if record is None or record.store_only or not record.terminal_confirmed:
                return
            kwargs["expected_owner_worker_id"] = self._worker_id
        elif self.heartbeat_enabled:
            if record is None or record.store_only or not self._has_final_terminal_ack(record) or kwargs.get("status") != record.status.value:
                logger.warning("Skipped completion data without an attributable terminal ack for run %s", run_id)
                return
            if not _store_supports_terminal_completion(self._store):
                logger.warning(
                    "Skipped terminal completion metadata for run %s: Store %s must explicitly support require_terminal and owner_worker_id or declare supports_terminal_completion_kwargs on the class defining update_run_completion",
                    run_id,
                    type(self._store).__name__,
                )
                return
            try:
                updated = await self._call_store_with_retry(
                    "update_run_completion",
                    run_id,
                    lambda: self._store.update_run_completion(run_id, require_terminal=True, owner_worker_id=self._worker_id, **kwargs),
                )
                if updated is not True:
                    logger.warning("Terminal completion metadata was not updated for run %s", run_id)
            except Exception:
                logger.warning("Failed to persist terminal completion metadata for run %s", run_id, exc_info=True)
            return
        try:
            updated = await self._call_store_with_retry(
                "update_run_completion",
                run_id,
                lambda: self._store.update_run_completion(run_id, **kwargs),
            )
            if updated is False:
                if getattr(self._store, "supports_terminal_recovery", False) is True:
                    # Never recreate or overwrite a different owner's row.
                    await self._mark_ownership_lost(record, reason="The durable store rejected owned completion metadata.", require_active=False)
                    return
                existing = await run_capture_io(lambda: self._store.get(run_id))
                requested_status = kwargs.get("status")
                if existing is not None and existing.get("status") == requested_status:
                    if record is not None and requested_status == record.status.value:
                        record.terminal_status_persisted = True
                if existing is not None and existing.get("status") != requested_status:
                    existing_status = existing.get("status")
                    logger.warning(
                        "Run completion update for %s skipped because store row is already at %s",
                        run_id,
                        existing_status,
                    )
                    if existing_status == "error" and record is not None and self.heartbeat_enabled:
                        await self._mark_ownership_lost(
                            record,
                            reason="A peer terminalized the run before completion data was persisted.",
                            require_active=False,
                        )
                    return
                if row_recovery_payload is None:
                    logger.warning("Failed to recreate missing run %s for completion persistence", run_id)
                    return
                if not await self._persist_snapshot_to_store(run_id, row_recovery_payload):
                    return
                recovered = await self._call_store_with_retry(
                    "update_run_completion",
                    run_id,
                    lambda: self._store.update_run_completion(run_id, **kwargs),
                )
                if recovered is False:
                    logger.warning("Run completion update for %s affected no rows after row recreation", run_id)
                    return
            if record is not None and kwargs.get("status") in (None, record.status.value):
                record.terminal_status_persisted = True
        except CaptureIOClosed:
            raise
        except Exception as exc:
            logger.warning("Run persistence failed run_id=%s operation=completion reason=run_metadata_write_failed exception_class=%s", run_id, exception_classification(exc))

    async def update_run_progress(self, run_id: str, **kwargs) -> None:
        """Persist a running token/message snapshot without changing status."""
        kwargs = {key: value for key, value in kwargs.items() if key not in {"startup_phase", "prestart_outcome"}}
        should_persist = True
        async with self._lock:
            record = self._runs.get(run_id)
            if record is not None:
                should_persist = record.status == RunStatus.running and not record.ownership_lost
            if record is not None and should_persist:
                for key, value in kwargs.items():
                    if hasattr(record, key) and value is not None:
                        setattr(record, key, value)
                record.updated_at = _now_iso()
        if should_persist and self._store is not None:
            try:
                await run_capture_io(lambda: self._store.update_run_progress(run_id, **kwargs))
            except CaptureIOClosed:
                raise
            except Exception as exc:
                logger.warning("Run persistence failed run_id=%s operation=progress reason=run_metadata_write_failed exception_class=%s", run_id, exception_classification(exc))

    async def update_finalizing_progress(self, run_id: str, **kwargs) -> None:
        """Persist final fields while the durable row is deliberately active."""
        kwargs = {key: value for key, value in kwargs.items() if key not in {"startup_phase", "prestart_outcome"}}
        should_persist = False
        async with self._lock:
            record = self._runs.get(run_id)
            if record is not None and not record.ownership_lost:
                should_persist = record.status not in (RunStatus.pending, RunStatus.running)
                if should_persist:
                    for key, value in kwargs.items():
                        if hasattr(record, key) and value is not None:
                            setattr(record, key, value)
                    record.updated_at = _now_iso()
        if should_persist and self._store is not None:
            try:
                # The local status is already staged as terminal, but the store
                # row intentionally remains running until checkpoint finalization.
                await run_capture_io(lambda: self._store.update_run_progress(run_id, **kwargs))
            except CaptureIOClosed:
                raise
            except Exception as exc:
                logger.warning("Run persistence failed run_id=%s operation=finalizing_progress reason=run_metadata_write_failed exception_class=%s", run_id, exception_classification(exc))

    @staticmethod
    def _validate_evidence_origin(origin: str) -> None:
        if origin not in EVIDENCE_ORIGINS:
            raise ValueError("invalid evidence origin")

    async def seal_completed_evidence(self, run_id: str, receipt) -> bool:
        """Best-effort evidence metadata is independent of business completion."""
        async with self._lock:
            record = self._runs.get(run_id)
            if record is None or record.ownership_lost:
                return False
            owner_worker_id = record.owner_worker_id
        seal = getattr(self._store, "seal_evidence", None)
        if seal is None or owner_worker_id is None:
            return False
        return await seal(run_id, owner_worker_id=owner_worker_id, receipt=receipt)

    async def mark_evidence_partial(self, run_id: str, *, error: str) -> bool:
        async with self._lock:
            record = self._runs.get(run_id)
            if record is None or record.ownership_lost:
                return False
            owner_worker_id = record.owner_worker_id
        mark_partial = getattr(self._store, "mark_evidence_partial", None)
        if mark_partial is None or owner_worker_id is None:
            return False
        return await mark_partial(run_id, owner_worker_id=owner_worker_id, error=error)

    async def create(
        self,
        thread_id: str,
        assistant_id: str | None = None,
        *,
        on_disconnect: DisconnectMode = DisconnectMode.cancel,
        metadata: dict | None = None,
        kwargs: dict | None = None,
        multitask_strategy: str = "reject",
        user_id: str | None = None,
        evidence_origin: str = "unknown",
        evidence_agent_id: str | None = None,
    ) -> RunRecord:
        """Create a new pending run and register it.

        Note: this method assumes no active run exists for the thread. It
        persists via ``store.put`` (upsert) rather than the atomic
        ``create_thread_operation_atomic`` primitive, so a concurrent insert for the
        same thread will hit the partial unique index and surface as a
        raw ``IntegrityError`` instead of a ``ConflictError``. Production
        callers should use :meth:`create_or_reject`.
        """
        kwargs = _new_run_kwargs(kwargs, on_disconnect=on_disconnect)
        if (intent := capture_from_kwargs(kwargs)) is not None and intent.version == 2 and not self.supports_output_inventory:
            raise OutputEvidenceError()
        from deerflow.runtime.assembly_evidence import EVIDENCE_KEY

        self._validate_evidence_origin(evidence_origin)
        run_id = str(uuid.uuid4())
        now = _now_iso()
        user_id = _resolve_record_user_id(user_id)
        lease_expires_at = self._compute_lease_expires_at()
        record = RunRecord(
            run_id=run_id,
            thread_id=thread_id,
            assistant_id=assistant_id,
            status=RunStatus.pending,
            on_disconnect=on_disconnect,
            startup_phase=RunStartupPhase.unstarted,
            multitask_strategy=multitask_strategy,
            metadata={key: value for key, value in (metadata or {}).items() if key != EVIDENCE_KEY},
            kwargs=kwargs or {},
            user_id=user_id,
            created_at=now,
            updated_at=now,
            owner_worker_id=self._worker_id,
            lease_expires_at=lease_expires_at,
            evidence_origin=evidence_origin,
            evidence_agent_id=evidence_agent_id,
        )
        lifetime = self._new_capture_lifetime() if self._captures_output(record) else None
        if lifetime is not None:
            self._associate_capture(lifetime, record)
        try:
            with self._capture_control(lifetime=lifetime):
                async with self._lock:
                    self._runs[run_id] = record
                    self._index_run_locked(record)
                    persisted = False
                    try:
                        await self._persist_new_run_to_store(record)
                        persisted = True
                    except CaptureIOClosed:
                        raise
                    except Exception:
                        logger.warning("Failed to persist run %s; rolled back in-memory record", run_id, exc_info=True)
                        raise
                    finally:
                        # Also covers cancellation, which bypasses ``except Exception``.
                        if not persisted:
                            self._runs.pop(run_id, None)
                            self._unindex_run_locked(run_id, record.thread_id)
        except BaseException:
            if lifetime is not None:
                await await_drained(self._finish_capture_lifetime(lifetime))
            raise
        logger.info("Run created: run_id=%s thread_id=%s", run_id, thread_id)
        return record

    async def get(
        self,
        run_id: str,
        *,
        user_id: str | None = None,
        raise_on_store_error: bool = False,
        require_store: bool = False,
    ) -> RunRecord | None:
        """Return a run record by ID, or ``None``.

        Args:
            run_id: The run ID to look up.
            user_id: Optional user ID for permission filtering when hydrating from store.
            raise_on_store_error: Propagate store hydration/mapping failures so
                lifecycle callers can distinguish them from a missing run.
            require_store: Require configured read authority, including for a
                cached positive. Together with raise_on_store_error, an actual
                miss cannot hide store failures. Cached positives are not a
                store-health probe.
        """
        if require_store and self._store is None:
            raise RunStoreUnavailable()
        async with self._lock:
            record = self._runs.get(run_id)
        if record is not None:
            return record
        if self._store is None:
            return None
        try:
            row = await run_capture_io(lambda: self._store.get(run_id, user_id=user_id))
        except CaptureIOClosed:
            raise
        except Exception:
            if raise_on_store_error:
                raise
            logger.warning("Failed to hydrate run %s from store", run_id, exc_info=True)
            return None
        # A conversion failure cannot disappear behind a local record inserted
        # while the store read was suspended. Preserve raise-only lifecycle
        # ordering by opting in only through the new pair of proof-read flags.
        mapped = self._strict_record_from_store(row) if require_store and raise_on_store_error and row is not None else None
        # Re-check after store await: a concurrent create() may have inserted the
        # in-memory record while the store call was in flight.
        async with self._lock:
            record = self._runs.get(run_id)
        if record is not None:
            return record
        if row is None:
            return None
        if mapped is not None:
            return mapped
        try:
            return self._record_from_store(row)
        except CaptureIOClosed:
            raise
        except Exception:
            if raise_on_store_error:
                raise
            logger.warning("Failed to map store row for run %s", run_id, exc_info=True)
            return None

    @property
    def supports_output_receipts(self) -> bool:
        """Whether this actual store supports the optional owned output seal."""
        return self._store is not None and getattr(self._store, "supports_output_receipts", False) is True

    @property
    def supports_output_inventory(self) -> bool:
        return self.supports_output_receipts and getattr(self._store, "supports_output_inventory", False) is True

    async def get_output_snapshot(self, run_id: str, *, user_id: str, require_inventory: bool = False) -> RunOutputSnapshot | None:
        """Read exact stored output authority; cached status is never a seal."""
        if not self.supports_output_receipts or type(require_inventory) is not bool or require_inventory and not self.supports_output_inventory or type(user_id) is not str or not user_id or type(run_id) is not str or not run_id:
            raise OutputEvidenceError()
        try:
            # Observe a foreign row rather than turning wrong owner into an
            # acknowledgeable absence. The explicit None bypasses ambient aliases.
            row = await await_drained(run_capture_io(lambda: self._store.get(run_id, user_id=None)))
            if row is None:
                return None
            if not isinstance(row, Mapping) or any(type(row.get(key)) is not str or not row[key] for key in ("run_id", "thread_id", "user_id")):
                raise OutputEvidenceError()
            if row["run_id"] != run_id or row["user_id"] != user_id:
                raise OutputEvidenceScopeError()
            intent = capture_from_kwargs(row.get("kwargs"))
            if row.get("operation_kind", "run") != "run" or intent is None or require_inventory and intent.version != 2:
                raise OutputEvidenceError()
            saved = receipt_from_kwargs(row["kwargs"])
            if intent.version == 2:
                if not self.supports_output_inventory:
                    raise OutputEvidenceError()
                inventory = inventory_evidence_from_kwargs(row["kwargs"])
                if (saved is None) != (inventory is None):
                    raise OutputEvidenceError()
                if saved is not None:
                    validate_inventory_pair(saved, inventory, status=row["status"])
                elif row["status"] not in {"pending", "running"} or not row.get("owner_worker_id"):
                    raise OutputEvidenceError()
                elif self.heartbeat_enabled or row.get("lease_expires_at") is not None:
                    lease = self._parse_lease_deadline(row.get("lease_expires_at"))
                    if lease is None or lease <= datetime.now(UTC):
                        raise OutputEvidenceError()
            if saved is not None:
                if saved.owner_user_id != user_id or saved.thread_id != row.get("thread_id") or saved.run_id != run_id:
                    raise OutputEvidenceScopeError()
                validate_receipt_status(saved, row["status"])
            async with self._lock:
                local = self._runs.get(run_id)
                finalizing = local is not None and local.user_id == user_id and local.thread_id == row.get("thread_id") and local.finalizing
                original = local
                if original is None:
                    original = next((record for record, _lifetime in self._capture_records.values() if record.run_id == run_id and record.user_id == user_id and record.thread_id == row["thread_id"]), None)
                lifetime = self._capture_lifetime(original) if original is not None else None
                if (
                    row["status"] in {"pending", "running"}
                    and original is not None
                    and original.user_id == user_id
                    and original.thread_id == row["thread_id"]
                    and row.get("owner_worker_id")
                    and row["owner_worker_id"] == original.owner_worker_id == self.worker_id
                    and (original.ownership_lost or lifetime is not None and lifetime.owner.retired)
                ):
                    raise OutputEvidenceError()
            return RunOutputSnapshot(
                run_id=run_id,
                thread_id=row["thread_id"],
                owner_user_id=user_id,
                assistant_id=row.get("assistant_id"),
                status=RunStatus(row["status"]),
                kwargs=row["kwargs"],
                local_finalizing=finalizing,
            )
        except OutputEvidenceScopeError:
            raise
        except CaptureIOClosed:
            raise
        except Exception:
            raise OutputEvidenceError() from None

    async def finalize_output(
        self, run_id: str, status: RunStatus, *, output_receipt: OutputReceipt, inventory_evidence=None, error: str | None = None, stop_reason: str | None = None, respect_cancellation: bool = True
    ) -> StatusFinalization:
        """Seal an opted-in local run through the existing terminal CAS owner."""
        if not self.supports_output_receipts or type(respect_cancellation) is not bool or type(status) is not RunStatus or status.value not in TERMINAL_STATUSES:
            raise OutputEvidenceError()
        wanted = validate_receipt_status(output_receipt, status)
        async with self._lock:
            record = self._runs.get(run_id)
            if record is None or record.store_only or record.run_id != wanted.run_id or record.thread_id != wanted.thread_id or record.user_id != wanted.owner_user_id:
                raise OutputEvidenceScopeError()
            intent = capture_from_kwargs(record.kwargs)
            if intent is not None and intent.version == 2:
                if not self.supports_output_inventory:
                    raise OutputEvidenceError()
                wanted, inventory_evidence = validate_inventory_pair(wanted, inventory_evidence, status=status)
                if inventory_evidence.kind == "unavailable":
                    authority = _current_inventory_authority()
                    existing_pair = record.terminal_confirmed and receipt_from_kwargs(record.kwargs) == wanted and inventory_evidence_from_kwargs(record.kwargs) == inventory_evidence
                    if not existing_pair and not (isinstance(authority, tuple) and len(authority) == 4 and authority[0] == "prestart" and authority[1] is record and authority[2:] == (wanted, inventory_evidence)):
                        raise OutputEvidenceError()
            elif inventory_evidence is not None:
                raise OutputEvidenceError()
        with self._capture_control(record):
            return await persist_terminal(
                self,
                record,
                status,
                error=error,
                stop_reason=stop_reason,
                respect_cancellation=respect_cancellation,
                output_receipt=wanted,
                **({"inventory_evidence": inventory_evidence} if intent is not None and intent.version == 2 else {}),
            )

    async def begin_inventory_finalization(self, record, status, *, output_receipt, inventory_evidence, cleanup_outcome=None, error=None, stop_reason=None):
        """Bind one audited candidate to its original host admission, without I/O."""
        output, inventory = validate_inventory_pair(output_receipt, inventory_evidence, status=status)
        if type(status) is not RunStatus or status.value not in TERMINAL_STATUSES:
            raise OutputEvidenceError() from None
        async with self._lock:
            lifetime = self._capture_lifetime(record)
            if (
                not self.supports_output_inventory
                or self._runs.get(record.run_id) is not record
                or record.store_only
                or record.ownership_lost
                or lifetime is None
                or capture_from_kwargs(record.kwargs).version != 2
                or not isinstance(inventory, EventInventoryEvidence)
                or (output.owner_user_id, output.thread_id, output.run_id) != (record.user_id, record.thread_id, record.run_id)
            ):
                raise OutputEvidenceError() from None
            lifetime.owner.check_open()
            if cleanup_outcome is not None and (
                type(cleanup_outcome) is not InventoryCleanupOutcome
                or cleanup_outcome.action is None
                or cleanup_outcome.receipt != output
                or not record.abort_event.is_set()
                or status is not (RunStatus.interrupted if cleanup_outcome.action == "interrupt" else RunStatus.error)
            ):
                raise OutputEvidenceError() from None
            attempt = _new_attempt(self, record, status, output, inventory, cleanup_outcome, error, stop_reason)
            lifetime.inventory_attempts.append(attempt)
            return attempt

    async def finalize_inventory(self, attempt, *, on_cancel):
        """Reuse terminal persistence; callbacks stay in this finalizer task."""
        if type(attempt) is not InventoryFinalizationAttempt or attempt._manager is not self or not callable(on_cancel):
            raise OutputEvidenceError() from None
        record = attempt._record
        lifetime = self._capture_lifetime(record)
        if lifetime is None or not any(value is attempt for value in lifetime.inventory_attempts) or self._runs.get(record.run_id) is not record or record.store_only:
            raise OutputEvidenceError() from None
        if attempt._result is not None:
            return attempt._result
        lifetime.owner.check_open()
        if attempt._running:
            raise OutputEvidenceError() from None
        attempt._running = True
        try:
            with self._capture_control(record), _inventory_authority(attempt):
                result = await persist_terminal(
                    self,
                    record,
                    attempt._status,
                    output_receipt=attempt._output,
                    inventory_evidence=attempt._inventory,
                    error=attempt._error,
                    stop_reason=attempt._stop_reason,
                    respect_cancellation=attempt._cleanup is None,
                    _inventory_attempt=attempt,
                )
                if result.cancel_action is not None:
                    decision = attempt._decision
                    if decision is None or decision.action != result.cancel_action:
                        raise OutputEvidenceError() from None
                    cleanup = await on_cancel(decision)
                    if type(cleanup) is not InventoryCleanupOutcome or cleanup.action != decision.action or record.status is not (RunStatus.interrupted if decision.action == "interrupt" else RunStatus.error):
                        raise OutputEvidenceError() from None
                    output = validate_receipt_status(cleanup.receipt, record.status)
                    if (output.owner_user_id, output.thread_id, output.run_id, output.input_message_id) != (attempt._output.owner_user_id, attempt._output.thread_id, attempt._output.run_id, attempt._output.input_message_id):
                        raise OutputEvidenceError() from None
                    attempt._cleanup, attempt._output, attempt._status = cleanup, output, record.status
                    attempt._inventory = build_unavailable_inventory_evidence(output, status=record.status, reason="cancellation_after_inventory_not_enumerated")
                    attempt._action_basis, attempt._action_bound = decision.action, True
                    result = await persist_terminal(
                        self, record, record.status, output_receipt=output, inventory_evidence=attempt._inventory, error=record.error, stop_reason=record.stop_reason, respect_cancellation=False, _inventory_attempt=attempt
                    )
                attempt._result = result
                return result
        except CaptureIOClosed:
            raise
        except BaseException:
            await self._mark_ownership_lost(record, reason="Run inventory finalization could not be confirmed.", require_active=False)
            raise
        finally:
            attempt._running = False

    def _strict_record_from_store(self, row) -> RunRecord:
        # Do not turn an observed malformed kwargs value into a legacy empty
        # mapping through the ordinary historical-null compatibility mapper.
        if not isinstance(row, Mapping) or not isinstance(row.get("kwargs"), Mapping):
            raise ValueError("Run store row has invalid kwargs")
        return self._record_from_store(row)

    async def aget(
        self,
        run_id: str,
        *,
        user_id: str | None = None,
        raise_on_store_error: bool = False,
        require_store: bool = False,
    ) -> RunRecord | None:
        """Return a run record by ID, checking the persistent store as fallback.

        Alias for :meth:`get` for backward compatibility.
        """
        return await self.get(
            run_id,
            user_id=user_id,
            raise_on_store_error=raise_on_store_error,
            **({"require_store": True} if require_store else {}),
        )

    async def list_by_thread(
        self,
        thread_id: str,
        *,
        user_id: str | None = None,
        limit: int = 100,
        before_created_at: str | None = None,
        before_run_id: str | None = None,
        raise_on_store_error: bool = False,
        require_store: bool = False,
    ) -> list[RunRecord]:
        """Return runs for a given thread, newest first, at most ``limit`` records.

        In-memory runs take precedence only when the same ``run_id`` exists in both
        memory and the backing store. The merged result is then sorted newest-first
        by ``(created_at, run_id)`` and trimmed to ``limit`` (default 100).
        Optional ``before_created_at`` + ``before_run_id`` is a keyset cursor for
        walking older pages; both must be provided together.

        Args:
            thread_id: The thread ID to filter by.
            user_id: Optional user ID for permission filtering when hydrating from store.
            limit: Maximum number of runs to return.
            before_created_at: ISO timestamp of the last run on the previous page.
            before_run_id: Run id of the last run on the previous page.
            raise_on_store_error: Refuse store/query/mapping failures instead
                of returning a partial page. Convert every returned row,
                including duplicates of live local records.
            require_store: Require configured authority before any cache-only
                page. Use both flags for an exhaustive supported lookup.
        """
        if require_store and self._store is None:
            raise RunStoreUnavailable()
        before_created_at = _cursor_part(before_created_at)
        before_run_id = _cursor_part(before_run_id)
        if (before_created_at is None) != (before_run_id is None):
            raise ValueError("before_created_at and before_run_id must be provided together")
        if before_created_at is not None:
            try:
                before_created_at = normalize_run_created_at_iso(before_created_at)
                datetime.fromisoformat(before_created_at)
            except ValueError:
                raise ValueError("before_created_at must be an ISO-8601 timestamp") from None

        def _page(records: list[RunRecord]) -> list[RunRecord]:
            return sorted(records, key=lambda record: run_sort_key(record.created_at, record.run_id), reverse=True)[:limit]

        async with self._lock:
            memory_records = [
                record
                for record in self._thread_records_locked(thread_id)
                if record.operation_kind == ThreadOperationKind.run
                and run_is_before_cursor(
                    record.created_at,
                    record.run_id,
                    before_created_at=before_created_at,
                    before_run_id=before_run_id,
                )
            ]
        if self._store is None:
            return _page(memory_records)
        records_by_id = {record.run_id: record for record in memory_records}
        # Query enough rows to cover both the requested page and every possible
        # in-memory/store duplicate. Local records can be older than persisted
        # rows, so subtracting them from the store limit can hide the actual
        # newest run before the merge; querying only ``limit`` can still lose a
        # distinct row when that page is occupied by duplicate local records.
        store_limit = limit + len(memory_records)
        store_kwargs: dict[str, Any] = {"user_id": user_id, "limit": store_limit}
        if before_created_at is not None and before_run_id is not None:
            store_kwargs["before_created_at"] = before_created_at
            store_kwargs["before_run_id"] = before_run_id
        try:
            rows = await run_capture_io(lambda: self._store.list_by_thread(thread_id, **store_kwargs))
        except CaptureIOClosed:
            raise
        except Exception:
            if raise_on_store_error:
                raise
            logger.warning("Failed to hydrate runs for thread %s from store", thread_id, exc_info=True)
            return _page(memory_records)
        for row in rows:
            if raise_on_store_error:
                mapped = self._strict_record_from_store(row) if require_store else self._record_from_store(row)
                if not isinstance(mapped.run_id, str) or not mapped.run_id:
                    raise ValueError("Run store row has no valid identity")
                if mapped.run_id not in records_by_id:
                    records_by_id[mapped.run_id] = mapped
                continue
            run_id = row.get("run_id")
            if run_id and run_id not in records_by_id:
                try:
                    records_by_id[run_id] = self._record_from_store(row)
                except CaptureIOClosed:
                    raise
                except Exception:
                    logger.warning("Failed to map store row for run %s", run_id, exc_info=True)
        return _page(list(records_by_id.values()))

    async def list_successful_regenerate_sources(
        self,
        thread_id: str,
        *,
        user_id: str | None | _AutoSentinel = AUTO,
    ) -> set[str]:
        """Return all source runs superseded by successful regenerations.

        Unlike :meth:`list_by_thread`, this query is intentionally unbounded.
        Current-process records override matching persisted status: a latest
        in-memory failure must not inherit an older successful store snapshot.
        Store failures propagate because supersession filtering is required for
        correct pagination.
        """
        resolved_user_id = resolve_user_id(user_id, method_name="RunManager.list_successful_regenerate_sources")
        async with self._lock:
            memory_records = [record for record in self._thread_records_locked(thread_id) if record.operation_kind == ThreadOperationKind.run and (resolved_user_id is None or record.user_id == resolved_user_id)]

        sources = set(await run_capture_io(lambda: self._store.list_successful_regenerate_sources(thread_id, user_id=resolved_user_id))) if self._store is not None else set()
        # _thread_records_locked preserves the insertion order of the thread
        # index. Applying records oldest-to-newest makes the latest in-memory
        # regeneration attempt authoritative when several attempts reference
        # the same source run (for example, a failed retry after a success).
        for record in memory_records:
            source = record.metadata.get("regenerate_from_run_id")
            if not isinstance(source, str) or not source:
                continue
            sources.discard(source)
            if record.status == RunStatus.success:
                sources.add(source)
        return sources

    @staticmethod
    def _record_status_value(record: RunRecord) -> str:
        status = record.status
        return status.value if isinstance(status, RunStatus) else str(status)

    @staticmethod
    def _compute_edit_replay_visibility(records: list[RunRecord]) -> EditReplayVisibility:
        latest_attempt_by_source: dict[str, tuple[str, str]] = {}
        failed_attempts: set[str] = set()
        for record in sorted(records, key=lambda item: item.created_at):
            metadata = record.metadata or {}
            if metadata.get("replay_kind") != "edit":
                continue
            source = metadata.get("regenerate_from_run_id")
            if not isinstance(source, str) or not source:
                continue
            status = RunManager._record_status_value(record)
            latest_attempt_by_source[source] = (record.run_id, status)
            if status in {RunStatus.error.value, RunStatus.timeout.value, RunStatus.interrupted.value}:
                failed_attempts.add(record.run_id)

        hidden_sources: set[str] = set()
        for source, (_, status) in latest_attempt_by_source.items():
            if status in {RunStatus.pending.value, RunStatus.running.value, RunStatus.success.value}:
                hidden_sources.add(source)
        return EditReplayVisibility(
            hidden_source_run_ids=hidden_sources,
            hidden_attempt_run_ids=failed_attempts,
        )

    async def list_edit_replay_visibility(
        self,
        thread_id: str,
        *,
        user_id: str | None | _AutoSentinel = AUTO,
    ) -> EditReplayVisibility:
        """Return run-id visibility rules for edit-and-rerun attempts.

        Store rows cover reload/multi-worker history. Current-process records
        override the same run ids because they may have newer terminal status
        than the persisted snapshot visible when this query started.
        """
        resolved_user_id = resolve_user_id(user_id, method_name="RunManager.list_edit_replay_visibility")
        records_by_id: dict[str, RunRecord] = {}
        if self._store is not None:
            rows = await run_capture_io(lambda: self._store.list_edit_regenerate_runs(thread_id, user_id=resolved_user_id))
            for row in rows:
                try:
                    record = self._record_from_store(row)
                except CaptureIOClosed:
                    raise
                except Exception:
                    logger.warning("Failed to map edit replay run row for %s", row.get("run_id"), exc_info=True)
                    continue
                records_by_id[record.run_id] = record

        async with self._lock:
            memory_records = [record for record in self._thread_records_locked(thread_id) if resolved_user_id is None or record.user_id == resolved_user_id]
        for record in memory_records:
            records_by_id[record.run_id] = record

        return self._compute_edit_replay_visibility(list(records_by_id.values()))

    def _captures_output(self, record: RunRecord) -> bool:
        return self.supports_output_receipts and record.operation_kind is ThreadOperationKind.run and capture_from_kwargs(record.kwargs) is not None

    async def _settle_prestart_locked(self, record: RunRecord, outcome: PrestartOutcome) -> bool:
        """Decide nonexecution under start_lock, then confirm its exact seal."""
        async with self._lock:
            if self._runs.get(record.run_id) is not record or record.store_only or record.ownership_lost:
                return False
            if record.startup_phase is RunStartupPhase.closed:
                return record.prestart_outcome is not None and record.prestart_outcome.confirmed
            if record.startup_phase is not RunStartupPhase.unstarted:
                return False
            record.startup_phase = RunStartupPhase.closed
            record.prestart_outcome = outcome
            record.status, record.error, record.stop_reason = outcome.status, outcome.error, outcome.stop_reason
            record.finalizing = record.task is not None and not record.task.done()
            record.abort_event.set()
            record.updated_at = _now_iso()

        def receipt(value):
            return UnavailableOutputReceipt(version=1, kind="unavailable", owner_user_id=record.user_id, thread_id=record.thread_id, run_id=record.run_id, input_message_id=None, reason=value.reason)

        async def seal(value, *, respect_cancellation):
            output = receipt(value)
            if capture_from_kwargs(record.kwargs).version == 2:
                inventory = build_unavailable_inventory_evidence(output, status=value.status, reason="prestart_not_enumerated")
                with _inventory_authority(("prestart", record, output, inventory)):
                    return await self.finalize_output(record.run_id, value.status, output_receipt=output, inventory_evidence=inventory, error=value.error, stop_reason=value.stop_reason, respect_cancellation=respect_cancellation)
            return await self.finalize_output(record.run_id, value.status, output_receipt=output, error=value.error, stop_reason=value.stop_reason, respect_cancellation=respect_cancellation)

        try:
            result = await seal(outcome, respect_cancellation=outcome.reason != "cancelled")
            if result.cancel_action is not None:
                record.abort_action = result.cancel_action
                outcome = PrestartOutcome(RunStatus.interrupted, "cancelled", stop_reason=record.stop_reason)
                async with self._lock:
                    record.prestart_outcome = outcome
                    record.status, record.error = outcome.status, outcome.error
                result = await seal(outcome, respect_cancellation=False)
            if result.output_receipt_confirmed and (capture_from_kwargs(record.kwargs).version != 2 or result.inventory_evidence_confirmed):
                async with self._lock:
                    record.prestart_outcome = replace(outcome, confirmed=True)
                return True
        except CaptureIOClosed:
            raise
        except Exception as error:
            logger.error("Run %s prestart output could not be confirmed: %s", record.run_id, failure_chain(error))
        await self._mark_ownership_lost(record, reason="Run prestart output could not be confirmed.", require_active=False)
        return False

    async def _close_unstarted_capture(self, record: RunRecord, outcome: PrestartOutcome) -> bool:
        if record.startup_phase is RunStartupPhase.closed and record.prestart_outcome is not None and record.prestart_outcome.confirmed:
            return True

        async def close():
            with self._capture_control(record):
                async with record.start_lock:
                    return await self._settle_prestart_locked(record, outcome)

        return await await_drained(close())

    async def _try_start_capture(self, record: RunRecord) -> RunStartOutcome:
        if record.startup_phase is not RunStartupPhase.unstarted:
            return RunStartOutcome.cancelled
        with self._capture_control(record):
            return await self._try_start_capture_owned(record)

    async def _try_start_capture_owned(self, record: RunRecord) -> RunStartOutcome:
        async with record.start_lock:
            async with self._lock:
                if self._runs.get(record.run_id) is not record or record.startup_phase is not RunStartupPhase.unstarted:
                    return RunStartOutcome.cancelled
                cancelled = record.abort_event.is_set()
                if record.status != RunStatus.pending and not cancelled:
                    return RunStartOutcome.cancelled
            if not cancelled:
                try:
                    updated = await self._call_store_with_retry("start_run", record.run_id, lambda: self._store.start_run(record.run_id))
                except CaptureIOClosed:
                    raise
                except Exception as exc:
                    raise RunStartupError(f"Failed to start run {record.run_id}: {exc}") from exc
                if not updated:
                    row = await run_capture_io(lambda: self._store.get(record.run_id, user_id=record.user_id))
                    if row is None or row.get("status") not in {"pending", "running"} or row.get("owner_worker_id") != self.worker_id or row.get("cancel_action") is None:
                        await self._mark_ownership_lost(record, reason="Run startup authority could not be confirmed.", require_active=False)
                        return RunStartOutcome.cancelled
                    record.abort_action = row["cancel_action"]
                    record.abort_event.set()
            async with self._lock:
                if not record.abort_event.is_set() and record.status == RunStatus.pending:
                    record.startup_phase = RunStartupPhase.started
                    record.status = RunStatus.running
                    record.updated_at = _now_iso()
                    return RunStartOutcome.started
            await self._settle_prestart_locked(record, PrestartOutcome(RunStatus.interrupted, "cancelled", stop_reason=record.stop_reason))
            return RunStartOutcome.cancelled

    async def try_start(self, run_id: str) -> RunStartOutcome:
        """Transition an uncancelled pending run to running before building the agent."""
        async with self._lock:
            record = self._runs.get(run_id)
        if record is None:
            raise RunStartupError(f"Cannot start unknown run {run_id}")
        if self._captures_output(record):
            return await await_drained(self._try_start_capture(record))

        async with record.start_lock:
            async with self._lock:
                if record.abort_event.is_set() or record.status != RunStatus.pending:
                    return RunStartOutcome.cancelled

            if self._store is not None:
                try:
                    updated = await self._call_store_with_retry(
                        "start_run",
                        run_id,
                        lambda: self._store.start_run(run_id),
                    )
                except CaptureIOClosed:
                    raise
                except Exception as exc:
                    raise RunStartupError(f"Failed to start run {run_id}: {exc}") from exc
                if updated is False:
                    async with self._lock:
                        if record.status == RunStatus.pending:
                            record.status = RunStatus.interrupted
                            record.terminal_status_persisted = False
                            record.abort_event.set()
                            record.updated_at = _now_iso()
                    return RunStartOutcome.cancelled

            async with self._lock:
                if record.abort_event.is_set() or record.status != RunStatus.pending:
                    restore_status = record.status
                    restore_error = record.error
                    restore_stop_reason = record.stop_reason
                else:
                    record.status = RunStatus.running
                    record.updated_at = _now_iso()
                    logger.info("Run %s -> %s", run_id, RunStatus.running.value)
                    return RunStartOutcome.started

            if self._store is not None:
                await self._persist_status(
                    record,
                    restore_status,
                    error=restore_error,
                    stop_reason=restore_stop_reason,
                )
            return RunStartOutcome.cancelled

    async def fail_start_if_pending(self, run_id: str, *, error: str) -> bool:
        """Mark an admitted run as failed if its worker task could not be attached."""
        async with self._lock:
            record = self._runs.get(run_id)
        if record is not None and self._captures_output(record):
            if record.status != RunStatus.pending:
                return False
            return await self._close_unstarted_capture(record, PrestartOutcome(RunStatus.error, "failed", error=error, stop_reason=record.stop_reason))
        async with self._lock:
            record = self._runs.get(run_id)
            if record is None or record.status != RunStatus.pending:
                return False
            record.status = RunStatus.error
            record.terminal_status_persisted = self._store is None
            record.error = error
            record.abort_event.set()
            record.updated_at = _now_iso()

        persisted = await self._persist_status(
            record,
            RunStatus.error,
            error=error,
        )
        async with self._lock:
            if self._runs.get(run_id) is record:
                record.terminal_status_persisted = persisted
        return True

    async def get_many_by_thread(
        self,
        thread_id: str,
        run_ids: set[str],
        *,
        user_id: str | None | _AutoSentinel = AUTO,
    ) -> dict[str, RunRecord]:
        """Batch-load selected thread runs with in-memory records preferred."""
        if not run_ids:
            return {}
        resolved_user_id = resolve_user_id(user_id, method_name="RunManager.get_many_by_thread")
        async with self._lock:
            records_by_id = {
                record.run_id: record for record in self._thread_records_locked(thread_id) if record.operation_kind == ThreadOperationKind.run and record.run_id in run_ids and (resolved_user_id is None or record.user_id == resolved_user_id)
            }
        if self._store is None:
            return records_by_id

        remaining = run_ids - records_by_id.keys()
        if not remaining:
            return records_by_id
        try:
            rows = await run_capture_io(lambda: self._store.get_many_by_thread(thread_id, set(remaining), user_id=resolved_user_id))
        except CaptureIOClosed:
            raise
        except Exception:
            logger.warning("Failed to batch-hydrate runs for thread %s", thread_id, exc_info=True)
            return records_by_id
        for run_id, row in rows.items():
            if run_id in records_by_id:
                continue
            try:
                records_by_id[run_id] = self._record_from_store(row)
            except CaptureIOClosed:
                raise
            except Exception:
                logger.warning("Failed to map store row for run %s", run_id, exc_info=True)
        return records_by_id

    async def set_status(
        self,
        run_id: str,
        status: RunStatus,
        *,
        error: str | None = None,
        stop_reason: str | None = None,
        goal_verdict: dict[str, Any] | None = None,
        persist: bool = True,
    ) -> None:
        """Transition a run to a new status."""
        async with self._lock:
            record = self._runs.get(run_id)
            if record is None:
                logger.warning("set_status called for unknown run %s", run_id)
                return
            if record.ownership_lost:
                logger.warning(
                    "Skipped local status transition to %s for run %s after lease ownership was lost",
                    status.value,
                    run_id,
                )
                return
            proof = record.terminal_commit_proof
            if (
                self.heartbeat_enabled
                and self._store is not None
                and proof is not None
                and proof.status != RunStatus.interrupted
                and self._terminal_outcome(record, status, error=error, stop_reason=stop_reason, goal_verdict=goal_verdict) != proof
            ):
                logger.info("Skipped status change after acknowledged terminal outcome for run %s", run_id)
                return
            record.status = status
            if not persist and self._store is not None and getattr(self._store, "supports_terminal_recovery", False) is True and status.value in TERMINAL_STATUSES:
                record.finalizing = True
            record.updated_at = _now_iso()
            if status not in (RunStatus.pending, RunStatus.running):
                record.terminal_status_revision += 1
                record.terminal_status_persisted = self._store is None
            if error is not None:
                record.error = error
            if stop_reason is not None:
                record.stop_reason = stop_reason
            if goal_verdict is not None:
                record.goal_verdict = copy.deepcopy(goal_verdict)
        if persist:
            persisted = await self._persist_status(record, status, error=error, stop_reason=stop_reason)
            if status not in (RunStatus.pending, RunStatus.running):
                async with self._lock:
                    record.terminal_status_persisted = self._has_final_terminal_ack(record) if self.heartbeat_enabled and self._store is not None else persisted
            if not persisted and self.heartbeat_enabled and status == RunStatus.success and not record.ownership_lost:
                await self._mark_ownership_lost(
                    record,
                    reason="Successful completion could not be confirmed in the durable run store.",
                    require_active=False,
                )
        if record.ownership_lost:
            return
        logger.info("Run %s -> %s", run_id, status.value)

    async def persist_current_status(self, run_id: str) -> bool:
        """Persist the status already staged on the in-memory run record."""
        async with self._lock:
            record = self._runs.get(run_id)
            if record is None:
                logger.warning("persist_current_status called for unknown run %s", run_id)
                return False
            status = record.status
            error = record.error
            stop_reason = record.stop_reason
        persisted = await self._persist_status(record, status, error=error, stop_reason=stop_reason)
        if status not in (RunStatus.pending, RunStatus.running):
            async with self._lock:
                record.terminal_status_persisted = self._has_final_terminal_ack(record) if self.heartbeat_enabled and self._store is not None else persisted
        if not persisted and self.heartbeat_enabled and status == RunStatus.success and not record.ownership_lost:
            await self._mark_ownership_lost(
                record,
                reason="Successful completion could not be confirmed in the durable run store.",
                require_active=False,
            )
        return persisted

    async def set_status_if_not_cancelled(
        self,
        run_id: str,
        status: RunStatus,
        *,
        error: str | None = None,
        stop_reason: str | None = None,
        goal_verdict: dict[str, Any] | None = None,
        persist: bool = True,
    ) -> str | None:
        """Set a terminal status unless a durable cancellation won first."""
        if persist and self._store is not None and getattr(self._store, "supports_terminal_recovery", False) is True:
            async with self._lock:
                record = self._runs.get(run_id)
            if record is None:
                return None
            result = await persist_terminal(self, record, status, error=error, stop_reason=stop_reason, respect_cancellation=True, goal_verdict=goal_verdict)
            return result.cancel_action
        if not persist or not self.heartbeat_enabled or self._store is None:
            await self.set_status(
                run_id,
                status,
                error=error,
                stop_reason=stop_reason,
                goal_verdict=goal_verdict,
                persist=persist,
            )
            return None

        async with self._lock:
            record = self._runs.get(run_id)
            if record is None or record.store_only or record.ownership_lost:
                return None
            outcome = self._terminal_outcome(record, status, error=error, stop_reason=stop_reason, goal_verdict=goal_verdict)
            if self._has_terminal_ack(record, outcome):
                return None
            if record.terminal_commit_proof is not None and record.terminal_commit_proof.status != RunStatus.interrupted:
                return None
        try:
            result = await self._terminal_operation(
                record,
                outcome,
                lambda: self._call_store_with_retry(
                    "finalize_if_not_cancelled",
                    run_id,
                    lambda: self._store.finalize_if_not_cancelled(
                        run_id,
                        status=status.value,
                        error=outcome.error,
                        stop_reason=outcome.stop_reason,
                        owner_worker_id=self._worker_id,
                        **({"goal_verdict": copy.deepcopy(outcome.goal_verdict)} if outcome.goal_verdict is not None else {}),
                    ),
                ),
                cas=True,
            )
        except asyncio.CancelledError:
            await self._fence_unconfirmed_terminal(record)
            raise
        except Exception as exc:
            logger.warning(
                "Unconfirmed terminal finalization for run %s (%s); heartbeat RunStore.finalize_if_not_cancelled must support owner_worker_id and atomically enforce a live owner lease",
                run_id,
                type(exc).__name__,
            )
            await self._fence_unconfirmed_terminal(record)
            return None
        if result.finalized is True:
            return None
        if result.cancel_action is not None:
            record.abort_action = result.cancel_action
            record.abort_event.set()
            return result.cancel_action
        # Rejected CAS is never retried through a weaker status/completion path.
        await self._fence_unconfirmed_terminal(record)
        return None

    async def _ensure_delivery_receipt(self, record: RunRecord) -> bool:
        """Idempotently persist a zero-delivery receipt during recovery."""
        if self._event_store is None:
            return True
        try:
            await run_capture_io(
                lambda: self._event_store.put_if_absent(
                    thread_id=record.thread_id,
                    run_id=record.run_id,
                    event_type="run.delivery",
                    category="outputs",
                    content={"presented": 0, "paths": [], "by_tool": {}},
                    # Recovery runs outside the owner's request: name the owner.
                    user_id=record.user_id,
                )
            )
            return True
        except CaptureIOClosed:
            raise
        except Exception:
            logger.warning(
                "Failed to backfill delivery receipt for recovered run %s; preserving its terminal status",
                record.run_id,
                exc_info=True,
            )
            return False

    async def set_finalizing(self, run_id: str, finalizing: bool) -> None:
        """Mark whether a run is performing post-cancel cleanup."""
        async with self._lock:
            record = self._runs.get(run_id)
            if record is None:
                logger.warning("set_finalizing called for unknown run %s", run_id)
                return
            record.finalizing = finalizing
            record.updated_at = _now_iso()

    async def wait_for_prior_finalizing(
        self,
        thread_id: str,
        run_id: str,
        *,
        poll_interval: float = 0.01,
        abort_event: asyncio.Event | None = None,
    ) -> None:
        """Wait until older same-thread runs have finished post-cancel cleanup."""
        while True:
            async with self._lock:
                found_current = False
                prior_finalizing = False
                for record in self._thread_records_locked(thread_id):
                    if record.run_id == run_id:
                        found_current = True
                        break
                    if record.finalizing:
                        prior_finalizing = True

                if not found_current or not prior_finalizing:
                    return

            if abort_event is None:
                await asyncio.sleep(poll_interval)
                continue
            try:
                await asyncio.wait_for(abort_event.wait(), timeout=poll_interval)
            except TimeoutError:
                continue
            return

    async def has_later_run(self, thread_id: str, run_id: str) -> bool:
        """Return whether a newer in-memory run has been admitted for the thread."""
        async with self._lock:
            seen_current = False
            for record in self._thread_records_locked(thread_id):
                if record.run_id == run_id:
                    seen_current = True
                    continue
                if seen_current:
                    return True
        return False

    async def has_later_started_run(self, thread_id: str, run_id: str) -> bool:
        """Return whether a newer same-thread run may have already advanced state."""
        async with self._lock:
            seen_current = False
            for record in self._thread_records_locked(thread_id):
                if record.run_id == run_id:
                    seen_current = True
                    continue
                if seen_current and (record.status != RunStatus.pending or record.finalizing):
                    return True
        return False

    async def _persist_model_name(self, run_id: str, model_name: str | None) -> None:
        """Best-effort persist model_name update to the backing store."""
        if self._store is None:
            return
        try:
            await self._call_store_with_retry(
                "update_model_name",
                run_id,
                lambda: self._store.update_model_name(run_id, model_name),
            )
        except CaptureIOClosed:
            raise
        except Exception:
            logger.warning("Failed to persist model_name update for run %s", run_id, exc_info=True)

    async def record_assembly_evidence(self, run_id: str, evidence: dict) -> None:
        """Publish only host assembly evidence, once, before execution."""
        from deerflow.runtime.assembly_evidence import EVIDENCE_KEY

        async with self._lock:
            record = self._runs.get(run_id)
            if record is None or evidence.get("run_id") != run_id or evidence.get("thread_id") != record.thread_id:
                raise ValueError("Assembly/run binding mismatch")
            if EVIDENCE_KEY in record.metadata:
                if record.metadata[EVIDENCE_KEY] != evidence:
                    raise ValueError("Run assembly evidence is immutable")
                return
            record.metadata = {**record.metadata, EVIDENCE_KEY: copy.deepcopy(evidence)}
            if not await self._persist_to_store(record):
                record.metadata.pop(EVIDENCE_KEY, None)
                raise RuntimeError("Unable to persist assembly evidence")

    async def update_model_name(self, run_id: str, model_name: str | None) -> None:
        """Update the model name for a run."""
        async with self._lock:
            record = self._runs.get(run_id)
            if record is None:
                logger.warning("update_model_name called for unknown run %s", run_id)
                return
            record.model_name = model_name
            record.updated_at = _now_iso()
        await self._persist_model_name(run_id, model_name)
        logger.info("Run %s model_name=%s", run_id, model_name)

    async def _request_durable_cancel(
        self,
        run_id: str,
        *,
        action: str,
    ) -> tuple[CancelOutcome, str | None]:
        """Record cancellation and return the first action that won."""
        if self._store is None:
            return CancelOutcome.unknown, None
        try:
            winning_action = await self._call_store_with_retry(
                "request_cancel",
                run_id,
                lambda: self._store.request_cancel(run_id, action=action),
            )
        except NotImplementedError:
            # Keep third-party stores that predate durable cancellation on the
            # old safe behavior instead of pretending the owner was notified.
            logger.info(
                "Run store does not support cross-worker cancellation for run %s",
                run_id,
            )
            return CancelOutcome.lease_valid_elsewhere, None
        except CaptureIOClosed:
            raise
        except Exception:
            logger.warning(
                "Failed to persist cancellation request for run %s",
                run_id,
                exc_info=True,
            )
            return CancelOutcome.unknown, None

        if winning_action is not None:
            logger.info(
                "Run %s cancellation requested (requested=%s,winner=%s)",
                run_id,
                action,
                winning_action,
            )
            return CancelOutcome.requested, winning_action

        # Completion may have won the race between the caller's read and the
        # guarded cancellation UPDATE. Re-read so the API reports that precise
        # terminal result rather than claiming the request was accepted.
        try:
            fresh = await run_capture_io(lambda: self._store.get(run_id))
        except CaptureIOClosed:
            raise
        except Exception:
            fresh = None
        if fresh is None:
            return CancelOutcome.unknown, None
        if fresh.get("status") not in ("pending", "running"):
            return CancelOutcome.not_cancellable, None
        # A legacy/partial store implementation may decline the request while
        # the owner is still live. Preserve the former lease-conflict signal.
        return CancelOutcome.lease_valid_elsewhere, None

    async def _request_remote_cancel(
        self,
        run_id: str,
        *,
        action: str,
    ) -> CancelOutcome:
        """Record cancellation for a run whose task belongs to another worker."""
        outcome, _ = await self._request_durable_cancel(
            run_id,
            action=action,
        )
        return outcome

    async def _signal_local_cancel(
        self,
        run_id: str,
        *,
        action: str,
    ) -> None:
        """Set process-local abort state without status persistence or cleanup."""
        async with self._lock:
            record = self._runs.get(run_id)
            if record is None or (record.status not in (RunStatus.pending, RunStatus.running) and not self._awaits_terminal_commit(record)) or record.abort_event.is_set():
                return

            record.abort_action = action
            record.abort_event.set()
            task_active = record.task is not None and not record.task.done()
            record.finalizing = task_active
            record.rollback_finalization_pending = action == "rollback" and task_active
            if task_active and record.status == RunStatus.running:
                record.task.cancel()
        logger.info("Run %s cancellation signalled locally (action=%s)", run_id, action)

    async def _cancel_capture(self, record: RunRecord, *, action: str) -> CancelOutcome:
        if type(action) is not str or action not in {"interrupt", "rollback"}:
            raise ValueError("Unsupported cancellation action")
        if record.startup_phase is RunStartupPhase.closed:
            saved = record.prestart_outcome
            return CancelOutcome.cancelled if saved is not None and saved.confirmed and saved.reason == "cancelled" else CancelOutcome.not_cancellable
        if record.terminal_confirmed and record.status.value in TERMINAL_STATUSES:
            return CancelOutcome.cancelled if record.status is RunStatus.interrupted else CancelOutcome.not_cancellable
        with self._capture_control(record):
            return await self._cancel_capture_owned(record, action=action)

    async def _cancel_capture_owned(self, record: RunRecord, *, action: str) -> CancelOutcome:
        outcome, winning = await self._request_durable_cancel(record.run_id, action=action)
        if outcome not in {CancelOutcome.requested, CancelOutcome.unknown}:
            return outcome
        task_to_cancel = None
        async with self._lock:
            if self._runs.get(record.run_id) is not record:
                return CancelOutcome.unknown
            record.abort_action = winning or action
            record.abort_event.set()
            unstarted = record.startup_phase is RunStartupPhase.unstarted
            if not unstarted:
                task_active = record.task is not None and not record.task.done()
                record.finalizing = task_active
                if task_active and record.status == RunStatus.running and record.task is not asyncio.current_task():
                    task_to_cancel = record.task
                if record.status in {RunStatus.pending, RunStatus.running}:
                    record.status = RunStatus.interrupted
                    record.updated_at = _now_iso()
        if task_to_cancel is not None:
            task_to_cancel.cancel()
        if not unstarted:
            # Missing/done tasks do not establish actual execution cleanup.
            return CancelOutcome.cancelled
        confirmed = await self._close_unstarted_capture(record, PrestartOutcome(RunStatus.interrupted, "cancelled", stop_reason=record.stop_reason))
        return CancelOutcome.cancelled if confirmed else CancelOutcome.unknown

    async def request_fenced_cancel(self, run_id, *, execution_fence, user_id):
        """Persist cancellation under the extension epoch; the run owner consumes it."""
        import inspect

        if not self.heartbeat_enabled or self._store is None or "execution_fence" not in inspect.signature(self._store.request_cancel).parameters:
            raise RunIdempotencyUnsupported("The configured RunStore does not support fenced cancellation")
        record = await self.get(run_id, user_id=user_id)
        if record is None:
            raise ConflictError("Run unavailable")
        await self._store.request_cancel(run_id, action="interrupt", execution_fence=execution_fence, user_id=user_id)

    async def cancel(self, run_id: str, *, action: str = "interrupt") -> CancelOutcome:
        """Cancel under the original local owner or a finite host lifetime."""
        record = self._runs.get(run_id)
        if record is not None and self._capture_lifetime(record) is not None:
            return await self._cancel_with_local_authority(run_id, action=action)
        async with self._maintenance_control():
            return await self._cancel_with_local_authority(run_id, action=action)

    async def _cancel_with_local_authority(self, run_id: str, *, action: str) -> CancelOutcome:
        """Request cancellation of a run.

        When the call lands on the owning worker the run is cancelled
        locally as before (in-memory abort + status persisted to store).

        When the call lands on a non-owning worker in a multi-worker
        deployment with heartbeat enabled:

        - **Lease expired** — the run's lease has passed the grace
          threshold, so this worker takes ownership and marks it as
          ``error``.  The owning worker is assumed dead (its heartbeat
          stopped renewing).

        - **Lease still valid** — durably records the cancellation action.
          The owner observes it on its next heartbeat and performs the same
          local abort/finalization path as a directly-routed request.

        In single-worker mode (``heartbeat_enabled=False``) store-only
        hydrated runs that aren't in-memory return ``not_active_locally``,
        preserving the original 409 behaviour.

        Args:
            run_id: The run ID to cancel.
            action: ``"interrupt"`` keeps checkpoint, ``"rollback"``
                    reverts to pre-run state.

        Returns:
            A :class:`CancelOutcome` enum describing what happened.
        """
        # ------------------------------------------------------------------
        # Local path — this worker owns the run in-memory.
        # ------------------------------------------------------------------
        async with self._lock:
            captured = self._runs.get(run_id)
        if captured is not None and self._captures_output(captured):
            return await await_drained(self._cancel_capture(captured, action=action))
        durable_owner = self._store is not None and (self.heartbeat_enabled or getattr(self._store, "supports_terminal_recovery", False) is True)
        async with self._lock:
            record = self._runs.get(run_id)
            if record is not None:
                if record.status == RunStatus.interrupted:
                    return CancelOutcome.cancelled  # idempotent
                if record.status not in (RunStatus.pending, RunStatus.running) and not record.scheduled_goal_cleanup_pending and not durable_owner:
                    return CancelOutcome.not_cancellable

        durable_cancel_won = False
        if record is not None and durable_owner:
            outcome, winning_action = await self._request_durable_cancel(
                run_id,
                action=action,
            )
            if outcome == CancelOutcome.requested:
                action = winning_action or action
                durable_cancel_won = True
            elif outcome == CancelOutcome.unknown:
                logger.warning(
                    "Proceeding with local cancellation for run %s after durable cancel persistence failed",
                    run_id,
                )
            elif outcome != CancelOutcome.lease_valid_elsewhere:
                return outcome

        worker_finalizes = False
        async with self._lock:
            record = self._runs.get(run_id)
            if record is not None:
                if record.status == RunStatus.interrupted or record.abort_event.is_set():
                    return CancelOutcome.cancelled
                if record.status not in (RunStatus.pending, RunStatus.running) and not record.scheduled_goal_cleanup_pending:
                    return CancelOutcome.cancelled if durable_cancel_won else CancelOutcome.not_cancellable
                record.abort_action = action
                record.abort_event.set()
                task_active = record.task is not None and not record.task.done()
                record.finalizing = task_active
                worker_finalizes = task_active and self._store is not None and getattr(self._store, "supports_terminal_recovery", False) is True
                record.rollback_finalization_pending = action == "rollback" and task_active
                if task_active and record.status == RunStatus.running:
                    record.task.cancel()
                record.status = RunStatus.interrupted
                record.terminal_status_persisted = self._store is None
                record.updated_at = _now_iso()

        # Persist outside the lock so store calls don't block other mutations.
        if record is not None:
            if worker_finalizes:
                # The durable cancellation is already recorded. Keep its row
                # active while the worker drains rollback/checkpoint/receipt
                # writes; an eager interrupted acknowledgement would race and
                # overwrite the worker's staged rollback outcome.
                logger.info("Run %s cancellation signalled; live worker owns terminal settlement (action=%s)", run_id, action)
                return CancelOutcome.cancelled
            if record.scheduled_goal_cleanup_pending:
                # The worker must remove its scheduled goal before releasing
                # the real durable slot. Abort/request state already records
                # cancellation; ordinary runs retain immediate persistence.
                return CancelOutcome.cancelled
            persisted = await self._persist_status(record, RunStatus.interrupted)
            async with self._lock:
                if self._runs.get(run_id) is record:
                    record.terminal_status_persisted = self._has_final_terminal_ack(record) if self.heartbeat_enabled and self._store is not None else persisted
            if not persisted and self._store is not None:
                # ``_persist_status`` already fetched ``existing`` internally;
                # re-check the store to see if a peer takeover flipped the
                # row to ``error`` between our in-memory cancel and the
                # guarded ``update_status``. If so, surface ``taken_over``
                # so the client sees a status consistent with the store.
                try:
                    existing = await run_capture_io(lambda: self._store.get(run_id))
                except CaptureIOClosed:
                    raise
                except Exception:
                    existing = None
                if existing is not None and existing.get("status") == "error":
                    # The in-memory ``record.status`` is still ``interrupted``
                    # (set under the lock above) while the store row is now
                    # ``error``.  This transient staleness is harmless: the
                    # ``_persist_status`` guard prevents the late finalisation
                    # write from overwriting the takeover, and the store is the
                    # authoritative source for subsequent reads.
                    logger.info("Run %s local cancel superseded by peer takeover", run_id)
                    return CancelOutcome.taken_over
            logger.info("Run %s cancelled (action=%s)", run_id, action)
            return CancelOutcome.cancelled

        if durable_cancel_won:
            return CancelOutcome.cancelled

        # ------------------------------------------------------------------
        # Non-local path — no in-memory record, must consult the store.
        # ------------------------------------------------------------------

        if not self.heartbeat_enabled:
            return CancelOutcome.not_active_locally

        if self._store is None:
            return CancelOutcome.unknown

        try:
            row = await run_capture_io(lambda: self._store.get(run_id))
        except CaptureIOClosed:
            raise
        except Exception:
            logger.warning("Failed to fetch run %s from store during cancel", run_id, exc_info=True)
            return CancelOutcome.unknown

        if row is None:
            return CancelOutcome.unknown

        store_status = row.get("status")
        if store_status == "interrupted":
            return CancelOutcome.requested
        if store_status not in ("pending", "running"):
            return CancelOutcome.not_cancellable

        grace_seconds = self.grace_seconds
        lease_expires_at: str | None = row.get("lease_expires_at")

        if not is_lease_expired(lease_expires_at, grace_seconds=grace_seconds):
            return await self._request_remote_cancel(run_id, action=action)

        take_over_msg = f"Run reclaimed by worker {self._worker_id}: the owning worker ({row.get('owner_worker_id') or 'unknown'}) stopped renewing its lease and is presumed dead."
        try:
            taken = await self._call_store_with_retry(
                "claim_for_takeover",
                run_id,
                lambda: self._store.claim_for_takeover(
                    run_id,
                    grace_seconds=grace_seconds,
                    error=take_over_msg,
                    # The owner is presumed dead, so a cross-worker consumer's
                    # only liveness edge is the durable row. Mark the takeover
                    # like periodic orphan recovery; without this stop reason
                    # SSE/`wait` subscribers that outlive the crashed owner
                    # loop on heartbeats forever (their fallback requires it).
                    stop_reason=ORPHAN_RECOVERY_STOP_REASON,
                ),
            )
        except CaptureIOClosed:
            raise
        except Exception:
            logger.warning("Take-over claim for run %s failed with exception", run_id, exc_info=True)
            return CancelOutcome.unknown

        if taken:
            if self._recovery_terminal_publisher is not None:
                target = self._recovery_terminal_publisher.authorize_takeover(
                    claimed=taken,
                    thread_id=row["thread_id"],
                    run_id=run_id,
                    user_id=row.get("user_id"),
                    operation_kind=row.get("operation_kind", "run"),
                )
                if target is not None:
                    await self._recovery_terminal_publisher.publish(target)
            logger.warning("Run %s taken over by worker %s (action=%s)", run_id, self._worker_id, action)
            return CancelOutcome.taken_over

        # The conditional UPDATE matched 0 rows. Two causes:
        #   (a) the owner renewed the lease → persist a cancellation request.
        #   (b) the row went terminal between our read and the claim
        #       (run finished, or another worker already took it over)
        #       → not_cancellable or taken_over.
        # Re-read to distinguish.
        try:
            fresh = await run_capture_io(lambda: self._store.get(run_id))
        except CaptureIOClosed:
            raise
        except Exception:
            fresh = None
        if fresh is None:
            return CancelOutcome.unknown
        fresh_status = fresh.get("status")
        if fresh_status not in ("pending", "running"):
            if fresh_status == "error":
                logger.info("Run %s takeover lost to another worker already at error", run_id)
                return CancelOutcome.taken_over
            return CancelOutcome.not_cancellable
        # Row is still active — lease was renewed by the owner while the
        # takeover raced. Notify that owner instead of exposing routing as 409.
        return await self._request_remote_cancel(run_id, action=action)

    def _compute_lease_expires_at(self) -> str | None:
        """Return the lease expiry ISO timestamp for a freshly created run.

        Returns ``None`` when heartbeat is disabled (single-worker mode) so
        reconciliation treats crashed runs as orphans (NULL lease) and
        reclaims them immediately, preserving pre-ownership behaviour.
        Multi-worker deployments enable heartbeat, which opts in to leases.
        """
        if self._run_ownership_config is None:
            return None
        if not self._run_ownership_config.heartbeat_enabled:
            return None
        lease_seconds = self._run_ownership_config.lease_seconds
        return (datetime.now(UTC) + timedelta(seconds=lease_seconds)).isoformat()

    async def lookup_idempotent_run(self, thread_id: str, key: str, *, user_id: str) -> RunRecord | None:
        """Read a detached durable key snapshot without changing admission state."""
        if any(type(value) is not str or not value for value in (thread_id, key, user_id)) or len(key) > 255:
            raise ValueError("Invalid run lookup scope")
        if self._store is None:
            raise RunStoreUnavailable()
        row = await await_drained(run_capture_io(lambda: self._store.get_by_idempotency_key(key)))
        if row is None:
            return None
        if not isinstance(row, Mapping) or row.get("idempotency_key") != key or any(type(row.get(name)) is not str or not row[name] for name in ("run_id", "thread_id", "user_id")):
            raise ValueError("Invalid run lookup result")
        if row["thread_id"] != thread_id or row["user_id"] != user_id:
            return None
        if row.get("operation_kind") != ThreadOperationKind.run.value or type(row.get("status")) is not str or not isinstance(row.get("metadata"), Mapping):
            raise ValueError("Invalid run lookup result")
        for name in ("created_at", "updated_at"):
            value = row.get(name)
            if type(value) is not str or datetime.fromisoformat(value).utcoffset() is None:
                raise ValueError("Invalid run lookup timestamp")
        # The memory store returns its own nested mappings. Neither response
        # serialization nor a caller may mutate that durable/live source.
        return self._strict_record_from_store(deepcopy(row))

    async def find_idempotent_run(self, thread_id: str, key: str, *, user_id: str | None = None) -> RunRecord | None:
        """Read a recorded retry without admitting or registering a peer run."""
        user_id = _resolve_record_user_id(user_id)

        def validated(record):
            if record.thread_id != thread_id or record.user_id != user_id:
                raise RuntimeError("Run idempotency key resolved to a different thread or user")
            record.idempotency_reused = True
            return record

        async with self._lock:
            for record in self._runs.values():
                if record.idempotency_key == key:
                    return validated(record)
        row = await run_capture_io(lambda: self._store.get_by_idempotency_key(key)) if self._store is not None else None
        # A same-worker admission may have committed while the store read
        # yielded. Preserve its task/stream identity instead of a peer handle.
        async with self._lock:
            for record in self._runs.values():
                if record.idempotency_key == key:
                    return validated(record)
        return validated(self._record_from_store(row)) if row is not None else None

    async def create_or_reject(
        self,
        thread_id: str,
        assistant_id: str | None = None,
        *,
        on_disconnect: DisconnectMode = DisconnectMode.cancel,
        metadata: dict | None = None,
        kwargs: dict | None = None,
        multitask_strategy: str = "reject",
        model_name: str | None = None,
        user_id: str | None = None,
        idempotency_key: str | None = None,
        evidence_origin: str = "unknown",
        evidence_agent_id: str | None = None,
        idempotency_request: dict[str, Any] | None = None,
        run_id: str | None = None,
        execution_fence=None,
    ) -> RunRecord:
        """Atomically admit a normal agent run for a thread."""
        captured = self.supports_output_receipts and capture_from_kwargs(kwargs or {}) is not None
        lifetime = self._new_capture_lifetime() if captured else None
        registration = None

        def witnessed(record, stage):
            nonlocal registration
            registration = (record, stage)
            if lifetime is not None and stage is not _AdmissionStage.reused:
                self._associate_capture(lifetime, record)

        admission = self._admit_thread_operation(
            thread_id,
            assistant_id,
            operation_kind=ThreadOperationKind.run,
            on_disconnect=on_disconnect,
            metadata=metadata,
            kwargs=kwargs,
            multitask_strategy=multitask_strategy,
            model_name=model_name,
            user_id=user_id,
            idempotency_key=idempotency_key,
            evidence_origin=evidence_origin,
            evidence_agent_id=evidence_agent_id,
            idempotency_request=idempotency_request,
            run_id=run_id,
            execution_fence=execution_fence,
            **({"_registration": witnessed} if captured else {}),
        )
        if not captured:
            return await admission

        async def own_admission():
            failed = False
            refused = False
            invoked = False
            try:
                with self._capture_control(lifetime=lifetime):
                    invoked = True
                    return await admission
            except CaptureIOClosed:
                raise
            except Exception as error:
                failed = True
                # A definite refusal left no row, so nothing can come back for this.
                refused = isinstance(error, ConflictError)
                if registration is not None and registration[1] is not _AdmissionStage.reused and not lifetime.owner.retired:
                    with self._capture_control(lifetime=lifetime):
                        if registration[1] is _AdmissionStage.constructed:
                            await self._recover_capture_admission(registration[0], witnessed)
                        else:
                            await self._close_unstarted_capture(registration[0], PrestartOutcome(RunStatus.error, "failed", error="Run admission failed before execution."))
                raise
            finally:
                if not invoked:
                    admission.close()
                reused = registration is not None and registration[1] is _AdmissionStage.reused
                if failed or reused:
                    await self._finish_capture_lifetime(lifetime, discard=reused or refused)

        try:
            return await await_drained(own_admission())
        except asyncio.CancelledError:
            # A committed SQL result must reach local registration before the
            # caller can disappear. This owner is only for actual run admission;
            # checkpoint self-handoff retains its original worker-task identity.
            if registration is not None and registration[1] is _AdmissionStage.registered:
                try:
                    if not lifetime.owner.retired:
                        with self._capture_control(lifetime=lifetime):
                            await self._close_cancelled_admission(registration[0])
                finally:
                    await await_drained(self._finish_capture_lifetime(lifetime))
            raise

    async def _recover_capture_admission(self, record: RunRecord, witnessed: Callable[[RunRecord, _AdmissionStage], None]) -> None:
        """Recover only this original constructed UUID after an unknown result."""
        try:
            row = await await_drained(run_capture_io(lambda: self._store.get(record.run_id, user_id=None)))
            if (
                not isinstance(row, Mapping)
                or (row.get("run_id"), row.get("thread_id"), row.get("user_id"), row.get("operation_kind"), row.get("owner_worker_id"), row.get("idempotency_key"))
                != (record.run_id, record.thread_id, record.user_id, "run", self.worker_id, record.idempotency_key)
                or record.owner_worker_id != self.worker_id
                or row.get("status") not in {"pending", "running"}
                or row.get("cancel_action") not in {None, "interrupt", "rollback"}
                or capture_from_kwargs(row.get("kwargs")) != capture_from_kwargs(record.kwargs)
                or receipt_from_kwargs(row.get("kwargs")) is not None
                or inventory_evidence_from_kwargs(row.get("kwargs")) is not None
                or type(row["kwargs"].get(OUTPUT_CAPTURE_DISCONNECT_KEY)) is not str
                or row["kwargs"][OUTPUT_CAPTURE_DISCONNECT_KEY] != record.on_disconnect.value
            ):
                return
            if self.heartbeat_enabled:
                lease = self._parse_lease_deadline(row.get("lease_expires_at"))
                if lease is None or lease <= datetime.now(UTC):
                    return
            async with record.start_lock:
                async with self._lock:
                    current = self._runs.get(record.run_id)
                    if record.startup_phase is not RunStartupPhase.unstarted or current is not None and current is not record:
                        return
                    self._runs[record.run_id] = record
                    self._index_run_locked(record)
                    witnessed(record, _AdmissionStage.registered)
                await self._settle_prestart_locked(record, PrestartOutcome(RunStatus.error, "failed", error="Run admission failed before execution."))
        except Exception:
            # Preserve the original admission exception. A missing/uncertain or
            # foreign row never grants upsert or terminal-write authority.
            logger.warning("Captured admission recovery could not confirm the original row")

    async def _close_cancelled_admission(self, record: RunRecord) -> None:
        """Terminalize an unseen replacement and confirm its durable state."""
        if self._captures_output(record):
            if not await self._close_unstarted_capture(record, PrestartOutcome(RunStatus.interrupted, "cancelled", stop_reason=record.stop_reason)):
                raise OutputEvidenceError()
            return
        await self.cancel(record.run_id)
        if self._store is None or (self.heartbeat_enabled and record.ownership_lost):
            return

        stored = await self._call_store_with_retry(
            "verify cancelled admission",
            record.run_id,
            lambda: self._store.get(record.run_id, user_id=record.user_id),
        )
        active_statuses = (RunStatus.pending.value, RunStatus.running.value)
        if stored is not None and stored.get("status") in active_statuses:
            # `_persist_status` is deliberately best-effort. This compensation
            # path needs a strict second CAS attempt because the caller never
            # receives the record and no worker can attach after it returns.
            # A peer terminal transition wins the CAS and is preserved below.
            if self.heartbeat_enabled:
                await self._persist_status(record, RunStatus.interrupted)
                if record.ownership_lost:
                    return
            else:
                await self._call_store_with_retry(
                    "terminalize cancelled admission",
                    record.run_id,
                    lambda: self._store.update_status(record.run_id, RunStatus.interrupted.value),
                )
            stored = await self._call_store_with_retry(
                "verify terminal cancelled admission",
                record.run_id,
                lambda: self._store.get(record.run_id, user_id=record.user_id),
            )
            if stored is not None and stored.get("status") in active_statuses:
                raise RuntimeError(f"Cancelled admission {record.run_id} remains active in the run store")

        if stored is None:
            async with self._lock:
                if self._runs.get(record.run_id) is record:
                    self._runs.pop(record.run_id, None)
                    self._unindex_run_locked(record.run_id, record.thread_id)
            return

        stored_status = RunStatus(stored.get("status") or RunStatus.pending.value)
        async with self._lock:
            if self._runs.get(record.run_id) is record:
                record.status = stored_status
                record.terminal_status_persisted = stored_status not in (
                    RunStatus.pending,
                    RunStatus.running,
                )
                record.error = stored.get("error")
                record.stop_reason = stored.get("stop_reason")
                record.updated_at = _now_iso()

    async def _admit_thread_operation(
        self,
        thread_id: str,
        assistant_id: str | None = None,
        *,
        operation_kind: ThreadOperationKind,
        on_disconnect: DisconnectMode = DisconnectMode.cancel,
        metadata: dict | None = None,
        kwargs: dict | None = None,
        multitask_strategy: str = "reject",
        model_name: str | None = None,
        user_id: str | None = None,
        idempotency_key: str | None = None,
        finalizing_run: RunRecord | None = None,
        _registration: Callable[[RunRecord, _AdmissionStage], None] | None = None,
        evidence_origin: str = "unknown",
        evidence_agent_id: str | None = None,
        idempotency_request: dict[str, Any] | None = None,
        run_id: str | None = None,
        execution_fence=None,
    ) -> RunRecord:
        """Atomically check for inflight runs and create a new one.

        For ``reject`` strategy, raises ``ConflictError`` if thread
        already has a pending/running run.  For ``interrupt``/``rollback``,
        cancels inflight runs before creating.

        Lock ordering invariant: the local ``self._lock`` is held across
        the local check, the store insert, and the local register, so the
        store insert can never succeed while a same-worker ConflictError
        is about to fire (which would leak a pending row in the store).
        Cross-process contention is resolved at the store level via a
        partial unique index on ``(thread_id) WHERE status IN
        ('pending','running')``.
        """
        kwargs = _new_run_kwargs(kwargs, on_disconnect=on_disconnect)
        if (intent := capture_from_kwargs(kwargs)) is not None and intent.version == 2 and not self.supports_output_inventory:
            raise OutputEvidenceError()
        from deerflow.runtime.assembly_evidence import EVIDENCE_KEY

        self._validate_evidence_origin(evidence_origin)
        run_id = run_id or str(uuid.uuid4())
        now = _now_iso()
        # Resolve before the idempotency checks below compare it with stored rows.
        user_id = _resolve_record_user_id(user_id)

        _supported_strategies = ("reject", "interrupt", "rollback")
        if multitask_strategy not in _supported_strategies:
            raise UnsupportedStrategyError(f"Multitask strategy '{multitask_strategy}' is not yet supported. Supported strategies: {', '.join(_supported_strategies)}")

        lease_expires_at = self._compute_lease_expires_at()
        grace_seconds = self._run_ownership_config.grace_seconds if self._run_ownership_config else 10

        if execution_fence is not None and (self._store is None or not _store_accepts_idempotency_parameter(self._store, "execution_fence") or multitask_strategy != "reject"):
            raise RunIdempotencyUnsupported("The configured RunStore does not support execution fencing")

        interrupted_records: list[RunRecord] = []
        record = RunRecord(
            run_id=run_id,
            thread_id=thread_id,
            assistant_id=assistant_id,
            status=RunStatus.pending,
            on_disconnect=on_disconnect,
            operation_kind=operation_kind,
            startup_phase=RunStartupPhase.unstarted if operation_kind is ThreadOperationKind.run else RunStartupPhase.unknown,
            multitask_strategy=multitask_strategy,
            metadata={key: value for key, value in (metadata or {}).items() if key != EVIDENCE_KEY},
            kwargs=kwargs or {},
            user_id=user_id,
            created_at=now,
            updated_at=now,
            model_name=model_name,
            owner_worker_id=self._worker_id,
            lease_expires_at=lease_expires_at,
            idempotency_key=idempotency_key,
            evidence_origin=evidence_origin,
            evidence_agent_id=evidence_agent_id,
            idempotency_request=idempotency_request,
        )

        async with self._lock:
            if finalizing_run is not None:
                valid = (
                    operation_kind is ThreadOperationKind.checkpoint_write
                    and multitask_strategy == "reject"
                    and idempotency_key is None
                    and self._runs.get(finalizing_run.run_id) is finalizing_run
                    and not finalizing_run.store_only
                    and finalizing_run.operation_kind is ThreadOperationKind.run
                    and finalizing_run.thread_id == thread_id
                    and finalizing_run.user_id == user_id
                    and finalizing_run.owner_worker_id == self.worker_id
                    and finalizing_run.status is RunStatus.success
                    and finalizing_run.terminal_confirmed
                    and finalizing_run.finalizing
                    and not finalizing_run.ownership_lost
                    and not finalizing_run.abort_event.is_set()
                    and asyncio.current_task() in {finalizing_run.task, finalizing_run.finalization_task} - {None}
                )
                try:
                    saved = receipt_from_kwargs(finalizing_run.kwargs)
                    valid = valid and self.supports_output_receipts and capture_from_kwargs(finalizing_run.kwargs) is not None and saved is not None
                    if valid:
                        validate_receipt_status(saved, finalizing_run.status)
                        if capture_from_kwargs(finalizing_run.kwargs).version == 2:
                            validate_inventory_pair(saved, inventory_evidence_from_kwargs(finalizing_run.kwargs), status=finalizing_run.status)
                        valid = (saved.run_id, saved.thread_id, saved.owner_user_id) == (finalizing_run.run_id, thread_id, user_id)
                except OutputEvidenceError:
                    valid = False
                if not valid:
                    raise ConflictError("The finalizing run cannot reserve this checkpoint write")
            if idempotency_key is not None and execution_fence is None:
                for existing in self._runs.values():
                    if existing.idempotency_key != idempotency_key:
                        continue
                    if existing.thread_id != thread_id or existing.user_id != user_id:
                        raise RunIdempotencyKeyMismatch("Run idempotency key resolved to a different thread or user")
                    if _registration is not None:
                        _registration(existing, _AdmissionStage.reused)
                    existing.idempotency_reused = True
                    return existing

            def reuse_idempotent_run(conflict: RunIdempotencyConflict) -> RunRecord:
                # A locally held record for this key already returned above, so
                # the conflicting row belongs to a peer or to a run this worker
                # has cleaned up. Return a store-only handle without registering
                # it: nothing here finalizes or cleans up that record, so a
                # registered copy would keep its admission-time status, reject
                # later admissions for the thread, and shadow the durable row
                # for get(), cancel(), and orphan reconciliation.
                existing = self._record_from_store(conflict.existing)
                if existing.thread_id != thread_id or existing.user_id != user_id:
                    raise RunIdempotencyKeyMismatch("Run idempotency key resolved to a different thread or user") from conflict
                existing.idempotency_reused = True
                if _registration is not None:
                    _registration(existing, _AdmissionStage.reused)
                return existing

            # 1) Local inflight check (same-worker guard; cross-worker is the
            #    store's partial unique index below).
            local_inflight = [r for r in self._thread_records_locked(thread_id) if r is not finalizing_run and (r.status in (RunStatus.pending, RunStatus.running) or r.finalizing or r.scheduled_goal_cleanup_pending)]

            if multitask_strategy in ("interrupt", "rollback") and any(self._captures_output(prior) for prior in local_inflight):
                if idempotency_key is not None and self._store is not None:
                    existing = await await_drained(run_capture_io(lambda: self._store.get_by_idempotency_key(idempotency_key)))
                    if existing is not None:
                        return reuse_idempotent_run(RunIdempotencyConflict(existing))
                raise ConflictError(f"Thread {thread_id} has an active captured run")

            if any(r.scheduled_goal_cleanup_pending for r in local_inflight):
                raise ConflictError(f"Thread {thread_id} has a scheduled goal awaiting finalization")

            if multitask_strategy in ("interrupt", "rollback") and any(record.operation_kind != ThreadOperationKind.run for record in local_inflight):
                raise ConflictError(f"Thread {thread_id} has an active checkpoint write")

            if multitask_strategy == "reject" and local_inflight:
                raise ConflictError(f"Thread {thread_id} already has an active run")

            if multitask_strategy in ("interrupt", "rollback") and local_inflight:
                logger.info(
                    "Preparing to cancel %d inflight run(s) on thread %s (strategy=%s)",
                    len(local_inflight),
                    thread_id,
                    multitask_strategy,
                )

            # 2) Persist to store while still holding the local lock. The
            #    store is the source of truth for cross-process atomicity.
            if self._store is not None:
                if idempotency_request is not None and not _store_accepts_idempotency_parameter(self._store, "idempotency_request"):
                    raise RunIdempotencyUnsupported("The configured RunStore does not support keyed resume idempotency")
                if idempotency_key is not None and not _store_accepts_idempotency_parameter(self._store, "idempotency_key"):
                    raise RunIdempotencyUnsupported("The configured RunStore does not support keyed run idempotency")
                if _registration is not None:
                    _registration(record, _AdmissionStage.constructed)
                if multitask_strategy == "reject":
                    create_kwargs = {
                        "run_id": run_id,
                        "thread_id": thread_id,
                        "owner_worker_id": self._worker_id,
                        "lease_expires_at": lease_expires_at,
                        "operation_kind": operation_kind.value,
                        "multitask_strategy": "reject",
                        "assistant_id": assistant_id,
                        "user_id": user_id,
                        "model_name": model_name,
                        "metadata": metadata,
                        "kwargs": kwargs,
                        "created_at": now,
                        "grace_seconds": grace_seconds,
                    }
                    if execution_fence is not None:
                        create_kwargs["execution_fence"] = execution_fence
                    if idempotency_key is not None:
                        create_kwargs["idempotency_key"] = idempotency_key
                    if evidence_origin != "unknown":
                        create_kwargs["evidence_origin"] = evidence_origin
                    if evidence_agent_id is not None:
                        create_kwargs["evidence_agent_id"] = evidence_agent_id
                    if idempotency_request is not None:
                        create_kwargs["idempotency_request"] = idempotency_request
                    try:
                        await self._call_store_with_retry(
                            "create_thread_operation_atomic",
                            run_id,
                            lambda: self._store.create_thread_operation_atomic(**create_kwargs),
                        )
                    except RunIdempotencyConflict as exc:
                        return reuse_idempotent_run(exc)
                    except ConflictError:
                        raise
                    except CaptureIOClosed:
                        raise
                    except Exception as exc:
                        if _is_unique_violation(exc):
                            raise ConflictError(f"Thread {thread_id} already has an active run") from exc
                        raise
                else:
                    create_kwargs = {
                        "run_id": run_id,
                        "thread_id": thread_id,
                        "owner_worker_id": self._worker_id,
                        "lease_expires_at": lease_expires_at,
                        "operation_kind": operation_kind.value,
                        "multitask_strategy": multitask_strategy,
                        "assistant_id": assistant_id,
                        "user_id": user_id,
                        "model_name": model_name,
                        "metadata": metadata,
                        "kwargs": kwargs,
                        "created_at": now,
                        "grace_seconds": grace_seconds,
                    }
                    if idempotency_key is not None:
                        create_kwargs["idempotency_key"] = idempotency_key
                    if evidence_origin != "unknown":
                        create_kwargs["evidence_origin"] = evidence_origin
                    if evidence_agent_id is not None:
                        create_kwargs["evidence_agent_id"] = evidence_agent_id
                    if idempotency_request is not None:
                        create_kwargs["idempotency_request"] = idempotency_request
                    # Interrupt / rollback: store-side claim + insert in one
                    # transaction. Retry on IntegrityError in case another
                    # worker races us between our SELECT FOR UPDATE and INSERT.
                    max_retries = 3
                    for attempt in range(max_retries):
                        try:
                            await self._atomic_replacement(
                                local_inflight,
                                multitask_strategy,
                                lambda: self._call_store_with_retry(
                                    "create_thread_operation_atomic",
                                    run_id,
                                    lambda: self._store.create_thread_operation_atomic(**create_kwargs),
                                ),
                            )
                            break
                        except RunIdempotencyConflict as exc:
                            return reuse_idempotent_run(exc)
                        except CaptureIOClosed:
                            raise
                        except Exception as exc:
                            is_unique = _is_unique_violation(exc)
                            if is_unique and attempt + 1 < max_retries:
                                continue
                            if is_unique:
                                # Exhausted retries on unique violation — surface
                                # as ConflictError to match the reject branch's
                                # contract (409, not 500). Same root cause: another
                                # worker won the race for this thread.
                                raise ConflictError(f"Thread {thread_id} already has an active run") from exc
                            raise
                    # ``create_thread_operation_atomic`` already marked any claimed store
                    # rows as interrupted in the same transaction; no extra
                    # store write is needed for them.

            # 3) Only now safe to register locally — store insert succeeded.
            self._runs[run_id] = record
            self._index_run_locked(record)
            if _registration is not None:
                _registration(record, _AdmissionStage.registered)
            self._arm_lease_deadline(record)

            # 4) Cancel local in-memory inflight (interrupt/rollback). The
            #    store-side counterparts were already cancelled in step 2.
            if self._store is None and multitask_strategy in ("interrupt", "rollback"):
                for r in local_inflight:
                    if r.finalizing:
                        continue
                    r.abort_action = multitask_strategy
                    r.abort_event.set()
                    task_active = r.task is not None and not r.task.done()
                    r.finalizing = task_active
                    if task_active:
                        r.task.cancel()
                    r.status = RunStatus.interrupted
                    r.terminal_status_persisted = self._store is None
                    r.updated_at = now
                    interrupted_records.append(r)

        # Outside the lock: persist interrupted status for locally-cancelled
        # runs. Store-side claimed rows are already finalised. Cancellation at
        # this point happens after the replacement was admitted, so close that
        # new run before propagating cancellation to the caller.
        try:
            # Preserve the admission cancellation boundary without repeating
            # a terminal write already acknowledged by the atomic transaction.
            await asyncio.sleep(0)
            for interrupted_record in interrupted_records:
                persisted = await self._persist_status(
                    interrupted_record,
                    RunStatus.interrupted,
                )
                async with self._lock:
                    if self._runs.get(interrupted_record.run_id) is interrupted_record:
                        interrupted_record.terminal_status_persisted = persisted
        except asyncio.CancelledError:
            cleanup = asyncio.create_task(self._close_cancelled_admission(record))
            cleanup.set_name(f"deerflow-close-cancelled-admission-{record.run_id}")
            while not cleanup.done():
                try:
                    await asyncio.shield(cleanup)
                except asyncio.CancelledError:
                    pass
                except CaptureIOClosed:
                    raise
                except Exception:
                    break
            try:
                cleanup.result()
            except asyncio.CancelledError:
                logger.error("Cancelled admission cleanup task was itself cancelled for run %s", record.run_id)
            except CaptureIOClosed:
                raise
            except Exception:
                logger.exception("Failed to close run %s after admission was cancelled", record.run_id)
            raise

        logger.info("Run created: run_id=%s thread_id=%s", run_id, thread_id)
        return record

    @asynccontextmanager
    async def reserve_thread_operation(
        self,
        thread_id: str,
        *,
        kind: ThreadOperationKind,
        user_id: str | None = None,
        finalizing_run: RunRecord | None = None,
    ) -> AsyncIterator[None]:
        """Hold exclusive durable admission for a non-run thread operation.

        The reservation is a short-lived pending row, so the same durable
        uniqueness constraint used by ``create_or_reject`` closes both sides of
        the race across Gateway workers.
        """
        if kind == ThreadOperationKind.run:
            raise ValueError("Normal runs must be admitted with create_or_reject()")
        owner = current_capture_io_owner()
        release = None
        if owner is not None:
            owner.check_open()
            if kind is not ThreadOperationKind.checkpoint_write or self._store is None:
                raise CaptureIOUnsupported() from None
            release = _CheckpointReservationRelease(self._store)
            owner.register_resource(release)
        record = None
        try:
            try:
                record = await self._admit_thread_operation(
                    thread_id,
                    operation_kind=kind,
                    multitask_strategy="reject",
                    user_id=user_id,
                    finalizing_run=finalizing_run,
                )
                if release is not None:
                    row = await run_capture_io(lambda: self._store.get(record.run_id, user_id=record.user_id))
                    owner.check_open()
                    if (
                        not isinstance(row, dict)
                        or (row.get("run_id"), row.get("user_id"), row.get("thread_id"), row.get("operation_kind"), row.get("owner_worker_id"), row.get("status"), row.get("cancel_action"))
                        != (record.run_id, record.user_id, thread_id, "checkpoint_write", self.worker_id, "pending", None)
                        or type(row.get("change_seq")) is not int
                        or row["change_seq"] < 0
                    ):
                        raise CaptureIOCleanupError() from None
                    release.scope = _ReservationScope(record.run_id, record.user_id, thread_id, self.worker_id, row["change_seq"])
            except ConflictError:
                # The atomic admission refused without constructing a row.
                raise
            except BaseException:
                if release is not None:
                    release.uncertain = True
                raise
            finally:
                if release is not None:
                    release.ready.set()
            reservation_task = asyncio.current_task()
            if reservation_task is None:
                raise RuntimeError("Thread operation reservation requires an active asyncio task")
            lease_lost = True
            async with self._lock:
                if self._runs.get(record.run_id) is record:
                    record.task = reservation_task
                    lease_lost = record.abort_event.is_set()
            if lease_lost:
                raise asyncio.CancelledError()
            yield
        except asyncio.CancelledError:
            if record is not None and record.abort_event.is_set():
                raise ConflictError(f"Thread {thread_id} reservation lease was lost") from None
            raise
        finally:
            try:
                if release is not None:
                    try:
                        # No retry or new ticket after retirement: only the
                        # already-confirmed exact release capability remains.
                        await owner.drain()
                        await release.drain()
                    except BaseException:
                        owner.retire()
                        raise
                    else:
                        owner.unregister_resource(release)
                elif record is not None and self._store is not None:
                    try:
                        await self._call_store_with_retry(
                            "release thread operation",
                            record.run_id,
                            lambda: self._store.delete_thread_operation(record.run_id, user_id=record.user_id),
                        )
                    except CaptureIOClosed:
                        raise
                    except Exception:
                        logger.warning(
                            "Failed to release persisted thread operation %s; leaving it for orphan reconciliation",
                            record.run_id,
                            exc_info=True,
                        )
            finally:
                async with self._lock:
                    removed = self._runs.pop(record.run_id, None) if record is not None else None
                    if removed is not None:
                        self._unindex_run_locked(record.run_id, removed.thread_id)

    async def reconcile_orphaned_inflight_runs(
        self,
        *,
        error: str,
        before: str | None = None,
        stop_reason: str | None = None,
    ) -> list[RunRecord]:
        async with self._maintenance_control():
            return await self._reconcile_orphaned_inflight_runs(error=error, before=before, stop_reason=stop_reason)

    async def _reconcile_orphaned_inflight_runs(
        self,
        *,
        error: str,
        before: str | None = None,
        stop_reason: str | None = None,
    ) -> list[RunRecord]:
        """Mark persisted active runs as failed when their lease has expired.

        In multi-worker deployments (Postgres), a run owned by Worker A that
        still shows ``pending`` / ``running`` after its lease expired means
        Worker A crashed or was partitioned. This worker (B) can safely claim
        and error it out because the lease was not renewed.

        Rows with a still-valid lease are skipped — they belong to another live
        worker. Rows with a NULL lease (pre-ownership data) are reclaimed as
        well, matching the original single-worker recovery behaviour. The
        candidate scan is only an optimization: each row is claimed with a
        lease-aware conditional update so a heartbeat renewal after the scan
        always wins over reconciliation.
        """
        if self._store is None:
            return []
        grace_seconds = self._run_ownership_config.grace_seconds if self._run_ownership_config else 10
        recover_evidence = getattr(self._store, "recover_unsealed_evidence", None)
        if callable(recover_evidence):
            try:
                evidence_before = before or _now_iso()
                while True:
                    async with self._lock:
                        local_finalizing = tuple(r.run_id for r in self._runs.values() if not r.ownership_lost and (r.finalizing or (r.task is not None and not r.task.done())))
                    recovered_count = await self._call_store_with_retry(
                        "recover_unsealed_evidence",
                        "*",
                        lambda: recover_evidence(before=evidence_before, grace_seconds=grace_seconds, exclude_run_ids=local_finalizing),
                    )
                    # Drain at startup too: single-worker mode has no heartbeat.
                    if not recovered_count:
                        break
            except Exception:
                # Evidence availability must not prevent normal run recovery.
                logger.warning("Failed to recover orphaned completed-run evidence", exc_info=True)
        try:
            rows = await self._call_store_with_retry(
                "list_inflight_with_expired_lease",
                "*",
                lambda: self._store.list_inflight_with_expired_lease(before=before, grace_seconds=grace_seconds),
            )
        except CaptureIOClosed:
            raise
        except Exception:
            logger.warning("Failed to list orphaned inflight runs for reconciliation", exc_info=True)
            return []

        recovered: list[RunRecord] = []
        now = _now_iso()
        for row in rows:
            try:
                record = self._record_from_store(row)
            except CaptureIOClosed:
                raise
            except Exception:
                logger.warning("Failed to map orphaned run row during reconciliation", exc_info=True)
                continue

            async with self._lock:
                live_record = self._runs.get(record.run_id)
                if live_record is not None and (live_record.status in (RunStatus.pending, RunStatus.running) or live_record.finalizing and self._captures_output(live_record)):
                    # Still owned by a local task — skip
                    continue

            try:
                claimed = await self._call_store_with_retry(
                    "claim_for_takeover",
                    record.run_id,
                    lambda: self._store.claim_for_takeover(
                        record.run_id,
                        grace_seconds=grace_seconds,
                        error=error,
                        stop_reason=stop_reason,
                    ),
                )
            except CaptureIOClosed:
                raise
            except Exception:
                logger.warning("Failed to claim orphaned run %s for reconciliation", record.run_id, exc_info=True)
                continue
            if not claimed:
                logger.info(
                    "Skipped orphaned run %s recovery because the takeover claim no longer matched",
                    record.run_id,
                )
                continue
            record.status = RunStatus.error
            record.terminal_status_persisted = True
            record.error = error
            record.stop_reason = stop_reason
            record.updated_at = now
            if record.operation_kind == ThreadOperationKind.run:
                # The atomic takeover above must win before writing a zero-delivery
                # receipt; otherwise a stale scan could race a heartbeat renewal and
                # permanently overwrite a live run's later detailed receipt. The
                # receipt remains best-effort, matching normal terminal delivery
                # when its event store is unavailable.
                await self._ensure_delivery_receipt(record)
                publisher = self._recovery_terminal_publisher
                if publisher is not None:
                    target = publisher.authorize_takeover(
                        claimed=claimed,
                        thread_id=record.thread_id,
                        run_id=record.run_id,
                        user_id=record.user_id,
                        operation_kind=record.operation_kind.value,
                    )
                    if target is not None:
                        await publisher.publish(target)
                recovered.append(record)

        if recovered:
            logger.warning("Recovered %d orphaned inflight run(s) as error", len(recovered))
        return recovered

    async def has_inflight(self, thread_id: str) -> bool:
        """Return ``True`` if *thread_id* has a pending or running run."""
        async with self._lock:
            return any(r.operation_kind == ThreadOperationKind.run and (r.status in (RunStatus.pending, RunStatus.running) or r.finalizing) for r in self._thread_records_locked(thread_id))

    async def cleanup(self, run_id: str, *, delay: float = 300) -> None:
        """Remove a run record after an optional delay.

        Eviction is only safe when a ``RunStore`` backs this manager: history
        then stays readable through the store fallback in ``get()`` /
        ``list_by_thread()``. Without one, dropping the record would erase the
        run's history entirely, so a store-less manager keeps the previous
        retain-forever behaviour and this returns immediately.
        """
        if self._store is None:
            return
        if delay > 0:
            await asyncio.sleep(delay)
        async with self._lock:
            record = self._runs.pop(run_id, None)
            if record is not None:
                self._unindex_run_locked(run_id, record.thread_id)
                lifetime = self._capture_lifetime(record)
                if lifetime is not None:
                    self._discard_evicted_capture(lifetime)
        logger.debug("Run record %s cleaned up", run_id)

    # ------------------------------------------------------------------
    # Lease heartbeat
    # ------------------------------------------------------------------

    @property
    def worker_id(self) -> str:
        """Return this worker's unique identifier."""
        return self._worker_id

    async def owns_active_admission(self, record: RunRecord) -> bool:
        """Confirm the existing run slot without holding a SQL writer transaction."""
        async with self._lock:
            if self._runs.get(record.run_id) is not record or record.ownership_lost:
                return False
        if self._store is None:
            return True
        row = await self._store.get(record.run_id, user_id=record.user_id)
        if row is None or row.get("thread_id") != record.thread_id or row.get("user_id") != record.user_id or row.get("status") not in ("pending", "running") or row.get("owner_worker_id") != record.owner_worker_id:
            return False
        if self.heartbeat_enabled:
            deadline = self._parse_lease_deadline(row.get("lease_expires_at"))
            if deadline is None or deadline <= datetime.now(UTC):
                await self._mark_ownership_lost(record, reason="Scheduled goal cleanup lost its active run lease.", require_active=False)
                return False
        return not record.ownership_lost

    async def scheduled_goal_source(self, thread_id: str, *, user_id: str, created_at: str, objective: str) -> RunRecord | None:
        """Resolve an exact durable scheduled-goal instance, never a history page."""
        from deerflow.runtime.goal import normalize_goal_objective

        if self._store is None:
            return None
        rows = await self._store.list_by_thread_created_at(thread_id, user_id=user_id, created_at=created_at)
        matches = []
        for row in rows:
            if row.get("thread_id") != thread_id or row.get("user_id") != user_id or canonical_run_created_at(row.get("created_at", "")) != canonical_run_created_at(created_at):
                continue
            metadata = row.get("metadata") or {}
            candidate = metadata.get("scheduled_goal_objective")
            if (
                row.get("status") not in {"success", "error", "timeout", "interrupted"}
                or row.get("operation_kind", "run") != "run"
                or not isinstance(metadata.get("scheduled_task_id"), str)
                or not metadata["scheduled_task_id"]
                or not isinstance(metadata.get("scheduled_task_run_id"), str)
                or not metadata["scheduled_task_run_id"]
                or not isinstance(candidate, str)
            ):
                continue
            if normalize_goal_objective(candidate) == objective:
                matches.append(row)
        if len(matches) > 1:
            raise RuntimeError("Scheduled goal source identity is ambiguous")
        return self._record_from_store(matches[0]) if matches else None

    @property
    def heartbeat_enabled(self) -> bool:
        """Return ``True`` when the heartbeat background task should run."""
        if self._run_ownership_config is None:
            return False
        return self._run_ownership_config.heartbeat_enabled

    @property
    def grace_seconds(self) -> int:
        """Return the configured grace seconds.

        All current callers are downstream of ``heartbeat_enabled``, which
        is False whenever ``_run_ownership_config`` is None.  The fallback
        matches the Pydantic model default and is defensive against future
        callers that might reach this property without that guard.
        """
        return self._run_ownership_config.grace_seconds if self._run_ownership_config else 10

    @staticmethod
    def _parse_lease_deadline(lease_expires_at: str | None) -> datetime | None:
        """Parse the last durably confirmed lease expiry.

        Missing or malformed deadlines are unsafe in heartbeat mode: the local
        worker has no bounded interval during which it can prove ownership.
        """
        if lease_expires_at is None:
            return None
        try:
            deadline = datetime.fromisoformat(lease_expires_at)
        except (TypeError, ValueError):
            return None
        if deadline.tzinfo is None:
            deadline = deadline.replace(tzinfo=UTC)
        return deadline

    async def _mark_ownership_lost(
        self,
        record: RunRecord,
        *,
        reason: str,
        require_active: bool = True,
        terminal_write_unconfirmed: bool = False,
    ) -> bool:
        """Fence one local run and cancel its execution task.

        No store write is attempted here: once the last confirmed lease has
        expired, this worker is no longer authorized to publish a terminal
        outcome. A peer reconciler owns durable terminalization.
        """
        return self._fence_local_record(record, reason=reason, require_active=require_active, terminal_write_unconfirmed=terminal_write_unconfirmed)

    def _fence_local_record(self, record: RunRecord, *, reason: str, require_active: bool = False, terminal_write_unconfirmed: bool = False) -> bool:
        # Event-loop-local state transition, with no await. In particular this
        # must not wait behind an admission holding _lock across Store I/O.
        task_to_cancel: asyncio.Task | None = None
        current = self._runs.get(record.run_id)
        if current is not record:
            return False
        if require_active:
            # A finalizing worker that has not confirmed its terminal still owns the row.
            if record.status not in (RunStatus.pending, RunStatus.running) and not (record.finalizing and not record.terminal_confirmed):
                return False
            if record.task is not None and record.task.done():
                return False
        if record.ownership_lost:
            if not terminal_write_unconfirmed:
                record.terminal_write_unconfirmed = False
            return True
        if self._has_final_terminal_ack(record):
            record.terminal_commit_pending = False
            return False
        self._clear_lease_deadline(record.run_id)
        record.ownership_lost = True
        record.terminal_confirmed = False
        record.terminal_write_unconfirmed = terminal_write_unconfirmed
        record.terminal_status_persisted = False
        # A fenced worker must not keep the peer-recoverable row alive.
        record.terminal_commit_pending = False
        record.abort_event.set()
        record.status = RunStatus.error
        record.error = reason
        record.updated_at = _now_iso()
        # Once the worker hands its terminal work to its own finalizer, that
        # finalizer is what is executing: stop it, not the worker awaiting it
        # (whose drain would re-raise the cancellation after finalization).
        finalizer = record.finalization_task if record.finalization_task is not None and not record.finalization_task.done() else None
        target = finalizer or record.task
        if target is not None and not target.done() and target is not asyncio.current_task():
            task_to_cancel = target

        if task_to_cancel is not None:
            task_to_cancel.cancel()
        logger.error("Run %s was fenced and local execution stopped: %s", record.run_id, reason)
        return True

    async def start_heartbeat(self) -> None:
        """Start the background lease-renewal task.

        No-op when ``heartbeat_enabled`` is ``False`` or the task is already running.
        """
        if not self.heartbeat_enabled:
            return
        if self._capture_admissions_sealed and self.supports_output_receipts:
            raise CaptureIOClosed() from None
        if self._heartbeat_task is not None and not self._heartbeat_task.done():
            return
        self._heartbeat_stop = asyncio.Event()
        task = asyncio.create_task(self._heartbeat_loop())
        task.set_name("deerflow-run-lease-heartbeat")
        self._heartbeat_task = task
        logger.info("Run lease heartbeat started for worker %s", self._worker_id)

    async def stop_heartbeat(self, *, timeout: float = 5.0) -> None:
        """Stop scheduling and boundedly drain owned lease checks and Store I/O."""
        deadline = asyncio.get_running_loop().time() + max(0.0, timeout)
        if self._heartbeat_stop is not None:
            self._heartbeat_stop.set()

        def owned_tasks() -> set[asyncio.Task]:
            tasks = set(self._lease_checks.values()) | set(self._lease_operations.values())
            if self._heartbeat_task is not None:
                tasks.add(self._heartbeat_task)
            return {task for task in tasks if not task.done()}

        tasks = owned_tasks()
        while tasks and asyncio.get_running_loop().time() < deadline:
            await asyncio.wait(tasks, timeout=max(0.0, deadline - asyncio.get_running_loop().time()))
            # A check/heartbeat already in progress can register child I/O
            # after the first snapshot. Include it in the same drain budget.
            tasks = owned_tasks()
        if tasks:
            for task in tasks:
                task.cancel()
            # One scheduling turn delivers cooperative cancellations, without
            # awaiting a Store that suppresses them beyond the drain deadline.
            await asyncio.sleep(0)
            unfinished = sum(not task.done() for task in tasks)
            if unfinished:
                logger.warning("Heartbeat stopped with %d lease task(s) still draining", unfinished)
        self._heartbeat_task = None
        self._heartbeat_stop = None
        logger.info("Run lease heartbeat stopped for worker %s", self._worker_id)

    async def _heartbeat_loop(self) -> None:
        """Periodically renew leases and reclaim orphaned runs from dead peers.

        Lease renewal runs every ``lease_seconds / 3``. Reconciliation
        (sweeping for expired leases owned by dead workers) runs every
        ``lease_seconds`` (every 3rd cycle) so orphaned runs are recovered
        without waiting for a pod restart.

        Both operations are guarded so a transient failure cannot take the
        heartbeat task down — a dead heartbeat means no lease is renewed
        again, and every active run eventually looks orphaned to peers.
        """
        if self._run_ownership_config is None or self._heartbeat_stop is None:
            return
        lease_seconds = self._run_ownership_config.lease_seconds
        interval = max(1, lease_seconds // 3)
        stop = self._heartbeat_stop
        cycle = 0

        while not stop.is_set():
            try:
                await asyncio.wait_for(stop.wait(), timeout=interval)
                break  # stop event was set
            except TimeoutError:
                pass  # interval elapsed

            cycle += 1
            try:
                await self._renew_leases(wait=False)
            except CaptureIOClosed:
                # One run retiring ends nothing here; every other lease needs this loop.
                if self._capture_admissions_sealed:
                    return
            except Exception:
                logger.warning("Heartbeat renewal cycle failed", exc_info=True)

            # Reconcile every 3rd cycle (= every lease_seconds). Startup
            # reconciliation (in langgraph_runtime) covers the initial
            # sweep; this periodic pass catches orphans whose lease
            # expires between restarts — e.g. Worker A crashes, its
            # replacement starts before the lease expires, and the
            # startup pass skips the still-valid lease.
            if cycle % 3 == 0:
                self._schedule_orphan_reconciliation()

    @staticmethod
    def _awaits_terminal_commit(record: RunRecord) -> bool:
        """Whether a run keeps its active durable row until a deferred terminal commit."""
        return record.scheduled_goal_cleanup_pending or record.terminal_commit_pending or record.rollback_finalization_pending

    async def _release_confirmed_terminal_barrier(self, record: RunRecord, *, timeout: float) -> bool:
        """Wait only for local attempts, never infer attribution from a row read."""
        deadline = asyncio.get_running_loop().time() + max(0.0, timeout)
        while not record.ownership_lost:
            if self._has_final_terminal_ack(record):
                record.scheduled_goal_cleanup_pending = False
                record.terminal_commit_pending = False
                return True
            attempts = set(self._terminal_attempts.get(record.run_id, ()))
            remaining = deadline - asyncio.get_running_loop().time()
            if not attempts or remaining <= 0:
                return False
            done, _ = await asyncio.wait(attempts, timeout=remaining, return_when=asyncio.FIRST_COMPLETED)
            if not done:
                # Check proof once more before treating the elapsed wait as
                # unknown; an ACK can arrive on the timeout scheduling turn.
                continue
        return False

    def _clear_lease_deadline(self, run_id: str) -> None:
        handle = self._lease_deadlines.pop(run_id, None)
        if handle is not None:
            handle.cancel()

    def _arm_lease_deadline(self, record: RunRecord) -> None:
        self._clear_lease_deadline(record.run_id)
        if not self.heartbeat_enabled or self._store is None or record.ownership_lost or self._has_final_terminal_ack(record):
            return
        expiry = self._parse_lease_deadline(record.lease_expires_at)
        remaining = max(0.0, (expiry - datetime.now(UTC)).total_seconds()) if expiry else 0.0

        def expired() -> None:
            self._lease_deadlines.pop(record.run_id, None)
            if self._runs.get(record.run_id) is not record or record.ownership_lost or self._has_final_terminal_ack(record):
                return
            current = self._parse_lease_deadline(record.lease_expires_at)
            if current is not None and current > datetime.now(UTC):
                self._arm_lease_deadline(record)
                return
            self._fence_local_record(record, reason="The last confirmed lease expired without a final terminal acknowledgment.")

        # This local-only guard survives stopped/failed heartbeat I/O. Shutdown
        # is not authority to continue execution past the last confirmed lease.
        self._lease_deadlines[record.run_id] = asyncio.get_running_loop().call_later(remaining, expired)

    def _supervise_lease_task(self, registry: dict, run_id: str, task: asyncio.Task) -> None:
        registry[run_id] = task

        def settled(completed: asyncio.Task) -> None:
            if registry.get(run_id) is completed:
                registry.pop(run_id, None)
            if not completed.cancelled():
                exc = completed.exception()
                if exc is not None:
                    logger.warning("Lease task for run %s failed (%s)", run_id, type(exc).__name__)

        task.add_done_callback(settled)

    async def _renew_leases(self, *, wait: bool = True) -> None:
        """Give every run an independent renewal decision and monotonic budget.

        The heartbeat starts checks without joining them, so its next tick can
        renew healthy runs while a different run is still waiting for Store I/O.
        Explicit callers may join the bounded decisions for one snapshot.
        """
        if self._store is None or self._run_ownership_config is None:
            return
        async with self._lock:
            records = [
                record
                for record in self._runs.values()
                if not record.ownership_lost
                and record.owner_worker_id == self._worker_id
                # A finalizing worker that has not confirmed its terminal still owns the row.
                and (record.status in (RunStatus.pending, RunStatus.running) or (record.finalizing and not record.terminal_confirmed) or self._awaits_terminal_commit(record))
                and (self._awaits_terminal_commit(record) or record.task is None or not record.task.done())
            ]
        checks = []
        for record in records:
            task = self._lease_checks.get(record.run_id)
            if task is None or task.done():
                task = asyncio.create_task(self._renew_one_lease(record), name=f"run-lease-check:{record.run_id}")
                self._supervise_lease_task(self._lease_checks, record.run_id, task)
            checks.append(task)
        if wait and checks:
            # Cancellation of a cycle must not detach or cancel another caller's
            # still-owned per-run decision. Shutdown drains the registries.
            await asyncio.wait(checks)

    def _rollback_retry_authorized(self, record: RunRecord, *, initial_allow_interrupted: bool) -> bool:
        """Whether an active-only rejection may be retried for this rollback."""
        proof = record.terminal_commit_proof
        return (
            not initial_allow_interrupted
            and record.rollback_finalization_pending
            and proof is not None
            and proof.status == RunStatus.interrupted
            and record.owner_worker_id == self._worker_id
            and self._runs.get(record.run_id) is record
            and not record.ownership_lost
            and not self._has_final_terminal_ack(record)
        )

    async def _await_rollback_retry_authority(self, record: RunRecord, *, initial_allow_interrupted: bool, deadline: float) -> bool:
        """Wait for an in-flight admission ACK, never infer it from the row."""
        loop = asyncio.get_running_loop()
        while loop.time() < deadline:
            if self._rollback_retry_authorized(record, initial_allow_interrupted=initial_allow_interrupted):
                return True
            attempts = set(self._terminal_attempts.get(record.run_id, ()))
            remaining = deadline - loop.time()
            if not attempts or remaining <= 0:
                break
            await asyncio.wait(attempts, timeout=remaining, return_when=asyncio.FIRST_COMPLETED)
        return loop.time() < deadline and self._rollback_retry_authorized(record, initial_allow_interrupted=initial_allow_interrupted)

    async def _renew_one_lease(self, record: RunRecord) -> None:
        """Renew one run inside the capture owner that may perform its Store I/O.

        A captured run renews under its own lifetime (a retired one has nothing
        left to renew); any other run gets a finite maintenance lifetime per
        check, because heartbeat checks outlive the cycle that started them.
        """
        lifetime = self._capture_lifetime(record)
        if lifetime is not None:
            if lifetime.owner.retired:
                return
            with capture_io_scope(lifetime.owner):
                await self._renew_one_lease_owned(record)
            return
        async with self._maintenance_control():
            await self._renew_one_lease_owned(record)

    async def _renew_one_lease_owned(self, record: RunRecord) -> None:
        assert self._store is not None and self._run_ownership_config is not None
        if self._has_final_terminal_ack(record):
            await self._release_confirmed_terminal_barrier(record, timeout=0)
            return
        self._arm_lease_deadline(record)
        confirmed = self._parse_lease_deadline(record.lease_expires_at)
        remaining = max(0.0, (confirmed - datetime.now(UTC)).total_seconds()) if confirmed else 0.0
        loop = asyncio.get_running_loop()
        deadline = loop.time() + remaining

        async def fence(reason: str) -> None:
            if not await self._release_confirmed_terminal_barrier(record, timeout=max(0.0, deadline - loop.time())):
                await self._mark_ownership_lost(record, reason=reason, require_active=False)

        if remaining <= 0:
            await fence("Lease ownership could not be confirmed before the last confirmed lease expired.")
            return
        if self._awaits_terminal_commit(record) and record.task is not None and record.task.done() and not self._terminal_attempts.get(record.run_id):
            await fence("The worker exited without an attributable terminal acknowledgment.")
            return
        if record.run_id in self._lease_operations:
            # A cancellation-resistant operation is still owned and observed.
            # Never start another renewal for this run while it is in flight.
            await fence("A previous lease renewal is still unconfirmed.")
            return

        initial_allow_interrupted = record.rollback_finalization_pending and record.terminal_commit_proof is not None and record.terminal_commit_proof.status == RunStatus.interrupted
        for retry_index in range(2):
            allow_interrupted = record.rollback_finalization_pending and record.terminal_commit_proof is not None and record.terminal_commit_proof.status == RunStatus.interrupted
            new_expiry = (datetime.now(UTC) + timedelta(seconds=self._run_ownership_config.lease_seconds)).isoformat()
            operation = asyncio.create_task(
                self._call_store_with_retry(
                    "renew_lease",
                    record.run_id,
                    lambda: self._store.renew_lease(
                        record.run_id,
                        owner_worker_id=self._worker_id,
                        lease_expires_at=new_expiry,
                        **({"allow_interrupted": True} if allow_interrupted else {}),
                    ),
                ),
                name=f"run-lease-io:{record.run_id}",
            )
            self._supervise_lease_task(self._lease_operations, record.run_id, operation)
            try:
                done, _ = await asyncio.wait({operation}, timeout=max(0.0, deadline - loop.time()))
                if not done:
                    operation.cancel()
                    # Do not join cancellation-resistant I/O. Its late result is
                    # observed by the supervisor and cannot mutate local authority.
                    await fence("Lease renewal exceeded the last confirmed lease deadline.")
                    return
                try:
                    renewal = operation.result()
                except CaptureIOClosed:
                    # This run's own owner retired mid-call; nothing is left to renew.
                    return
                except (Exception, asyncio.CancelledError):
                    if loop.time() >= deadline:
                        await fence("Lease renewal failed through the last confirmed lease deadline.")
                    else:
                        logger.warning("Lease renewal failed for run %s; will retry before expiry", record.run_id)
                    return
                if self._has_final_terminal_ack(record):
                    await self._release_confirmed_terminal_barrier(record, timeout=0)
                    return
                if record.ownership_lost or self._runs.get(record.run_id) is not record:
                    return
                if loop.time() >= deadline:
                    await fence("Lease renewal completed after the last confirmed lease had already expired.")
                    return
                if renewal.renewed is True:
                    record.lease_expires_at = new_expiry
                    self._arm_lease_deadline(record)
                    if renewal.cancel_action is not None:
                        action = renewal.cancel_action if renewal.cancel_action in ("interrupt", "rollback") else "interrupt"
                        # Signal here, before joining any other run's decision.
                        await self._signal_local_cancel(record.run_id, action=action)
                    return
                if (
                    retry_index == 0
                    and not initial_allow_interrupted
                    and await self._await_rollback_retry_authority(
                        record,
                        initial_allow_interrupted=initial_allow_interrupted,
                        deadline=deadline,
                    )
                ):
                    # The first request raced atomic rollback admission. Re-check
                    # the interrupted row once under the same per-run decision,
                    # with owner and live-lease predicates still enforced in Store.
                    continue
                await fence("The durable store rejected lease renewal for this worker.")
                return
            except asyncio.CancelledError:
                operation.cancel()
                raise

    async def _reconcile_orphans_periodic(self) -> None:
        async with self._maintenance_control():
            await self._reconcile_orphans_periodic_controlled()

    async def _reconcile_orphans_periodic_controlled(self) -> None:
        """Sweep for expired leases owned by dead peers.

        Scheduled as a single-flight background task by ``_heartbeat_loop``.
        This keeps both the store scan/status writes and the Gateway callback
        off the lease-renewal loop. Startup reconciliation handles the initial
        sweep; this periodic pass catches orphans whose lease expires between
        restarts.
        """
        recovered = await self.reconcile_orphaned_inflight_runs(
            error=LEASE_ORPHAN_RECOVERY_ERROR,
            stop_reason=ORPHAN_RECOVERY_STOP_REASON,
        )
        if recovered:
            logger.warning(
                "Periodic reconciliation recovered %d orphaned run(s) as error",
                len(recovered),
            )
            if self._on_orphans_recovered is not None:
                try:
                    owner = current_capture_io_owner()
                    if owner is not None:
                        owner.check_open()
                    await self._on_orphans_recovered(recovered)
                except CaptureIOClosed:
                    raise
                except Exception:
                    logger.warning(
                        "Periodic orphan recovery callback failed for %d run(s): run_ids=%s",
                        len(recovered),
                        [record.run_id for record in recovered],
                        exc_info=True,
                    )

    def _schedule_orphan_reconciliation(self) -> None:
        """Start one supervised recovery pass unless one is already running."""
        if self._capture_admissions_sealed and self.supports_output_receipts:
            return
        task = self._orphan_recovery_task
        if task is not None and not task.done():
            logger.debug("Skipping periodic orphan reconciliation: previous pass is still running")
            return
        # This manager-owned background pass gets its own provisional lifetime;
        # it must not inherit one arbitrary worker's producer retirement.
        with capture_io_scope(None):
            task = asyncio.create_task(self._reconcile_orphans_periodic())
        task.set_name("deerflow-periodic-orphan-recovery")
        self._orphan_recovery_task = task
        task.add_done_callback(self._orphan_reconciliation_done)

    def _orphan_reconciliation_done(self, task: asyncio.Task[None]) -> None:
        """Clear and inspect the supervised single-flight recovery task."""
        if self._orphan_recovery_task is task:
            self._orphan_recovery_task = None
        if task.cancelled():
            return
        try:
            task.result()
        except CaptureIOClosed:
            return
        except Exception:
            logger.warning("Periodic orphan reconciliation failed", exc_info=True)

    async def _drain_orphan_recovery_task(self, *, timeout: float) -> None:
        """Boundedly await the supervised recovery pass during shutdown."""
        task = self._orphan_recovery_task
        if task is None or task.done():
            return
        _, pending = await asyncio.wait((task,), timeout=max(0.0, timeout))
        if pending:
            task.cancel()
            await asyncio.sleep(0)  # Deliver cancellation without joining I/O.
            if task.done():
                self._orphan_reconciliation_done(task)
            # Retain the single-flight reference and done callback. Cancellation
            # is a request; a Store may keep using its resources after return.
            logger.warning(
                "Orphan recovery drain exceeded %.1fs on shutdown; cancellation requested, Store I/O may still be active",
                timeout,
            )

    async def shutdown(self, *, timeout: float = 5.0) -> None:
        """Use one grace, then retire and drain finite captured operations."""
        self.seal_capture_io_admissions()
        if self._capture_shutdown_task is None or self._capture_shutdown_task.done():
            deadline = asyncio.get_running_loop().time() + timeout
            self._capture_shutdown_task = asyncio.create_task(self._shutdown_capture_and_tasks(deadline=deadline, timeout=timeout), name="capture-run-shutdown")
        await await_drained(asyncio.shield(self._capture_shutdown_task))

    async def drain_late_admitted_tasks(self, *, deadline: float) -> None:
        """Resnapshot already admitted tasks using the host's original D.

        A resident admission can attach an ordinary worker after the first
        snapshot. Caller cancellation drains this final existing stop sequence;
        this method opens neither a capture lifetime nor another grace period.
        """
        remaining = max(0.0, deadline - asyncio.get_running_loop().time())
        await await_drained(self._shutdown_tasks(deadline=deadline, timeout=remaining))

    async def _shutdown_capture_and_tasks(self, *, deadline: float, timeout: float) -> None:
        # This registry is loop-owned and does not wait for _lock. An admitted
        # transaction may be holding that lock through the whole grace.
        lifetimes = tuple(value for value in self._capture_lifetimes.values() if not value.finished)
        legacy = asyncio.create_task(self._shutdown_tasks(deadline=deadline, timeout=timeout))
        grace = {legacy}
        for lifetime in lifetimes:
            grace.update(task for task in lifetime.controls if not task.done())
            if lifetime.record is not None and lifetime.record.task is not None and not lifetime.record.task.done():
                grace.add(lifetime.record.task)
        await asyncio.wait(grace, timeout=max(0.0, deadline - asyncio.get_running_loop().time()))
        for lifetime in lifetimes:
            lifetime.owner.retire()
            task = lifetime.record.task if lifetime.record is not None else None
            if task is not None and not task.done() and task is not asyncio.current_task():
                task.cancel()
        # Pending graph/control continuations are not arbitrary atomic units.
        # The gate is now closed; only admitted delegate/close work must drain.
        results = await asyncio.gather(legacy, *(self._finish_capture_lifetime(lifetime) for lifetime in lifetimes), return_exceptions=True)
        for result in results:
            if isinstance(result, BaseException):
                if lifetimes:
                    raise CaptureIOCleanupError() from None
                raise result

    async def _shutdown_tasks(self, *, deadline: float, timeout: float) -> None:
        """Cancel and bounded-await all in-flight runs on process shutdown.

        Signals active runs first so their cancellation/cleanup can overlap a
        bounded heartbeat stop. The heartbeat may perform one final benign
        lease renewal before it observes the stop event.

        Chat runs execute in fire-and-forget background ``asyncio`` tasks that
        write checkpoints through a shared checkpointer. On shutdown the
        checkpointer's resources (e.g. the postgres connection pool owned by the
        gateway's ``AsyncExitStack``) are torn down; if a run task is still
        mid-graph at that point, langgraph's
        ``AsyncPregelLoop._checkpointer_put_after_previous`` runs its
        ``finally: await checkpointer.aput(...)`` against the closed pool. Because
        that put runs in a langgraph-internal task (not on ``run_agent``'s call
        stack), the resulting ``psycopg_pool.PoolClosed`` is not catchable by the
        worker and surfaces as an unhandled exception during ``asyncio.run()``
        shutdown (bytedance/deer-flow issue #3373).

        Draining in-flight runs *before* the checkpointer is closed lets each
        run that settles within ``timeout`` flush its final checkpoint while
        resources are still open. Only runs that do **not** settle on their own
        are marked ``interrupted`` — a run that completes (e.g. ``success``)
        during the drain keeps its real terminal status instead of being
        blanket-overwritten. Runs whose terminal status is staged in memory with
        a deferred commit are still writing their final duration checkpoint and
        then commit that status. Committed terminal workers may still be running
        hooks, publishing END or closing resources. Both remain visible in this
        bounded drain and are awaited without cancellation (#5542). The whole
        drain, including the trailing status persistence, is bounded by ``timeout`` so a run stuck in cleanup (or a
        slow store under DB pressure) cannot hang worker shutdown — the
        precondition for the signal-reentrancy deadlock guarded by
        ``app.gateway.app._SHUTDOWN_HOOK_TIMEOUT_SECONDS``. Runs still active
        after ``timeout`` are logged and may still race teardown.
        """
        loop = asyncio.get_running_loop()

        async with self._lock:
            inflight = [
                record
                for record in self._runs.values()
                if (record.status in (RunStatus.pending, RunStatus.running) or (record.finalizing and not record.terminal_confirmed) or self._capture_lifetime(record) is not None) and record.task is not None and not record.task.done()
            ]
            # Terminal workers still own finalization, including after the
            # durable commit (hooks, END and journal/sandbox cleanup). Await them
            # without cancellation so resource teardown cannot overtake a worker
            # that settles within this drain's budget.
            finalizing = [record for record in self._runs.values() if record.status not in (RunStatus.pending, RunStatus.running) and record.task is not None and not record.task.done()]
            finalizing_ids = {record.run_id for record in finalizing}
            # HartMesh's inflight already keeps captured/finalizing workers; RunRecord
            # compares by value, so de-duplicate by identity before appending.
            inflight_identities = {id(record) for record in inflight}
            finalizing = [record for record in finalizing if id(record) not in inflight_identities]
            for record in inflight:
                if record.status not in (RunStatus.pending, RunStatus.running):
                    # Its graph has ended; drain staged finalization without
                    # changing its outcome or cancelling an uncertain commit.
                    continue
                record.abort_action = "interrupt"
                record.abort_event.set()
                if record.stop_reason is None:
                    record.stop_reason = GATEWAY_STOP_REASON
                record.task.cancel()  # type: ignore[union-attr]  # filtered above
                # Status is decided AFTER the drain (below), not here: a run that
                # completes on its own during the drain must keep its real status.
            inflight += finalizing

        await self.stop_heartbeat(timeout=max(0.0, deadline - loop.time()))

        if not inflight:
            await self._drain_orphan_recovery_task(timeout=max(0.0, deadline - loop.time()))
            return

        tasks = [record.task for record in inflight]
        _, pending = await asyncio.wait(tasks, timeout=max(0.0, deadline - loop.time()))

        # Only mark/persist ``interrupted`` for runs that did not settle on their
        # own (still pending after the timeout, or ended cancelled). A run that
        # finished normally during the drain keeps the status it set for itself.
        to_persist: list[RunRecord] = []
        async with self._lock:
            for record in inflight:
                task = record.task
                if task not in pending and not task.cancelled():
                    # Completed on its own — retrieve any surfaced exception so it
                    # is not reported as "never retrieved", and keep its status.
                    error = task.exception()  # type: ignore[union-attr]  # done & not cancelled
                    if error is not None and record.run_id in finalizing_ids:
                        logger.warning(
                            "Run %s failed during terminal finalization on shutdown; staged status %s may not be committed",
                            record.run_id,
                            record.status.value,
                            exc_info=(type(error), error, error.__traceback__),
                        )
                    continue
                if record.run_id in finalizing_ids:
                    # The staged terminal status is committed by the run's own
                    # finalizer; shutdown must not overwrite it with interrupted.
                    continue
                if self._capture_lifetime(record) is not None:
                    # Retirement/Task cancellation is not execution cleanup or
                    # authority for a status-only write over a capture row.
                    continue
                if record.status in (RunStatus.pending, RunStatus.running):
                    record.status = RunStatus.interrupted
                    record.terminal_status_persisted = self._store is None
                    record.updated_at = _now_iso()
                if record.status == RunStatus.interrupted:
                    to_persist.append(record)

        # Bound the trailing status persistence within the remaining budget so a
        # slow store (``_call_store_with_retry`` can back off under DB pressure)
        # cannot push shutdown past ``timeout``.
        if to_persist:
            remaining = deadline - loop.time()
            if remaining <= 0:
                logger.warning("Run drain budget exhausted before persisting %d interrupted run(s) on shutdown", len(to_persist))
            else:
                try:
                    results = await asyncio.wait_for(
                        asyncio.gather(*(self._persist_status(record, RunStatus.interrupted, stop_reason=record.stop_reason) for record in to_persist), return_exceptions=True),
                        timeout=remaining,
                    )
                except TimeoutError:
                    logger.warning("Run drain status persistence exceeded the %.1fs budget; %d record(s) may not be persisted", timeout, len(to_persist))
                else:
                    # ``_persist_status`` is best-effort: it catches and logs its
                    # own failures, returning ``False``. Inspect the aggregate so a
                    # partial failure is surfaced at shutdown level (with the
                    # run_id) instead of being silently swallowed by the gather.
                    for record, result in zip(to_persist, results):
                        if isinstance(result, Exception):
                            logger.warning("Unexpected error persisting interrupted status for run %s during shutdown: %r", record.run_id, result)
                        elif result is False:
                            logger.warning("Could not persist interrupted status for run %s during shutdown", record.run_id)
                        record.terminal_status_persisted = result is True

        if pending:
            logger.warning("Run drain exceeded %.1fs on shutdown; %d run task(s) still active and may race checkpointer teardown", timeout, len(pending))
        logger.info("Drained %d in-flight run(s) on shutdown (%d settled within %.1fs)", len(inflight), len(inflight) - len(pending), timeout)
        await self._drain_orphan_recovery_task(timeout=max(0.0, deadline - loop.time()))


class CancelOutcome(StrEnum):
    """Result of a :meth:`RunManager.cancel` call."""

    cancelled = "cancelled"
    requested = "requested"
    taken_over = "taken_over"
    lease_valid_elsewhere = "lease_valid_elsewhere"
    not_cancellable = "not_cancellable"
    not_active_locally = "not_active_locally"
    unknown = "unknown"


class ConflictError(Exception):
    """Raised when multitask_strategy=reject and thread has inflight runs."""


class UnsupportedStrategyError(Exception):
    """Raised when a multitask_strategy value is not yet implemented."""


class RunStoreUnavailable(RuntimeError):
    """The caller requires read authority that this manager lacks."""

    def __init__(self):
        super().__init__("Run store read authority is unavailable")
