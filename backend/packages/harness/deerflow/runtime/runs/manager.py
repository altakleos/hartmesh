"""In-memory run registry with optional persistent RunStore backing."""

from __future__ import annotations

import asyncio
import logging
import socket
import sqlite3
import uuid
from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from enum import StrEnum
from typing import TYPE_CHECKING, Any

from sqlalchemy.exc import IntegrityError as SAIntegrityError

from deerflow.runtime.user_context import AUTO, _AutoSentinel, resolve_user_id
from deerflow.utils.time import is_lease_expired
from deerflow.utils.time import now_iso as _now_iso

from .lifecycle_query import LifecyclePage, LifecycleQuery, LifecycleVisibilityScope
from .schemas import DisconnectMode, RunStatus, ThreadOperationKind
from .store.base import (
    AdmissionOutcome,
    CancellationRequestOutcome,
    EditReplayVisibility,
    LifecycleTransition,
    LifecycleType,
    RunEnsureResult,
    lifecycle_type_for_status,
)

if TYPE_CHECKING:
    from deerflow.config.run_ownership_config import RunOwnershipConfig
    from deerflow.runtime.events.store.base import RunEventStore
    from deerflow.runtime.runs.store.base import RunStore

logger = logging.getLogger(__name__)

ORPHAN_RECOVERY_STOP_REASON = "orphan_recovered"
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


@dataclass(slots=True)
class _AdmissionCancellation:
    """Cancellation observed while an atomic store decision is in flight."""

    requested: bool = False


@dataclass(frozen=True, slots=True)
class _UnresolvedAdmissionCandidate:
    """Bounded identity needed to close an admission whose commit is unknown."""

    run_id: str
    thread_id: str
    user_id: str | None
    owner_worker_id: str
    external_scope: str | None
    external_key: str | None
    caller_intent_digest: str | None
    caller_intent_digest_version: str | None
    replacement_action: str | None = None
    predecessor_run_ids: tuple[str, ...] = ()
    commit_proven: bool = False


def _generate_worker_id() -> str:
    """Generate a unique worker identifier: ``hostname:hex_uuid``."""
    return f"{socket.gethostname()}:{uuid.uuid4().hex}"


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
    # True only while the application admission coordinator owns the bounded
    # commit-to-worker handoff for this process-local record.
    attachment_supervised: bool = field(default=False, repr=False)
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
    stop_reason: str | None = None
    accepted_invocation: Any | None = field(default=None, repr=False)
    external_scope: str | None = None
    external_key: str | None = None
    request_digest: str | None = None
    request_digest_version: str | None = None
    caller_intent_json: dict[str, Any] | None = None
    caller_intent_digest: str | None = None
    caller_intent_digest_version: str | None = None
    execution_evidence_json: dict[str, Any] | None = None
    execution_evidence_digest: str | None = None
    execution_lease_renewal: Callable[[], Awaitable[bool]] | None = field(
        default=None,
        repr=False,
    )
    state_version: int = 0
    pending_lifecycle_type: LifecycleType | None = field(default=None, repr=False)


@dataclass(frozen=True)
class RunAdmission:
    """Result of durable normal-run admission."""

    record: RunRecord
    outcome: AdmissionOutcome


class RunStartOutcome(StrEnum):
    """Result of the pending-to-running startup barrier."""

    started = "started"
    cancelled = "cancelled"


class RunStartupError(RuntimeError):
    """Raised when durable startup cannot be resolved safely."""


class AcceptedEvidenceIntegrityError(RuntimeError):
    """Raised when a retained idempotent run has contradictory evidence."""

    def __init__(self) -> None:
        super().__init__("accepted_evidence_invalid")


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
        worker_id: str | None = None,
        run_ownership_config: RunOwnershipConfig | None = None,
        event_store: RunEventStore | None = None,
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
        self._worker_id = worker_id or _generate_worker_id()
        self._run_ownership_config = run_ownership_config
        self._event_store = event_store
        self._on_orphans_recovered = on_orphans_recovered
        self._heartbeat_task: asyncio.Task | None = None
        self._heartbeat_stop: asyncio.Event | None = None
        self._orphan_recovery_task: asyncio.Task[None] | None = None
        self._unresolved_admissions: dict[str, _UnresolvedAdmissionCandidate] = {}
        self._reported_unresolved_integrity: set[str] = set()
        self._admission_compensation_task: asyncio.Task[None] | None = None

    def _index_run_locked(self, record: RunRecord) -> None:
        """Register *record* in the thread index. Caller must hold ``self._lock``."""
        self._runs_by_thread.setdefault(record.thread_id, {})[record.run_id] = None

    def _unindex_run_locked(self, run_id: str, thread_id: str) -> None:
        """Drop *run_id* from the thread index. Caller must hold ``self._lock``."""
        bucket = self._runs_by_thread.get(thread_id)
        if bucket is not None:
            bucket.pop(run_id, None)
            if not bucket:
                self._runs_by_thread.pop(thread_id, None)

    @staticmethod
    def _sync_record_from_store_row(record: RunRecord, row: dict[str, Any]) -> None:
        record.status = RunStatus(row.get("status") or RunStatus.pending.value)
        record.state_version = row.get("state_version") or 0
        record.error = row.get("error")
        record.stop_reason = row.get("stop_reason")
        record.owner_worker_id = row.get("owner_worker_id")
        record.lease_expires_at = row.get("lease_expires_at")
        record.execution_evidence_json = row.get("execution_evidence_json")
        record.execution_evidence_digest = row.get("execution_evidence_digest")
        record.updated_at = row.get("updated_at") or record.updated_at

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
        }
        if record.user_id is not None:
            payload["user_id"] = record.user_id
        if record.stop_reason is not None:
            payload["stop_reason"] = record.stop_reason
        if record.operation_kind == ThreadOperationKind.run and record.accepted_invocation is not None:
            payload.update(record.accepted_invocation.to_persisted())
        if record.operation_kind == ThreadOperationKind.run and record.external_scope is not None:
            payload.update(
                external_scope=record.external_scope,
                external_key=record.external_key,
                request_digest=record.request_digest,
                request_digest_version=record.request_digest_version,
                caller_intent_json=record.caller_intent_json,
                caller_intent_digest=record.caller_intent_digest,
                caller_intent_digest_version=record.caller_intent_digest_version,
            )
        return payload

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
                return await operation()
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
                    exc_info=True,
                )
                if delay > 0:
                    await asyncio.sleep(delay)
                delay = min(policy.max_delay, delay * policy.backoff_factor if delay else policy.initial_delay)
                attempt += 1

    def _register_unresolved_admission(
        self,
        candidate: _UnresolvedAdmissionCandidate,
    ) -> None:
        """Retain one ambiguous candidate until storage proves its outcome."""

        self._unresolved_admissions[candidate.run_id] = candidate
        task = self._admission_compensation_task
        if task is None or task.done():
            self._admission_compensation_task = asyncio.create_task(
                self._reconcile_unresolved_admissions(),
                name="deerflow-unresolved-admission-compensation",
            )

    def admission_compensations_ready(self) -> bool:
        """Return whether no commit-ambiguous admission awaits compensation."""

        return not self._unresolved_admissions

    def _fence_replacement_predecessors_locked(
        self,
        candidate: _UnresolvedAdmissionCandidate,
    ) -> None:
        """Stop local predecessors after the exact replacement commit is proven."""

        action = candidate.replacement_action
        if action not in ("interrupt", "rollback"):
            return
        updated_at = _now_iso()
        for run_id in candidate.predecessor_run_ids:
            previous = self._runs.get(run_id)
            if previous is None or previous.finalizing:
                continue
            previous.abort_action = action
            previous.abort_event.set()
            task_active = previous.task is not None and not previous.task.done()
            previous.finalizing = task_active
            if task_active:
                previous.task.cancel()
            previous.status = RunStatus.error if action == "rollback" else RunStatus.interrupted
            previous.error = "Rolled back by user" if action == "rollback" else "Cancelled by newer run"
            previous.updated_at = updated_at

    def _known_candidate_for_record(
        self,
        record: RunRecord,
    ) -> _UnresolvedAdmissionCandidate:
        """Capture the exact persisted identity of one known-created row."""

        return _UnresolvedAdmissionCandidate(
            run_id=record.run_id,
            thread_id=record.thread_id,
            user_id=record.user_id,
            owner_worker_id=record.owner_worker_id or self._worker_id,
            external_scope=record.external_scope,
            external_key=record.external_key,
            caller_intent_digest=record.caller_intent_digest,
            caller_intent_digest_version=record.caller_intent_digest_version,
            commit_proven=True,
        )

    def _sync_compensated_candidate_locked(
        self,
        candidate: _UnresolvedAdmissionCandidate,
        row: dict[str, Any],
    ) -> None:
        """Project a proven terminal compensation into its local record."""

        record = self._runs.get(candidate.run_id)
        if record is None:
            return
        self._sync_record_from_store_row(record, row)
        record.attachment_supervised = False
        record.finalizing = False
        record.abort_event.set()

    @staticmethod
    def _unresolved_candidate_matches(
        candidate: _UnresolvedAdmissionCandidate,
        row: dict[str, Any],
    ) -> bool:
        expected = {
            "run_id": candidate.run_id,
            "thread_id": candidate.thread_id,
            "user_id": candidate.user_id,
            "owner_worker_id": candidate.owner_worker_id,
            "external_scope": candidate.external_scope,
            "external_key": candidate.external_key,
            "caller_intent_digest": candidate.caller_intent_digest,
            "caller_intent_digest_version": candidate.caller_intent_digest_version,
        }
        return all(row.get(field) == value for field, value in expected.items())

    async def _resolve_unresolved_admission(
        self,
        candidate: _UnresolvedAdmissionCandidate,
    ) -> bool:
        """Prove absence or terminalize one exact candidate without execution."""

        store = self._store
        if store is None:
            return True
        try:
            row = await store.get(candidate.run_id, user_id=candidate.user_id)
        except asyncio.CancelledError:
            raise
        except Exception:
            return False
        if row is None:
            return not candidate.commit_proven
        if not self._unresolved_candidate_matches(candidate, row):
            if candidate.run_id not in self._reported_unresolved_integrity:
                self._reported_unresolved_integrity.add(candidate.run_id)
                logger.error(
                    "Unresolved admission identity mismatch code=admission_candidate_integrity_failed run_id=%s",
                    candidate.run_id,
                )
            return False
        async with self._lock:
            self._fence_replacement_predecessors_locked(candidate)
        if row.get("status") not in (
            RunStatus.pending.value,
            RunStatus.running.value,
        ):
            async with self._lock:
                self._sync_compensated_candidate_locked(candidate, row)
            return True
        try:
            if store.durable_lifecycle:
                transition = await store.transition_run_atomic(
                    candidate.run_id,
                    expected_state_version=row["state_version"],
                    expected_statuses=(
                        RunStatus.pending.value,
                        RunStatus.running.value,
                    ),
                    transition=LifecycleTransition(
                        lifecycle_type=LifecycleType.failed,
                        status=RunStatus.error.value,
                        error="worker_attachment_failed",
                        stop_reason="worker_attachment_failed",
                        reason="worker_attachment_failed",
                    ),
                    user_id=candidate.user_id,
                )
                terminal = transition.row
            else:
                await store.update_status(
                    candidate.run_id,
                    RunStatus.error.value,
                    error="worker_attachment_failed",
                    stop_reason="worker_attachment_failed",
                )
                terminal = await store.get(
                    candidate.run_id,
                    user_id=candidate.user_id,
                )
        except asyncio.CancelledError:
            raise
        except Exception:
            return False
        completed = terminal is not None and terminal.get("status") not in (
            RunStatus.pending.value,
            RunStatus.running.value,
        )
        if completed:
            async with self._lock:
                self._sync_compensated_candidate_locked(candidate, terminal)
        return completed

    async def _reconcile_unresolved_admissions(self) -> None:
        """Retry ambiguous candidate compensation at one bounded cadence."""

        try:
            while self._unresolved_admissions:
                for run_id, candidate in tuple(self._unresolved_admissions.items()):
                    if await self._resolve_unresolved_admission(candidate):
                        if self._unresolved_admissions.get(run_id) is candidate:
                            self._unresolved_admissions.pop(run_id, None)
                            self._reported_unresolved_integrity.discard(run_id)
                if self._unresolved_admissions:
                    await asyncio.sleep(0.1)
        finally:
            if self._admission_compensation_task is asyncio.current_task():
                self._admission_compensation_task = None

    async def drain_admission_compensations(self, *, timeout: float) -> bool:
        """Boundedly wait for every unresolved candidate to become terminal."""

        if timeout < 0:
            raise ValueError("admission compensation timeout must be non-negative")
        if not self._unresolved_admissions:
            return True
        task = self._admission_compensation_task
        if task is None or task.done():
            self._register_unresolved_admission(next(iter(self._unresolved_admissions.values())))
            task = self._admission_compensation_task
        if task is None:
            return not self._unresolved_admissions
        done, _ = await asyncio.wait((task,), timeout=timeout)
        if done:
            task.result()
        return not self._unresolved_admissions

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

    async def _persist_status(
        self,
        record: RunRecord,
        status: RunStatus,
        *,
        error: str | None = None,
        stop_reason: str | None = None,
        lifecycle_type: LifecycleType | None = None,
    ) -> bool:
        """Best-effort persist a status transition to the backing store."""
        if record.ownership_lost:
            logger.warning(
                "Skipped status update to %s for run %s after lease ownership was lost",
                status.value,
                record.run_id,
            )
            return False
        if self._store is None:
            return True
        row_recovery_payload = self._store_put_payload(record, error=error, stop_reason=stop_reason)
        try:
            if self._store.durable_lifecycle and record.operation_kind == ThreadOperationKind.run:
                # Cancellation callers identify themselves explicitly. Abort
                # state is also used for timeout, attachment failure, worker
                # loss, and shutdown, so it cannot safely choose a lifecycle
                # type on its own.
                mapped_type = lifecycle_type or lifecycle_type_for_status(status.value)
                desired_transition = LifecycleTransition(
                    lifecycle_type=mapped_type,
                    status=status.value,
                    error=error,
                    stop_reason=stop_reason,
                    reason=stop_reason,
                )
                transition_result = await self._call_store_with_retry(
                    "transition_run_atomic",
                    record.run_id,
                    lambda: self._store.transition_run_atomic(
                        record.run_id,
                        expected_state_version=record.state_version,
                        expected_statuses=("pending", "running", "interrupted"),
                        transition=desired_transition,
                    ),
                )
                updated = transition_result.applied
                if not updated and transition_result.row is not None and transition_result.row.get("status") in ("pending", "running") and transition_result.row.get("cancel_action") is not None:
                    # A remote cancellation request increments the version
                    # without changing status. Let that committed request win
                    # regardless of which terminal write discovered it, then
                    # finalize against its returned version.
                    retry_state_version = transition_result.row["state_version"]
                    cancel_action = transition_result.row["cancel_action"]
                    cancelled_transition = LifecycleTransition(
                        lifecycle_type=LifecycleType.cancelled,
                        status=("error" if cancel_action == "rollback" else "interrupted"),
                        error=("Rolled back by user" if cancel_action == "rollback" else None),
                    )
                    transition_result = await self._call_store_with_retry(
                        "transition_run_atomic",
                        record.run_id,
                        lambda: self._store.transition_run_atomic(
                            record.run_id,
                            expected_state_version=retry_state_version,
                            expected_statuses=("pending", "running"),
                            transition=cancelled_transition,
                        ),
                    )
                    updated = transition_result.applied
                if transition_result.row is not None:
                    self._sync_record_from_store_row(record, transition_result.row)
                if updated:
                    record.pending_lifecycle_type = None
            else:
                updated = await self._call_store_with_retry(
                    "update_status",
                    record.run_id,
                    lambda: self._store.update_status(
                        record.run_id,
                        status.value,
                        error=error,
                        stop_reason=stop_reason,
                    ),
                )
            if updated is False:
                # Status transitions are guarded by active status (and, for
                # lifecycle stores, state version).
                # False can mean either:
                #   (a) the row is missing; compatibility stores may recreate
                #       it, but lifecycle stores fail closed.
                #   (b) the row is terminal — either a peer takeover (``error``)
                #       or a local cancel/completion race (``interrupted`` /
                #       ``success``). The log severity branches on which.
                existing = await self._store.get(record.run_id)
                if existing is not None:
                    existing_status = existing.get("status")
                    if existing_status == status.value:
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
                if self._store.durable_lifecycle and record.operation_kind == ThreadOperationKind.run:
                    logger.error(
                        "Refused to recreate missing authoritative lifecycle row for run %s",
                        record.run_id,
                    )
                    return False
                return await self._persist_snapshot_to_store(record.run_id, row_recovery_payload)
            return True
        except Exception:
            logger.warning("Failed to persist status update for run %s", record.run_id, exc_info=True)
            return False

    @staticmethod
    def _record_from_store(row: dict[str, Any]) -> RunRecord:
        """Build a read-only runtime record from a serialized store row.

        NULL status/on_disconnect columns (e.g. from rows written before those
        columns were added) default to ``pending`` and ``cancel`` respectively.
        """
        from deerflow.runtime.accepted_invocation import AcceptedInvocation

        return RunRecord(
            run_id=row["run_id"],
            thread_id=row["thread_id"],
            assistant_id=row.get("assistant_id"),
            status=RunStatus(row.get("status") or RunStatus.pending.value),
            on_disconnect=DisconnectMode(row.get("on_disconnect") or DisconnectMode.cancel.value),
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
            lease_expires_at=row.get("lease_expires_at"),
            stop_reason=row.get("stop_reason"),
            accepted_invocation=AcceptedInvocation.from_persisted(row),
            external_scope=row.get("external_scope"),
            external_key=row.get("external_key"),
            request_digest=row.get("request_digest"),
            request_digest_version=row.get("request_digest_version"),
            caller_intent_json=row.get("caller_intent_json"),
            caller_intent_digest=row.get("caller_intent_digest"),
            caller_intent_digest_version=row.get("caller_intent_digest_version"),
            execution_evidence_json=row.get("execution_evidence_json"),
            execution_evidence_digest=row.get("execution_evidence_digest"),
            state_version=row.get("state_version") or 0,
        )

    @classmethod
    def _replay_record_from_store(cls, row: dict[str, Any]) -> RunRecord:
        """Hydrate replay evidence or expose only one bounded integrity code."""

        try:
            return cls._record_from_store(row)
        except Exception as exc:
            from deerflow.diagnostics import bounded_diagnostic, log_bounded_failure

            log_bounded_failure(
                logger,
                bounded_diagnostic(
                    code="accepted_evidence_invalid",
                    operation="hydrate_idempotent_replay",
                    error=exc,
                    capability_id="run_store",
                ),
            )
            raise AcceptedEvidenceIntegrityError() from None

    async def update_run_completion(self, run_id: str, **kwargs) -> None:
        """Persist token usage and completion data to the backing store."""
        row_recovery_payload: dict[str, Any] | None = None
        record: RunRecord | None = None
        async with self._lock:
            record = self._runs.get(run_id)
            if record is not None and record.ownership_lost:
                logger.warning("Skipped completion persistence for run %s after lease ownership was lost", run_id)
                return
            if record is not None:
                for key, value in kwargs.items():
                    if key == "status":
                        continue
                    if hasattr(record, key) and value is not None:
                        setattr(record, key, value)
                record.updated_at = _now_iso()
                row_recovery_payload = self._store_put_payload(record, error=kwargs.get("error"))
        if self._store is None:
            return
        try:
            updated = await self._call_store_with_retry(
                "update_run_completion",
                run_id,
                lambda: self._store.update_run_completion(run_id, **kwargs),
            )
            if updated is False:
                existing = await self._store.get(run_id)
                requested_status = kwargs.get("status")
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
                if self._store.durable_lifecycle:
                    logger.error(
                        "Refused to recreate missing authoritative lifecycle row %s during completion persistence",
                        run_id,
                    )
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
        except Exception:
            logger.warning("Failed to persist run completion for %s", run_id, exc_info=True)

    async def update_run_progress(self, run_id: str, **kwargs) -> None:
        """Persist a running token/message snapshot without changing status."""
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
                await self._store.update_run_progress(run_id, **kwargs)
            except Exception:
                logger.warning("Failed to persist run progress for %s", run_id, exc_info=True)

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
    ) -> RunRecord:
        """Create a new pending run and register it.

        Note: this method assumes no active run exists for the thread. It
        persists via ``store.put`` (upsert) rather than the atomic
        ``create_thread_operation_atomic`` primitive, so a concurrent insert for the
        same thread will hit the partial unique index and surface as a
        raw ``IntegrityError`` instead of a ``ConflictError``. Production
        callers should use :meth:`create_or_reject`.
        """
        run_id = str(uuid.uuid4())
        now = _now_iso()
        lease_expires_at = self._compute_lease_expires_at()
        record = RunRecord(
            run_id=run_id,
            thread_id=thread_id,
            assistant_id=assistant_id,
            status=RunStatus.pending,
            on_disconnect=on_disconnect,
            multitask_strategy=multitask_strategy,
            metadata=metadata or {},
            kwargs=kwargs or {},
            user_id=user_id,
            created_at=now,
            updated_at=now,
            owner_worker_id=self._worker_id,
            lease_expires_at=lease_expires_at,
        )
        async with self._lock:
            self._runs[run_id] = record
            self._index_run_locked(record)
            persisted = False
            try:
                await self._persist_new_run_to_store(record)
                if self._store is not None and self._store.durable_lifecycle:
                    stored = await self._store.get(run_id)
                    if stored is None:
                        raise RuntimeError(f"Durable admission for run {run_id} committed no row")
                    self._sync_record_from_store_row(record, stored)
                    record.store_only = False
                persisted = True
            except Exception:
                logger.warning("Failed to persist run %s; rolled back in-memory record", run_id, exc_info=True)
                raise
            finally:
                # Also covers cancellation, which bypasses ``except Exception``.
                if not persisted:
                    self._runs.pop(run_id, None)
                    self._unindex_run_locked(run_id, record.thread_id)
        logger.info("Run created: run_id=%s thread_id=%s", run_id, thread_id)
        return record

    async def get(self, run_id: str, *, user_id: str | None = None) -> RunRecord | None:
        """Return a run record by ID, or ``None``.

        Args:
            run_id: The run ID to look up.
            user_id: Optional user ID for permission filtering when hydrating from store.
        """
        async with self._lock:
            record = self._runs.get(run_id)
        if record is not None:
            return record
        if self._store is None:
            return None
        try:
            row = await self._store.get(run_id, user_id=user_id)
        except Exception as exc:
            from deerflow.diagnostics import bounded_diagnostic, log_bounded_failure

            log_bounded_failure(
                logger,
                bounded_diagnostic(
                    code="run_hydration_failed",
                    operation="load_run",
                    error=exc,
                    capability_id="run_store",
                ),
            )
            return None
        # Re-check after store await: a concurrent create() may have inserted the
        # in-memory record while the store call was in flight.
        async with self._lock:
            record = self._runs.get(run_id)
        if record is not None:
            return record
        if row is None:
            return None
        try:
            return self._record_from_store(row)
        except Exception as exc:
            from deerflow.diagnostics import bounded_diagnostic, log_bounded_failure

            log_bounded_failure(
                logger,
                bounded_diagnostic(
                    code="accepted_evidence_invalid",
                    operation="hydrate_accepted_invocation",
                    error=exc,
                    capability_id="run_store",
                ),
            )
            return None

    async def aget(self, run_id: str, *, user_id: str | None = None) -> RunRecord | None:
        """Return a run record by ID, checking the persistent store as fallback.

        Alias for :meth:`get` for backward compatibility.
        """
        return await self.get(run_id, user_id=user_id)

    async def list_by_thread(self, thread_id: str, *, user_id: str | None = None, limit: int = 100) -> list[RunRecord]:
        """Return runs for a given thread, newest first, at most ``limit`` records.

        In-memory runs take precedence only when the same ``run_id`` exists in both
        memory and the backing store. The merged result is then sorted newest-first
        by ``created_at`` and trimmed to ``limit`` (default 100).

        Args:
            thread_id: The thread ID to filter by.
            user_id: Optional user ID for permission filtering when hydrating from store.
            limit: Maximum number of runs to return.
        """
        async with self._lock:
            memory_records = [record for record in self._thread_records_locked(thread_id) if record.operation_kind == ThreadOperationKind.run]
        if self._store is None:
            return sorted(memory_records, key=lambda r: r.created_at, reverse=True)[:limit]
        records_by_id = {record.run_id: record for record in memory_records}
        # Query enough rows to cover both the requested page and every possible
        # in-memory/store duplicate. Local records can be older than persisted
        # rows, so subtracting them from the store limit can hide the actual
        # newest run before the merge; querying only ``limit`` can still lose a
        # distinct row when that page is occupied by duplicate local records.
        store_limit = limit + len(memory_records)
        try:
            rows = await self._store.list_by_thread(thread_id, user_id=user_id, limit=store_limit)
        except Exception:
            logger.warning("Failed to hydrate runs for thread %s from store", thread_id, exc_info=True)
            return sorted(memory_records, key=lambda r: r.created_at, reverse=True)[:limit]
        for row in rows:
            run_id = row.get("run_id")
            if run_id and run_id not in records_by_id:
                try:
                    records_by_id[run_id] = self._record_from_store(row)
                except Exception as exc:
                    from deerflow.diagnostics import bounded_diagnostic, log_bounded_failure

                    log_bounded_failure(
                        logger,
                        bounded_diagnostic(
                            code="accepted_evidence_invalid",
                            operation="hydrate_accepted_invocation",
                            error=exc,
                            capability_id="run_store",
                        ),
                    )
        return sorted(records_by_id.values(), key=lambda record: record.created_at, reverse=True)[:limit]

    async def query_lifecycle(self, query: LifecycleQuery) -> LifecyclePage:
        """Read authoritative lifecycle evidence from the configured durable store."""

        if self._store is None or not self._store.durable_lifecycle:
            raise RuntimeError("the configured run store has no durable lifecycle query support")
        return await self._store.query_lifecycle(query)

    async def context_visible_in_scope(
        self,
        thread_id: str,
        scope: LifecycleVisibilityScope,
    ) -> bool:
        """Check one exact context against a host-resolved finite scope."""

        if self._store is None or not self._store.durable_lifecycle:
            return False
        return await self._store.context_visible_in_scope(thread_id, scope)

    async def prune_lifecycle_through(self, cursor: str) -> str:
        """Administratively prune a committed lifecycle prefix."""

        if self._store is None or not self._store.durable_lifecycle:
            raise RuntimeError("the configured run store has no durable lifecycle pruning support")
        return await self._store.prune_lifecycle_through(cursor)

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

        sources = set(await self._store.list_successful_regenerate_sources(thread_id, user_id=resolved_user_id)) if self._store is not None else set()
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
            rows = await self._store.list_edit_regenerate_runs(thread_id, user_id=resolved_user_id)
            for row in rows:
                try:
                    record = self._record_from_store(row)
                except Exception:
                    logger.warning("Failed to map edit replay run row for %s", row.get("run_id"), exc_info=True)
                    continue
                records_by_id[record.run_id] = record

        async with self._lock:
            memory_records = [record for record in self._thread_records_locked(thread_id) if resolved_user_id is None or record.user_id == resolved_user_id]
        for record in memory_records:
            records_by_id[record.run_id] = record

        return self._compute_edit_replay_visibility(list(records_by_id.values()))

    async def try_start(
        self,
        run_id: str,
        *,
        execution_evidence: object | None = None,
    ) -> RunStartOutcome:
        """Transition an uncancelled pending run to running before building the agent."""
        async with self._lock:
            record = self._runs.get(run_id)
        if record is None:
            raise RunStartupError(f"Cannot start unknown run {run_id}")

        async with record.start_lock:
            async with self._lock:
                if record.abort_event.is_set() or record.status != RunStatus.pending:
                    return RunStartOutcome.cancelled

            if self._store is not None:
                evidence_json = None
                evidence_digest = None
                if execution_evidence is not None:
                    from deerflow.sandbox.sandbox_provider import (
                        AcceptedSkillExecutionEvidenceV1,
                        AcceptedSkillExecutionEvidenceV2,
                    )

                    if not isinstance(
                        execution_evidence,
                        (
                            AcceptedSkillExecutionEvidenceV1,
                            AcceptedSkillExecutionEvidenceV2,
                        ),
                    ):
                        raise RunStartupError("Invalid sandbox execution evidence")
                    if execution_evidence.run_id != run_id:
                        raise RunStartupError(
                            "Sandbox execution evidence belongs to a different run",
                        )
                    evidence_json = execution_evidence.to_persisted()
                    evidence_digest = execution_evidence.digest
                try:
                    if evidence_json is None:
                        updated = await self._call_store_with_retry(
                            "start_run",
                            run_id,
                            lambda: self._store.start_run(run_id),
                        )
                    else:
                        updated = await self._call_store_with_retry(
                            "start_run",
                            run_id,
                            lambda: self._store.start_run(
                                run_id,
                                execution_evidence_json=evidence_json,
                                execution_evidence_digest=evidence_digest,
                            ),
                        )
                except Exception as exc:
                    raise RunStartupError(f"Failed to start run {run_id}: {exc}") from exc
                if updated is False:
                    try:
                        stored = await self._store.get(run_id)
                    except Exception:
                        stored = None
                    async with self._lock:
                        if record.status == RunStatus.pending:
                            if stored is not None:
                                self._sync_record_from_store_row(record, stored)
                                if stored.get("cancel_action") is not None:
                                    record.abort_action = stored["cancel_action"]
                            record.abort_event.set()
                            record.updated_at = _now_iso()
                    return RunStartOutcome.cancelled
                if self._store.durable_lifecycle:
                    stored = await self._store.get(run_id)
                    if stored is not None:
                        record.state_version = stored.get("state_version") or record.state_version

            async with self._lock:
                if record.abort_event.is_set() or record.status != RunStatus.pending:
                    restore_status = record.status
                    restore_error = record.error
                    restore_stop_reason = record.stop_reason
                else:
                    record.status = RunStatus.running
                    if execution_evidence is not None:
                        record.execution_evidence_json = execution_evidence.to_persisted()
                        record.execution_evidence_digest = execution_evidence.digest
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

    async def set_execution_lease_renewal(
        self,
        run_id: str,
        callback: Callable[[], Awaitable[bool]] | None,
    ) -> None:
        """Bind process-local material renewal to this run's durable ownership."""

        async with self._lock:
            record = self._runs.get(run_id)
            if record is None:
                raise RunStartupError(f"Cannot bind execution lease for unknown run {run_id}")
            record.execution_lease_renewal = callback

    async def fail_start_if_pending(self, run_id: str, *, error: str) -> bool:
        """Mark an admitted run failed and report whether that failure won."""
        async with self._lock:
            record = self._runs.get(run_id)
            if record is None or record.status != RunStatus.pending or record.abort_event.is_set():
                return False
            record.finalizing = True
            record.error = error
            record.stop_reason = "worker_attachment_failed"
            record.abort_event.set()
            record.updated_at = _now_iso()
            candidate = self._known_candidate_for_record(record)

        try:
            persisted = await self._persist_status(
                record,
                RunStatus.error,
                error=error,
                stop_reason="worker_attachment_failed",
                lifecycle_type=LifecycleType.failed,
            )
            if self._store is not None:
                stored = await self._call_store_with_retry(
                    "verify worker attachment failure",
                    run_id,
                    lambda: self._store.get(run_id, user_id=record.user_id),
                )
                if stored is not None and stored.get("status") in (
                    RunStatus.pending.value,
                    RunStatus.running.value,
                ):
                    if self._store.durable_lifecycle:
                        transition = await self._call_store_with_retry(
                            "terminalize worker attachment failure",
                            run_id,
                            lambda: self._store.transition_run_atomic(
                                run_id,
                                expected_state_version=stored["state_version"],
                                expected_statuses=(
                                    RunStatus.pending.value,
                                    RunStatus.running.value,
                                ),
                                transition=LifecycleTransition(
                                    lifecycle_type=LifecycleType.failed,
                                    status=RunStatus.error.value,
                                    error=error,
                                    stop_reason="worker_attachment_failed",
                                    reason="worker_attachment_failed",
                                ),
                                user_id=record.user_id,
                            ),
                        )
                        if transition.row is not None:
                            stored = transition.row
                    else:
                        await self._call_store_with_retry(
                            "terminalize worker attachment failure",
                            run_id,
                            lambda: self._store.update_status(
                                run_id,
                                RunStatus.error.value,
                                error=error,
                                stop_reason="worker_attachment_failed",
                            ),
                        )
                        stored = await self._call_store_with_retry(
                            "verify terminal worker attachment failure",
                            run_id,
                            lambda: self._store.get(run_id, user_id=record.user_id),
                        )
                if stored is not None:
                    async with self._lock:
                        if self._runs.get(run_id) is record:
                            self._sync_record_from_store_row(record, stored)
                    if stored.get("status") in (
                        RunStatus.pending.value,
                        RunStatus.running.value,
                    ):
                        raise RunStartupError("worker attachment failure could not be terminalized")
                elif persisted is False:
                    raise RunStartupError("worker attachment failure could not be verified")
            else:
                record.status = RunStatus.error
        except asyncio.CancelledError:
            self._register_unresolved_admission(candidate)
            raise
        except Exception:
            self._register_unresolved_admission(candidate)
            raise
        async with self._lock:
            current = self._runs.get(run_id)
            if current is record and record.status not in (
                RunStatus.pending,
                RunStatus.running,
            ):
                record.attachment_supervised = False
                record.finalizing = False
            return bool(current is record and record.status == RunStatus.error and record.error == error and record.stop_reason == "worker_attachment_failed")

    async def attach_worker_once(
        self,
        run_id: str,
        worker,
        task_factory: Callable[[Any], asyncio.Task],
    ) -> asyncio.Task:
        """Atomically transfer one supervised creator row to one worker task."""

        async with self._lock:
            record = self._runs.get(run_id)
            if record is None:
                raise RunStartupError("Cannot attach a worker to an unknown run")
            if record.status != RunStatus.pending or record.abort_event.is_set():
                raise RunStartupError("Cannot attach a worker to an inactive run")
            if record.owner_worker_id not in (None, self._worker_id):
                raise RunStartupError("Cannot attach a worker without local ownership")
            if not record.attachment_supervised or record.task is not None:
                raise RunStartupError("Run worker attachment was already resolved")
            task = task_factory(worker)
            if not isinstance(task, asyncio.Task):
                raise RunStartupError("Worker task factory did not return an asyncio task")
            record.task = task
            record.attachment_supervised = False
            return task

    async def finalize_pending_cancellation(self, run_id: str) -> bool:
        """Terminalize a cancellation that won before graph preflight began."""

        async with self._lock:
            record = self._runs.get(run_id)
            if record is None:
                return False
            if record.status not in (RunStatus.pending, RunStatus.running):
                if record.abort_event.is_set():
                    record.finalizing = False
                    return True
                return False
            action = record.abort_action
            if not record.abort_event.is_set() or action not in ("interrupt", "rollback"):
                return False
            record.status = RunStatus.error if action == "rollback" else RunStatus.interrupted
            record.error = "Rolled back by user" if action == "rollback" else None
            record.stop_reason = None
            record.pending_lifecycle_type = LifecycleType.cancelled
            record.updated_at = _now_iso()

        await self._persist_status(
            record,
            record.status,
            error=record.error,
            lifecycle_type=LifecycleType.cancelled,
        )
        async with self._lock:
            current = self._runs.get(run_id)
            if current is record:
                record.finalizing = False
                return record.status not in (RunStatus.pending, RunStatus.running)
        return False

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
            rows = await self._store.get_many_by_thread(thread_id, set(remaining), user_id=resolved_user_id)
        except Exception:
            logger.warning("Failed to batch-hydrate runs for thread %s", thread_id, exc_info=True)
            return records_by_id
        for run_id, row in rows.items():
            if run_id in records_by_id:
                continue
            try:
                records_by_id[run_id] = self._record_from_store(row)
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
        persist: bool = True,
        lifecycle_type: LifecycleType | None = None,
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
            record.status = status
            record.updated_at = _now_iso()
            if error is not None:
                record.error = error
            if stop_reason is not None:
                record.stop_reason = stop_reason
            record.pending_lifecycle_type = lifecycle_type
        if persist:
            persisted = await self._persist_status(
                record,
                status,
                error=error,
                stop_reason=stop_reason,
                lifecycle_type=lifecycle_type,
            )
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
            lifecycle_type = record.pending_lifecycle_type
        persisted = await self._persist_status(
            record,
            status,
            error=error,
            stop_reason=stop_reason,
            lifecycle_type=lifecycle_type,
        )
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
        persist: bool = True,
    ) -> str | None:
        """Set a terminal status unless a durable cancellation won first."""
        if not persist or self._store is None or (not self._store.durable_lifecycle and not self.heartbeat_enabled):
            await self.set_status(
                run_id,
                status,
                error=error,
                stop_reason=stop_reason,
                persist=persist,
            )
            return None

        try:
            result = await self._call_store_with_retry(
                "finalize_if_not_cancelled",
                run_id,
                lambda: self._store.finalize_if_not_cancelled(
                    run_id,
                    status=status.value,
                    error=error,
                    stop_reason=stop_reason,
                ),
            )
        except Exception:
            async with self._lock:
                record = self._runs.get(run_id)
            if record is not None:
                await self._mark_ownership_lost(
                    record,
                    reason=("The durable store could not confirm whether cancellation or completion won."),
                    require_active=False,
                )
            return None

        if result.cancel_action is not None:
            async with self._lock:
                record = self._runs.get(run_id)
                if record is not None:
                    record.abort_action = result.cancel_action
                    record.abort_event.set()
            return result.cancel_action

        await self.set_status(
            run_id,
            status,
            error=error,
            stop_reason=stop_reason,
            persist=not result.finalized,
        )
        if result.finalized and self._store.durable_lifecycle:
            stored = await self._store.get(run_id)
            if stored is not None:
                async with self._lock:
                    record = self._runs.get(run_id)
                    if record is not None:
                        self._sync_record_from_store_row(record, stored)
        return None

    async def _ensure_delivery_receipt(self, record: RunRecord) -> bool:
        """Idempotently persist a zero-delivery receipt during recovery."""
        if self._event_store is None:
            return True
        try:
            await self._event_store.put_if_absent(
                thread_id=record.thread_id,
                run_id=record.run_id,
                event_type="run.delivery",
                category="outputs",
                content={"presented": 0, "paths": [], "by_tool": {}},
            )
            return True
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
        except Exception:
            logger.warning("Failed to persist model_name update for run %s", run_id, exc_info=True)

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
            if self._store.durable_lifecycle:
                result = await self._call_store_with_retry(
                    "request_cancel_compat",
                    run_id,
                    lambda: self._store.request_cancel_compat(run_id, action=action),
                )
                if result.row is not None:
                    async with self._lock:
                        local_record = self._runs.get(run_id)
                        if local_record is not None:
                            self._sync_record_from_store_row(local_record, result.row)
                if result.outcome in (
                    CancellationRequestOutcome.requested,
                    CancellationRequestOutcome.already_requested,
                    CancellationRequestOutcome.stale,
                ):
                    winning_action = result.row.get("cancel_action") if result.row is not None else action
                elif result.outcome == CancellationRequestOutcome.already_terminal:
                    return CancelOutcome.not_cancellable, None
                else:
                    return CancelOutcome.unknown, None
            else:
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
            fresh = await self._store.get(run_id)
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
            if record is None or record.status not in (RunStatus.pending, RunStatus.running) or record.abort_event.is_set():
                return

            record.abort_action = action
            record.abort_event.set()
            task_active = record.task is not None and not record.task.done()
            record.finalizing = task_active
            if task_active and record.status == RunStatus.running:
                record.task.cancel()
        logger.info("Run %s cancellation signalled locally (action=%s)", run_id, action)

    async def request_cancel_fenced(
        self,
        run_id: str,
        *,
        action: str,
        expected_state_version: int,
        user_id: str | None = None,
    ) -> CancellationRequestOutcome:
        """Record a version-fenced cancellation and notify a local owner."""

        if self._store is None or not self._store.durable_lifecycle:
            raise RuntimeError("the configured run store has no fenced cancellation support")
        result = await self._call_store_with_retry(
            "request_cancel_fenced",
            run_id,
            lambda: self._store.request_cancel_fenced(
                run_id,
                action=action,
                expected_state_version=expected_state_version,
                user_id=user_id,
            ),
        )
        signalled_action: str | None = None
        if result.row is not None:
            async with self._lock:
                local_record = self._runs.get(run_id)
                if local_record is not None:
                    self._sync_record_from_store_row(local_record, result.row)
                    winning_action = result.row.get("cancel_action")
                    if winning_action in ("interrupt", "rollback") and local_record.status in (RunStatus.pending, RunStatus.running):
                        local_record.abort_action = winning_action
                        local_record.abort_event.set()
                        task_active = local_record.task is not None and not local_record.task.done()
                        local_record.finalizing = task_active
                        if task_active and local_record.status == RunStatus.running:
                            local_record.task.cancel()
                        signalled_action = winning_action
        if signalled_action is not None:
            logger.info(
                "Run %s cancellation signalled locally (action=%s)",
                run_id,
                signalled_action,
            )
        return result.outcome

    async def cancel(self, run_id: str, *, action: str = "interrupt") -> CancelOutcome:
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
            record = self._runs.get(run_id)
            if record is not None:
                if record.status == RunStatus.interrupted or (record.abort_event.is_set() and record.status in (RunStatus.error, RunStatus.timeout)):
                    return CancelOutcome.cancelled  # idempotent
                if record.status not in (RunStatus.pending, RunStatus.running) and (not self.heartbeat_enabled or self._store is None):
                    return CancelOutcome.not_cancellable

        durable_cancel_won = False
        durable_cancel_supported = self._store is not None and (self._store.durable_lifecycle or self.heartbeat_enabled)
        if record is not None and durable_cancel_supported:
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

        async with self._lock:
            record = self._runs.get(run_id)
            if record is not None:
                if record.status == RunStatus.interrupted or record.abort_event.is_set():
                    return CancelOutcome.cancelled
                if record.status not in (RunStatus.pending, RunStatus.running):
                    return CancelOutcome.cancelled if durable_cancel_won else CancelOutcome.not_cancellable
                record.abort_action = action
                record.abort_event.set()
                task_active = record.task is not None and not record.task.done()
                record.finalizing = task_active
                if task_active and record.status == RunStatus.running:
                    record.task.cancel()
                cancel_status = RunStatus.error if action == "rollback" else RunStatus.interrupted
                record.status = cancel_status
                if action == "rollback":
                    record.error = "Rolled back by user"
                record.pending_lifecycle_type = LifecycleType.cancelled
                record.updated_at = _now_iso()

        # Persist outside the lock so store calls don't block other mutations.
        if record is not None:
            persisted = await self._persist_status(
                record,
                record.status,
                error=record.error,
                lifecycle_type=LifecycleType.cancelled,
            )
            if not persisted and self._store is not None:
                # ``_persist_status`` already fetched ``existing`` internally;
                # re-check the store to see if a peer takeover flipped the
                # row to ``error`` between our in-memory cancel and the
                # guarded ``update_status``. If so, surface ``taken_over``
                # so the client sees a status consistent with the store.
                try:
                    existing = await self._store.get(run_id)
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
            row = await self._store.get(run_id)
        except Exception:
            logger.warning("Failed to fetch run %s from store during cancel", run_id, exc_info=True)
            return CancelOutcome.unknown

        if row is None:
            return CancelOutcome.unknown

        store_status = row.get("status")
        if row.get("cancel_action") == action:
            return CancelOutcome.requested
        if store_status == "interrupted":
            return CancelOutcome.requested
        if store_status not in ("pending", "running"):
            return CancelOutcome.not_cancellable

        grace_seconds = self.grace_seconds
        lease_expires_at: str | None = row.get("lease_expires_at")

        if not is_lease_expired(lease_expires_at, grace_seconds=grace_seconds):
            return await self._request_remote_cancel(run_id, action=action)

        take_over_msg = f"Run reclaimed by worker {self._worker_id}: the owning worker ({row.get('owner_worker_id') or 'unknown'}) stopped renewing its lease and is presumed dead."
        takeover_kwargs: dict[str, Any] = {
            "grace_seconds": grace_seconds,
            "error": take_over_msg,
            "stop_reason": ORPHAN_RECOVERY_STOP_REASON,
        }
        if self._store.durable_lifecycle:
            takeover_kwargs["expected_state_version"] = row.get("state_version") or 0
        try:
            taken = await self._call_store_with_retry(
                "claim_for_takeover",
                run_id,
                lambda: self._store.claim_for_takeover(
                    run_id,
                    **takeover_kwargs,
                ),
            )
        except Exception:
            logger.warning("Take-over claim for run %s failed with exception", run_id, exc_info=True)
            return CancelOutcome.unknown

        if taken:
            logger.warning("Run %s taken over by worker %s (action=%s)", run_id, self._worker_id, action)
            return CancelOutcome.taken_over

        # The conditional UPDATE matched 0 rows. Two causes:
        #   (a) the owner renewed the lease → persist a cancellation request.
        #   (b) the row went terminal between our read and the claim
        #       (run finished, or another worker already took it over)
        #       → not_cancellable or taken_over.
        # Re-read to distinguish.
        try:
            fresh = await self._store.get(run_id)
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

    async def create_or_reject(
        self,
        thread_id: str,
        assistant_id: str | None = None,
        *,
        candidate_run_id: str | None = None,
        on_disconnect: DisconnectMode = DisconnectMode.cancel,
        metadata: dict | None = None,
        kwargs: dict | None = None,
        multitask_strategy: str = "reject",
        model_name: str | None = None,
        user_id: str | None = None,
        accepted_invocation: Any | None = None,
    ) -> RunRecord:
        """Atomically admit a normal agent run for a thread."""
        admission = await self._admit_thread_operation(
            thread_id,
            assistant_id,
            operation_kind=ThreadOperationKind.run,
            on_disconnect=on_disconnect,
            metadata=metadata,
            kwargs=kwargs,
            multitask_strategy=multitask_strategy,
            model_name=model_name,
            user_id=user_id,
            accepted_invocation=accepted_invocation,
            candidate_run_id=candidate_run_id,
        )
        return admission.record

    async def get_by_external_identity(
        self,
        external_scope: str,
        external_key: str,
        *,
        user_id: str | None = None,
    ) -> RunRecord | None:
        """Return a visible normal run for one normalized external identity."""

        async with self._lock:
            for record in self._runs.values():
                if record.external_scope != external_scope or record.external_key != external_key:
                    continue
                if user_id is not None and record.user_id != user_id:
                    return None
                return record
        if self._store is None:
            return None
        row = await self._store.get_by_external_identity(external_scope, external_key)
        if row is None or (user_id is not None and row.get("user_id") != user_id):
            return None
        return self._replay_record_from_store(row)

    async def ensure_or_reject(
        self,
        thread_id: str,
        assistant_id: str | None = None,
        *,
        candidate_run_id: str | None = None,
        external_scope: str,
        external_key: str,
        request_digest: str,
        request_digest_version: str,
        caller_intent_json: dict[str, Any],
        caller_intent_digest: str,
        caller_intent_digest_version: str,
        on_disconnect: DisconnectMode = DisconnectMode.cancel,
        metadata: dict | None = None,
        kwargs: dict | None = None,
        multitask_strategy: str = "reject",
        model_name: str | None = None,
        user_id: str | None = None,
        accepted_invocation: Any | None = None,
    ) -> RunAdmission:
        """Atomically ensure one normal run for an external identity."""

        return await self._admit_thread_operation(
            thread_id,
            assistant_id,
            operation_kind=ThreadOperationKind.run,
            on_disconnect=on_disconnect,
            metadata=metadata,
            kwargs=kwargs,
            multitask_strategy=multitask_strategy,
            model_name=model_name,
            user_id=user_id,
            accepted_invocation=accepted_invocation,
            external_scope=external_scope,
            external_key=external_key,
            request_digest=request_digest,
            request_digest_version=request_digest_version,
            caller_intent_json=caller_intent_json,
            caller_intent_digest=caller_intent_digest,
            caller_intent_digest_version=caller_intent_digest_version,
            candidate_run_id=candidate_run_id,
        )

    async def _await_atomic_admission_result(
        self,
        operation: str,
        run_id: str,
        call: Callable[[], Awaitable[Any]],
        cancellation: _AdmissionCancellation,
        reconcile: Callable[[Exception], Awaitable[Any]] | None = None,
    ) -> Any:
        """Drain one atomic store decision even when its caller is cancelled.

        A durable store may commit before its coroutine returns the materialized
        row.  Propagating cancellation into that coroutine would make the caller
        unable to distinguish a rollback from a committed, unseen admission.  A
        dedicated shielded task therefore reaches a definite result. Cancellation
        is recorded in a shared state object before that result is inspected, so
        an exceptional decision cannot erase it and trigger another admission.
        """

        async def decide() -> Any:
            try:
                return await self._call_store_with_retry(operation, run_id, call)
            except Exception as exc:
                if reconcile is None:
                    raise
                return await reconcile(exc)

        decision = asyncio.create_task(
            decide(),
            name=f"deerflow-atomic-admission-{run_id}",
        )
        while not decision.done():
            try:
                await asyncio.shield(decision)
            except asyncio.CancelledError:
                current = asyncio.current_task()
                if current is None or current.cancelling() == 0:
                    # The store task, rather than this request, was cancelled.
                    # Its atomic implementation owns rollback semantics.
                    raise
                cancellation.requested = True
        return decision.result()

    async def _close_cancelled_admission(self, record: RunRecord) -> None:
        """Terminalize an unseen run or release an unseen reservation."""
        record.attachment_supervised = False
        if record.operation_kind != ThreadOperationKind.run:
            try:
                if self._store is not None:
                    await self._call_store_with_retry(
                        "release cancelled thread operation",
                        record.run_id,
                        lambda: self._store.delete_thread_operation(
                            record.run_id,
                            user_id=record.user_id,
                        ),
                    )
                    stored = await self._call_store_with_retry(
                        "verify cancelled thread operation release",
                        record.run_id,
                        lambda: self._store.get(record.run_id, user_id=record.user_id),
                    )
                    if stored is not None:
                        raise RuntimeError("cancelled thread operation remains active")
            finally:
                async with self._lock:
                    if self._runs.get(record.run_id) is record:
                        self._runs.pop(record.run_id, None)
                        self._unindex_run_locked(record.run_id, record.thread_id)
            return

        await self.cancel(record.run_id)
        if self._store is None:
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
            if self._store.durable_lifecycle:
                cancel_request = await self._call_store_with_retry(
                    "record cancelled admission request",
                    record.run_id,
                    lambda: self._store.request_cancel_compat(
                        record.run_id,
                        action="interrupt",
                        user_id=record.user_id,
                    ),
                )
                winning_row = cancel_request.row
                if winning_row is not None and winning_row.get("status") in active_statuses:
                    winning_action = winning_row.get("cancel_action") or "interrupt"
                    await self._call_store_with_retry(
                        "terminalize cancelled admission",
                        record.run_id,
                        lambda: self._store.transition_run_atomic(
                            record.run_id,
                            expected_state_version=winning_row["state_version"],
                            expected_statuses=active_statuses,
                            transition=LifecycleTransition(
                                lifecycle_type=LifecycleType.cancelled,
                                status=("error" if winning_action == "rollback" else "interrupted"),
                                error=("Rolled back by user" if winning_action == "rollback" else None),
                            ),
                            user_id=record.user_id,
                        ),
                    )
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

        async with self._lock:
            if self._runs.get(record.run_id) is record:
                self._sync_record_from_store_row(record, stored)

    async def _drain_cancelled_admission_cleanup(self, record: RunRecord) -> None:
        """Finish compensation despite repeated request cancellation."""

        cleanup = asyncio.create_task(
            self._close_cancelled_admission(record),
            name=f"deerflow-close-cancelled-admission-{record.run_id}",
        )
        while not cleanup.done():
            try:
                await asyncio.shield(cleanup)
            except asyncio.CancelledError:
                continue
            except Exception:
                break
        try:
            cleanup.result()
        except asyncio.CancelledError as exc:
            from deerflow.diagnostics import bounded_diagnostic, log_bounded_failure

            log_bounded_failure(
                logger,
                bounded_diagnostic(
                    code="cancelled_admission_cleanup_cancelled",
                    operation="close_cancelled_admission",
                    error=exc,
                    capability_id="run_store",
                ),
            )
        except Exception as exc:
            from deerflow.diagnostics import bounded_diagnostic, log_bounded_failure

            log_bounded_failure(
                logger,
                bounded_diagnostic(
                    code="cancelled_admission_cleanup_failed",
                    operation="close_cancelled_admission",
                    error=exc,
                    capability_id="run_store",
                ),
            )

    async def _admit_thread_operation(
        self,
        thread_id: str,
        assistant_id: str | None = None,
        *,
        candidate_run_id: str | None = None,
        operation_kind: ThreadOperationKind,
        on_disconnect: DisconnectMode = DisconnectMode.cancel,
        metadata: dict | None = None,
        kwargs: dict | None = None,
        multitask_strategy: str = "reject",
        model_name: str | None = None,
        user_id: str | None = None,
        accepted_invocation: Any | None = None,
        external_scope: str | None = None,
        external_key: str | None = None,
        request_digest: str | None = None,
        request_digest_version: str | None = None,
        caller_intent_json: dict[str, Any] | None = None,
        caller_intent_digest: str | None = None,
        caller_intent_digest_version: str | None = None,
    ) -> RunAdmission:
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
        attachment_supervised = operation_kind == ThreadOperationKind.run and candidate_run_id is not None
        if candidate_run_id is None:
            run_id = str(uuid.uuid4())
        else:
            try:
                parsed_candidate = uuid.UUID(candidate_run_id)
            except (TypeError, ValueError, AttributeError):
                raise ValueError("candidate_run_id must be a canonical UUID") from None
            run_id = str(parsed_candidate)
            if run_id != candidate_run_id:
                raise ValueError("candidate_run_id must be a canonical UUID")
        now = _now_iso()

        _supported_strategies = ("reject", "interrupt", "rollback")
        if multitask_strategy not in _supported_strategies:
            raise UnsupportedStrategyError(f"Multitask strategy '{multitask_strategy}' is not yet supported. Supported strategies: {', '.join(_supported_strategies)}")

        lease_expires_at = self._compute_lease_expires_at()
        grace_seconds = self._run_ownership_config.grace_seconds if self._run_ownership_config else 10

        interrupted_records: list[RunRecord] = []
        created_store_row: dict[str, Any] | None = None
        claimed_store_rows: dict[str, dict[str, Any]] = {}
        admission_cancellation = _AdmissionCancellation()
        record = RunRecord(
            run_id=run_id,
            thread_id=thread_id,
            assistant_id=assistant_id,
            status=RunStatus.pending,
            on_disconnect=on_disconnect,
            operation_kind=operation_kind,
            multitask_strategy=multitask_strategy,
            metadata=metadata or {},
            kwargs=kwargs or {},
            user_id=user_id,
            created_at=now,
            updated_at=now,
            model_name=model_name,
            owner_worker_id=self._worker_id,
            lease_expires_at=lease_expires_at,
            accepted_invocation=accepted_invocation if operation_kind == ThreadOperationKind.run else None,
            external_scope=external_scope if operation_kind == ThreadOperationKind.run else None,
            external_key=external_key if operation_kind == ThreadOperationKind.run else None,
            request_digest=request_digest if operation_kind == ThreadOperationKind.run else None,
            request_digest_version=request_digest_version if operation_kind == ThreadOperationKind.run else None,
            caller_intent_json=caller_intent_json if operation_kind == ThreadOperationKind.run else None,
            caller_intent_digest=caller_intent_digest if operation_kind == ThreadOperationKind.run else None,
            caller_intent_digest_version=caller_intent_digest_version if operation_kind == ThreadOperationKind.run else None,
        )

        async with self._lock:
            local_keyed = next(
                (current for current in self._runs.values() if external_scope is not None and external_key is not None and current.external_scope == external_scope and current.external_key == external_key),
                None,
            )
            if local_keyed is not None:
                if local_keyed.user_id != user_id:
                    raise IdempotencyConflictError("Idempotency key is not visible to this principal")
                same_intent = local_keyed.caller_intent_json == caller_intent_json and local_keyed.caller_intent_digest == caller_intent_digest and local_keyed.caller_intent_digest_version == caller_intent_digest_version
                outcome = AdmissionOutcome.known_same if same_intent else AdmissionOutcome.key_conflict
                return RunAdmission(record=local_keyed, outcome=outcome)

            # 1) Local inflight check (same-worker guard; cross-worker is the
            #    store's partial unique index below).
            local_inflight = [r for r in self._thread_records_locked(thread_id) if r.status in (RunStatus.pending, RunStatus.running) or r.finalizing]

            if multitask_strategy in ("interrupt", "rollback") and any(record.operation_kind != ThreadOperationKind.run for record in local_inflight):
                raise ConflictError(f"Thread {thread_id} has an active checkpoint write")

            if multitask_strategy == "reject" and local_inflight:
                active_run = next(
                    (current for current in local_inflight if current.operation_kind == ThreadOperationKind.run),
                    None,
                )
                raise ConflictError(
                    f"Thread {thread_id} already has an active run",
                    active_run_id=(active_run.run_id if active_run is not None else None),
                )

            if multitask_strategy in ("interrupt", "rollback") and local_inflight:
                logger.info(
                    "Preparing to cancel %d inflight run(s) on thread %s (strategy=%s)",
                    len(local_inflight),
                    thread_id,
                    multitask_strategy,
                )

            async def reconcile_store_failure(
                error: Exception,
                *,
                keyed: bool,
            ) -> RunEnsureResult | tuple[dict[str, Any], tuple[dict[str, Any], ...]]:
                """Resolve a lost store response by this attempt's candidate ID."""

                if self._store is None:
                    raise error
                unresolved = _UnresolvedAdmissionCandidate(
                    run_id=run_id,
                    thread_id=thread_id,
                    user_id=user_id,
                    owner_worker_id=self._worker_id,
                    external_scope=external_scope if keyed else None,
                    external_key=external_key if keyed else None,
                    caller_intent_digest=(caller_intent_digest if keyed else None),
                    caller_intent_digest_version=(caller_intent_digest_version if keyed else None),
                    replacement_action=(multitask_strategy if multitask_strategy in ("interrupt", "rollback") else None),
                    predecessor_run_ids=tuple(previous.run_id for previous in local_inflight),
                )
                try:
                    candidate = await self._call_store_with_retry(
                        "reconcile candidate admission",
                        run_id,
                        lambda: self._store.get(run_id, user_id=user_id),
                    )
                except Exception:
                    self._register_unresolved_admission(unresolved)
                    raise error from None

                if candidate is not None:
                    expected = {
                        "run_id": run_id,
                        "thread_id": thread_id,
                        "user_id": user_id,
                        "owner_worker_id": self._worker_id,
                    }
                    if keyed:
                        expected.update(
                            {
                                "external_scope": external_scope,
                                "external_key": external_key,
                                "caller_intent_digest": caller_intent_digest,
                                "caller_intent_digest_version": caller_intent_digest_version,
                            }
                        )
                    if any(candidate.get(field) != value for field, value in expected.items()):
                        self._register_unresolved_admission(unresolved)
                        raise AcceptedEvidenceIntegrityError() from None

                    # The exact candidate proves that this attempt's atomic
                    # replacement committed. Fence locally executing
                    # predecessors before any further store read can fail;
                    # their durable terminal transitions were part of that
                    # same transaction.
                    self._fence_replacement_predecessors_locked(unresolved)

                    claimed: list[dict[str, Any]] = []
                    try:
                        for previous in local_inflight:
                            previous_row = await self._call_store_with_retry(
                                "reconcile replaced admission",
                                previous.run_id,
                                lambda previous_id=previous.run_id, previous_user=previous.user_id: self._store.get(
                                    previous_id,
                                    user_id=previous_user,
                                ),
                            )
                            if previous_row is not None:
                                claimed.append(previous_row)
                                if self._store.durable_lifecycle:
                                    self._sync_record_from_store_row(
                                        previous,
                                        previous_row,
                                    )
                    except Exception:
                        self._register_unresolved_admission(unresolved)
                        raise error from None
                    if keyed:
                        return RunEnsureResult(
                            outcome=AdmissionOutcome.created,
                            row=candidate,
                            claimed=tuple(claimed),
                        )
                    return candidate, tuple(claimed)

                if keyed and external_scope is not None and external_key is not None:
                    try:
                        existing = await self._call_store_with_retry(
                            "reconcile external admission",
                            run_id,
                            lambda: self._store.get_by_external_identity(
                                external_scope,
                                external_key,
                            ),
                        )
                    except Exception:
                        raise error from None
                    if existing is not None:
                        if existing.get("user_id") != user_id:
                            raise IdempotencyConflictError("Idempotency key is not visible to this principal") from None
                        same_intent = existing.get("caller_intent_json") == caller_intent_json and existing.get("caller_intent_digest") == caller_intent_digest and existing.get("caller_intent_digest_version") == caller_intent_digest_version
                        return RunEnsureResult(
                            outcome=(AdmissionOutcome.known_same if same_intent else AdmissionOutcome.key_conflict),
                            row=existing,
                        )
                raise error from None

            # 2) Persist to store while still holding the local lock. The
            #    store is the source of truth for cross-process atomicity.
            if self._store is not None:
                accepted_persisted = accepted_invocation.to_persisted() if accepted_invocation is not None and operation_kind == ThreadOperationKind.run else {}
                keyed = external_scope is not None and external_key is not None
                if keyed:
                    try:
                        store_admission = await self._await_atomic_admission_result(
                            "ensure_run_atomic",
                            run_id,
                            lambda: self._store.ensure_run_atomic(
                                run_id=run_id,
                                thread_id=thread_id,
                                owner_worker_id=self._worker_id,
                                lease_expires_at=lease_expires_at,
                                external_scope=external_scope,
                                external_key=external_key,
                                request_digest=request_digest,
                                request_digest_version=request_digest_version,
                                caller_intent_json=caller_intent_json,
                                caller_intent_digest=caller_intent_digest,
                                caller_intent_digest_version=caller_intent_digest_version,
                                multitask_strategy=multitask_strategy,
                                assistant_id=assistant_id,
                                user_id=user_id,
                                model_name=model_name,
                                metadata=metadata,
                                kwargs=kwargs,
                                created_at=now,
                                grace_seconds=grace_seconds,
                                **accepted_persisted,
                            ),
                            admission_cancellation,
                            lambda error: reconcile_store_failure(error, keyed=True),
                        )
                    except ConflictError:
                        if admission_cancellation.requested:
                            raise asyncio.CancelledError() from None
                        raise
                    except Exception as exc:
                        if admission_cancellation.requested:
                            raise asyncio.CancelledError() from None
                        if _is_unique_violation(exc):
                            raise ConflictError(f"Thread {thread_id} already has an active run") from exc
                        raise
                    if store_admission.outcome is not AdmissionOutcome.created:
                        stored_record = self._replay_record_from_store(store_admission.row)
                        if stored_record.user_id != user_id:
                            raise IdempotencyConflictError("Idempotency key is not visible to this principal")
                        if admission_cancellation.requested:
                            raise asyncio.CancelledError()
                        return RunAdmission(record=stored_record, outcome=store_admission.outcome)
                    created_store_row = store_admission.row
                    claimed_store_rows = {row["run_id"]: row for row in store_admission.claimed}
                elif multitask_strategy == "reject":
                    try:
                        atomic_result = await self._await_atomic_admission_result(
                            "create_thread_operation_atomic",
                            run_id,
                            lambda: self._store.create_thread_operation_atomic(
                                run_id=run_id,
                                thread_id=thread_id,
                                owner_worker_id=self._worker_id,
                                lease_expires_at=lease_expires_at,
                                operation_kind=operation_kind.value,
                                multitask_strategy="reject",
                                assistant_id=assistant_id,
                                user_id=user_id,
                                model_name=model_name,
                                metadata=metadata,
                                kwargs=kwargs,
                                created_at=now,
                                grace_seconds=grace_seconds,
                                **accepted_persisted,
                            ),
                            admission_cancellation,
                            lambda error: reconcile_store_failure(error, keyed=False),
                        )
                        created_store_row, claimed_rows = atomic_result
                        claimed_store_rows = {row["run_id"]: row for row in claimed_rows}
                    except ConflictError:
                        if admission_cancellation.requested:
                            raise asyncio.CancelledError() from None
                        raise
                    except Exception as exc:
                        if admission_cancellation.requested:
                            raise asyncio.CancelledError() from None
                        if _is_unique_violation(exc):
                            raise ConflictError(f"Thread {thread_id} already has an active run") from exc
                        raise
                else:
                    # Interrupt / rollback: store-side claim + insert in one
                    # transaction. Retry on IntegrityError in case another
                    # worker races us between our SELECT FOR UPDATE and INSERT.
                    max_retries = 3
                    for attempt in range(max_retries):
                        try:
                            atomic_result = await self._await_atomic_admission_result(
                                "create_thread_operation_atomic",
                                run_id,
                                lambda: self._store.create_thread_operation_atomic(
                                    run_id=run_id,
                                    thread_id=thread_id,
                                    owner_worker_id=self._worker_id,
                                    lease_expires_at=lease_expires_at,
                                    operation_kind=operation_kind.value,
                                    multitask_strategy=multitask_strategy,
                                    assistant_id=assistant_id,
                                    user_id=user_id,
                                    model_name=model_name,
                                    metadata=metadata,
                                    kwargs=kwargs,
                                    created_at=now,
                                    grace_seconds=grace_seconds,
                                    **accepted_persisted,
                                ),
                                admission_cancellation,
                                lambda error: reconcile_store_failure(error, keyed=False),
                            )
                            created_store_row, claimed_rows = atomic_result
                            claimed_store_rows = {row["run_id"]: row for row in claimed_rows}
                            break
                        except Exception as exc:
                            if admission_cancellation.requested:
                                raise asyncio.CancelledError() from None
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

                if created_store_row is not None:
                    self._sync_record_from_store_row(record, created_store_row)
                    record.store_only = False

            # 3) Only now safe to register locally — store insert succeeded.
            record.attachment_supervised = attachment_supervised
            self._runs[run_id] = record
            self._index_run_locked(record)

            # 4) Cancel local in-memory inflight (interrupt/rollback). The
            #    store-side counterparts were already cancelled in step 2.
            if multitask_strategy in ("interrupt", "rollback"):
                for r in local_inflight:
                    if r.finalizing:
                        continue
                    r.abort_action = multitask_strategy
                    r.abort_event.set()
                    task_active = r.task is not None and not r.task.done()
                    r.finalizing = task_active
                    if task_active:
                        r.task.cancel()
                    claimed_row = claimed_store_rows.get(r.run_id)
                    if claimed_row is not None and self._store.durable_lifecycle:
                        self._sync_record_from_store_row(r, claimed_row)
                    else:
                        r.status = RunStatus.error if multitask_strategy == "rollback" else RunStatus.interrupted
                        r.error = "Rolled back by user" if multitask_strategy == "rollback" else "Cancelled by newer run"
                    r.updated_at = now
                    interrupted_records.append(r)

        # Outside the lock: persist interrupted status for locally-cancelled
        # runs. Store-side claimed rows are already finalised. Cancellation at
        # this point happens after the replacement was admitted, so close that
        # new run before propagating cancellation to the caller.
        try:
            for interrupted_record in interrupted_records:
                if self._store is None or not self._store.durable_lifecycle:
                    await self._persist_status(
                        interrupted_record,
                        interrupted_record.status,
                    )
        except asyncio.CancelledError:
            admission_cancellation.requested = True
        except Exception:
            if not admission_cancellation.requested:
                raise

        if admission_cancellation.requested:
            await self._drain_cancelled_admission_cleanup(record)
            raise asyncio.CancelledError()

        logger.info("Run created: run_id=%s thread_id=%s", run_id, thread_id)
        return RunAdmission(record=record, outcome=AdmissionOutcome.created)

    @asynccontextmanager
    async def reserve_thread_operation(
        self,
        thread_id: str,
        *,
        kind: ThreadOperationKind,
        user_id: str | None = None,
    ) -> AsyncIterator[None]:
        """Hold exclusive durable admission for a non-run thread operation.

        The reservation is a short-lived pending row, so the same durable
        uniqueness constraint used by ``create_or_reject`` closes both sides of
        the race across Gateway workers.
        """
        if kind == ThreadOperationKind.run:
            raise ValueError("Normal runs must be admitted with create_or_reject()")
        admission = await self._admit_thread_operation(
            thread_id,
            operation_kind=kind,
            multitask_strategy="reject",
            user_id=user_id,
        )
        record = admission.record
        try:
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
            if record.abort_event.is_set():
                raise ConflictError(f"Thread {thread_id} reservation lease was lost") from None
            raise
        finally:
            try:
                if self._store is not None:
                    try:
                        await self._call_store_with_retry(
                            "release thread operation",
                            record.run_id,
                            lambda: self._store.delete_thread_operation(record.run_id, user_id=record.user_id),
                        )
                    except Exception:
                        logger.warning(
                            "Failed to release persisted thread operation %s; leaving it for orphan reconciliation",
                            record.run_id,
                            exc_info=True,
                        )
            finally:
                async with self._lock:
                    removed = self._runs.pop(record.run_id, None)
                    if removed is not None:
                        self._unindex_run_locked(record.run_id, removed.thread_id)

    async def reconcile_orphaned_inflight_runs(
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
        effective_stop_reason = stop_reason or ORPHAN_RECOVERY_STOP_REASON
        grace_seconds = self._run_ownership_config.grace_seconds if self._run_ownership_config else 10
        try:
            rows = await self._call_store_with_retry(
                "list_inflight_with_expired_lease",
                "*",
                lambda: self._store.list_inflight_with_expired_lease(before=before, grace_seconds=grace_seconds),
            )
        except Exception:
            logger.warning("Failed to list orphaned inflight runs for reconciliation", exc_info=True)
            return []

        recovered: list[RunRecord] = []
        now = _now_iso()
        for row in rows:
            try:
                record = self._record_from_store(row)
            except Exception as exc:
                from deerflow.diagnostics import bounded_diagnostic, log_bounded_failure

                log_bounded_failure(
                    logger,
                    bounded_diagnostic(
                        code="accepted_evidence_invalid",
                        operation="hydrate_orphaned_invocation",
                        error=exc,
                        capability_id="run_store",
                    ),
                )
                continue

            async with self._lock:
                live_record = self._runs.get(record.run_id)
                if live_record is not None and live_record.status in (RunStatus.pending, RunStatus.running):
                    # Still owned by a local task — skip
                    continue

            try:
                takeover_kwargs = {
                    "grace_seconds": grace_seconds,
                    "error": error,
                    "stop_reason": effective_stop_reason,
                }
                if self._store.durable_lifecycle:
                    takeover_kwargs["expected_state_version"] = row.get("state_version") or 0
                claimed = await self._call_store_with_retry(
                    "claim_for_takeover",
                    record.run_id,
                    lambda: self._store.claim_for_takeover(
                        record.run_id,
                        **takeover_kwargs,
                    ),
                )
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
            record.error = error
            record.stop_reason = effective_stop_reason
            record.updated_at = now
            if self._store.durable_lifecycle:
                stored = await self._store.get(record.run_id)
                if stored is not None:
                    self._sync_record_from_store_row(record, stored)
            if record.operation_kind == ThreadOperationKind.run:
                # The atomic takeover above must win before writing a zero-delivery
                # receipt; otherwise a stale scan could race a heartbeat renewal and
                # permanently overwrite a live run's later detailed receipt. The
                # receipt remains best-effort, matching normal terminal delivery
                # when its event store is unavailable.
                await self._ensure_delivery_receipt(record)
                recovered.append(record)

        if recovered:
            logger.warning("Recovered %d orphaned inflight run(s) as error", len(recovered))
        return recovered

    async def has_inflight(self, thread_id: str) -> bool:
        """Return ``True`` if *thread_id* has a pending or running run."""
        async with self._lock:
            return any(r.operation_kind == ThreadOperationKind.run and (r.status in (RunStatus.pending, RunStatus.running) or r.finalizing) for r in self._thread_records_locked(thread_id))

    async def cleanup(self, run_id: str, *, delay: float = 300) -> None:
        """Remove a run record after an optional delay."""
        if delay > 0:
            await asyncio.sleep(delay)
        async with self._lock:
            record = self._runs.pop(run_id, None)
            if record is not None:
                self._unindex_run_locked(run_id, record.thread_id)
        logger.debug("Run record %s cleaned up", run_id)

    # ------------------------------------------------------------------
    # Lease heartbeat
    # ------------------------------------------------------------------

    @property
    def worker_id(self) -> str:
        """Return this worker's unique identifier."""
        return self._worker_id

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
    ) -> bool:
        """Fence one local run and cancel its execution task.

        No store write is attempted here: once the last confirmed lease has
        expired, this worker is no longer authorized to publish a terminal
        outcome. A peer reconciler owns durable terminalization.
        """
        task_to_cancel: asyncio.Task | None = None
        async with self._lock:
            current = self._runs.get(record.run_id)
            if current is not record:
                return False
            if require_active:
                if record.status not in (RunStatus.pending, RunStatus.running):
                    return False
                if record.task is not None and record.task.done():
                    return False
            if record.ownership_lost:
                return True
            record.ownership_lost = True
            record.abort_event.set()
            record.status = RunStatus.error
            record.error = reason
            record.updated_at = _now_iso()
            if record.task is not None and not record.task.done() and record.task is not asyncio.current_task():
                task_to_cancel = record.task

        if task_to_cancel is not None:
            task_to_cancel.cancel()
        logger.error("Run %s lost lease ownership; local execution was fenced: %s", record.run_id, reason)
        return True

    async def start_heartbeat(self) -> None:
        """Start the background lease-renewal task.

        No-op when ``heartbeat_enabled`` is ``False`` or the task is already running.
        """
        if not self.heartbeat_enabled:
            return
        if self._heartbeat_task is not None and not self._heartbeat_task.done():
            return
        self._heartbeat_stop = asyncio.Event()
        task = asyncio.create_task(self._heartbeat_loop())
        task.set_name("deerflow-run-lease-heartbeat")
        self._heartbeat_task = task
        logger.info("Run lease heartbeat started for worker %s", self._worker_id)

    async def stop_heartbeat(self, *, timeout: float = 5.0) -> None:
        """Stop the background heartbeat task within ``timeout`` seconds."""
        if self._heartbeat_stop is not None:
            self._heartbeat_stop.set()
        if self._heartbeat_task is not None and not self._heartbeat_task.done():
            _, pending = await asyncio.wait(
                (self._heartbeat_task,),
                timeout=max(0.0, timeout),
            )
            if pending:
                self._heartbeat_task.cancel()
                try:
                    await self._heartbeat_task
                except asyncio.CancelledError:
                    pass
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
                await self._renew_leases()
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

    async def _renew_leases(self) -> None:
        """Renew locally-owned leases, failing closed at their deadlines.

        ``RunRecord.lease_expires_at`` advances only after a successful durable
        renewal, so it is the last confirmed ownership deadline. Transient
        exceptions are tolerated before that deadline; a call that blocks or
        keeps failing through it fences the local run.
        """
        if self._store is None or self._run_ownership_config is None:
            return
        lease_seconds = self._run_ownership_config.lease_seconds
        cancellations: list[tuple[str, str]] = []

        async with self._lock:
            # A taskless pending row is live only while the application
            # admission coordinator explicitly owns its commit-to-worker
            # handoff. Arbitrary ``task is None`` rows are never renewed.
            active_runs = [
                (rid, record)
                for rid, record in self._runs.items()
                if record.status in (RunStatus.pending, RunStatus.running) and record.owner_worker_id == self._worker_id and ((record.task is None and record.attachment_supervised) or (record.task is not None and not record.task.done()))
            ]

        for run_id, record in active_runs:
            confirmed_deadline = self._parse_lease_deadline(record.lease_expires_at)
            if confirmed_deadline is None or confirmed_deadline <= datetime.now(UTC):
                await self._mark_ownership_lost(
                    record,
                    reason="Lease ownership could not be confirmed before the last confirmed lease expired.",
                )
                continue

            remaining = (confirmed_deadline - datetime.now(UTC)).total_seconds()
            new_expiry = (datetime.now(UTC) + timedelta(seconds=lease_seconds)).isoformat()
            try:
                async with asyncio.timeout(remaining):
                    renewal = await self._call_store_with_retry(
                        "renew_lease",
                        run_id,
                        lambda: self._store.renew_lease(
                            run_id,
                            owner_worker_id=self._worker_id,
                            lease_expires_at=new_expiry,
                        ),
                    )
                if renewal.renewed:
                    if confirmed_deadline <= datetime.now(UTC):
                        await self._mark_ownership_lost(
                            record,
                            reason="Lease renewal completed after the last confirmed lease had already expired.",
                        )
                        continue
                    if record.execution_lease_renewal is not None:
                        external_remaining = (confirmed_deadline - datetime.now(UTC)).total_seconds()
                        try:
                            if external_remaining <= 0:
                                external_renewed = False
                            else:
                                async with asyncio.timeout(external_remaining):
                                    external_renewed = await record.execution_lease_renewal()
                        except Exception:
                            external_renewed = False
                        if not external_renewed:
                            await self._mark_ownership_lost(
                                record,
                                reason=("The accepted sandbox attempt could not be renewed after durable run ownership renewal."),
                            )
                            continue
                    # Unsynced write is benign: ``lease_expires_at`` is the
                    # only field on an existing record this path mutates, so
                    # there is no concurrent writer to race against
                    # (``set_status`` / ``_persist_status`` touch other
                    # fields). Re-acquiring ``self._lock`` here would
                    # serialise against unrelated run mutations for no gain.
                    record.lease_expires_at = new_expiry
                    if renewal.cancel_action is not None:
                        action = renewal.cancel_action
                        if action not in ("interrupt", "rollback"):
                            logger.warning(
                                "Run %s has invalid durable cancel action %r; using interrupt",
                                run_id,
                                action,
                            )
                            action = "interrupt"
                        cancellations.append((run_id, action))
                else:
                    # ``renew_lease`` returned False — the row was claimed
                    # by another worker (status is no longer pending/running,
                    # or ``owner_worker_id`` changed). Stop the local task so
                    # we don't waste CPU or overwrite the takeover status on
                    # finalisation.
                    async with self._lock:
                        still_active = self._runs.get(run_id) is record and record.status in (RunStatus.pending, RunStatus.running) and record.owner_worker_id == self._worker_id and (record.task is None or not record.task.done())
                    if still_active:
                        logger.warning(
                            "Run %s lease renewal failed (status=%s,owner=%s) – worker likely taken over; aborting local task",
                            run_id,
                            record.status.value,
                            record.owner_worker_id,
                        )
                        await self._mark_ownership_lost(
                            record,
                            reason="The durable store rejected lease renewal for this worker.",
                        )
            except Exception:
                if confirmed_deadline <= datetime.now(UTC):
                    await self._mark_ownership_lost(
                        record,
                        reason="Lease ownership could not be confirmed before the last confirmed lease expired.",
                    )
                else:
                    logger.warning(
                        "Failed to renew lease for run %s before its confirmed deadline; will retry",
                        run_id,
                        exc_info=True,
                    )

        # Keep cancellation status writes and cleanup out of the sole renewal
        # loop. After every local lease has had a chance to renew, only signal
        # the owning worker task; that task performs normal terminal handling.
        for run_id, action in cancellations:
            await self._signal_local_cancel(
                run_id,
                action=action,
            )

    async def _reconcile_orphans_periodic(self) -> None:
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
                    await self._on_orphans_recovered(recovered)
                except Exception:
                    logger.warning(
                        "Periodic orphan recovery callback failed for %d run(s): run_ids=%s",
                        len(recovered),
                        [record.run_id for record in recovered],
                        exc_info=True,
                    )

    def _schedule_orphan_reconciliation(self) -> None:
        """Start one supervised recovery pass unless one is already running."""
        task = self._orphan_recovery_task
        if task is not None and not task.done():
            logger.debug("Skipping periodic orphan reconciliation: previous pass is still running")
            return
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
            await asyncio.gather(task, return_exceptions=True)
            logger.warning(
                "Orphan recovery drain exceeded %.1fs on shutdown; cancelled the active pass",
                timeout,
            )

    async def shutdown(self, *, timeout: float = 5.0) -> bool:
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

        Returns ``True`` only when every local run task settled and each
        required interrupted transition was persisted within the deadline.
        The Gateway uses this proof to decide whether a final memory flush can
        race a late run writer.

        Draining in-flight runs *before* the checkpointer is closed lets each
        run that settles within ``timeout`` flush its final checkpoint while
        resources are still open. Only runs that do **not** settle on their own
        are marked ``interrupted`` — a run that completes (e.g. ``success``)
        during the drain keeps its real terminal status instead of being
        blanket-overwritten. The whole drain, including the trailing status
        persistence, is bounded by ``timeout`` so a run stuck in cleanup (or a
        slow store under DB pressure) cannot hang worker shutdown — the
        precondition for the signal-reentrancy deadlock guarded by
        ``app.gateway.app._SHUTDOWN_HOOK_TIMEOUT_SECONDS``. Runs still active
        after ``timeout`` are logged and may still race teardown.
        """
        loop = asyncio.get_running_loop()
        deadline = loop.time() + timeout

        compensation_drain = asyncio.create_task(
            self.drain_admission_compensations(
                timeout=max(0.0, deadline - loop.time()),
            ),
            name="deerflow-drain-admission-compensations",
        )

        async def finish_compensation_drain() -> bool:
            done, _ = await asyncio.wait(
                (compensation_drain,),
                timeout=max(0.0, deadline - loop.time()),
            )
            if not done:
                return False
            return compensation_drain.result()

        async with self._lock:
            unattached = [record for record in self._runs.values() if record.status in (RunStatus.pending, RunStatus.running) and record.task is None and record.attachment_supervised]
            inflight = [record for record in self._runs.values() if record.status in (RunStatus.pending, RunStatus.running) and record.task is not None and not record.task.done()]
            for record in inflight:
                record.abort_action = "interrupt"
                record.abort_event.set()
                record.task.cancel()  # type: ignore[union-attr]  # filtered above
                # Status is decided AFTER the drain (below), not here: a run that
                # completes on its own during the drain must keep its real status.

        attachment_complete = True
        if unattached:
            remaining = deadline - loop.time()
            if remaining <= 0:
                attachment_complete = False
            else:
                try:
                    attachment_results = await asyncio.wait_for(
                        asyncio.gather(
                            *(
                                self.fail_start_if_pending(
                                    record.run_id,
                                    error="worker_attachment_failed",
                                )
                                for record in unattached
                            ),
                            return_exceptions=True,
                        ),
                        timeout=remaining,
                    )
                except TimeoutError:
                    attachment_complete = False
                else:
                    attachment_complete = all(result is True for result in attachment_results)
            if not attachment_complete:
                logger.warning("Run drain did not terminalize every supervised worker attachment")

        await self.stop_heartbeat(timeout=max(0.0, deadline - loop.time()))

        if not inflight:
            await self._drain_orphan_recovery_task(timeout=max(0.0, deadline - loop.time()))
            return attachment_complete and await finish_compensation_drain()

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
                    task.exception()  # type: ignore[union-attr]  # done & not cancelled
                    continue
                if record.status not in (RunStatus.pending, RunStatus.running):
                    # Cancellation raced a real terminal commit. Preserve it
                    # and never emit a second shutdown terminal transition.
                    continue
                record.status = RunStatus.interrupted
                record.updated_at = _now_iso()
                to_persist.append(record)

        # Bound the trailing status persistence within the remaining budget so a
        # slow store (``_call_store_with_retry`` can back off under DB pressure)
        # cannot push shutdown past ``timeout``.
        persistence_complete = True
        if to_persist:
            remaining = deadline - loop.time()
            if remaining <= 0:
                persistence_complete = False
                logger.warning("Run drain budget exhausted before persisting %d interrupted run(s) on shutdown", len(to_persist))
            else:
                try:
                    results = await asyncio.wait_for(
                        asyncio.gather(*(self._persist_status(record, RunStatus.interrupted) for record in to_persist), return_exceptions=True),
                        timeout=remaining,
                    )
                except TimeoutError:
                    persistence_complete = False
                    logger.warning("Run drain status persistence exceeded the %.1fs budget; %d record(s) may not be persisted", timeout, len(to_persist))
                else:
                    # ``_persist_status`` is best-effort: it catches and logs its
                    # own failures, returning ``False``. Inspect the aggregate so a
                    # partial failure is surfaced at shutdown level (with the
                    # run_id) instead of being silently swallowed by the gather.
                    for record, result in zip(to_persist, results):
                        if isinstance(result, Exception):
                            persistence_complete = False
                            logger.warning("Unexpected error persisting interrupted status for run %s during shutdown: %r", record.run_id, result)
                        elif result is False:
                            persistence_complete = False
                            logger.warning("Could not persist interrupted status for run %s during shutdown", record.run_id)

        if pending:
            logger.warning("Run drain exceeded %.1fs on shutdown; %d run task(s) still active and may race checkpointer teardown", timeout, len(pending))
        logger.info("Drained %d in-flight run(s) on shutdown (%d settled within %.1fs)", len(inflight), len(inflight) - len(pending), timeout)
        await self._drain_orphan_recovery_task(timeout=max(0.0, deadline - loop.time()))
        compensation_complete = await finish_compensation_drain()
        return not pending and persistence_complete and attachment_complete and compensation_complete


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

    def __init__(self, message: str, *, active_run_id: str | None = None) -> None:
        super().__init__(message)
        self.active_run_id = active_run_id


class IdempotencyConflictError(ConflictError):
    """Raised when an external identity is bound to a different request."""


class UnsupportedStrategyError(Exception):
    """Raised when a multitask_strategy value is not yet implemented."""
