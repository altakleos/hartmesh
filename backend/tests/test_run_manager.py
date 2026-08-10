"""Tests for RunManager."""

import asyncio
import logging
import re
import sqlite3
from dataclasses import replace
from typing import Any

import pytest
from sqlalchemy.exc import DatabaseError as SQLAlchemyDatabaseError

from deerflow.config.run_ownership_config import RunOwnershipConfig
from deerflow.runtime import DisconnectMode, RunManager, RunStatus, ThreadOperationKind
from deerflow.runtime.events.store.memory import MemoryRunEventStore
from deerflow.runtime.runs.manager import (
    CancelOutcome,
    ConflictError,
    PersistenceRetryPolicy,
    RunStartOutcome,
    RunStartupError,
    _admission_compensation_retry_delay,
    _AdmissionTerminalDisposition,
    _UnresolvedAdmissionCandidate,
)
from deerflow.runtime.runs.store.base import CancellationRequestOutcome
from deerflow.runtime.runs.store.memory import MemoryRunStore

ISO_RE = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}")


@pytest.fixture
def manager() -> RunManager:
    return RunManager()


class FlakyStatusRunStore(MemoryRunStore):
    """Memory run store that simulates transient SQLite status-write failures."""

    def __init__(self, *, status_failures: int) -> None:
        super().__init__()
        self.status_failures = status_failures
        self.status_update_attempts = 0

    async def update_status(self, run_id, status, *, error=None, stop_reason=None):
        self.status_update_attempts += 1
        if self.status_failures > 0:
            self.status_failures -= 1
            raise sqlite3.OperationalError("database is locked")
        return await super().update_status(run_id, status, error=error, stop_reason=stop_reason)


class MissingRowStatusRunStore(MemoryRunStore):
    """Memory run store that reports a missing row for status updates."""

    async def update_status(self, run_id, status, *, error=None, stop_reason=None):
        await super().update_status(run_id, status, error=error, stop_reason=stop_reason)
        return False


class PermanentStatusRunStore(MemoryRunStore):
    """Memory run store that simulates a permanent SQLAlchemy write failure."""

    def __init__(self) -> None:
        super().__init__()
        self.status_update_attempts = 0

    async def update_status(self, run_id, status, *, error=None, stop_reason=None):
        self.status_update_attempts += 1
        raise SQLAlchemyDatabaseError(
            "UPDATE runs SET status = :status WHERE run_id = :run_id",
            {"status": status, "run_id": run_id},
            sqlite3.DatabaseError("no such table: runs"),
        )


class FailingTakeoverRunStore(MemoryRunStore):
    """Memory run store that always fails takeover claims."""

    def __init__(self) -> None:
        super().__init__()
        self.takeover_attempts = 0

    async def claim_for_takeover(self, run_id, *, grace_seconds, error, stop_reason=None):
        self.takeover_attempts += 1
        raise sqlite3.OperationalError("database is locked")


class MissingCompletionRunStore(MemoryRunStore):
    """Memory run store that reports one missing row for completion updates."""

    def __init__(self) -> None:
        super().__init__()
        self.completion_update_attempts = 0

    async def update_run_completion(self, run_id, *, status, **kwargs):
        self.completion_update_attempts += 1
        if self.completion_update_attempts == 1:
            return False
        return await super().update_run_completion(run_id, status=status, **kwargs)


class AlwaysMissingCompletionRunStore(MemoryRunStore):
    """Memory run store that keeps reporting missing rows for completion updates."""

    def __init__(self) -> None:
        super().__init__()
        self.completion_update_attempts = 0

    async def update_run_completion(self, run_id, *, status, **kwargs):
        self.completion_update_attempts += 1
        return False


class FailingDeleteRunStore(MemoryRunStore):
    """Run store that cannot release a persisted thread-operation row."""

    async def delete(self, run_id, *, user_id=None):
        raise RuntimeError("delete failed")


class LostLeaseRunStore(MemoryRunStore):
    """Run store that reports a reservation was taken over."""

    async def update_lease(self, run_id, *, owner_worker_id, lease_expires_at):
        return False


class PausedLostLeaseRunStore(MemoryRunStore):
    """Run store whose failed renewal can be released after reservation cleanup."""

    def __init__(self) -> None:
        super().__init__()
        self.renewal_started = asyncio.Event()
        self.finish_renewal = asyncio.Event()

    async def update_lease(self, run_id, *, owner_worker_id, lease_expires_at):
        self.renewal_started.set()
        await self.finish_renewal.wait()
        return False


class CommitBeforeReturnRunStore(MemoryRunStore):
    """Expose cancellation after atomic commit but before its result is returned."""

    # A custom store must opt in explicitly; inheriting implementation code is
    # not proof of lifecycle atomicity in the public RunStore contract.
    durable_lifecycle = True

    def __init__(self) -> None:
        super().__init__()
        self.pause_after_commit = False
        self.pause_after_ensure = False
        self.atomic_committed = asyncio.Event()
        self.release_result = asyncio.Event()
        self.ensure_decided = asyncio.Event()
        self.release_ensure = asyncio.Event()

    async def create_thread_operation_atomic(self, run_id, **kwargs):
        result = await super().create_thread_operation_atomic(run_id, **kwargs)
        if self.pause_after_commit:
            self.atomic_committed.set()
            await self.release_result.wait()
        return result

    async def ensure_run_atomic(self, run_id, **kwargs):
        result = await super().ensure_run_atomic(run_id, **kwargs)
        if self.pause_after_ensure:
            self.ensure_decided.set()
            await self.release_ensure.wait()
        return result


class ResponseLostAfterCommitRunStore(MemoryRunStore):
    """Commit one candidate row, then lose the store response exactly once."""

    durable_lifecycle = True

    def __init__(self) -> None:
        super().__init__()
        self.lose_create_response = False
        self.lose_ensure_response = False

    async def create_thread_operation_atomic(self, run_id, **kwargs):
        result = await super().create_thread_operation_atomic(run_id, **kwargs)
        if self.lose_create_response:
            self.lose_create_response = False
            raise OSError("response lost after durable commit")
        return result

    async def ensure_run_atomic(self, run_id, **kwargs):
        result = await super().ensure_run_atomic(run_id, **kwargs)
        if self.lose_ensure_response:
            self.lose_ensure_response = False
            raise OSError("response lost after durable commit")
        return result


class ResponseLostWithUnavailableCandidateReadStore(ResponseLostAfterCommitRunStore):
    """Lose the commit response, then recover exact-candidate reads on demand."""

    def __init__(self) -> None:
        super().__init__()
        self.candidate_reads_available = False

    async def get(self, run_id, *, user_id=None):
        if not self.candidate_reads_available:
            raise OSError("candidate lookup unavailable")
        return await super().get(run_id, user_id=user_id)


class ResponseLostWithUnavailablePredecessorReadStore(ResponseLostAfterCommitRunStore):
    """Find the committed candidate, then lose the displaced-row response."""

    def __init__(self) -> None:
        super().__init__()
        self.predecessor_run_id: str | None = None
        self.candidate_run_id: str | None = None
        self.fail_predecessor_read = False
        self.candidate_reads_available = True

    async def get(self, run_id, *, user_id=None):
        if run_id == self.candidate_run_id and not self.candidate_reads_available:
            raise OSError("candidate lookup unavailable")
        if run_id == self.predecessor_run_id and self.fail_predecessor_read:
            self.fail_predecessor_read = False
            self.candidate_reads_available = False
            raise OSError("predecessor lookup unavailable")
        return await super().get(run_id, user_id=user_id)


class FailStartPersistenceUnavailableStore(MemoryRunStore):
    """Lose every terminal-write and verification call until recovery."""

    durable_lifecycle = True

    def __init__(self) -> None:
        super().__init__()
        self.fail_terminalization = False

    async def transition_run_atomic(self, *args, **kwargs):
        if self.fail_terminalization:
            raise OSError("terminal transition unavailable")
        return await super().transition_run_atomic(*args, **kwargs)

    async def transition_owned_run_atomic(self, *args, **kwargs):
        if self.fail_terminalization:
            raise OSError("owned terminal transition unavailable")
        return await super().transition_owned_run_atomic(*args, **kwargs)

    async def get(self, *args, **kwargs):
        if self.fail_terminalization:
            raise OSError("terminal verification unavailable")
        return await super().get(*args, **kwargs)


class CancelledAdmissionPersistenceUnavailableStore(CommitBeforeReturnRunStore):
    """Commit an admission, then make cancellation persistence unavailable."""

    durable_lifecycle = True

    def __init__(self) -> None:
        super().__init__()
        self.compensation_available = True

    async def request_cancel_compat(self, *args, **kwargs):
        if not self.compensation_available:
            raise OSError("cancel request unavailable")
        return await super().request_cancel_compat(*args, **kwargs)

    async def request_cancel_owned(self, *args, **kwargs):
        if not self.compensation_available:
            raise OSError("owned cancel request unavailable")
        return await super().request_cancel_owned(*args, **kwargs)

    async def transition_run_atomic(self, *args, **kwargs):
        if not self.compensation_available:
            raise OSError("cancel transition unavailable")
        return await super().transition_run_atomic(*args, **kwargs)

    async def transition_owned_run_atomic(self, *args, **kwargs):
        if not self.compensation_available:
            raise OSError("owned cancel transition unavailable")
        return await super().transition_owned_run_atomic(*args, **kwargs)

    async def get(self, *args, **kwargs):
        if not self.compensation_available:
            raise OSError("cancel verification unavailable")
        return await super().get(*args, **kwargs)

    async def authoritative_get(self, run_id: str):
        """Read the backing row without the injected availability failure."""

        return await super().get(run_id)


class PausedOwnedCompensationRunStore(MemoryRunStore):
    """Pause exact compensation before its authoritative ownership check."""

    durable_lifecycle = True

    def __init__(self) -> None:
        super().__init__()
        self.compensation_started = asyncio.Event()
        self.release_compensation = asyncio.Event()

    async def transition_owned_run_atomic(self, *args, **kwargs):
        self.compensation_started.set()
        await self.release_compensation.wait()
        return await super().transition_owned_run_atomic(*args, **kwargs)


class TasklessCancellationUnavailableStore(MemoryRunStore):
    """Fail exact taskless cancellation until the durable store recovers."""

    durable_lifecycle = True

    def __init__(self) -> None:
        super().__init__()
        self.compensation_available = False

    async def request_cancel_owned(self, *args, **kwargs):
        if not self.compensation_available:
            raise OSError("owned cancellation unavailable")
        return await super().request_cancel_owned(*args, **kwargs)

    async def transition_owned_run_atomic(self, *args, **kwargs):
        if not self.compensation_available:
            raise OSError("owned terminal transition unavailable")
        return await super().transition_owned_run_atomic(*args, **kwargs)


class PausedOwnedCancellationRunStore(MemoryRunStore):
    """Pause owned cancellation before or after its atomic mutation."""

    durable_lifecycle = True

    def __init__(self, *, pause_after_commit: bool) -> None:
        super().__init__()
        self.pause_after_commit = pause_after_commit
        self.cancellation_paused = asyncio.Event()
        self.release_cancellation = asyncio.Event()
        self.terminal_paused = asyncio.Event()
        self.release_terminal = asyncio.Event()

    async def request_cancel_owned(self, *args, **kwargs):
        if not self.pause_after_commit:
            self.cancellation_paused.set()
            await self.release_cancellation.wait()
        result = await super().request_cancel_owned(*args, **kwargs)
        if self.pause_after_commit:
            self.cancellation_paused.set()
            await self.release_cancellation.wait()
        return result

    async def transition_owned_run_atomic(self, *args, **kwargs):
        self.terminal_paused.set()
        await self.release_terminal.wait()
        return await super().transition_owned_run_atomic(*args, **kwargs)


class PausedOwnedStatusRunStore(MemoryRunStore):
    """Pause an ownership-fenced status transition before its store check."""

    durable_lifecycle = True

    def __init__(self) -> None:
        super().__init__()
        self.pause_owned_transition = False
        self.transition_paused = asyncio.Event()
        self.release_transition = asyncio.Event()

    async def transition_owned_run_atomic(self, *args, **kwargs):
        if self.pause_owned_transition:
            self.transition_paused.set()
            await self.release_transition.wait()
        return await super().transition_owned_run_atomic(*args, **kwargs)


class CancelledAmbiguousAdmissionStore(MemoryRunStore):
    """Commit, lose the response, and hide the candidate until recovery."""

    durable_lifecycle = True

    def __init__(self) -> None:
        super().__init__()
        self.atomic_committed = asyncio.Event()
        self.release_response_loss = asyncio.Event()
        self.candidate_reads_available = False

    async def create_thread_operation_atomic(self, run_id, **kwargs):
        await super().create_thread_operation_atomic(run_id, **kwargs)
        self.atomic_committed.set()
        await self.release_response_loss.wait()
        raise OSError("response lost after durable commit")

    async def ensure_run_atomic(self, run_id, **kwargs):
        await super().ensure_run_atomic(run_id, **kwargs)
        self.atomic_committed.set()
        await self.release_response_loss.wait()
        raise OSError("response lost after durable commit")

    async def get(self, run_id, *, user_id=None):
        if not self.candidate_reads_available:
            raise OSError("candidate lookup unavailable")
        return await super().get(run_id, user_id=user_id)

    async def authoritative_get(self, run_id: str):
        """Read the backing row without the injected availability failure."""

        return await super().get(run_id)


class CancelledUniqueRetryRunStore(MemoryRunStore):
    """Return a retryable unique failure after the request was cancelled."""

    durable_lifecycle = True

    def __init__(self) -> None:
        super().__init__()
        self.atomic_attempts = 0
        self.first_attempt_started = asyncio.Event()
        self.release_first_attempt = asyncio.Event()

    async def create_thread_operation_atomic(self, run_id, **kwargs):
        self.atomic_attempts += 1
        if self.atomic_attempts == 1:
            self.first_attempt_started.set()
            await self.release_first_attempt.wait()
            raise sqlite3.IntegrityError("UNIQUE constraint failed: runs.thread_id")
        return await super().create_thread_operation_atomic(run_id, **kwargs)


async def _stored_statuses(store: MemoryRunStore, *run_ids: str) -> dict[str, Any]:
    rows = {}
    for run_id in run_ids:
        row = await store.get(run_id)
        rows[run_id] = row["status"] if row else None
    return rows


def test_admission_compensation_backoff_is_deterministic_and_capped() -> None:
    assert [_admission_compensation_retry_delay(round_number) for round_number in range(1, 9)] == [
        0.1,
        0.2,
        0.4,
        0.8,
        1.6,
        3.2,
        5.0,
        5.0,
    ]
    with pytest.raises(ValueError, match="positive integer"):
        _admission_compensation_retry_delay(0)


@pytest.mark.anyio
async def test_unresolved_candidate_registration_only_refines_identity_and_terminal_intent() -> None:
    store = ResponseLostWithUnavailableCandidateReadStore()
    manager = RunManager(store=store)
    poor = _UnresolvedAdmissionCandidate(
        run_id="00000000-0000-0000-0000-000000000101",
        thread_id="thread-candidate-merge",
        user_id="owner-1",
        owner_worker_id="worker-1",
        external_scope="scope-1",
        external_key="delivery-1",
        caller_intent_digest="a" * 64,
        caller_intent_digest_version="intent-v1",
        replacement_action="rollback",
        predecessor_run_ids=("predecessor-1",),
    )
    refined = _UnresolvedAdmissionCandidate(
        run_id=poor.run_id,
        thread_id=poor.thread_id,
        user_id=poor.user_id,
        owner_worker_id=poor.owner_worker_id,
        external_scope=poor.external_scope,
        external_key=poor.external_key,
        caller_intent_digest=poor.caller_intent_digest,
        caller_intent_digest_version=poor.caller_intent_digest_version,
        predecessor_run_ids=("predecessor-2",),
        commit_proven=True,
        terminal_disposition=_AdmissionTerminalDisposition.cancelled,
        cancellation_action="interrupt",
    )

    manager._register_unresolved_admission(poor)
    manager._register_unresolved_admission(refined)

    retained = manager._unresolved_admissions[poor.run_id]
    assert retained.commit_proven is True
    assert retained.replacement_action == "rollback"
    assert retained.predecessor_run_ids == ("predecessor-1", "predecessor-2")
    assert retained.terminal_disposition is _AdmissionTerminalDisposition.cancelled
    assert retained.cancellation_action == "interrupt"

    contradictory = replace(refined, thread_id="another-thread")
    with pytest.raises(RunStartupError, match="identity conflict"):
        manager._register_unresolved_admission(contradictory)
    assert manager.admission_compensations_ready() is False
    assert manager._unresolved_admissions[poor.run_id] == retained

    task = manager._admission_compensation_task
    assert task is not None
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task


@pytest.mark.anyio
async def test_unresolved_candidate_refinement_wakes_a_sleeping_compensator(
    monkeypatch,
) -> None:
    manager = RunManager(store=MemoryRunStore())
    candidate = _UnresolvedAdmissionCandidate(
        run_id="00000000-0000-0000-0000-000000000102",
        thread_id="thread-candidate-wakeup",
        user_id="owner-1",
        owner_worker_id=manager.worker_id,
        external_scope=None,
        external_key=None,
        caller_intent_digest=None,
        caller_intent_digest_version=None,
    )
    first_attempt = asyncio.Event()
    second_attempt = asyncio.Event()
    attempts = 0

    async def remain_unresolved(_candidate) -> bool:
        nonlocal attempts
        attempts += 1
        (first_attempt if attempts == 1 else second_attempt).set()
        return False

    monkeypatch.setattr(manager, "_resolve_unresolved_admission", remain_unresolved)
    monkeypatch.setattr(
        "deerflow.runtime.runs.manager._admission_compensation_retry_delay",
        lambda _round: 60.0,
    )

    manager._register_unresolved_admission(candidate)
    await asyncio.wait_for(first_attempt.wait(), timeout=1)
    await asyncio.sleep(0)
    manager._register_unresolved_admission(
        replace(candidate, commit_proven=True),
    )
    await asyncio.wait_for(second_attempt.wait(), timeout=0.2)

    task = manager._admission_compensation_task
    assert task is not None
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task


@pytest.mark.anyio
async def test_reservation_delete_failure_preserves_body_error_and_clears_local_record(caplog):
    store = FailingDeleteRunStore()
    manager = RunManager(
        store=store,
        persistence_retry_policy=PersistenceRetryPolicy(max_attempts=1, initial_delay=0),
    )

    with caplog.at_level(logging.WARNING), pytest.raises(ValueError, match="body failed"):
        async with manager.reserve_thread_operation(
            "thread-1",
            kind=ThreadOperationKind.checkpoint_write,
        ):
            raise ValueError("body failed")

    assert not await manager.has_inflight("thread-1")
    assert manager._runs == {}
    assert manager._runs_by_thread == {}
    assert len(await store.list_inflight()) == 1
    assert "leaving it for orphan reconciliation" in caplog.text


@pytest.mark.anyio
async def test_reservation_lease_loss_surfaces_as_conflict_after_cancelling_body():
    store = LostLeaseRunStore()
    manager = RunManager(
        store=store,
        run_ownership_config=RunOwnershipConfig(
            lease_seconds=30,
            grace_seconds=10,
            heartbeat_enabled=True,
        ),
    )
    entered = asyncio.Event()

    async def hold_reservation() -> None:
        async with manager.reserve_thread_operation(
            "thread-1",
            kind=ThreadOperationKind.checkpoint_write,
        ):
            entered.set()
            await asyncio.Event().wait()

    task = asyncio.create_task(hold_reservation())
    await entered.wait()

    await manager._renew_leases()

    with pytest.raises(ConflictError, match="reservation lease was lost"):
        await task
    assert not await manager.has_inflight("thread-1")
    assert await store.list_inflight() == []


@pytest.mark.anyio
async def test_reservation_cancelled_while_attaching_task_is_released(monkeypatch):
    store = MemoryRunStore()
    manager = RunManager(store=store)
    admitted = asyncio.Event()
    return_from_admission = asyncio.Event()
    original_admit = manager._admit_thread_operation

    async def pause_after_admission(*args, **kwargs):
        record = await original_admit(*args, **kwargs)
        admitted.set()
        await return_from_admission.wait()
        return record

    monkeypatch.setattr(manager, "_admit_thread_operation", pause_after_admission)

    async def reserve() -> None:
        async with manager.reserve_thread_operation(
            "thread-1",
            kind=ThreadOperationKind.checkpoint_write,
        ):
            raise AssertionError("cancelled reservation must not enter its body")

    task = asyncio.create_task(reserve())
    await admitted.wait()
    await manager._lock.acquire()
    return_from_admission.set()
    await asyncio.sleep(0)
    task.cancel()
    manager._lock.release()

    with pytest.raises(asyncio.CancelledError):
        await task
    assert not await manager.has_inflight("thread-1")
    assert manager._runs == {}
    assert manager._runs_by_thread == {}
    assert await store.list_inflight() == []


@pytest.mark.anyio
async def test_late_failed_renewal_does_not_cancel_released_reservation():
    store = PausedLostLeaseRunStore()
    manager = RunManager(
        store=store,
        run_ownership_config=RunOwnershipConfig(
            lease_seconds=30,
            grace_seconds=10,
            heartbeat_enabled=True,
        ),
    )
    entered = asyncio.Event()
    leave_body = asyncio.Event()
    context_exited = asyncio.Event()
    finish_request = asyncio.Event()

    async def request() -> None:
        async with manager.reserve_thread_operation(
            "thread-1",
            kind=ThreadOperationKind.checkpoint_write,
        ):
            entered.set()
            await leave_body.wait()
        context_exited.set()
        await finish_request.wait()

    request_task = asyncio.create_task(request())
    await entered.wait()
    renewal_task = asyncio.create_task(manager._renew_leases())
    await store.renewal_started.wait()

    leave_body.set()
    await context_exited.wait()
    assert not await manager.has_inflight("thread-1")

    store.finish_renewal.set()
    await renewal_task
    assert not request_task.done()

    finish_request.set()
    await request_task


@pytest.mark.anyio
async def test_create_and_get(manager: RunManager):
    """Created run should be retrievable with new fields."""
    record = await manager.create(
        "thread-1",
        "lead_agent",
        metadata={"key": "val"},
        kwargs={"input": {}},
        multitask_strategy="reject",
    )
    assert record.status == RunStatus.pending
    assert record.thread_id == "thread-1"
    assert record.assistant_id == "lead_agent"
    assert record.metadata == {"key": "val"}
    assert record.kwargs == {"input": {}}
    assert record.multitask_strategy == "reject"
    assert ISO_RE.match(record.created_at)
    assert ISO_RE.match(record.updated_at)

    fetched = await manager.get(record.run_id)
    assert fetched is record


@pytest.mark.anyio
async def test_status_transitions(manager: RunManager):
    """Status should transition pending -> running -> success."""
    record = await manager.create("thread-1")
    assert record.status == RunStatus.pending

    await manager.set_status(record.run_id, RunStatus.running)
    assert record.status == RunStatus.running
    assert ISO_RE.match(record.updated_at)

    await manager.set_status(record.run_id, RunStatus.success)
    assert record.status == RunStatus.success


@pytest.mark.anyio
async def test_cancel(manager: RunManager):
    """Cancel should set abort_event and transition to interrupted."""
    record = await manager.create("thread-1")
    await manager.set_status(record.run_id, RunStatus.running)

    cancelled = await manager.cancel(record.run_id)
    assert cancelled == CancelOutcome.cancelled
    assert record.abort_event.is_set()
    assert record.status == RunStatus.interrupted


@pytest.mark.anyio
async def test_cancel_persists_interrupted_status_to_store():
    """Cancel should persist interrupted status to the backing store."""
    store = MemoryRunStore()
    manager = RunManager(store=store)
    record = await manager.create("thread-1")
    await manager.set_status(record.run_id, RunStatus.running)

    cancelled = await manager.cancel(record.run_id)

    stored = await store.get(record.run_id)
    assert cancelled == CancelOutcome.cancelled
    assert stored is not None
    assert stored["status"] == "interrupted"


@pytest.mark.anyio
async def test_status_persistence_retries_transient_sqlite_lock():
    """Transient SQLite lock errors should not leave a final status stale."""
    store = FlakyStatusRunStore(status_failures=2)
    manager = RunManager(store=store)
    record = await manager.create("thread-1")
    await manager.set_status(record.run_id, RunStatus.running)

    await manager.set_status(record.run_id, RunStatus.success)

    stored = await store.get(record.run_id)
    assert stored is not None
    assert stored["status"] == "success"
    assert store.status_update_attempts >= 4


@pytest.mark.anyio
async def test_status_persistence_recreates_missing_store_row():
    """A final status update should recreate a run row if initial persistence was lost."""
    store = MissingRowStatusRunStore()
    manager = RunManager(store=store)
    record = await manager.create("thread-1")
    await store.delete(record.run_id)

    await manager.set_status(record.run_id, RunStatus.error, error="boom")

    stored = await store.get(record.run_id)
    assert stored is not None
    assert stored["status"] == "error"
    assert stored["error"] == "boom"


@pytest.mark.anyio
async def test_status_persistence_does_not_retry_permanent_sqlalchemy_errors():
    """Permanent SQLAlchemy failures should not be retried as SQLite pressure."""
    store = PermanentStatusRunStore()
    manager = RunManager(
        store=store,
        persistence_retry_policy=PersistenceRetryPolicy(max_attempts=5, initial_delay=0),
    )
    record = await manager.create("thread-1")

    await manager.set_status(record.run_id, RunStatus.error, error="boom")

    assert store.status_update_attempts == 1


@pytest.mark.anyio
async def test_try_start_respects_durable_and_racing_cancels():
    """Startup must not resurrect durable or locally racing cancels."""
    store = MemoryRunStore()
    manager = RunManager(store=store)
    record = await manager.create_or_reject("thread-1")
    await store.update_status(record.run_id, RunStatus.interrupted.value)

    assert await manager.try_start(record.run_id) == RunStartOutcome.cancelled
    assert record.status == RunStatus.interrupted
    assert (await store.get(record.run_id))["status"] == RunStatus.interrupted.value

    record = await manager.create_or_reject("thread-2")
    original_start_run = store.start_run

    async def start_then_cancel(run_id):
        updated = await original_start_run(run_id)
        await manager.cancel(record.run_id)
        return updated

    store.start_run = start_then_cancel

    assert await manager.try_start(record.run_id) == RunStartOutcome.cancelled
    assert record.status == RunStatus.interrupted
    assert (await store.get(record.run_id))["status"] == RunStatus.interrupted.value


@pytest.mark.anyio
async def test_fail_start_if_pending_marks_pending_run_error_and_persists():
    """Worker attach failures should finalize only runs still pending startup."""
    store = MemoryRunStore()
    manager = RunManager(store=store)
    record = await manager.create_or_reject("thread-1")
    error = "Failed to attach run worker: boom"

    assert await manager.fail_start_if_pending(record.run_id, error=error) is True

    stored = await store.get(record.run_id)
    assert record.status == RunStatus.error
    assert record.error == error
    assert record.abort_event.is_set()
    assert stored is not None
    assert stored["status"] == RunStatus.error.value
    assert stored["error"] == error

    running = await manager.create_or_reject("thread-2")
    assert await manager.try_start(running.run_id) == RunStartOutcome.started

    assert await manager.fail_start_if_pending(running.run_id, error="late") is False

    stored_running = await store.get(running.run_id)
    assert running.status == RunStatus.running
    assert running.error is None
    assert stored_running is not None
    assert stored_running["status"] == RunStatus.running.value
    assert stored_running["error"] is None


@pytest.mark.anyio
async def test_fail_start_persistence_uncertainty_stays_supervised_until_compensated():
    store = FailStartPersistenceUnavailableStore()
    manager = RunManager(store=store)
    record = await manager.create_or_reject(
        "thread-fail-start-persistence",
        candidate_run_id="c82eb225-e312-4eb2-aaf8-597b10df3c21",
    )
    store.fail_terminalization = True

    assert (
        await manager.fail_start_if_pending(
            record.run_id,
            error="worker attachment failed",
        )
        is True
    )

    assert record.status is RunStatus.pending
    assert record.attachment_supervised is True
    assert record.abort_event.is_set()
    assert manager.admission_compensations_ready() is False
    assert await manager.shutdown(timeout=0.01) is False

    store.fail_terminalization = False
    assert await manager.drain_admission_compensations(timeout=1) is True

    stored = await store.get(record.run_id)
    assert stored is not None
    assert stored["status"] == RunStatus.error.value
    assert stored["stop_reason"] == "worker_attachment_failed"
    assert record.status is RunStatus.error
    assert record.attachment_supervised is False
    assert record.finalizing is False
    assert await manager.shutdown(timeout=1) is True


@pytest.mark.anyio
async def test_fail_start_reports_cancel_winner_committed_during_persistence():
    """A durable cancel committed before local signalling beats attach failure."""

    class PausedCancelStore(MemoryRunStore):
        durable_lifecycle = True

        def __init__(self):
            super().__init__()
            self.cancel_committed = asyncio.Event()
            self.release_cancel_result = asyncio.Event()

        async def request_cancel_fenced(self, *args, **kwargs):
            result = await super().request_cancel_fenced(*args, **kwargs)
            self.cancel_committed.set()
            await self.release_cancel_result.wait()
            return result

    store = PausedCancelStore()
    manager = RunManager(store=store)
    record = await manager.create_or_reject("thread-cancel-attach-race")
    cancel_task = asyncio.create_task(
        manager.request_cancel_fenced(
            record.run_id,
            action="interrupt",
            expected_state_version=record.state_version,
        )
    )
    await asyncio.wait_for(store.cancel_committed.wait(), timeout=1)

    retained_failure = await manager.fail_start_if_pending(
        record.run_id,
        error="thread metadata unavailable",
    )
    store.release_cancel_result.set()
    outcome = await asyncio.wait_for(cancel_task, timeout=1)

    assert retained_failure is False
    assert outcome is CancellationRequestOutcome.requested
    assert record.status is RunStatus.interrupted
    assert record.error is None
    assert record.stop_reason is None
    row = await store.get(record.run_id)
    assert row is not None
    assert row["status"] == RunStatus.interrupted.value
    assert row["error"] is None
    assert row.get("stop_reason") is None


@pytest.mark.anyio
async def test_completion_persistence_recreates_missing_store_row():
    """Completion updates should recreate a missing row and persist final counters."""
    store = MissingCompletionRunStore()
    manager = RunManager(store=store)
    record = await manager.create("thread-1")
    await manager.set_status(record.run_id, RunStatus.running)
    await manager.set_status(record.run_id, RunStatus.success)
    await store.delete(record.run_id)

    await manager.update_run_completion(
        record.run_id,
        status="success",
        total_tokens=42,
        llm_call_count=2,
        last_ai_message="done",
    )

    stored = await store.get(record.run_id)
    assert stored is not None
    assert stored["status"] == "success"
    assert stored["total_tokens"] == 42
    assert stored["llm_call_count"] == 2
    assert stored["last_ai_message"] == "done"
    assert store.completion_update_attempts == 2


@pytest.mark.anyio
async def test_completion_persistence_warns_when_recreated_row_still_missing(caplog):
    """A second zero-row completion update after recreation should not be silent."""
    store = AlwaysMissingCompletionRunStore()
    manager = RunManager(store=store)
    record = await manager.create("thread-1")
    await manager.set_status(record.run_id, RunStatus.success)
    caplog.set_level(logging.WARNING, logger="deerflow.runtime.runs.manager")

    await manager.update_run_completion(record.run_id, status="success", total_tokens=42)

    assert store.completion_update_attempts == 2
    assert "affected no rows after row recreation" in caplog.text


@pytest.mark.anyio
async def test_reconcile_orphaned_inflight_runs_marks_stale_rows_error():
    """Startup recovery should turn persisted active rows into explicit errors."""
    store = MemoryRunStore()
    await store.put("pending-run", thread_id="thread-1", status="pending", created_at="2026-01-01T00:00:00+00:00")
    await store.put("running-run", thread_id="thread-1", status="running", created_at="2026-01-01T00:00:01+00:00")
    await store.put("success-run", thread_id="thread-1", status="success", created_at="2026-01-01T00:00:02+00:00")
    manager = RunManager(store=store)

    recovered = await manager.reconcile_orphaned_inflight_runs(
        error="Gateway restarted before this run reached a durable final state.",
        before="2026-01-01T00:00:02+00:00",
    )

    assert {record.run_id for record in recovered} == {"pending-run", "running-run"}
    assert await _stored_statuses(store, "pending-run", "running-run", "success-run") == {
        "pending-run": "error",
        "running-run": "error",
        "success-run": "success",
    }


@pytest.mark.anyio
async def test_reconcile_orphaned_run_backfills_delivery_after_atomic_takeover():
    """Lease recovery must durably backfill the terminal receipt exactly once."""
    store = MemoryRunStore()
    events = MemoryRunEventStore()
    await store.put("running-run", thread_id="thread-1", status="running", created_at="2026-01-01T00:00:00+00:00")
    manager = RunManager(store=store, event_store=events)

    first = await manager.reconcile_orphaned_inflight_runs(error="worker crashed", before="2026-01-01T00:00:01+00:00")
    second = await manager.reconcile_orphaned_inflight_runs(error="worker crashed", before="2026-01-01T00:00:01+00:00")

    assert [record.run_id for record in first] == ["running-run"]
    assert second == []
    delivery = await events.list_events("thread-1", "running-run", event_types=["run.delivery"])
    assert len(delivery) == 1
    assert delivery[0]["content"] == {"presented": 0, "paths": [], "by_tool": {}}
    assert (await store.get("running-run"))["status"] == "error"


@pytest.mark.anyio
async def test_reconcile_preserves_delivery_written_before_worker_crash():
    """A crash after the receipt but before status persistence keeps its facts."""
    store = MemoryRunStore()
    events = MemoryRunEventStore()
    await store.put("running-run", thread_id="thread-1", status="running", created_at="2026-01-01T00:00:00+00:00")
    await events.put_if_absent(
        thread_id="thread-1",
        run_id="running-run",
        event_type="run.delivery",
        category="outputs",
        content={"presented": 1, "paths": ["report.md"], "by_tool": {"present_files": ["report.md"]}},
    )
    manager = RunManager(store=store, event_store=events)

    recovered = await manager.reconcile_orphaned_inflight_runs(error="worker crashed", before="2026-01-01T00:00:01+00:00")

    assert [record.run_id for record in recovered] == ["running-run"]
    delivery = await events.list_events("thread-1", "running-run", event_types=["run.delivery"])
    assert len(delivery) == 1
    assert delivery[0]["content"]["presented"] == 1


@pytest.mark.anyio
async def test_reconcile_preserves_terminal_takeover_when_delivery_backfill_fails():
    """A receipt-store outage must not undo an atomically claimed orphan."""

    class FailingReceiptStore(MemoryRunEventStore):
        async def put_if_absent(self, **kwargs):
            raise RuntimeError("event store unavailable")

    store = MemoryRunStore()
    await store.put("running-run", thread_id="thread-1", status="running", created_at="2026-01-01T00:00:00+00:00")
    manager = RunManager(store=store, event_store=FailingReceiptStore())

    recovered = await manager.reconcile_orphaned_inflight_runs(error="worker crashed", before="2026-01-01T00:00:01+00:00")

    assert [record.run_id for record in recovered] == ["running-run"]
    assert (await store.get("running-run"))["status"] == "error"


@pytest.mark.anyio
async def test_reconcile_orphaned_inflight_runs_skips_live_local_run():
    """Startup recovery should not mark an active row orphaned when this worker owns it."""
    store = MemoryRunStore()
    manager = RunManager(store=store)
    record = await manager.create("thread-1")
    await manager.set_status(record.run_id, RunStatus.running)

    recovered = await manager.reconcile_orphaned_inflight_runs(
        error="Gateway restarted before this run reached a durable final state.",
    )

    stored = await store.get(record.run_id)
    assert recovered == []
    assert stored["status"] == "running"


@pytest.mark.anyio
async def test_reconcile_orphaned_inflight_runs_skips_rows_when_takeover_claim_fails():
    """Startup recovery must not report a row as recovered if the takeover claim failed."""
    store = FailingTakeoverRunStore()
    await store.put("running-run", thread_id="thread-1", status="running", created_at="2026-01-01T00:00:00+00:00")
    manager = RunManager(
        store=store,
        persistence_retry_policy=PersistenceRetryPolicy(max_attempts=2, initial_delay=0),
    )

    recovered = await manager.reconcile_orphaned_inflight_runs(
        error="Gateway restarted before this run reached a durable final state.",
        before="2026-01-01T00:00:01+00:00",
    )

    stored = await store.get("running-run")
    assert recovered == []
    assert stored["status"] == "running"
    assert store.takeover_attempts == 2


@pytest.mark.anyio
async def test_cancel_not_inflight(manager: RunManager):
    """Cancelling a completed run should return not_cancellable."""
    record = await manager.create("thread-1")
    await manager.set_status(record.run_id, RunStatus.success)

    cancelled = await manager.cancel(record.run_id)
    assert cancelled == CancelOutcome.not_cancellable


@pytest.mark.anyio
async def test_list_by_thread(manager: RunManager):
    """Same thread should return multiple runs."""
    r1 = await manager.create("thread-1")
    r2 = await manager.create("thread-1")
    await manager.create("thread-2")

    runs = await manager.list_by_thread("thread-1")
    assert len(runs) == 2
    # Newest first: r2 was created after r1.
    assert runs[0].run_id == r2.run_id
    assert runs[1].run_id == r1.run_id


@pytest.mark.anyio
async def test_list_by_thread_is_stable_when_timestamps_tie(manager: RunManager, monkeypatch: pytest.MonkeyPatch):
    """Ordering should be stable (insertion order) even when timestamps tie."""
    monkeypatch.setattr("deerflow.runtime.runs.manager._now_iso", lambda: "2026-01-01T00:00:00+00:00")

    r1 = await manager.create("thread-1")
    r2 = await manager.create("thread-1")

    runs = await manager.list_by_thread("thread-1")
    assert [run.run_id for run in runs] == [r1.run_id, r2.run_id]


@pytest.mark.anyio
async def test_has_inflight(manager: RunManager):
    """has_inflight should be True when a run is pending or running."""
    record = await manager.create("thread-1")
    assert await manager.has_inflight("thread-1") is True

    await manager.set_status(record.run_id, RunStatus.success)
    assert await manager.has_inflight("thread-1") is False


@pytest.mark.anyio
async def test_has_inflight_ignores_checkpoint_write_reservation(manager: RunManager):
    """Internal checkpoint writers are not user-visible runs."""
    async with manager.reserve_thread_operation(
        "thread-1",
        kind=ThreadOperationKind.checkpoint_write,
    ):
        assert await manager.has_inflight("thread-1") is False


@pytest.mark.anyio
async def test_cleanup(manager: RunManager):
    """After cleanup, the run should be gone."""
    record = await manager.create("thread-1")
    run_id = record.run_id

    await manager.cleanup(run_id, delay=0)
    assert await manager.get(run_id) is None


@pytest.mark.anyio
async def test_set_status_with_error(manager: RunManager):
    """Error message should be stored on the record."""
    record = await manager.create("thread-1")
    await manager.set_status(record.run_id, RunStatus.error, error="Something went wrong")
    assert record.status == RunStatus.error
    assert record.error == "Something went wrong"


@pytest.mark.anyio
async def test_get_nonexistent(manager: RunManager):
    """Getting a nonexistent run should return None."""
    assert await manager.get("does-not-exist") is None


@pytest.mark.anyio
async def test_get_hydrates_store_only_run():
    """Store-only runs should be readable after process restart."""
    store = MemoryRunStore()
    await store.put(
        "run-store-only",
        thread_id="thread-1",
        assistant_id="lead_agent",
        status="success",
        multitask_strategy="reject",
        metadata={"source": "store"},
        kwargs={"input": "value"},
        created_at="2026-01-01T00:00:00+00:00",
        model_name="model-a",
    )
    manager = RunManager(store=store)

    record = await manager.get("run-store-only")

    assert record is not None
    assert record.run_id == "run-store-only"
    assert record.thread_id == "thread-1"
    assert record.assistant_id == "lead_agent"
    assert record.status == RunStatus.success
    assert record.on_disconnect == DisconnectMode.cancel
    assert record.metadata == {"source": "store"}
    assert record.kwargs == {"input": "value"}
    assert record.model_name == "model-a"
    assert record.task is None
    assert record.store_only is True


@pytest.mark.anyio
async def test_get_hydrates_run_with_null_enum_fields():
    """Rows with NULL status/on_disconnect must hydrate with safe defaults, not raise."""
    store = MemoryRunStore()
    # Simulate a SQL row where the nullable status column is NULL
    await store.put(
        "run-null-status",
        thread_id="thread-1",
        status=None,
        created_at="2026-01-01T00:00:00+00:00",
    )
    manager = RunManager(store=store)

    record = await manager.get("run-null-status")

    assert record is not None
    assert record.status == RunStatus.pending
    assert record.on_disconnect == DisconnectMode.cancel
    assert record.store_only is True


@pytest.mark.anyio
async def test_list_by_thread_hydrates_run_with_null_enum_fields():
    """list_by_thread must not skip rows with NULL status; applies safe defaults."""
    store = MemoryRunStore()
    await store.put(
        "run-null-status-list",
        thread_id="thread-null",
        status=None,
        created_at="2026-01-01T00:00:00+00:00",
    )
    manager = RunManager(store=store)

    runs = await manager.list_by_thread("thread-null")

    assert len(runs) == 1
    assert runs[0].run_id == "run-null-status-list"
    assert runs[0].status == RunStatus.pending
    assert runs[0].on_disconnect == DisconnectMode.cancel


@pytest.mark.anyio
async def test_create_record_is_not_store_only(manager: RunManager):
    """In-memory records created via create() must have store_only=False."""
    record = await manager.create("thread-1")
    assert record.store_only is False


@pytest.mark.anyio
async def test_create_rolls_back_in_memory_record_on_store_failure():
    """create() must fail and hide the run when the initial store write fails."""
    from unittest.mock import AsyncMock

    store = MemoryRunStore()
    store.put = AsyncMock(side_effect=RuntimeError("db down"))
    manager = RunManager(store=store)

    with pytest.raises(RuntimeError, match="db down"):
        await manager.create("thread-1")

    assert manager._runs == {}
    assert await manager.list_by_thread("thread-1") == []


@pytest.mark.anyio
async def test_create_rolls_back_in_memory_record_on_store_cancellation():
    """create() must also roll back when cancelled during the initial store write."""
    store = MemoryRunStore()

    async def cancelled_put(run_id, **kwargs):
        raise asyncio.CancelledError

    store.put = cancelled_put
    manager = RunManager(store=store)

    with pytest.raises(asyncio.CancelledError):
        await manager.create("thread-1")

    assert manager._runs == {}
    assert await manager.list_by_thread("thread-1") == []


@pytest.mark.anyio
async def test_create_does_not_expose_run_until_store_persist_completes():
    """Concurrent readers must wait until the new run has been persisted."""
    store = MemoryRunStore()
    manager = RunManager(store=store)
    original_put = store.put
    put_started = asyncio.Event()
    allow_put = asyncio.Event()

    async def blocking_put(run_id, **kwargs):
        put_started.set()
        await allow_put.wait()
        return await original_put(run_id, **kwargs)

    store.put = blocking_put
    create_task = asyncio.create_task(manager.create("thread-1"))
    list_task = None

    try:
        await put_started.wait()
        list_task = asyncio.create_task(manager.list_by_thread("thread-1"))
        await asyncio.sleep(0)
        assert not list_task.done()

        allow_put.set()
        record = await create_task
        runs = await list_task

        assert [run.run_id for run in runs] == [record.run_id]
    finally:
        allow_put.set()
        cleanup_tasks = []
        for task in (list_task, create_task):
            if task is None:
                continue
            if not task.done():
                task.cancel()
            cleanup_tasks.append(task)
        await asyncio.gather(*cleanup_tasks, return_exceptions=True)


@pytest.mark.anyio
async def test_get_prefers_in_memory_record_over_store():
    """In-memory records retain task/control state when store has same run."""
    store = MemoryRunStore()
    manager = RunManager(store=store)
    record = await manager.create("thread-1")
    await store.update_status(record.run_id, "success")

    fetched = await manager.get(record.run_id)

    assert fetched is record
    assert fetched.status == RunStatus.pending


@pytest.mark.anyio
async def test_list_by_thread_merges_store_runs_newest_first():
    """list_by_thread should merge memory and store rows with memory precedence."""
    store = MemoryRunStore()
    await store.put("old-store", thread_id="thread-1", status="success", created_at="2026-01-01T00:00:00+00:00")
    await store.put("other-thread", thread_id="thread-2", status="success", created_at="2026-01-03T00:00:00+00:00")
    manager = RunManager(store=store)
    memory_record = await manager.create("thread-1")

    runs = await manager.list_by_thread("thread-1")

    assert [run.run_id for run in runs] == [memory_record.run_id, "old-store"]
    assert runs[0] is memory_record


@pytest.mark.anyio
async def test_list_by_thread_limit_does_not_let_old_memory_hide_new_store_run():
    """A local row must not consume the store query's newest-run limit."""
    store = MemoryRunStore()
    manager = RunManager(store=store)
    old_memory = await manager.create("thread-1")
    old_memory.created_at = "2026-01-01T00:00:00+00:00"
    await store.put(
        "new-store",
        thread_id="thread-1",
        status="success",
        created_at="2026-01-02T00:00:00+00:00",
    )

    runs = await manager.list_by_thread("thread-1", limit=1)

    assert [run.run_id for run in runs] == ["new-store"]


@pytest.mark.anyio
async def test_create_defaults(manager: RunManager):
    """Create with no optional args should use defaults."""
    record = await manager.create("thread-1")
    assert record.metadata == {}
    assert record.kwargs == {}
    assert record.multitask_strategy == "reject"
    assert record.assistant_id is None


@pytest.mark.anyio
async def test_model_name_create_or_reject():
    """create_or_reject should accept and persist model_name."""
    from deerflow.runtime.runs.schemas import DisconnectMode

    store = MemoryRunStore()
    mgr = RunManager(store=store)

    record = await mgr.create_or_reject(
        "thread-1",
        assistant_id="lead_agent",
        on_disconnect=DisconnectMode.cancel,
        metadata={"key": "val"},
        kwargs={"input": {}},
        multitask_strategy="reject",
        model_name="anthropic.claude-sonnet-4-20250514-v1:0",
    )
    assert record.model_name == "anthropic.claude-sonnet-4-20250514-v1:0"
    assert record.status == RunStatus.pending

    # Verify model_name was persisted to store
    stored = await store.get(record.run_id)
    assert stored is not None
    assert stored["model_name"] == "anthropic.claude-sonnet-4-20250514-v1:0"

    # Verify retrieval returns the model_name via in-memory record
    fetched = await mgr.get(record.run_id)
    assert fetched is not None
    assert fetched.model_name == "anthropic.claude-sonnet-4-20250514-v1:0"


@pytest.mark.anyio
async def test_create_or_reject_interrupt_persists_interrupted_status_to_store():
    """interrupt strategy should persist interrupted status for old runs."""
    store = MemoryRunStore()
    manager = RunManager(store=store)
    old = await manager.create("thread-1")
    await manager.set_status(old.run_id, RunStatus.running)

    new = await manager.create_or_reject("thread-1", multitask_strategy="interrupt")

    stored_old = await store.get(old.run_id)
    assert new.run_id != old.run_id
    assert old.status == RunStatus.interrupted
    assert stored_old is not None
    assert stored_old["status"] == "interrupted"


@pytest.mark.anyio
async def test_create_or_reject_does_not_interrupt_old_run_when_new_run_store_write_fails():
    """A failed new-run persist must not cancel the existing inflight run."""
    from unittest.mock import AsyncMock

    store = MemoryRunStore()
    manager = RunManager(store=store)
    old = await manager.create("thread-1")
    await manager.set_status(old.run_id, RunStatus.running)
    store.create_thread_operation_atomic = AsyncMock(side_effect=RuntimeError("db down"))

    with pytest.raises(RuntimeError, match="db down"):
        await manager.create_or_reject("thread-1", multitask_strategy="interrupt")

    stored_old = await store.get(old.run_id)
    assert list(manager._runs) == [old.run_id]
    assert old.status == RunStatus.running
    assert old.abort_event.is_set() is False
    assert stored_old is not None
    assert stored_old["status"] == "running"


@pytest.mark.anyio
async def test_create_or_reject_does_not_interrupt_old_run_when_new_run_store_write_is_cancelled():
    """Cancellation during new-run persist must not cancel the existing run."""
    store = MemoryRunStore()
    manager = RunManager(store=store)
    old = await manager.create("thread-1")
    await manager.set_status(old.run_id, RunStatus.running)

    async def cancelled_create(run_id, **kwargs):
        raise asyncio.CancelledError

    store.create_thread_operation_atomic = cancelled_create

    with pytest.raises(asyncio.CancelledError):
        await manager.create_or_reject("thread-1", multitask_strategy="interrupt")

    stored_old = await store.get(old.run_id)
    assert list(manager._runs) == [old.run_id]
    assert old.status == RunStatus.running
    assert old.abort_event.is_set() is False
    assert stored_old is not None
    assert stored_old["status"] == "running"


@pytest.mark.anyio
@pytest.mark.parametrize("strategy", ["interrupt", "rollback"])
@pytest.mark.parametrize("keyed", [False, True])
async def test_cancelled_atomic_admission_reconciles_commit_before_return(
    strategy: str,
    keyed: bool,
) -> None:
    """A committed unseen replacement is registered, closed, and never orphaned."""

    store = CommitBeforeReturnRunStore()
    manager = RunManager(store=store)
    old = await manager.create("thread-1")
    await manager.set_status(old.run_id, RunStatus.running)
    store.pause_after_commit = True

    if keyed:
        admission = manager.ensure_or_reject(
            "thread-1",
            external_scope="scope-1",
            external_key="delivery-1",
            request_digest="a" * 64,
            request_digest_version="request-v1",
            caller_intent_json={"message": "follow-up"},
            caller_intent_digest="b" * 64,
            caller_intent_digest_version="intent-v1",
            multitask_strategy=strategy,
        )
    else:
        admission = manager.create_or_reject(
            "thread-1",
            multitask_strategy=strategy,
        )
    admission_task = asyncio.create_task(admission)

    await asyncio.wait_for(store.atomic_committed.wait(), timeout=1)
    admission_task.cancel()
    store.release_result.set()

    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(admission_task, timeout=1)

    stored_rows = await store.list_by_thread("thread-1")
    assert len(stored_rows) == 2
    stored_replacement = next(row for row in stored_rows if row["run_id"] != old.run_id)
    expected_old_status = RunStatus.error.value if strategy == "rollback" else RunStatus.interrupted.value
    assert next(row for row in stored_rows if row["run_id"] == old.run_id)["status"] == expected_old_status
    assert stored_replacement["status"] == RunStatus.interrupted.value
    assert all(row["status"] not in (RunStatus.pending.value, RunStatus.running.value) for row in stored_rows)

    local_rows = await manager.list_by_thread("thread-1")
    local_replacement = next(row for row in local_rows if row.run_id == stored_replacement["run_id"])
    assert local_replacement.status == RunStatus.interrupted
    assert local_replacement.abort_event.is_set()
    assert not await manager.has_inflight("thread-1")


@pytest.mark.anyio
async def test_cancelled_admission_store_outage_remains_supervised_until_durable_terminal_proof() -> None:
    store = CancelledAdmissionPersistenceUnavailableStore()
    store.pause_after_commit = True
    manager = RunManager(
        store=store,
        persistence_retry_policy=PersistenceRetryPolicy(max_attempts=1, initial_delay=0),
    )
    candidate_run_id = "37b12572-2e3e-4a75-a293-40cf28d7acee"
    admission_task = asyncio.create_task(
        manager.create_or_reject(
            "thread-cancel-store-outage",
            candidate_run_id=candidate_run_id,
        )
    )

    await asyncio.wait_for(store.atomic_committed.wait(), timeout=1)
    store.compensation_available = False
    admission_task.cancel()
    store.release_result.set()

    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(admission_task, timeout=1)

    durable = await store.authoritative_get(candidate_run_id)
    local = await manager.get(candidate_run_id)
    assert durable is not None
    assert local is not None
    assert durable["status"] == RunStatus.pending.value
    assert local.status == RunStatus.pending
    assert local.attachment_supervised is True
    assert local.abort_event.is_set()
    assert manager.admission_compensations_ready() is False
    with pytest.raises(RunStartupError, match="inactive run"):
        await manager.attach_worker_once(candidate_run_id, None, asyncio.create_task)

    store.compensation_available = True
    assert await manager.drain_admission_compensations(timeout=1) is True

    durable = await store.authoritative_get(candidate_run_id)
    local = await manager.get(candidate_run_id)
    assert durable is not None
    assert local is not None
    assert durable["status"] == RunStatus.interrupted.value
    assert local.status == RunStatus.interrupted
    assert local.attachment_supervised is False
    events = await store.list_lifecycle_events(run_id=candidate_run_id)
    assert [event["lifecycle_type"].value for event in events] == [
        "accepted",
        "cancellation_requested",
        "cancelled",
    ]


@pytest.mark.anyio
@pytest.mark.parametrize("keyed", [False, True])
async def test_cancelled_response_lost_candidate_retains_cancellation_disposition(
    keyed: bool,
) -> None:
    store = CancelledAmbiguousAdmissionStore()
    manager = RunManager(
        store=store,
        persistence_retry_policy=PersistenceRetryPolicy(max_attempts=1, initial_delay=0),
    )
    candidate_run_id = "00000000-0000-0000-0000-000000000103"
    if keyed:
        admission = manager.ensure_or_reject(
            "thread-cancelled-ambiguous",
            candidate_run_id=candidate_run_id,
            external_scope="scope-1",
            external_key="delivery-1",
            request_digest="a" * 64,
            request_digest_version="request-v1",
            caller_intent_json={"message": "hello"},
            caller_intent_digest="b" * 64,
            caller_intent_digest_version="intent-v1",
        )
    else:
        admission = manager.create_or_reject(
            "thread-cancelled-ambiguous",
            candidate_run_id=candidate_run_id,
        )
    admission_task = asyncio.create_task(admission)

    await asyncio.wait_for(store.atomic_committed.wait(), timeout=1)
    admission_task.cancel()
    store.release_response_loss.set()
    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(admission_task, timeout=1)

    unresolved = manager._unresolved_admissions[candidate_run_id]
    assert unresolved.terminal_disposition is _AdmissionTerminalDisposition.cancelled
    assert unresolved.cancellation_action == "interrupt"
    retained = await store.authoritative_get(candidate_run_id)
    assert retained is not None
    assert retained["status"] == RunStatus.pending.value

    store.candidate_reads_available = True
    assert await manager.drain_admission_compensations(timeout=1) is True
    retained = await store.authoritative_get(candidate_run_id)
    assert retained is not None
    assert retained["status"] == RunStatus.interrupted.value
    events = await store.list_lifecycle_events(run_id=candidate_run_id)
    assert [event["lifecycle_type"].value for event in events] == [
        "accepted",
        "cancellation_requested",
        "cancelled",
    ]


@pytest.mark.anyio
async def test_taskless_attachment_lease_loss_preserves_authoritative_state_until_same_process_recovers(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        "deerflow.runtime.runs.manager._admission_compensation_retry_delay",
        lambda _round: 60.0,
    )
    config = RunOwnershipConfig(
        lease_seconds=30,
        grace_seconds=0,
        heartbeat_enabled=True,
    )
    store = MemoryRunStore()
    owner = RunManager(
        store=store,
        worker_id="worker-owner",
        run_ownership_config=config,
    )
    candidate_run_id = "00000000-0000-0000-0000-000000000104"
    record = await owner.create_or_reject(
        "thread-taskless-ownership-loss",
        candidate_run_id=candidate_run_id,
    )
    expired = "2000-01-01T00:00:00+00:00"
    record.lease_expires_at = expired
    store._runs[candidate_run_id]["lease_expires_at"] = expired

    assert await owner._mark_ownership_lost(
        record,
        reason="lease expired before worker attachment",
    )

    retained = await store.get(candidate_run_id)
    assert retained is not None
    assert retained["status"] == RunStatus.pending.value
    assert record.status == RunStatus.pending
    assert record.ownership_lost is True
    assert record.abort_event.is_set()
    assert record.attachment_supervised is True
    assert owner.admission_compensations_ready() is False
    assert await owner.shutdown(timeout=0.01) is False

    recovered = await owner.reconcile_orphaned_inflight_runs(
        error="same-process orphan recovery",
        stop_reason="orphan_recovered",
    )
    assert [recovered_record.run_id for recovered_record in recovered] == [
        candidate_run_id,
    ]
    assert await owner.drain_admission_compensations(timeout=0.2) is True

    retained = await store.get(candidate_run_id)
    assert retained is not None
    assert retained["status"] == RunStatus.error.value
    assert record.status == RunStatus.error
    assert record.stop_reason == "orphan_recovered"
    assert record.attachment_supervised is False


@pytest.mark.anyio
async def test_unresolved_compensation_cannot_commit_after_lease_ownership_is_lost() -> None:
    config = RunOwnershipConfig(
        lease_seconds=30,
        grace_seconds=0,
        heartbeat_enabled=True,
    )
    store = PausedOwnedCompensationRunStore()
    manager = RunManager(
        store=store,
        worker_id="worker-owner",
        run_ownership_config=config,
    )
    candidate_run_id = "00000000-0000-0000-0000-000000000105"
    record = await manager.create_or_reject(
        "thread-compensation-ownership-race",
        candidate_run_id=candidate_run_id,
    )
    manager._register_unresolved_admission(manager._known_candidate_for_record(record))

    await asyncio.wait_for(store.compensation_started.wait(), timeout=1)
    expired = "2000-01-01T00:00:00+00:00"
    record.lease_expires_at = expired
    store._runs[candidate_run_id]["lease_expires_at"] = expired
    assert await manager._mark_ownership_lost(
        record,
        reason="lease expired while compensation was waiting",
    )
    store.release_compensation.set()

    await asyncio.sleep(0)
    retained = await store.get(candidate_run_id)
    assert retained is not None
    assert retained["status"] == RunStatus.pending.value
    assert manager.admission_compensations_ready() is False

    recovered = await manager.reconcile_orphaned_inflight_runs(
        error="same-process orphan recovery",
        stop_reason="orphan_recovered",
    )
    assert [recovered_record.run_id for recovered_record in recovered] == [
        candidate_run_id,
    ]
    assert await manager.drain_admission_compensations(timeout=0.2) is True

    retained = await store.get(candidate_run_id)
    assert retained is not None
    assert retained["status"] == RunStatus.error.value
    assert retained["stop_reason"] == "orphan_recovered"
    events = await store.list_lifecycle_events(run_id=candidate_run_id)
    assert [event["lifecycle_type"].value for event in events] == [
        "accepted",
        "failed",
    ]


@pytest.mark.anyio
async def test_ordinary_cancel_of_taskless_creator_stays_supervised_until_durable_terminal_proof() -> None:
    store = TasklessCancellationUnavailableStore()
    manager = RunManager(
        store=store,
        persistence_retry_policy=PersistenceRetryPolicy(max_attempts=1, initial_delay=0),
    )
    candidate_run_id = "00000000-0000-0000-0000-000000000106"
    record = await manager.create_or_reject(
        "thread-taskless-ordinary-cancel",
        candidate_run_id=candidate_run_id,
    )

    outcome = await manager.cancel(candidate_run_id)

    retained = await store.get(candidate_run_id)
    assert retained is not None
    assert outcome == CancelOutcome.requested
    assert retained["status"] == RunStatus.pending.value
    assert record.status == RunStatus.pending
    assert record.abort_event.is_set()
    assert record.task is None
    assert record.attachment_supervised is True
    assert manager.admission_compensations_ready() is False

    store.compensation_available = True
    assert await manager.drain_admission_compensations(timeout=1) is True

    retained = await store.get(candidate_run_id)
    assert retained is not None
    assert retained["status"] == RunStatus.interrupted.value
    assert record.status == RunStatus.interrupted
    assert record.attachment_supervised is False
    events = await store.list_lifecycle_events(run_id=candidate_run_id)
    assert [event["lifecycle_type"].value for event in events] == [
        "accepted",
        "cancellation_requested",
        "cancelled",
    ]


@pytest.mark.anyio
@pytest.mark.parametrize("pause_after_commit", [False, True])
async def test_cancelling_taskless_compensation_retains_exact_candidate_until_recovery(
    pause_after_commit: bool,
) -> None:
    store = PausedOwnedCancellationRunStore(
        pause_after_commit=pause_after_commit,
    )
    manager = RunManager(store=store)
    candidate_run_id = "00000000-0000-0000-0000-000000000107" if pause_after_commit else "00000000-0000-0000-0000-000000000108"
    record = await manager.create_or_reject(
        "thread-cancel-compensation-cancelled",
        candidate_run_id=candidate_run_id,
    )
    cancel_task = asyncio.create_task(manager.cancel(candidate_run_id))

    await asyncio.wait_for(store.cancellation_paused.wait(), timeout=1)
    cancel_task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await cancel_task

    retained = await store.get(candidate_run_id)
    assert retained is not None
    assert retained["status"] == RunStatus.pending.value
    assert record.status == RunStatus.pending
    assert record.abort_event.is_set()
    assert record.task is None
    assert record.attachment_supervised is True
    assert manager.admission_compensations_ready() is False
    with pytest.raises(RunStartupError, match="inactive run"):
        await manager.attach_worker_once(candidate_run_id, None, asyncio.create_task)

    store.release_cancellation.set()
    store.release_terminal.set()
    assert await manager.drain_admission_compensations(timeout=1) is True
    retained = await store.get(candidate_run_id)
    assert retained is not None
    assert retained["status"] == RunStatus.interrupted.value
    events = await store.list_lifecycle_events(run_id=candidate_run_id)
    assert [event["lifecycle_type"].value for event in events] == [
        "accepted",
        "cancellation_requested",
        "cancelled",
    ]


@pytest.mark.anyio
async def test_cancel_and_worker_attachment_have_one_atomic_winner() -> None:
    class PausedCloseManager(RunManager):
        def __init__(self, *args, **kwargs) -> None:
            super().__init__(*args, **kwargs)
            self.close_entered = asyncio.Event()
            self.release_close = asyncio.Event()

        async def _close_cancelled_admission(self, record, *, action="interrupt"):
            self.close_entered.set()
            await self.release_close.wait()
            return await super()._close_cancelled_admission(
                record,
                action=action,
            )

    store = MemoryRunStore()
    manager = PausedCloseManager(store=store)
    candidate_run_id = "00000000-0000-0000-0000-000000000109"
    record = await manager.create_or_reject(
        "thread-cancel-attach-race",
        candidate_run_id=candidate_run_id,
    )
    cancel_task = asyncio.create_task(manager.cancel(candidate_run_id))
    await asyncio.wait_for(manager.close_entered.wait(), timeout=1)

    worker_started = asyncio.Event()
    worker_release = asyncio.Event()

    async def worker() -> None:
        worker_started.set()
        await worker_release.wait()

    attached = await manager.attach_worker_once(
        candidate_run_id,
        worker(),
        asyncio.create_task,
    )
    await asyncio.wait_for(worker_started.wait(), timeout=1)
    manager.release_close.set()

    assert await cancel_task == CancelOutcome.cancelled
    done, _ = await asyncio.wait((attached,), timeout=0.1)
    retained = await store.get(candidate_run_id)
    assert retained is not None
    assert retained["status"] == RunStatus.interrupted.value
    assert attached in done
    assert attached.cancelled()
    assert record.abort_event.is_set()
    worker_release.set()
    if not attached.done():
        attached.cancel()
    await asyncio.gather(attached, return_exceptions=True)


@pytest.mark.anyio
async def test_shutdown_cannot_fail_taskless_creator_after_lease_loss() -> None:
    config = RunOwnershipConfig(
        lease_seconds=30,
        grace_seconds=0,
        heartbeat_enabled=True,
    )
    store = PausedOwnedCompensationRunStore()
    manager = RunManager(
        store=store,
        worker_id="worker-owner",
        run_ownership_config=config,
    )
    candidate_run_id = "00000000-0000-0000-0000-000000000110"
    record = await manager.create_or_reject(
        "thread-shutdown-ownership-race",
        candidate_run_id=candidate_run_id,
    )
    shutdown_task = asyncio.create_task(manager.shutdown(timeout=0.2))
    await asyncio.wait_for(store.compensation_started.wait(), timeout=1)

    expired = "2000-01-01T00:00:00+00:00"
    record.lease_expires_at = expired
    store._runs[candidate_run_id]["lease_expires_at"] = expired
    assert await manager._mark_ownership_lost(
        record,
        reason="lease expired during shutdown compensation",
    )
    store.release_compensation.set()

    assert await shutdown_task is False
    retained = await store.get(candidate_run_id)
    assert retained is not None
    assert retained["status"] == RunStatus.pending.value
    assert manager.admission_compensations_ready() is False

    recovered = await manager.reconcile_orphaned_inflight_runs(
        error="same-process orphan recovery",
        stop_reason="orphan_recovered",
    )
    assert [item.run_id for item in recovered] == [candidate_run_id]
    assert await manager.drain_admission_compensations(timeout=0.2) is True
    retained = await store.get(candidate_run_id)
    assert retained is not None
    assert retained["status"] == RunStatus.error.value
    assert retained["stop_reason"] == "orphan_recovered"


@pytest.mark.anyio
async def test_local_terminal_status_cannot_commit_after_lease_loss() -> None:
    config = RunOwnershipConfig(
        lease_seconds=30,
        grace_seconds=0,
        heartbeat_enabled=True,
    )
    store = PausedOwnedStatusRunStore()
    manager = RunManager(
        store=store,
        worker_id="worker-owner",
        run_ownership_config=config,
    )
    record = await manager.create("thread-terminal-ownership-race")
    assert await manager.try_start(record.run_id) == RunStartOutcome.started
    store.pause_owned_transition = True
    completion = asyncio.create_task(
        manager.set_status(record.run_id, RunStatus.success),
    )
    await asyncio.wait_for(store.transition_paused.wait(), timeout=1)

    expired = "2000-01-01T00:00:00+00:00"
    record.lease_expires_at = expired
    store._runs[record.run_id]["lease_expires_at"] = expired
    assert await manager._mark_ownership_lost(
        record,
        reason="lease expired during terminal persistence",
        require_active=False,
    )
    store.release_transition.set()
    await completion

    retained = await store.get(record.run_id)
    assert retained is not None
    assert retained["status"] == RunStatus.running.value
    assert record.ownership_lost is True


@pytest.mark.anyio
@pytest.mark.parametrize("keyed", [False, True])
async def test_response_lost_creator_is_reconciled_by_candidate_id(
    keyed: bool,
) -> None:
    store = ResponseLostAfterCommitRunStore()
    manager = RunManager(store=store)
    candidate_run_id = "a9af5c6d-fad6-4b04-b2c2-76e72913b4d2"

    if keyed:
        store.lose_ensure_response = True
        admitted = await manager.ensure_or_reject(
            "thread-response-loss",
            candidate_run_id=candidate_run_id,
            external_scope="scope-1",
            external_key="delivery-1",
            request_digest="a" * 64,
            request_digest_version="request-v1",
            caller_intent_json={"message": "hello"},
            caller_intent_digest="b" * 64,
            caller_intent_digest_version="intent-v1",
        )
        assert admitted.outcome.value == "created"
        record = admitted.record
    else:
        store.lose_create_response = True
        record = await manager.create_or_reject(
            "thread-response-loss",
            candidate_run_id=candidate_run_id,
        )

    assert record.run_id == candidate_run_id
    assert record.attachment_supervised is True
    assert (await store.get(candidate_run_id))["status"] == RunStatus.pending.value


@pytest.mark.anyio
async def test_unresolved_candidate_is_terminalized_after_store_recovery() -> None:
    store = ResponseLostWithUnavailableCandidateReadStore()
    store.lose_create_response = True
    manager = RunManager(store=store)
    candidate_run_id = "d11cd8f8-d78d-4546-a680-e071643b8ea3"

    with pytest.raises(OSError, match="response lost after durable commit"):
        await manager.create_or_reject(
            "thread-unresolved-candidate",
            candidate_run_id=candidate_run_id,
        )

    assert manager.admission_compensations_ready() is False
    store.candidate_reads_available = True
    assert await manager.drain_admission_compensations(timeout=1) is True
    assert manager.admission_compensations_ready() is True

    retained = await store.get(candidate_run_id)
    assert retained is not None
    assert retained["status"] == RunStatus.error.value
    assert retained["stop_reason"] == "worker_attachment_failed"
    events = await store.list_lifecycle_events(run_id=candidate_run_id)
    assert [event["lifecycle_type"].value for event in events] == [
        "accepted",
        "failed",
    ]


@pytest.mark.anyio
@pytest.mark.parametrize("strategy", ["interrupt", "rollback"])
@pytest.mark.parametrize("keyed", [False, True])
async def test_response_lost_replacement_remains_compensated_when_predecessor_read_fails(
    strategy: str,
    keyed: bool,
) -> None:
    store = ResponseLostWithUnavailablePredecessorReadStore()
    manager = RunManager(store=store)
    predecessor = await manager.create("thread-replacement-response-loss")
    await manager.set_status(predecessor.run_id, RunStatus.running)
    predecessor_task = asyncio.create_task(asyncio.Event().wait())
    predecessor.task = predecessor_task
    await asyncio.sleep(0)
    candidate_run_id = "e13c82b8-2dbd-4a02-9463-7188de5608e1"
    store.predecessor_run_id = predecessor.run_id
    store.candidate_run_id = candidate_run_id
    store.fail_predecessor_read = True
    store.lose_ensure_response = keyed
    store.lose_create_response = not keyed

    with pytest.raises(OSError, match="response lost after durable commit"):
        if keyed:
            await manager.ensure_or_reject(
                "thread-replacement-response-loss",
                candidate_run_id=candidate_run_id,
                external_scope="scope-1",
                external_key="delivery-1",
                request_digest="a" * 64,
                request_digest_version="request-v1",
                caller_intent_json={"message": "replacement"},
                caller_intent_digest="b" * 64,
                caller_intent_digest_version="intent-v1",
                multitask_strategy=strategy,
            )
        else:
            await manager.create_or_reject(
                "thread-replacement-response-loss",
                candidate_run_id=candidate_run_id,
                multitask_strategy=strategy,
            )

    assert manager.admission_compensations_ready() is False
    assert candidate_run_id not in manager._runs
    await asyncio.sleep(0)
    assert predecessor.abort_event.is_set()
    assert predecessor_task.cancelled()
    assert predecessor.status is (RunStatus.error if strategy == "rollback" else RunStatus.interrupted)
    store.candidate_reads_available = True
    assert await manager.drain_admission_compensations(timeout=1) is True

    retained = await store.get(candidate_run_id)
    assert retained is not None
    assert retained["status"] == RunStatus.error.value
    assert retained["stop_reason"] == "worker_attachment_failed"
    events = await store.list_lifecycle_events(run_id=candidate_run_id)
    assert [event["lifecycle_type"].value for event in events] == [
        "accepted",
        "failed",
    ]
    retained_predecessor = await store.get(predecessor.run_id)
    assert retained_predecessor is not None
    assert retained_predecessor["status"] == (RunStatus.error.value if strategy == "rollback" else RunStatus.interrupted.value)


@pytest.mark.anyio
@pytest.mark.parametrize("strategy", ["interrupt", "rollback"])
@pytest.mark.parametrize("keyed", [False, True])
async def test_delayed_candidate_reconciliation_fences_committed_replacement_predecessor(
    strategy: str,
    keyed: bool,
) -> None:
    store = ResponseLostWithUnavailableCandidateReadStore()
    manager = RunManager(store=store)
    store.candidate_reads_available = True
    predecessor = await manager.create("thread-delayed-replacement")
    await manager.set_status(predecessor.run_id, RunStatus.running)
    predecessor_task = asyncio.create_task(asyncio.Event().wait())
    predecessor.task = predecessor_task
    await asyncio.sleep(0)
    candidate_run_id = "25c31a53-f81f-4f2c-8bbb-9e67e176978c"
    store.candidate_reads_available = False
    store.lose_ensure_response = keyed
    store.lose_create_response = not keyed

    with pytest.raises(OSError, match="response lost after durable commit"):
        if keyed:
            await manager.ensure_or_reject(
                "thread-delayed-replacement",
                candidate_run_id=candidate_run_id,
                external_scope="scope-1",
                external_key="delivery-delayed",
                request_digest="c" * 64,
                request_digest_version="request-v1",
                caller_intent_json={"message": "replacement"},
                caller_intent_digest="d" * 64,
                caller_intent_digest_version="intent-v1",
                multitask_strategy=strategy,
            )
        else:
            await manager.create_or_reject(
                "thread-delayed-replacement",
                candidate_run_id=candidate_run_id,
                multitask_strategy=strategy,
            )

    assert predecessor.abort_event.is_set() is False
    store.candidate_reads_available = True
    assert await manager.drain_admission_compensations(timeout=1) is True
    await asyncio.sleep(0)

    assert predecessor.abort_event.is_set()
    assert predecessor_task.cancelled()
    assert predecessor.status is (RunStatus.error if strategy == "rollback" else RunStatus.interrupted)
    retained = await store.get(candidate_run_id)
    assert retained is not None
    assert retained["status"] == RunStatus.error.value
    assert retained["stop_reason"] == "worker_attachment_failed"


@pytest.mark.anyio
async def test_shutdown_reports_unresolved_candidate_until_store_recovers() -> None:
    store = ResponseLostWithUnavailableCandidateReadStore()
    store.lose_create_response = True
    manager = RunManager(store=store)

    with pytest.raises(OSError, match="response lost after durable commit"):
        await manager.create_or_reject(
            "thread-unresolved-shutdown",
            candidate_run_id="46b76be7-e5a0-4385-a419-fcf7bde7b090",
        )

    assert await manager.shutdown(timeout=0.01) is False
    store.candidate_reads_available = True
    assert await manager.drain_admission_compensations(timeout=1) is True


@pytest.mark.anyio
async def test_direct_admission_without_candidate_has_no_attachment_supervisor() -> None:
    manager = RunManager(store=MemoryRunStore())

    record = await manager.create_or_reject("thread-unsupervised")

    assert record.attachment_supervised is False


@pytest.mark.anyio
async def test_worker_attachment_is_exactly_once() -> None:
    manager = RunManager(store=MemoryRunStore())
    record = await manager.create_or_reject(
        "thread-attach-once",
        candidate_run_id="fa0d7c92-82a5-44a3-a88e-5cb0e095a793",
    )

    async def worker() -> None:
        return None

    first_worker = worker()
    task = await manager.attach_worker_once(
        record.run_id,
        first_worker,
        asyncio.create_task,
    )
    await task

    second_worker = worker()
    with pytest.raises(RuntimeError, match="already resolved"):
        await manager.attach_worker_once(
            record.run_id,
            second_worker,
            asyncio.create_task,
        )
    second_worker.close()


@pytest.mark.anyio
async def test_shutdown_terminalizes_taskless_supervised_admission() -> None:
    store = MemoryRunStore()
    manager = RunManager(store=store)
    record = await manager.create_or_reject(
        "thread-shutdown-attachment",
        candidate_run_id="3bd65d48-0362-42d4-a472-d718b31d90e4",
    )

    assert record.task is None
    assert record.attachment_supervised is True

    assert await manager.shutdown(timeout=1) is True

    retained = await store.get(record.run_id)
    assert retained is not None
    assert retained["status"] == RunStatus.error.value
    assert retained["stop_reason"] == "worker_attachment_failed"
    events = await store.list_lifecycle_events(run_id=record.run_id)
    assert [event["lifecycle_type"].value for event in events] == [
        "accepted",
        "failed",
    ]


@pytest.mark.anyio
@pytest.mark.parametrize("strategy", ["interrupt", "rollback"])
async def test_cancelled_atomic_unique_failure_does_not_retry_admission(
    strategy: str,
) -> None:
    """Once cancelled, a retryable store race cannot create replacement work."""

    store = CancelledUniqueRetryRunStore()
    manager = RunManager(store=store)
    old = await manager.create("thread-1")
    await manager.set_status(old.run_id, RunStatus.running)

    admission_task = asyncio.create_task(
        manager.create_or_reject(
            "thread-1",
            multitask_strategy=strategy,
        )
    )
    await asyncio.wait_for(store.first_attempt_started.wait(), timeout=1)
    admission_task.cancel()
    store.release_first_attempt.set()

    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(admission_task, timeout=1)

    assert store.atomic_attempts == 1
    stored_rows = await store.list_by_thread("thread-1")
    assert [row["run_id"] for row in stored_rows] == [old.run_id]
    assert stored_rows[0]["status"] == RunStatus.running.value
    assert list(manager._runs) == [old.run_id]
    assert old.status == RunStatus.running
    assert not old.abort_event.is_set()


@pytest.mark.anyio
async def test_cancelled_reservation_releases_commit_before_return() -> None:
    """Cancellation cannot strand a committed thread-operation reservation."""

    store = CommitBeforeReturnRunStore()
    manager = RunManager(store=store)
    store.pause_after_commit = True

    async def reserve() -> None:
        async with manager.reserve_thread_operation(
            "thread-1",
            kind=ThreadOperationKind.checkpoint_write,
        ):
            raise AssertionError("cancelled reservation must not enter its body")

    reservation_task = asyncio.create_task(reserve())
    await asyncio.wait_for(store.atomic_committed.wait(), timeout=1)
    reservation_task.cancel()
    store.release_result.set()

    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(reservation_task, timeout=1)

    assert await store.list_inflight() == []
    assert manager._runs == {}
    assert manager._runs_by_thread == {}
    assert not await manager.has_inflight("thread-1")


@pytest.mark.anyio
async def test_cancelled_known_replay_does_not_close_retained_run() -> None:
    """Cancelling a known-equal lookup never cancels the accepted invocation."""

    store = CommitBeforeReturnRunStore()
    identity = {
        "external_scope": "scope-1",
        "external_key": "delivery-1",
        "request_digest": "a" * 64,
        "request_digest_version": "request-v1",
        "caller_intent_json": {"message": "original"},
        "caller_intent_digest": "b" * 64,
        "caller_intent_digest_version": "intent-v1",
    }
    writer = RunManager(store=store)
    accepted = await writer.ensure_or_reject("thread-1", **identity)
    store.pause_after_ensure = True
    reader = RunManager(store=store)

    replay_task = asyncio.create_task(reader.ensure_or_reject("thread-1", **identity))
    await asyncio.wait_for(store.ensure_decided.wait(), timeout=1)
    replay_task.cancel()
    store.release_ensure.set()

    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(replay_task, timeout=1)

    retained = await store.get(accepted.record.run_id)
    assert retained is not None
    assert retained["status"] == RunStatus.pending.value
    assert retained["cancel_action"] is None
    assert reader._runs == {}


@pytest.mark.anyio
@pytest.mark.parametrize("strategy", ["interrupt", "rollback"])
async def test_create_or_reject_cancellation_after_registration_interrupts_replacement(
    monkeypatch: pytest.MonkeyPatch,
    strategy: str,
) -> None:
    """Cancellation after admission must not leave the replacement active."""

    class LegacyMemoryRunStore(MemoryRunStore):
        pass

    store = LegacyMemoryRunStore()
    manager = RunManager(store=store)
    old = await manager.create("thread-1")
    await manager.set_status(old.run_id, RunStatus.running)
    persist_started = asyncio.Event()
    release_persist = asyncio.Event()
    original_persist_status = manager._persist_status

    async def blocking_persist_status(record: Any, status: RunStatus, **kwargs: Any) -> bool:
        persist_started.set()
        await asyncio.wait_for(release_persist.wait(), timeout=1)
        return await original_persist_status(record, status, **kwargs)

    monkeypatch.setattr(manager, "_persist_status", blocking_persist_status)
    create_task = asyncio.create_task(manager.create_or_reject("thread-1", multitask_strategy=strategy))
    await asyncio.wait_for(persist_started.wait(), timeout=1)
    create_task.cancel()
    release_persist.set()

    with pytest.raises(asyncio.CancelledError):
        _ = await create_task

    records = await manager.list_by_thread("thread-1")
    replacement = next(record for record in records if record.run_id != old.run_id)
    stored_replacement = await store.get(replacement.run_id)
    assert not await manager.has_inflight("thread-1")
    assert replacement.status == RunStatus.interrupted
    assert replacement.abort_event.is_set()
    assert stored_replacement is not None
    assert stored_replacement["status"] == RunStatus.interrupted.value


@pytest.mark.anyio
async def test_create_or_reject_repeated_cancellation_drains_replacement_cleanup(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Repeated cancellation must not abandon the durable cleanup task."""

    class LegacyMemoryRunStore(MemoryRunStore):
        pass

    store = LegacyMemoryRunStore()
    manager = RunManager(store=store)
    old = await manager.create("thread-1")
    await manager.set_status(old.run_id, RunStatus.running)
    old_persist_started = asyncio.Event()
    release_old_persist = asyncio.Event()
    replacement_persist_started = asyncio.Event()
    release_replacement_persist = asyncio.Event()
    original_persist_status = manager._persist_status
    original_update_status = store.update_status

    async def staged_persist_status(record: Any, status: RunStatus, **kwargs: Any) -> bool:
        if record.run_id == old.run_id:
            old_persist_started.set()
            await asyncio.wait_for(release_old_persist.wait(), timeout=1)
        return await original_persist_status(record, status, **kwargs)

    async def staged_update_status(run_id: str, status: str, **kwargs: Any) -> bool:
        if run_id != old.run_id and status == RunStatus.interrupted.value:
            replacement_persist_started.set()
            await asyncio.wait_for(release_replacement_persist.wait(), timeout=1)
        return await original_update_status(run_id, status, **kwargs)

    monkeypatch.setattr(manager, "_persist_status", staged_persist_status)
    monkeypatch.setattr(store, "update_status", staged_update_status)
    create_task = asyncio.create_task(manager.create_or_reject("thread-1", multitask_strategy="interrupt"))
    await asyncio.wait_for(old_persist_started.wait(), timeout=1)
    replacement = next(record for record in manager._runs.values() if record.run_id != old.run_id)

    create_task.cancel()
    release_old_persist.set()
    await asyncio.wait_for(replacement_persist_started.wait(), timeout=1)
    create_task.cancel()
    await asyncio.sleep(0)
    assert not create_task.done()

    release_replacement_persist.set()
    with pytest.raises(asyncio.CancelledError):
        _ = await asyncio.wait_for(create_task, timeout=1)

    stored_replacement = await store.get(replacement.run_id)
    assert replacement.status == RunStatus.interrupted
    assert stored_replacement is not None
    assert stored_replacement["status"] == RunStatus.interrupted.value


@pytest.mark.anyio
async def test_create_or_reject_retries_replacement_when_cancel_status_cannot_persist(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A failed best-effort update must get a strict durable retry."""

    class FailFirstReplacementInterruptStore(MemoryRunStore):
        failed = False

        async def get(self, run_id: str, *, user_id: str | None = None) -> dict[str, Any] | None:
            raw = await super().get(run_id)
            if raw is not None and raw.get("status") == RunStatus.pending.value and raw.get("user_id") is not None and user_id != raw.get("user_id"):
                raise RuntimeError("replacement lookup was not owner-scoped")
            return await super().get(run_id, user_id=user_id)

        async def update_status(self, run_id: str, status: str, **kwargs: Any) -> bool:
            row = await super().get(run_id)
            if not self.failed and status == RunStatus.interrupted.value and row is not None and row.get("status") == RunStatus.pending.value:
                self.failed = True
                raise RuntimeError("replacement status write failed")
            return await super().update_status(run_id, status, **kwargs)

    store = FailFirstReplacementInterruptStore()
    manager = RunManager(store=store)
    old = await manager.create("thread-1", user_id="owner-1")
    await manager.set_status(old.run_id, RunStatus.running)
    old_persist_started = asyncio.Event()
    release_old_persist = asyncio.Event()
    original_persist_status = manager._persist_status

    async def block_old_persist(record: Any, status: RunStatus, **kwargs: Any) -> bool:
        if record.run_id != old.run_id:
            return await original_persist_status(record, status, **kwargs)
        old_persist_started.set()
        await asyncio.wait_for(release_old_persist.wait(), timeout=1)
        return await original_persist_status(record, status, **kwargs)

    monkeypatch.setattr(manager, "_persist_status", block_old_persist)
    create_task = asyncio.create_task(manager.create_or_reject("thread-1", multitask_strategy="interrupt", user_id="owner-1"))
    await asyncio.wait_for(old_persist_started.wait(), timeout=1)
    replacement = next(record for record in manager._runs.values() if record.run_id != old.run_id)
    create_task.cancel()
    release_old_persist.set()

    with pytest.raises(asyncio.CancelledError):
        _ = await asyncio.wait_for(create_task, timeout=1)

    assert await manager.drain_admission_compensations(timeout=1) is True
    stored_replacement = await store.get(replacement.run_id, user_id="owner-1")
    assert stored_replacement is not None
    assert stored_replacement["status"] == RunStatus.interrupted.value
    assert replacement.status == RunStatus.interrupted
    assert not await manager.has_inflight("thread-1")


@pytest.mark.anyio
async def test_create_or_reject_cleanup_failure_preserves_caller_cancellation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Cleanup IO failure must not replace the caller's CancelledError."""

    class FailingReplacementCleanupStore(MemoryRunStore):
        replacement_update_failed = False

        async def update_status(self, run_id: str, status: str, **kwargs: Any) -> bool:
            row = await super().get(run_id)
            if status == RunStatus.interrupted.value and row is not None and row.get("status") == RunStatus.pending.value:
                self.replacement_update_failed = True
                raise RuntimeError("replacement status unavailable")
            return await super().update_status(run_id, status, **kwargs)

        async def get(self, run_id: str, *, user_id: str | None = None) -> dict[str, Any] | None:
            row = await super().get(run_id, user_id=user_id)
            if self.replacement_update_failed and row is not None and row.get("status") == RunStatus.pending.value:
                raise RuntimeError("replacement verification unavailable")
            return row

    store = FailingReplacementCleanupStore()
    manager = RunManager(store=store)
    old = await manager.create("thread-1")
    await manager.set_status(old.run_id, RunStatus.running)
    old_persist_started = asyncio.Event()
    release_old_persist = asyncio.Event()
    original_persist_status = manager._persist_status

    async def block_old_persist(record: Any, status: RunStatus, **kwargs: Any) -> bool:
        if record.run_id != old.run_id:
            return await original_persist_status(record, status, **kwargs)
        old_persist_started.set()
        await asyncio.wait_for(release_old_persist.wait(), timeout=1)
        return await original_persist_status(record, status, **kwargs)

    monkeypatch.setattr(manager, "_persist_status", block_old_persist)
    create_task = asyncio.create_task(manager.create_or_reject("thread-1", multitask_strategy="interrupt"))
    await asyncio.wait_for(old_persist_started.wait(), timeout=1)
    create_task.cancel()
    release_old_persist.set()

    with pytest.raises(asyncio.CancelledError):
        _ = await asyncio.wait_for(create_task, timeout=1)


@pytest.mark.anyio
async def test_create_or_reject_preserves_peer_terminal_status_during_cancel_retry(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A peer terminal transition must win the strict cancellation retry."""

    class PeerWinsReplacementInterruptStore(MemoryRunStore):
        replacement_attempts = 0

        async def update_status(self, run_id: str, status: str, **kwargs: Any) -> bool:
            row = await self.get(run_id)
            if status == RunStatus.interrupted.value and row is not None and row.get("status") == RunStatus.pending.value:
                self.replacement_attempts += 1
                if self.replacement_attempts == 1:
                    raise RuntimeError("replacement status write failed")
                await super().update_status(run_id, RunStatus.error.value, error="peer takeover")
                return False
            return await super().update_status(run_id, status, **kwargs)

    store = PeerWinsReplacementInterruptStore()
    manager = RunManager(store=store)
    old = await manager.create("thread-1")
    await manager.set_status(old.run_id, RunStatus.running)
    old_persist_started = asyncio.Event()
    release_old_persist = asyncio.Event()
    original_persist_status = manager._persist_status

    async def block_old_persist(record: Any, status: RunStatus, **kwargs: Any) -> bool:
        if record.run_id != old.run_id:
            return await original_persist_status(record, status, **kwargs)
        old_persist_started.set()
        await asyncio.wait_for(release_old_persist.wait(), timeout=1)
        return await original_persist_status(record, status, **kwargs)

    monkeypatch.setattr(manager, "_persist_status", block_old_persist)
    create_task = asyncio.create_task(manager.create_or_reject("thread-1", multitask_strategy="interrupt"))
    await asyncio.wait_for(old_persist_started.wait(), timeout=1)
    replacement = next(record for record in manager._runs.values() if record.run_id != old.run_id)
    create_task.cancel()
    release_old_persist.set()

    with pytest.raises(asyncio.CancelledError):
        _ = await asyncio.wait_for(create_task, timeout=1)

    stored_replacement = await store.get(replacement.run_id)
    assert stored_replacement is not None
    assert stored_replacement["status"] == RunStatus.error.value
    assert stored_replacement["error"] == "peer takeover"
    assert replacement.status == RunStatus.error
    assert replacement.error == "peer takeover"
    assert not await manager.has_inflight("thread-1")


@pytest.mark.anyio
async def test_create_or_reject_rollback_persists_error_status_to_store():
    """Rollback preserves its authoritative error mapping for old runs."""
    store = MemoryRunStore()
    manager = RunManager(store=store)
    old = await manager.create("thread-1")
    await manager.set_status(old.run_id, RunStatus.running)

    new = await manager.create_or_reject("thread-1", multitask_strategy="rollback")

    stored_old = await store.get(old.run_id)
    assert new.run_id != old.run_id
    assert old.status == RunStatus.error
    assert stored_old is not None
    assert stored_old["status"] == "error"
    assert stored_old["error"] == "Rolled back by user"


@pytest.mark.anyio
async def test_model_name_default_is_none():
    """create_or_reject without model_name should default to None."""
    from deerflow.runtime.runs.schemas import DisconnectMode

    store = MemoryRunStore()
    mgr = RunManager(store=store)

    record = await mgr.create_or_reject(
        "thread-1",
        on_disconnect=DisconnectMode.cancel,
        model_name=None,
    )
    assert record.model_name is None

    stored = await store.get(record.run_id)
    assert stored["model_name"] is None


# ---------------------------------------------------------------------------
# Store fallback tests (simulates gateway restart scenario)
# ---------------------------------------------------------------------------


@pytest.fixture
def manager_with_store() -> RunManager:
    """RunManager backed by a MemoryRunStore."""
    return RunManager(store=MemoryRunStore())


@pytest.mark.anyio
async def test_list_by_thread_returns_store_records_after_restart(manager_with_store: RunManager):
    """After in-memory state is cleared (simulating restart), list_by_thread
    should still return runs from the persistent store."""
    mgr = manager_with_store
    r1 = await mgr.create("thread-1", "agent-1")
    await mgr.set_status(r1.run_id, RunStatus.success)
    r2 = await mgr.create("thread-1", "agent-2")
    await mgr.set_status(r2.run_id, RunStatus.error, error="boom")

    # Clear in-memory dict to simulate a restart
    mgr._runs.clear()

    runs = await mgr.list_by_thread("thread-1")
    assert len(runs) == 2
    statuses = {r.run_id: r.status for r in runs}
    assert statuses[r1.run_id] == RunStatus.success
    assert statuses[r2.run_id] == RunStatus.error
    # Verify other fields survive the round-trip
    for r in runs:
        assert r.thread_id == "thread-1"
        assert ISO_RE.match(r.created_at)


@pytest.mark.anyio
async def test_list_by_thread_merges_in_memory_and_store(manager_with_store: RunManager):
    """In-memory runs should be included alongside store-only records."""
    mgr = manager_with_store

    # Create a run and let it complete (will be in both memory and store)
    r1 = await mgr.create("thread-1")
    await mgr.set_status(r1.run_id, RunStatus.success)

    # Simulate restart: clear memory, then create a new in-memory run
    mgr._runs.clear()
    r2 = await mgr.create("thread-1")

    runs = await mgr.list_by_thread("thread-1")
    assert len(runs) == 2
    run_ids = {r.run_id for r in runs}
    assert r1.run_id in run_ids
    assert r2.run_id in run_ids

    # r2 should be the in-memory record (has live state)
    r2_record = next(r for r in runs if r.run_id == r2.run_id)
    assert r2_record is r2  # same object reference


@pytest.mark.anyio
async def test_list_by_thread_no_store():
    """Without a store, list_by_thread should only return in-memory runs."""
    mgr = RunManager()
    await mgr.create("thread-1")

    mgr._runs.clear()
    runs = await mgr.list_by_thread("thread-1")
    assert runs == []


@pytest.mark.anyio
async def test_aget_returns_in_memory_record(manager_with_store: RunManager):
    """aget should return the in-memory record when available."""
    mgr = manager_with_store
    r1 = await mgr.create("thread-1", "agent-1")

    result = await mgr.aget(r1.run_id)
    assert result is r1  # same object


@pytest.mark.anyio
async def test_aget_falls_back_to_store(manager_with_store: RunManager):
    """aget should return a record from the store when not in memory."""
    mgr = manager_with_store
    r1 = await mgr.create("thread-1", "agent-1")
    await mgr.set_status(r1.run_id, RunStatus.success)

    mgr._runs.clear()

    result = await mgr.aget(r1.run_id)
    assert result is not None
    assert result.run_id == r1.run_id
    assert result.status == RunStatus.success
    assert result.thread_id == "thread-1"
    assert result.assistant_id == "agent-1"


@pytest.mark.anyio
async def test_aget_falls_back_to_store_with_user_filter():
    """aget should honor user_id when reading store-only records."""
    store = MemoryRunStore()
    await store.put("run-1", thread_id="thread-1", user_id="user-1", status="success")
    mgr = RunManager(store=store)

    allowed = await mgr.aget("run-1", user_id="user-1")
    denied = await mgr.aget("run-1", user_id="user-2")
    assert allowed is not None
    assert denied is None


@pytest.mark.anyio
async def test_aget_returns_none_for_unknown(manager_with_store: RunManager):
    """aget should return None for a run ID that doesn't exist anywhere."""
    result = await manager_with_store.aget("nonexistent-run-id")
    assert result is None


@pytest.mark.anyio
async def test_aget_store_failure_is_graceful():
    """If the store raises, aget should return None instead of propagating."""
    from unittest.mock import AsyncMock

    store = MemoryRunStore()
    store.get = AsyncMock(side_effect=RuntimeError("db down"))
    mgr = RunManager(store=store)

    result = await mgr.aget("some-id")
    assert result is None


@pytest.mark.anyio
async def test_list_by_thread_store_failure_is_graceful():
    """If the store raises, list_by_thread should return only in-memory runs."""
    from unittest.mock import AsyncMock

    store = MemoryRunStore()
    store.list_by_thread = AsyncMock(side_effect=RuntimeError("db down"))
    mgr = RunManager(store=store)

    r1 = await mgr.create("thread-1")
    runs = await mgr.list_by_thread("thread-1")
    assert len(runs) == 1
    assert runs[0].run_id == r1.run_id


@pytest.mark.anyio
async def test_list_by_thread_falls_back_to_store_with_user_filter():
    """list_by_thread should return only the requesting user's store records."""
    store = MemoryRunStore()
    await store.put("run-1", thread_id="thread-1", user_id="user-1", status="success")
    await store.put("run-2", thread_id="thread-1", user_id="user-2", status="success")
    mgr = RunManager(store=store)

    runs = await mgr.list_by_thread("thread-1", user_id="user-1")
    assert [r.run_id for r in runs] == ["run-1"]


# ---------------------------------------------------------------------------
# Per-thread index (thread_id -> run_ids): keeps per-thread queries
# O(runs-in-thread) instead of scanning every in-memory run, and stays
# consistent with ``_runs`` across create / cleanup / rollback.
# ---------------------------------------------------------------------------


class _FailingPutRunStore(MemoryRunStore):
    """Memory run store whose every ``put`` and atomic operation create fails."""

    async def put(self, run_id, **kwargs):
        raise ValueError("simulated persist failure")

    async def create_thread_operation_atomic(self, run_id, **kwargs):
        raise ValueError("simulated persist failure")


@pytest.mark.anyio
async def test_thread_index_scopes_runs_per_thread(manager: RunManager):
    a1 = await manager.create("thread-a")
    a2 = await manager.create("thread-a")
    b1 = await manager.create("thread-b")

    # The index mirrors _runs membership, bucketed by thread.
    assert set(manager._runs_by_thread["thread-a"]) == {a1.run_id, a2.run_id}
    assert set(manager._runs_by_thread["thread-b"]) == {b1.run_id}

    # Per-thread queries return only that thread's runs (no cross-thread leak).
    assert {r.run_id for r in await manager.list_by_thread("thread-a")} == {a1.run_id, a2.run_id}
    assert {r.run_id for r in await manager.list_by_thread("thread-b")} == {b1.run_id}
    assert await manager.list_by_thread("thread-missing") == []


@pytest.mark.anyio
async def test_thread_index_preserves_insertion_order(manager: RunManager):
    # The index is insertion-ordered (dict-as-ordered-set) so list_by_thread
    # keeps the stable tie-breaking the full-scan implementation guaranteed.
    first = await manager.create("thread-a")
    second = await manager.create("thread-a")
    assert list(manager._runs_by_thread["thread-a"]) == [first.run_id, second.run_id]


@pytest.mark.anyio
async def test_thread_index_cleanup_prunes_run_and_empty_bucket(manager: RunManager):
    a1 = await manager.create("thread-a")
    a2 = await manager.create("thread-a")

    await manager.cleanup(a1.run_id, delay=0)
    assert a1.run_id not in manager._runs
    assert set(manager._runs_by_thread["thread-a"]) == {a2.run_id}

    await manager.cleanup(a2.run_id, delay=0)
    # Empty buckets are pruned so the index cannot grow without bound.
    assert "thread-a" not in manager._runs_by_thread
    assert await manager.list_by_thread("thread-a") == []


@pytest.mark.anyio
async def test_has_inflight_reflects_index(manager: RunManager):
    record = await manager.create("thread-a")
    assert await manager.has_inflight("thread-a") is True
    assert await manager.has_inflight("thread-b") is False

    await manager.set_status(record.run_id, RunStatus.success)
    assert await manager.has_inflight("thread-a") is False


@pytest.mark.anyio
async def test_create_or_reject_inflight_is_thread_scoped(manager: RunManager):
    await manager.create_or_reject("thread-a", multitask_strategy="reject")
    # A different thread is unaffected by thread-a's active run.
    await manager.create_or_reject("thread-b", multitask_strategy="reject")
    # A second active run on the same thread is rejected.
    with pytest.raises(ConflictError):
        await manager.create_or_reject("thread-a", multitask_strategy="reject")


@pytest.mark.anyio
async def test_failed_create_unindexes_run():
    manager = RunManager(store=_FailingPutRunStore())
    with pytest.raises(ValueError):
        await manager.create("thread-a")
    # A rolled-back run must leave no trace in either _runs or the index.
    assert manager._runs == {}
    assert "thread-a" not in manager._runs_by_thread


@pytest.mark.anyio
async def test_failed_create_or_reject_unindexes_run():
    # Symmetric to test_failed_create_unindexes_run: create_or_reject has its own
    # insert + rollback-unindex site, so a persist failure there must also leave
    # neither _runs nor the index holding the rolled-back run. This closes the last
    # mutation path not exercised by an index-consistency test.
    manager = RunManager(store=_FailingPutRunStore())
    with pytest.raises(ValueError):
        await manager.create_or_reject("thread-a", multitask_strategy="reject")
    assert manager._runs == {}
    assert "thread-a" not in manager._runs_by_thread
