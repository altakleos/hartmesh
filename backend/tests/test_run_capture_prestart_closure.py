"""Actual SQL startup/closure authority; no graph or model provenance claim."""

import asyncio
from dataclasses import FrozenInstanceError
from uuid import uuid4

import pytest
from _run_output_store_test_support import RAW_OWNER
from _run_output_store_test_support import output_store as output_store

from deerflow.runtime.runs.manager import CancelOutcome, RunManager, RunStartOutcome, RunStartupPhase
from deerflow.runtime.runs.output import UnavailableOutputReceipt, receipt_from_kwargs, validate_receipt_status
from deerflow.runtime.runs.schemas import RunStatus
from deerflow.runtime.runs.store.memory import MemoryRunStore


async def pending(environment, *, capture=True):
    manager = RunManager(environment.repository)
    kwargs = {"retained": {"values": ["original"]}}
    if capture:
        kwargs["output_capture"] = {"version": 1}
    record = await manager.create_or_reject(uuid4().hex, user_id=RAW_OWNER, kwargs=kwargs)
    return manager, record


def assert_seal(row, record, *, status, reason):
    assert row["status"] == status
    receipt = receipt_from_kwargs(row["kwargs"])
    assert receipt is not None, "Known prestart outcome must seal status and unavailable evidence atomically"
    validate_receipt_status(receipt, row["status"])
    assert (receipt.owner_user_id, receipt.thread_id, receipt.run_id) == (RAW_OWNER, record.thread_id, record.run_id)
    assert receipt.kind == "unavailable" and receipt.reason == reason
    assert receipt.input_message_id is None
    assert row["kwargs"]["retained"] == {"values": ["original"]}


@pytest.mark.asyncio
@pytest.mark.parametrize("capture", [False, True])
@pytest.mark.parametrize("action", ["interrupt", "rollback"])
async def test_cancel_before_start_preserves_ordinary_behavior_and_seals_capture(output_store, capture, action):
    manager, record = await pending(output_store, capture=capture)
    assert await manager.cancel(record.run_id, action=action) == CancelOutcome.cancelled
    row = await output_store.repository.get(record.run_id, user_id=RAW_OWNER)
    assert row["status"] == "interrupted"
    assert await manager.try_start(record.run_id) == RunStartOutcome.cancelled
    if capture:
        assert_seal(row, record, status="interrupted", reason="cancelled")
    else:
        assert "output_evidence" not in row["kwargs"]


@pytest.mark.asyncio
@pytest.mark.parametrize("capture", [False, True])
async def test_task_attachment_failure_seals_failed_without_reclassifying_abort(output_store, capture):
    manager, record = await pending(output_store, capture=capture)
    assert await manager.fail_start_if_pending(record.run_id, error="Synthetic task attachment failure")
    row = await output_store.repository.get(record.run_id, user_id=RAW_OWNER)
    assert row["status"] == "error" and row["error"] == "Synthetic task attachment failure"
    assert record.abort_event.is_set()
    assert await manager.try_start(record.run_id) == RunStartOutcome.cancelled
    if capture:
        assert_seal(row, record, status="error", reason="failed")
    else:
        assert "output_evidence" not in row["kwargs"]


@pytest.mark.asyncio
async def test_unseen_capture_compensation_never_uses_status_only_update(output_store, monkeypatch):
    manager, record = await pending(output_store)
    updates = []
    original_update = output_store.repository.update_status

    async def unavailable_best_effort(*args, **kwargs):
        return False

    async def observed_update(*args, **kwargs):
        updates.append(True)
        return await original_update(*args, **kwargs)

    monkeypatch.setattr(manager, "_persist_status", unavailable_best_effort)
    monkeypatch.setattr(output_store.repository, "update_status", observed_update)
    await manager._close_cancelled_admission(record)
    row = await output_store.repository.get(record.run_id, user_id=RAW_OWNER)
    assert updates == [], "Captured compensation cannot substitute an unsealed raw update"
    assert_seal(row, record, status="interrupted", reason="cancelled")


@pytest.mark.asyncio
@pytest.mark.parametrize("operation", ["cancel", "fail_start"])
async def test_prestart_closure_confirms_a_lost_atomic_commit_response(output_store, monkeypatch, operation):
    manager, record = await pending(output_store)
    original = output_store.repository.finalize_owned
    writes = []

    async def lost_response(*args, **kwargs):
        result = await original(*args, **kwargs)
        writes.append(kwargs.get("output_receipt"))
        if result and len(writes) == 1:
            raise OSError("Synthetic lost commit response")
        return result

    monkeypatch.setattr(output_store.repository, "finalize_owned", lost_response)
    if operation == "cancel":
        assert await manager.cancel(record.run_id) == CancelOutcome.cancelled
        status, reason = "interrupted", "cancelled"
    else:
        assert await manager.fail_start_if_pending(record.run_id, error="Synthetic attachment failure")
        status, reason = "error", "failed"
    row = await output_store.repository.get(record.run_id, user_id=RAW_OWNER)
    assert_seal(row, record, status=status, reason=reason)
    assert len(writes) == 1 and writes[0] is not None
    assert record.terminal_confirmed and not record.ownership_lost


@pytest.mark.asyncio
@pytest.mark.parametrize("operation", ["cancel", "fail_start"])
async def test_repeated_caller_cancellation_retains_the_whole_prestart_closure(output_store, monkeypatch, operation):
    manager, record = await pending(output_store)
    original = output_store.repository.finalize_owned
    entered, release = asyncio.Event(), asyncio.Event()

    async def committed_then_held(*args, **kwargs):
        result = await original(*args, **kwargs)
        entered.set()
        await release.wait()
        return result

    monkeypatch.setattr(output_store.repository, "finalize_owned", committed_then_held)
    call = manager.cancel(record.run_id) if operation == "cancel" else manager.fail_start_if_pending(record.run_id, error="Synthetic attach failure")
    task = asyncio.create_task(call)
    try:
        await asyncio.wait_for(entered.wait(), 5)
        task.cancel()
        task.cancel()
        done, _ = await asyncio.wait({task}, timeout=0.05)
        assert not done, "Started prestart closure remains owned through its committed-result boundary"
        release.set()
        with pytest.raises(asyncio.CancelledError):
            await task
        row = await output_store.repository.get(record.run_id, user_id=RAW_OWNER)
        assert_seal(row, record, status="interrupted" if operation == "cancel" else "error", reason="cancelled" if operation == "cancel" else "failed")
    finally:
        release.set()
        await asyncio.gather(task, return_exceptions=True)


@pytest.mark.asyncio
@pytest.mark.parametrize("task_state", ["missing", "done", "fenced"])
async def test_previously_started_task_state_is_not_cancellation_cleanup_proof(output_store, task_state):
    manager, record = await pending(output_store)
    assert await manager.try_start(record.run_id) == RunStartOutcome.started
    if task_state == "done":
        record.task = asyncio.create_task(asyncio.sleep(0))
        await record.task
    elif task_state == "fenced":
        await manager._mark_ownership_lost(record, reason="Synthetic unconfirmed source", require_active=False)
    await manager.cancel(record.run_id)
    row = await output_store.repository.get(record.run_id, user_id=RAW_OWNER)
    assert row["status"] == "running", "Missing/done task does not establish cleanup of a previously started run"
    assert "output_evidence" not in row["kwargs"]


@pytest.mark.asyncio
async def test_live_worker_keeps_active_row_until_its_actual_cleanup_and_seal(output_store):
    manager, record = await pending(output_store)
    assert await manager.try_start(record.run_id) == RunStartOutcome.started
    entered, cleaned = asyncio.Event(), asyncio.Event()
    release = asyncio.Event()

    async def owned_worker():
        entered.set()
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            cleaned.set()
            await release.wait()
            receipt = UnavailableOutputReceipt(version=1, kind="unavailable", owner_user_id=RAW_OWNER, thread_id=record.thread_id, run_id=record.run_id, input_message_id=None, reason="cancelled")
            result = await manager.finalize_output(record.run_id, RunStatus.interrupted, output_receipt=receipt, respect_cancellation=False)
            assert result.output_receipt_confirmed

    task = asyncio.create_task(owned_worker())
    record.task = task
    try:
        await asyncio.wait_for(entered.wait(), 5)
        assert await manager.cancel(record.run_id) == CancelOutcome.cancelled
        await asyncio.wait_for(cleaned.wait(), 5)
        row = await output_store.repository.get(record.run_id, user_id=RAW_OWNER)
        assert row["status"] == "running" and "output_evidence" not in row["kwargs"]
        release.set()
        await task
        assert_seal(await output_store.repository.get(record.run_id, user_id=RAW_OWNER), record, status="interrupted", reason="cancelled")
    finally:
        release.set()
        if not task.done():
            task.cancel()
        await asyncio.gather(task, return_exceptions=True)


class ObservedStartLock(asyncio.Lock):
    def __init__(self):
        super().__init__()
        self.waited = asyncio.Event()

    async def acquire(self):
        if self.locked():
            self.waited.set()
        return await super().acquire()


@pytest.mark.asyncio
async def test_prestart_cancel_waits_for_the_actual_start_barrier(output_store, monkeypatch):
    manager, record = await pending(output_store)
    record.start_lock = lock = ObservedStartLock()
    entered, release = asyncio.Event(), asyncio.Event()
    original = output_store.repository.start_run

    async def held_start(*args, **kwargs):
        entered.set()
        await release.wait()
        return await original(*args, **kwargs)

    monkeypatch.setattr(output_store.repository, "start_run", held_start)
    start = asyncio.create_task(manager.try_start(record.run_id))
    cancel = observed = None
    try:
        await asyncio.wait_for(entered.wait(), 5)
        cancel = asyncio.create_task(manager.cancel(record.run_id))
        observed = asyncio.create_task(lock.waited.wait())
        await asyncio.wait({cancel, observed}, timeout=2, return_when=asyncio.FIRST_COMPLETED)
        assert lock.waited.is_set() and not cancel.done(), "Prestart closure must join try_start's held authoritative lock"
        release.set()
        assert await start == RunStartOutcome.cancelled
        assert await cancel == CancelOutcome.cancelled
        assert_seal(await output_store.repository.get(record.run_id, user_id=RAW_OWNER), record, status="interrupted", reason="cancelled")
    finally:
        release.set()
        if observed is not None:
            observed.cancel()
        await asyncio.gather(*[t for t in (start, cancel, observed) if t is not None], return_exceptions=True)


@pytest.mark.asyncio
async def test_start_sql_race_closes_proven_unstarted_capture_with_an_atomic_seal(output_store, monkeypatch):
    manager, record = await pending(output_store)
    record.start_lock = lock = ObservedStartLock()
    entered, release, decided, cleanup = (asyncio.Event() for _ in range(4))
    original = output_store.repository.start_run

    async def start_then_hold(*args, **kwargs):
        result = await original(*args, **kwargs)
        entered.set()
        await release.wait()
        return result

    async def starting_worker():
        assert await manager.try_start(record.run_id) == RunStartOutcome.cancelled
        decided.set()
        await cleanup.wait()

    monkeypatch.setattr(output_store.repository, "start_run", start_then_hold)
    task = asyncio.create_task(starting_worker())
    record.task = task
    cancel = observed = None
    try:
        await asyncio.wait_for(entered.wait(), 5)
        cancel = asyncio.create_task(manager.cancel(record.run_id))
        observed = asyncio.create_task(lock.waited.wait())
        # The old manager returns early; the corrected manager joins startup's
        # actual exclusion. Neither condition needs an unbounded timing loop.
        await asyncio.wait({cancel, observed}, timeout=2, return_when=asyncio.FIRST_COMPLETED)
        release.set()
        await asyncio.wait_for(decided.wait(), 5)
        await cancel
        row = await output_store.repository.get(record.run_id, user_id=RAW_OWNER)
        assert_seal(row, record, status="interrupted", reason="cancelled")
    finally:
        release.set()
        cleanup.set()
        if observed is not None:
            observed.cancel()
        await asyncio.gather(*[t for t in (task, cancel, observed) if t is not None], return_exceptions=True)


@pytest.mark.asyncio
async def test_existing_prestart_failed_seal_is_immutable_to_late_cancel_or_failure(output_store):
    manager, record = await pending(output_store)
    receipt = UnavailableOutputReceipt(version=1, kind="unavailable", owner_user_id=RAW_OWNER, thread_id=record.thread_id, run_id=record.run_id, input_message_id=None, reason="failed")
    assert (await manager.finalize_output(record.run_id, RunStatus.error, output_receipt=receipt)).output_receipt_confirmed
    before = await output_store.repository.get(record.run_id, user_id=RAW_OWNER)
    assert not await manager.fail_start_if_pending(record.run_id, error="late failure")
    assert await manager.cancel(record.run_id) == CancelOutcome.not_cancellable
    after = await output_store.repository.get(record.run_id, user_id=RAW_OWNER)
    assert after["kwargs"] == before["kwargs"] and after["change_seq"] == before["change_seq"]


@pytest.mark.asyncio
async def test_memory_ordinary_start_failure_and_cancel_keep_their_existing_contract():
    store = MemoryRunStore()
    manager = RunManager(store)
    cancelled = await manager.create_or_reject("memory-cancel")
    assert await manager.cancel(cancelled.run_id) == CancelOutcome.cancelled
    assert (await store.get(cancelled.run_id))["status"] == "interrupted"
    failed = await manager.create_or_reject("memory-fail")
    assert await manager.fail_start_if_pending(failed.run_id, error="Synthetic failure")
    row = await store.get(failed.run_id)
    assert row["status"] == "error" and "output_evidence" not in row["kwargs"]


@pytest.mark.asyncio
async def test_startup_decision_is_host_only_frozen_and_not_generic_progress(output_store):
    from app.gateway.routers.thread_runs import _record_to_response

    manager, record = await pending(output_store)
    assert record.startup_phase is RunStartupPhase.unstarted and record.prestart_outcome is None
    row = await output_store.repository.get(record.run_id, user_id=RAW_OWNER)
    hydrated = manager._record_from_store({**row, "startup_phase": "unstarted", "prestart_outcome": {"confirmed": True}})
    assert hydrated.startup_phase is RunStartupPhase.unknown and hydrated.prestart_outcome is None
    assert "startup_phase" not in _record_to_response(record).model_dump()
    assert "prestart_outcome" not in _record_to_response(record).model_dump()
    assert not {"startup_phase", "prestart_outcome"} & row["kwargs"].keys()
    assert await manager.try_start(record.run_id) == RunStartOutcome.started
    await manager.update_run_progress(record.run_id, startup_phase=RunStartupPhase.unstarted, prestart_outcome="forged")
    assert record.startup_phase is RunStartupPhase.started and record.prestart_outcome is None
    other_manager, other = await pending(output_store)
    assert await other_manager.fail_start_if_pending(other.run_id, error="Synthetic failure")
    decision = other.prestart_outcome
    assert decision is not None and decision.confirmed
    with pytest.raises(FrozenInstanceError):
        decision.reason = "cancelled"
    await other_manager.update_finalizing_progress(other.run_id, startup_phase=RunStartupPhase.unstarted, prestart_outcome="forged")
    await other_manager.update_run_completion(other.run_id, status="error", startup_phase=RunStartupPhase.unstarted, prestart_outcome="forged")
    assert other.startup_phase is RunStartupPhase.closed and other.prestart_outcome is decision


@pytest.mark.asyncio
async def test_unknown_startup_state_does_not_authorize_a_prestart_seal(output_store):
    manager, record = await pending(output_store)
    record.startup_phase = RunStartupPhase.unknown
    await manager.cancel(record.run_id)
    row = await output_store.repository.get(record.run_id, user_id=RAW_OWNER)
    assert row["status"] == "pending" and "output_evidence" not in row["kwargs"]


@pytest.mark.asyncio
async def test_orphan_reconciliation_preserves_locally_finalizing_capture(output_store):
    manager, record = await pending(output_store)
    assert await manager.try_start(record.run_id) == RunStartOutcome.started
    await manager.set_status(record.run_id, RunStatus.success, persist=False)
    await manager.set_finalizing(record.run_id, True)
    before = await output_store.repository.get(record.run_id, user_id=RAW_OWNER)
    recovered = await manager.reconcile_orphaned_inflight_runs(error="Synthetic peer recovery")
    assert recovered == [], "Locally finalizing capture remains owned even with staged terminal status"
    assert await output_store.repository.get(record.run_id, user_id=RAW_OWNER) == before


@pytest.mark.asyncio
async def test_invalid_capture_cancel_action_cannot_become_a_proven_cancellation(output_store):
    manager, record = await pending(output_store)
    before = await output_store.repository.get(record.run_id, user_id=RAW_OWNER)
    with pytest.raises(ValueError):
        await manager.cancel(record.run_id, action="unsupported")
    assert await output_store.repository.get(record.run_id, user_id=RAW_OWNER) == before


@pytest.mark.asyncio
async def test_closed_prestart_outcome_retains_attached_worker_finalizing_barrier(output_store):
    from deerflow.runtime.runs.manager import ConflictError

    manager, record = await pending(output_store)
    entered, release = asyncio.Event(), asyncio.Event()

    async def attached_worker():
        entered.set()
        await release.wait()
        await manager.set_finalizing(record.run_id, False)

    task = asyncio.create_task(attached_worker())
    record.task = task
    try:
        await asyncio.wait_for(entered.wait(), 5)
        assert await manager.fail_start_if_pending(record.run_id, error="Synthetic metadata failure")
        assert record.finalizing, "A closed prestart result does not finish its attached worker's local cleanup"
        with pytest.raises(ConflictError):
            await manager.create_or_reject(record.thread_id, user_id=RAW_OWNER, multitask_strategy="interrupt")
        release.set()
        await task
        assert not record.finalizing
    finally:
        release.set()
        await asyncio.gather(task, return_exceptions=True)
