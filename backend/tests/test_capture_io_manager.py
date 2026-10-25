"""Actual admission SQL and shutdown lifetimes before capture-owner wiring.

These cases use the real core and existing manager entry points. Missing new
manager accessors are API scaffolds; SQL context/lifetime failures are separate
behavioral observations. No graph/model or native containment is simulated.
"""

import asyncio
from contextvars import ContextVar
from dataclasses import asdict
from uuid import uuid4

import pytest
from _run_output_store_test_support import RAW_OWNER
from _run_output_store_test_support import output_store as output_store
from sqlalchemy import event, select, update
from sqlalchemy.util import await_only

from deerflow.persistence.run.model import RunRow
from deerflow.runtime.runs.capture_io import CaptureIOCleanupError, CaptureIOClosed, CaptureIOOwner, capture_io_scope, current_capture_io_owner
from deerflow.runtime.runs.manager import RunManager, RunStartupPhase
from deerflow.runtime.runs.output import UnavailableOutputReceipt
from deerflow.runtime.runs.schemas import RunStatus, ThreadOperationKind


class QueryBarrier:
    """Pause after actual driver execution, retaining its cursor/session scope."""

    def __init__(self, environment, predicate):
        self.environment, self.predicate = environment, predicate
        self.entered, self.release = asyncio.Event(), asyncio.Event()
        self.owner = None
        self.statement_kind = None
        self.seen = False

    def pause(self, _connection, _cursor, statement, _parameters, _context, _many):
        if self.seen or not self.predicate(statement.lstrip().upper()):
            return
        self.seen = True
        self.owner = current_capture_io_owner()
        self.statement_kind = statement.lstrip().split()[0].upper()

        async def wait():
            self.entered.set()
            await self.release.wait()

        await_only(wait())

    def install(self):
        event.listen(self.environment.engine.sync_engine, "after_cursor_execute", self.pause)

    def remove(self):
        event.remove(self.environment.engine.sync_engine, "after_cursor_execute", self.pause)


async def pending(environment, *, capture=True, key=None):
    manager = RunManager(environment.repository)
    values = {"retained": {"values": ["original"]}}
    if capture:
        values["output_capture"] = {"version": 1}
    record = await manager.create_or_reject(uuid4().hex, "lead_agent", user_id=RAW_OWNER, kwargs=values, idempotency_key=key)
    return manager, record


def unavailable(record, reason):
    return UnavailableOutputReceipt(version=1, kind="unavailable", owner_user_id=RAW_OWNER, thread_id=record.thread_id, run_id=record.run_id, input_message_id=None, reason=reason)


async def independent_commit(environment, record):
    async with environment.sf() as session, session.begin():
        await session.execute(update(RunRow).where(RunRow.run_id == record.run_id).values(total_tokens=37))
    row = await environment.repository.get(record.run_id, user_id=RAW_OWNER)
    assert row["total_tokens"] == 37
    assert environment.engine.sync_engine.pool.checkedout() == 0
    return row


@pytest.mark.asyncio
@pytest.mark.parametrize("capture", [False, True])
async def test_actual_admission_binds_capture_owner_before_first_executed_SQL(output_store, capture):
    manager = RunManager(output_store.repository)
    barrier = QueryBarrier(output_store, lambda statement: statement.startswith(("SELECT", "INSERT", "UPDATE")) and "RUN" in statement)
    barrier.install()
    values = {"output_capture": {"version": 1}} if capture else {}
    creating = asyncio.create_task(manager.create_or_reject(uuid4().hex, user_id=RAW_OWNER, kwargs=values))
    try:
        await asyncio.wait_for(barrier.entered.wait(), 5)
        assert manager._lock.locked()
        barrier.release.set()
        record = await creating
        await independent_commit(output_store, record)
        if capture:
            assert isinstance(barrier.owner, CaptureIOOwner), "Actual captured admission SQL has no original host owner"
        else:
            assert barrier.owner is None
        assert record.startup_phase is RunStartupPhase.unstarted
        assert record.user_id == RAW_OWNER
    finally:
        barrier.release.set()
        await asyncio.gather(creating, return_exceptions=True)
        barrier.remove()
        await manager.shutdown(timeout=0)


@pytest.mark.asyncio
async def test_shutdown_seals_late_capture_admission_while_executed_SQL_holds_registry_lock(output_store):
    manager = RunManager(output_store.repository)
    barrier = QueryBarrier(output_store, lambda statement: statement.startswith("INSERT INTO RUNS"))
    barrier.install()
    creating = asyncio.create_task(manager.create_or_reject(uuid4().hex, user_id=RAW_OWNER, kwargs={"output_capture": {"version": 1}}))
    stopping = late = None
    try:
        await asyncio.wait_for(barrier.entered.wait(), 5)
        assert manager._lock.locked()
        stopping = asyncio.create_task(manager.shutdown(timeout=0.02))
        await asyncio.sleep(0)
        late = asyncio.create_task(manager.create_or_reject(uuid4().hex, user_id=RAW_OWNER, kwargs={"output_capture": {"version": 1}}))
        done, _ = await asyncio.wait({late}, timeout=0.08)
        assert late in done, "The admission seal waited for the SQL-held manager lock past the shared grace"
        with pytest.raises(CaptureIOClosed):
            await late
        assert isinstance(barrier.owner, CaptureIOOwner)
        # A correct admission seal can reject before D. Observe retirement
        # itself rather than treating that early refusal as elapsed grace.
        await asyncio.wait_for(barrier.owner.wait_retired(), 0.3)
        assert barrier.owner.retired
        assert not stopping.done(), "Retirement cannot detach the already executed native transaction"
    finally:
        barrier.release.set()
        await asyncio.gather(*(task for task in (creating, stopping, late) if task is not None), return_exceptions=True)
        barrier.remove()
    assert output_store.engine.sync_engine.pool.checkedout() == 0


@pytest.mark.asyncio
@pytest.mark.parametrize("state", ["success", "prestart_failed"])
@pytest.mark.parametrize("capture", [False, True])
async def test_confirmed_finishing_task_is_in_capture_grace_without_rewriting_its_proof(output_store, state, capture):
    manager, record = await pending(output_store, capture=capture)
    if state == "prestart_failed":
        assert await manager.fail_start_if_pending(record.run_id, error="Synthetic prestart failure")
    else:
        await manager.try_start(record.run_id)
        if capture:
            result = await manager.finalize_output(record.run_id, RunStatus.success, output_receipt=unavailable(record, "unsupported_output"))
            assert result.output_receipt_confirmed
        else:
            await manager.set_status(record.run_id, RunStatus.success)
    record.finalizing = True
    before = await output_store.repository.get(record.run_id, user_id=RAW_OWNER)
    role = ContextVar("capture_finishing_query", default=False)
    barrier = QueryBarrier(output_store, lambda statement: role.get() and statement.startswith("SELECT") and "FROM RUNS" in statement)
    barrier.install()

    async def finishing():
        token = role.set(True)
        try:
            return await output_store.repository.get(record.run_id, user_id=RAW_OWNER)
        finally:
            role.reset(token)

    record.task = asyncio.create_task(finishing())
    stopping = None
    stopped_early = None
    try:
        await asyncio.wait_for(barrier.entered.wait(), 5)
        assert output_store.engine.sync_engine.pool.checkedout() > 0
        stopping = asyncio.create_task(manager.shutdown(timeout=0.5))
        done, _ = await asyncio.wait({stopping}, timeout=0.04)
        stopped_early = stopping in done
    finally:
        barrier.release.set()
        await asyncio.gather(*(task for task in (record.task, stopping) if task is not None), return_exceptions=True)
        barrier.remove()
    row = await independent_commit(output_store, record)
    if capture:
        assert stopped_early is False, "Confirmed capture finalization was omitted while its actual SQL scope remained live"
    else:
        assert stopped_early is True, "The new capture lifetime must not broaden ordinary confirmed-task shutdown"
    assert (row["status"], row["kwargs"], row["error"], row["stop_reason"]) == (before["status"], before["kwargs"], before["error"], before["stop_reason"])


@pytest.mark.asyncio
async def test_no_task_prestart_SQL_closure_remains_owned_after_shutdown_grace(output_store):
    manager, record = await pending(output_store)
    assert record.task is None
    role = ContextVar("capture_no_task_closure", default=False)
    barrier = QueryBarrier(output_store, lambda statement: role.get() and statement.startswith("SELECT") and "FROM RUNS" in statement)
    barrier.install()

    async def close():
        token = role.set(True)
        try:
            return await manager.fail_start_if_pending(record.run_id, error="Synthetic task attachment failure")
        finally:
            role.reset(token)

    closing = asyncio.create_task(close())
    stopping = None
    stopped_early = None
    try:
        await asyncio.wait_for(barrier.entered.wait(), 5)
        stopping = asyncio.create_task(manager.shutdown(timeout=0.02))
        done, _ = await asyncio.wait({stopping}, timeout=0.08)
        stopped_early = stopping in done
    finally:
        barrier.release.set()
        await asyncio.gather(*(task for task in (closing, stopping) if task is not None), return_exceptions=True)
        barrier.remove()
    await independent_commit(output_store, record)
    assert stopped_early is False, "A captured closure without record.task escaped the finite-operation drain"
    assert isinstance(barrier.owner, CaptureIOOwner) and barrier.owner.retired


def contains_owner(value):
    if isinstance(value, CaptureIOOwner):
        return True
    if isinstance(value, dict):
        return any(contains_owner(item) for item in value.values())
    if isinstance(value, (tuple, list, set)):
        return any(contains_owner(item) for item in value)
    return False


@pytest.mark.asyncio
async def test_capture_record_and_store_projections_never_acquire_runtime_capabilities(output_store):
    manager, record = await pending(output_store)
    try:
        assert not contains_owner(vars(record))
        assert not contains_owner(asdict(record))
        assert not contains_owner(manager._store_put_payload(record))
        row = await output_store.repository.get(record.run_id, user_id=RAW_OWNER)
        assert not contains_owner(row)
        assert row["kwargs"]["retained"] == {"values": ["original"]}
    finally:
        await manager.shutdown(timeout=0)


@pytest.mark.asyncio
async def test_peer_reuse_and_unknown_store_handle_are_not_locally_executed_or_resealed(output_store):
    owner, record = await pending(output_store, key=uuid4().hex)
    peer = RunManager(output_store.repository)
    before = await output_store.repository.get(record.run_id, user_id=RAW_OWNER)
    reused = await peer.create_or_reject(record.thread_id, "lead_agent", user_id=RAW_OWNER, kwargs={"output_capture": {"version": 1}}, idempotency_key=record.idempotency_key)
    assert reused.store_only and reused.idempotency_reused
    assert reused.startup_phase is RunStartupPhase.unknown
    assert record.run_id not in peer._runs
    await peer.shutdown(timeout=0)
    after = await output_store.repository.get(record.run_id, user_id=RAW_OWNER)
    assert after == before
    await owner.shutdown(timeout=0)


@pytest.mark.asyncio
async def test_manager_original_record_owner_accessor_is_available(output_store):
    manager, record = await pending(output_store)
    try:
        assert callable(getattr(manager, "get_capture_io_owner", None)), "New manager accessor API scaffold is unavailable; not an old behavioral regression"
        owner = manager.get_capture_io_owner(record)
        assert isinstance(owner, CaptureIOOwner)
        assert manager.get_capture_io_owner(record) is owner
        owner.retire()
        assert manager.get_capture_io_owner(record) is owner
    finally:
        await manager.shutdown(timeout=0)


@pytest.mark.asyncio
@pytest.mark.parametrize("retire", [False, True])
async def test_known_goal_reservation_releases_its_exact_row_after_retirement(output_store, retire):
    manager, record = await pending(output_store)
    await manager.try_start(record.run_id)
    result = await manager.finalize_output(record.run_id, RunStatus.success, output_receipt=unavailable(record, "unsupported_output"))
    assert result.output_receipt_confirmed
    record.finalizing = True
    record.task = asyncio.current_task()
    owner = manager.get_capture_io_owner(record)
    reservation_id = None
    refusal = None
    try:
        with capture_io_scope(owner):
            try:
                async with manager.reserve_thread_operation(record.thread_id, kind=ThreadOperationKind.checkpoint_write, user_id=RAW_OWNER, finalizing_run=record):
                    async with output_store.sf() as session:
                        reservation = await session.scalar(select(RunRow).where(RunRow.thread_id == record.thread_id, RunRow.operation_kind == "checkpoint_write"))
                        assert reservation is not None and reservation.owner_worker_id == manager.worker_id
                        reservation_id = reservation.run_id
                    if retire:
                        owner.retire()
            except CaptureIOClosed as error:
                refusal = error
        async with output_store.sf() as session:
            persisted = await session.get(RunRow, reservation_id)
        assert persisted is None, "Retirement discarded release of an already-admitted exact checkpoint reservation"
        assert refusal is None
        assert record.task is asyncio.current_task(), "Goal self-handoff must remain on its actual original worker task"
    finally:
        # Own the baseline's exact leftover fixture row, after observing it.
        if reservation_id is not None:
            await output_store.repository.delete_thread_operation(reservation_id, user_id=RAW_OWNER)
        record.task = None
        await manager.shutdown(timeout=0)


@pytest.mark.asyncio
@pytest.mark.parametrize("change", ["peer", "version"])
async def test_reservation_cleanup_does_not_remove_changed_admission(output_store, change):
    manager, record = await pending(output_store)
    await manager.try_start(record.run_id)
    assert (await manager.finalize_output(record.run_id, RunStatus.success, output_receipt=unavailable(record, "unsupported_output"))).output_receipt_confirmed
    record.finalizing, record.task = True, asyncio.current_task()
    reservation_id = None
    try:
        try:
            with capture_io_scope(manager.get_capture_io_owner(record)):
                async with manager.reserve_thread_operation(record.thread_id, kind=ThreadOperationKind.checkpoint_write, user_id=RAW_OWNER, finalizing_run=record):
                    async with output_store.sf() as session:
                        row = await session.scalar(select(RunRow).where(RunRow.thread_id == record.thread_id, RunRow.operation_kind == "checkpoint_write"))
                        reservation_id = row.run_id
                        if change == "peer":
                            row.owner_worker_id = "peer-worker"
                        else:
                            row.change_seq += 100
                        await session.commit()
        except CaptureIOCleanupError:
            pass
        row = await output_store.repository.get(reservation_id, user_id=RAW_OWNER)
        assert row is not None, "Reservation cleanup removed a row whose admission authority had changed"
    finally:
        if reservation_id is not None:
            await output_store.repository.delete_thread_operation(reservation_id, user_id=RAW_OWNER)
        record.task = None
        try:
            await manager.shutdown(timeout=0)
        except CaptureIOCleanupError:
            pass  # A failed release remains failed, even after test-owned cleanup.


@pytest.mark.asyncio
async def test_reconciliation_scan_is_owned_but_next_claim_refuses_after_shutdown_deadline(output_store):
    from datetime import UTC, datetime, timedelta

    db = output_store
    run_id = str(uuid4())
    await db.repository.put(
        run_id, thread_id=str(uuid4()), user_id=RAW_OWNER, status="running", owner_worker_id="expired-peer", lease_expires_at=(datetime.now(UTC) - timedelta(minutes=2)).isoformat(), kwargs={"output_capture": {"version": 1}}
    )
    manager = RunManager(db.repository)
    role = ContextVar("capture_maintenance_scan", default=False)
    barrier = QueryBarrier(db, lambda statement: role.get() and statement.startswith("SELECT") and "FROM RUNS" in statement)
    barrier.install()

    async def reconcile():
        token = role.set(True)
        try:
            return await manager.reconcile_orphaned_inflight_runs(error="Orphaned")
        finally:
            role.reset(token)

    scanning = asyncio.create_task(reconcile())
    closing = None
    premature = None
    try:
        await asyncio.wait_for(barrier.entered.wait(), 3)
        closing = asyncio.create_task(manager.shutdown(timeout=0.02))
        done, _ = await asyncio.wait({closing}, timeout=0.07)
        premature = closing in done
    finally:
        barrier.release.set()
        await asyncio.gather(*(task for task in (scanning, closing) if task is not None), return_exceptions=True)
        barrier.remove()
    row = await db.repository.get(run_id, user_id=RAW_OWNER)
    assert premature is False, "Manager drain omitted an already-executed reconciliation SQL read"
    assert isinstance(barrier.owner, CaptureIOOwner) and barrier.owner.retired
    assert row["status"] == "running" and row["owner_worker_id"] == "expired-peer"
    assert run_id not in manager._runs, "Maintenance must never adopt peer RunRecords as locally executed"


@pytest.mark.asyncio
async def test_reconciliation_without_retirement_keeps_existing_peer_outcome(output_store):
    from datetime import UTC, datetime, timedelta

    db = output_store
    run_id = str(uuid4())
    await db.repository.put(run_id, thread_id=str(uuid4()), user_id=RAW_OWNER, status="running", owner_worker_id="expired-peer", lease_expires_at=(datetime.now(UTC) - timedelta(minutes=2)).isoformat(), kwargs={})
    manager = RunManager(db.repository)
    recovered = await manager.reconcile_orphaned_inflight_runs(error="Orphaned")
    assert [record.run_id for record in recovered] == [run_id]
    assert (await db.repository.get(run_id, user_id=RAW_OWNER))["status"] == "error"
    assert run_id not in manager._runs
    await manager.shutdown(timeout=0)


@pytest.mark.asyncio
async def test_registry_uses_original_record_identity_even_if_its_kwargs_change(output_store):
    from dataclasses import replace

    from deerflow.runtime.runs.capture_io import CaptureIOUnsupported

    manager, record = await pending(output_store)
    owner = manager.get_capture_io_owner(record)
    clone = replace(record)
    try:
        with pytest.raises(CaptureIOUnsupported):
            manager.get_capture_io_owner(clone)
        record.kwargs.pop("output_capture")
        assert manager.get_capture_io_owner(record) is owner
        owner.retire()
        assert manager.get_capture_io_owner(record) is owner
        assert not contains_owner(asdict(record))
    finally:
        await manager.shutdown(timeout=0)


@pytest.mark.asyncio
async def test_retired_original_owner_cannot_be_replaced_by_a_fresh_admission(output_store):
    manager, record = await pending(output_store, key=str(uuid4()))
    owner = manager.get_capture_io_owner(record)
    await manager.shutdown(timeout=0)
    before = await output_store.repository.get(record.run_id, user_id=RAW_OWNER)
    with pytest.raises(CaptureIOClosed):
        await manager.create_or_reject(str(uuid4()), user_id=RAW_OWNER, kwargs={"output_capture": {"version": 1}})
    assert manager.get_capture_io_owner(record) is owner and owner.retired
    assert await output_store.repository.get(record.run_id, user_id=RAW_OWNER) == before


@pytest.mark.asyncio
async def test_ambiguous_reservation_release_is_fixed_retained_and_never_replayed(output_store, monkeypatch):
    import traceback

    manager, record = await pending(output_store)
    await manager.try_start(record.run_id)
    assert (await manager.finalize_output(record.run_id, RunStatus.success, output_receipt=unavailable(record, "unsupported_output"))).output_receipt_confirmed
    record.finalizing, record.task = True, asyncio.current_task()
    owner = manager.get_capture_io_owner(record)
    original = output_store.repository.release_owned_checkpoint_reservation
    calls = []
    marker = "private-reservation-release-diagnostic"

    async def lost(*args, **kwargs):
        calls.append(args[0])
        assert await original(*args, **kwargs)
        raise RuntimeError(marker)

    monkeypatch.setattr(output_store.repository, "release_owned_checkpoint_reservation", lost)
    try:
        try:
            with capture_io_scope(owner):
                async with manager.reserve_thread_operation(record.thread_id, kind=ThreadOperationKind.checkpoint_write, user_id=RAW_OWNER, finalizing_run=record):
                    pass
        except Exception as error:
            caught = error
        else:
            pytest.fail("Ambiguous release was reported as confirmed")
        assert isinstance(caught, CaptureIOCleanupError)
        assert marker not in "".join(traceback.format_exception(caught))
        for _ in range(2):
            with pytest.raises(CaptureIOCleanupError):
                await owner.drain()
        assert len(calls) == 1
        assert await output_store.repository.get(calls[0], user_id=RAW_OWNER) is None
    finally:
        record.task = None
        try:
            await manager.shutdown(timeout=0)
        except CaptureIOCleanupError:
            pass


@pytest.mark.asyncio
async def test_captured_heartbeat_uses_original_owner_and_cannot_restart_after_retirement(output_store, monkeypatch):
    from deerflow.config.run_ownership_config import RunOwnershipConfig

    db = output_store
    manager = RunManager(db.repository, run_ownership_config=RunOwnershipConfig(heartbeat_enabled=True))
    record = await manager.create_or_reject(str(uuid4()), user_id=RAW_OWNER, kwargs={"output_capture": {"version": 1}})
    owner = manager.get_capture_io_owner(record)
    seen = []
    original = db.repository.renew_lease

    async def renew(*args, **kwargs):
        seen.append(current_capture_io_owner())
        return await original(*args, **kwargs)

    monkeypatch.setattr(db.repository, "renew_lease", renew)
    await manager._renew_leases()
    assert seen == [owner]
    owner.retire()
    before = await db.repository.get(record.run_id, user_id=RAW_OWNER)
    await manager._renew_leases()
    assert seen == [owner]
    assert await db.repository.get(record.run_id, user_id=RAW_OWNER) == before
    await manager.shutdown(timeout=0)


@pytest.mark.asyncio
async def test_late_admitted_task_host_API_scaffold(output_store):
    manager = RunManager(output_store.repository)
    assert callable(getattr(manager, "drain_late_admitted_tasks", None)), "Additive host snapshot API is not available; availability scaffold only"


@pytest.mark.asyncio
@pytest.mark.parametrize("capture", [False, True])
async def test_late_attached_task_is_stopped_under_original_absolute_deadline(output_store, capture):
    manager, record = await pending(output_store, capture=capture)
    deadline = asyncio.get_running_loop().time() + 0.5
    await manager.shutdown(timeout=0)
    before = await output_store.repository.get(record.run_id, user_id=RAW_OWNER)
    entered = asyncio.Event()

    async def late_worker():
        entered.set()
        await asyncio.Event().wait()

    task = asyncio.create_task(late_worker())
    record.task = task
    try:
        await asyncio.wait_for(entered.wait(), 1)
        await manager.drain_late_admitted_tasks(deadline=deadline)
        assert task.done() and task.cancelled()
        row = await output_store.repository.get(record.run_id, user_id=RAW_OWNER)
        if capture:
            assert row == before, "A late Task cannot justify a terminal capture outcome"
        else:
            assert row["status"] == "interrupted" and record.status is RunStatus.interrupted
    finally:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)


@pytest.mark.asyncio
@pytest.mark.parametrize("state", ["success", "prestart_failed", "active", "uncertain", "close_failed"])
@pytest.mark.parametrize("cleanup_first", [False, True])
async def test_eviction_releases_only_confirmed_fully_drained_original_lifetimes(output_store, state, cleanup_first):
    from deerflow.runtime.runs.capture_io import CaptureIOUnsupported

    manager, record = await pending(output_store)
    owner = manager.get_capture_io_owner(record)
    if state == "prestart_failed":
        assert await manager.fail_start_if_pending(record.run_id, error="Confirmed original prestart failure")
    elif state in {"success", "close_failed"}:
        await manager.try_start(record.run_id)
        assert (await manager.finalize_output(record.run_id, RunStatus.success, output_receipt=unavailable(record, "unsupported_output"))).output_receipt_confirmed
    elif state == "uncertain":
        await manager._mark_ownership_lost(record, reason="Known uncertainty", require_active=False)

    class Resource:
        def retire(self):
            pass

        async def drain(self):
            if state == "close_failed":
                raise ValueError("Diagnostic close failure")

    resource = Resource()
    owner.register_resource(resource)
    lifetime = manager._capture_lifetime(record)
    try:
        if cleanup_first:
            await manager.cleanup(record.run_id, delay=0)
        try:
            await manager.finish_capture_io(record)
        except CaptureIOCleanupError:
            assert state == "close_failed"
        if not cleanup_first:
            await manager.cleanup(record.run_id, delay=0)
        assert record.run_id not in manager._runs
        if state in {"success", "prestart_failed"}:
            assert id(record) not in manager._capture_records, "Normal eviction permanently retained a confirmed, fully drained original record"
            assert owner not in manager._capture_lifetimes
            with pytest.raises(CaptureIOUnsupported):
                manager.get_capture_io_owner(record)
        else:
            assert manager._capture_lifetime(record) is lifetime
            assert manager.get_capture_io_owner(record) is owner
            assert owner in manager._capture_lifetimes
        row = await output_store.repository.get(record.run_id, user_id=RAW_OWNER)
        if state in {"success", "prestart_failed", "close_failed"}:
            assert "output_evidence" in row["kwargs"]
        else:
            assert row["status"] == "pending" and "output_evidence" not in row["kwargs"]
    finally:
        try:
            await manager.shutdown(timeout=0)
        except CaptureIOCleanupError:
            assert state == "close_failed"
