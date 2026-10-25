"""Capture predecessor admission over real SQL, without fake final text."""

import asyncio
import inspect
from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest
from _run_output_store_test_support import RAW_OWNER
from _run_output_store_test_support import output_store as output_store
from sqlalchemy import delete, func, select, update

from deerflow.config.run_ownership_config import RunOwnershipConfig
from deerflow.persistence.run.model import RunRow
from deerflow.runtime.runs.manager import ConflictError, RunManager, RunStartupPhase
from deerflow.runtime.runs.output import OUTPUT_CAPTURE_DISCONNECT_KEY, OutputEvidenceError, UnavailableOutputReceipt, receipt_to_dict
from deerflow.runtime.runs.schemas import RunStatus, ThreadOperationKind
from deerflow.runtime.runs.store.base import RunIdempotencyConflict


async def admitted(environment, *, capture=True, strategy="reject", key=None):
    manager = RunManager(environment.repository)
    kwargs = {"output_capture": {"version": 1}} if capture else {}
    record = await manager.create_or_reject(uuid4().hex, user_id=RAW_OWNER, kwargs=kwargs, multitask_strategy=strategy, idempotency_key=key)
    return manager, record


@pytest.mark.asyncio
@pytest.mark.parametrize("status", ["pending", "running"])
@pytest.mark.parametrize("strategy", ["interrupt", "rollback"])
async def test_captured_local_active_predecessor_refuses_replacement_before_any_effect(output_store, status, strategy):
    manager, record = await admitted(output_store)
    if status == "running":
        await manager.try_start(record.run_id)
    before = await output_store.repository.get(record.run_id, user_id=RAW_OWNER)
    with pytest.raises(ConflictError):
        await manager.create_or_reject(record.thread_id, user_id=RAW_OWNER, multitask_strategy=strategy)
    after = await output_store.repository.get(record.run_id, user_id=RAW_OWNER)
    assert after == before and not record.abort_event.is_set()
    assert len(await output_store.repository.list_by_thread(record.thread_id, user_id=RAW_OWNER)) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("strategy", ["interrupt", "rollback"])
async def test_captured_locally_finalizing_sealed_predecessor_cannot_be_replaced(output_store, strategy):
    manager, record = await admitted(output_store)
    # A real public unavailable seal exercises admission authority only. There
    # is no invented AI identity, model event or text-source claim here.
    receipt = UnavailableOutputReceipt(version=1, kind="unavailable", owner_user_id=RAW_OWNER, thread_id=record.thread_id, run_id=record.run_id, input_message_id=None, reason="failed")
    assert (await manager.finalize_output(record.run_id, RunStatus.error, output_receipt=receipt)).output_receipt_confirmed
    await manager.set_finalizing(record.run_id, True)
    before = await output_store.repository.get(record.run_id, user_id=RAW_OWNER)
    try:
        with pytest.raises(ConflictError):
            await manager.create_or_reject(record.thread_id, user_id=RAW_OWNER, multitask_strategy=strategy)
        assert await output_store.repository.get(record.run_id, user_id=RAW_OWNER) == before
        assert len(await output_store.repository.list_by_thread(record.thread_id, user_id=RAW_OWNER)) == 1
    finally:
        await manager.set_finalizing(record.run_id, False)


@pytest.mark.asyncio
@pytest.mark.parametrize("strategy", ["interrupt", "rollback"])
async def test_peer_exact_idempotent_capture_reuse_precedes_predecessor_refusal(output_store, strategy):
    key = "capture-peer-" + uuid4().hex
    manager, record = await admitted(output_store, strategy=strategy, key=key)
    before = await output_store.repository.get(record.run_id, user_id=RAW_OWNER)
    peer = RunManager(output_store.repository)
    reused = await peer.create_or_reject(record.thread_id, user_id=RAW_OWNER, kwargs={"output_capture": {"version": 1}}, multitask_strategy=strategy, idempotency_key=key)
    assert reused.run_id == record.run_id and reused.idempotency_reused and reused.store_only
    assert await output_store.repository.get(record.run_id, user_id=RAW_OWNER) == before
    assert not record.abort_event.is_set()


@pytest.mark.asyncio
@pytest.mark.parametrize("strategy", ["interrupt", "rollback"])
async def test_ordinary_local_predecessor_keeps_existing_replacement_behavior(output_store, strategy):
    manager, record = await admitted(output_store, capture=False)
    replacement = await manager.create_or_reject(record.thread_id, user_id=RAW_OWNER, multitask_strategy=strategy)
    assert replacement.run_id != record.run_id and record.abort_event.is_set()
    assert (await output_store.repository.get(record.run_id, user_id=RAW_OWNER))["status"] == "interrupted"
    assert (await output_store.repository.get(replacement.run_id, user_id=RAW_OWNER))["status"] == "pending"


@pytest.mark.asyncio
async def test_authorized_confirmed_finalizer_keeps_its_checkpoint_self_handoff(output_store):
    manager, record = await admitted(output_store)
    receipt = UnavailableOutputReceipt(version=1, kind="unavailable", owner_user_id=RAW_OWNER, thread_id=record.thread_id, run_id=record.run_id, input_message_id=None, reason="unsupported_input")
    assert (await manager.finalize_output(record.run_id, RunStatus.success, output_receipt=receipt)).output_receipt_confirmed
    record.task = asyncio.current_task()
    await manager.set_finalizing(record.run_id, True)
    try:
        async with manager.reserve_thread_operation(record.thread_id, kind=ThreadOperationKind.checkpoint_write, user_id=RAW_OWNER, finalizing_run=record):
            # Public run history deliberately excludes non-run reservations.
            async with output_store.sf() as session:
                count = await session.scalar(select(func.count()).select_from(RunRow).where(RunRow.thread_id == record.thread_id, RunRow.operation_kind == "checkpoint_write"))
            assert count == 1
            assert record.finalizing and record.terminal_confirmed
    finally:
        record.task = None
        await manager.set_finalizing(record.run_id, False)


@pytest.mark.asyncio
@pytest.mark.parametrize("reuse", [False, True])
async def test_cancelled_capture_admission_retains_committed_identity_before_cleanup(output_store, monkeypatch, reuse):
    from deerflow.runtime.runs.output import receipt_from_kwargs

    manager = RunManager(output_store.repository)
    thread, key = uuid4().hex, uuid4().hex
    kwargs = {"output_capture": {"version": 1}}
    existing = None
    if reuse:
        first = RunManager(output_store.repository)
        existing = await first.create_or_reject(thread, user_id=RAW_OWNER, kwargs=kwargs, idempotency_key=key)
    original = output_store.repository.create_thread_operation_atomic
    entered, release = asyncio.Event(), asyncio.Event()
    identities = []

    async def committed_then_hold(*args, **values):
        try:
            result = await original(*args, **values)
        except RunIdempotencyConflict as conflict:
            identities.append(conflict.existing["run_id"])
            entered.set()
            await release.wait()
            raise
        identities.append(result[0]["run_id"])
        entered.set()
        await release.wait()
        return result

    monkeypatch.setattr(output_store.repository, "create_thread_operation_atomic", committed_then_hold)
    operation = asyncio.create_task(manager.create_or_reject(thread, user_id=RAW_OWNER, kwargs=kwargs, idempotency_key=key))
    try:
        await asyncio.wait_for(entered.wait(), 5)
        operation.cancel()
        operation.cancel()
        done, _ = await asyncio.wait({operation}, timeout=0.05)
        assert not done, "Admission must retain its committed identity before propagating cancellation"
        release.set()
        with pytest.raises(asyncio.CancelledError):
            await operation
        assert len(identities) == 1
        row = await output_store.repository.get(identities[0], user_id=RAW_OWNER)
        if reuse:
            assert row["run_id"] == existing.run_id and row["status"] == "pending"
            assert "output_evidence" not in row["kwargs"] and not existing.abort_event.is_set()
        else:
            seal = receipt_from_kwargs(row["kwargs"])
            assert row["status"] == "interrupted" and seal is not None and seal.reason == "cancelled"
            local = await manager.get(row["run_id"], user_id=RAW_OWNER)
            assert not local.store_only and local.task is None
    finally:
        release.set()
        await asyncio.gather(operation, return_exceptions=True)


@pytest.mark.asyncio
async def test_malformed_capture_does_not_abandon_an_unawaited_admission(output_store, monkeypatch):
    manager = RunManager(output_store.repository)
    original = manager._admit_thread_operation
    coroutines = []

    def observed(*args, **kwargs):
        coroutine = original(*args, **kwargs)
        coroutines.append(coroutine)
        return coroutine

    monkeypatch.setattr(manager, "_admit_thread_operation", observed)
    try:
        with pytest.raises(OutputEvidenceError):
            await manager.create_or_reject(uuid4().hex, user_id=RAW_OWNER, kwargs={"output_capture": {"version": 3}})
        assert all(inspect.getcoroutinestate(coroutine) == inspect.CORO_CLOSED for coroutine in coroutines)
    finally:
        for coroutine in coroutines:
            coroutine.close()


@pytest.mark.asyncio
async def test_same_key_retry_cannot_change_original_cancelled_admission_provenance(output_store, monkeypatch):
    from deerflow.runtime.runs.output import receipt_from_kwargs

    manager, predecessor = await admitted(output_store, capture=False)
    original = manager._persist_status
    entered, release = asyncio.Event(), asyncio.Event()
    key = uuid4().hex

    async def persist_then_hold(record, *args, **kwargs):
        result = await original(record, *args, **kwargs)
        if record is predecessor:
            entered.set()
            await release.wait()
        return result

    monkeypatch.setattr(manager, "_persist_status", persist_then_hold)
    call = dict(user_id=RAW_OWNER, kwargs={"output_capture": {"version": 1}}, multitask_strategy="interrupt", idempotency_key=key)
    creator = asyncio.create_task(manager.create_or_reject(predecessor.thread_id, **call))
    try:
        # This is the real post-registration predecessor-persistence await;
        # the manager registry lock is free while the original is still owned.
        await asyncio.wait_for(entered.wait(), 5)
        creator.cancel()
        reused = await manager.create_or_reject(predecessor.thread_id, **call)
        assert reused.idempotency_reused and reused.task is None
        release.set()
        with pytest.raises(asyncio.CancelledError):
            await creator
        row = await output_store.repository.get(reused.run_id, user_id=RAW_OWNER)
        seal = receipt_from_kwargs(row["kwargs"])
        assert row["status"] == "interrupted" and seal is not None and seal.reason == "cancelled", "Retry's shared sticky flag cannot turn the original new registration into a reused invocation"
    finally:
        release.set()
        await asyncio.gather(creator, return_exceptions=True)


@pytest.mark.asyncio
@pytest.mark.parametrize("with_key", [False, True])
@pytest.mark.parametrize("heartbeat", [False, True])
async def test_lost_capture_admission_commit_response_retains_original_identity(output_store, monkeypatch, with_key, heartbeat):
    manager = RunManager(output_store.repository, run_ownership_config=RunOwnershipConfig(heartbeat_enabled=heartbeat))
    original = output_store.repository.create_thread_operation_atomic
    identities = []

    async def committed_response_lost(*args, **kwargs):
        result = await original(*args, **kwargs)
        identities.append(result[0]["run_id"])
        raise OSError("Synthetic lost admission commit response")

    monkeypatch.setattr(output_store.repository, "create_thread_operation_atomic", committed_response_lost)
    with pytest.raises(OSError):
        await manager.create_or_reject(uuid4().hex, user_id=RAW_OWNER, kwargs={"output_capture": {"version": 1}}, idempotency_key=uuid4().hex if with_key else None)
    assert len(identities) == 1
    stored = await output_store.repository.get(identities[0], user_id=RAW_OWNER)
    local = await manager.get(identities[0], user_id=RAW_OWNER)
    assert stored is not None and local is not None
    assert not local.store_only, "A lost committed result cannot discard the locally constructed admission identity"
    assert local.startup_phase is RunStartupPhase.closed and local.task is None
    assert stored["status"] == "error" and stored["error"] == "Run admission failed before execution."
    assert stored["kwargs"]["output_evidence"] == dict(version=1, kind="unavailable", owner_user_id=RAW_OWNER, thread_id=local.thread_id, run_id=local.run_id, input_message_id=None, reason="failed")
    assert local.prestart_outcome.confirmed and local.terminal_confirmed and not local.ownership_lost


@pytest.mark.asyncio
@pytest.mark.parametrize("observation", ["missing", "read_error", "owner", "thread", "worker", "key", "capture", "disconnect", "lease", "terminal_unsealed", "terminal_sealed"])
async def test_lost_result_refuses_uncertain_or_foreign_row_without_upsert(output_store, monkeypatch, observation):
    manager = RunManager(output_store.repository, run_ownership_config=RunOwnershipConfig(heartbeat_enabled=observation == "lease"))
    original_create = output_store.repository.create_thread_operation_atomic
    original_get = output_store.repository.get
    original_finalize = output_store.repository.finalize_owned
    observed, writes = [], []

    async def reject_new_write(*args, **kwargs):
        writes.append(True)
        raise AssertionError("Uncertain/foreign result cannot authorize a closure/upsert")

    async def failed_read(*args, **kwargs):
        raise OSError("Synthetic followup read failure")

    async def committed_then_changed(*args, **kwargs):
        result = await original_create(*args, **kwargs)
        row = result[0]
        values = {}
        if observation in {"owner", "thread", "worker", "key"}:
            column = {"owner": "user_id", "thread": "thread_id", "worker": "owner_worker_id", "key": "idempotency_key"}[observation]
            values[column] = "another-" + uuid4().hex
        elif observation in {"capture", "disconnect"}:
            changed = dict(row["kwargs"])
            changed["output_capture" if observation == "capture" else OUTPUT_CAPTURE_DISCONNECT_KEY] = {"version": 2} if observation == "capture" else "continue"
            values["kwargs_json"] = changed
        elif observation == "terminal_unsealed":
            values["status"] = "error"
        elif observation == "lease":
            values["lease_expires_at"] = datetime.now(UTC) - timedelta(minutes=1)
        if observation == "missing" or values:
            async with output_store.sf() as session:
                statement = delete(RunRow).where(RunRow.run_id == row["run_id"]) if observation == "missing" else update(RunRow).where(RunRow.run_id == row["run_id"]).values(**values)
                await session.execute(statement)
                await session.commit()
        elif observation == "terminal_sealed":
            seal = UnavailableOutputReceipt(version=1, kind="unavailable", owner_user_id=RAW_OWNER, thread_id=row["thread_id"], run_id=row["run_id"], input_message_id=None, reason="failed")
            assert await original_finalize(
                row["run_id"], owner_worker_id=manager.worker_id, expected_change_seq=row["change_seq"], expected_status="pending", expected_cancel_action=None, require_lease=False, status="error", output_receipt=seal
            )
            assert (await original_get(row["run_id"], user_id=None))["kwargs"]["output_evidence"] == receipt_to_dict(seal)
        observed.append((row["run_id"], await original_get(row["run_id"], user_id=None)))
        if observation == "read_error":
            monkeypatch.setattr(output_store.repository, "get", failed_read)
        raise OSError("Synthetic lost admission result")

    monkeypatch.setattr(output_store.repository, "create_thread_operation_atomic", committed_then_changed)
    monkeypatch.setattr(output_store.repository, "finalize_owned", reject_new_write)
    monkeypatch.setattr(output_store.repository, "put", reject_new_write)
    with pytest.raises(OSError, match="Synthetic lost admission result"):
        await manager.create_or_reject(uuid4().hex, user_id=RAW_OWNER, kwargs={"output_capture": {"version": 1}}, idempotency_key=uuid4().hex)
    assert len(observed) == 1 and writes == []
    identity, before = observed[0]
    assert await original_get(identity, user_id=None) == before
    assert identity not in manager._runs, "Recovery cannot fabricate local custody from uncertainty or foreign/terminal rows"
