"""Original-record lifecycle and v1 custom-store ABI for inventory capture."""

import asyncio
from contextlib import asynccontextmanager
from uuid import uuid4

import pytest
from _run_output_store_test_support import RAW_OWNER
from _run_output_store_test_support import output_store as output_store

from deerflow.persistence.run.sql import RunRepository
from deerflow.runtime.runs.manager import RunManager
from deerflow.runtime.runs.output import UnavailableOutputReceipt
from deerflow.runtime.runs.schemas import RunStatus


async def admitted(e, *, start=True):
    manager = RunManager(e.repository)
    record = await manager.create_or_reject(uuid4().hex, user_id=RAW_OWNER, kwargs={"output_capture": {"version": 2}, "input": {"messages": [{"type": "human", "id": "genuine-human", "content": "original"}]}})
    if start:
        await manager.try_start(record.run_id)
    row = await e.repository.get(record.run_id, user_id=RAW_OWNER)
    return manager, record, row


@pytest.mark.asyncio
@pytest.mark.parametrize("creation", ["create", "create_or_reject"])
async def test_new_admission_discards_caller_inventory_evidence(output_store, creation):
    manager = RunManager(output_store.repository)
    supplied = {"output_capture": {"version": 1}, "inventory_evidence": {"kind": "unavailable", "forged": "caller"}}
    record = await getattr(manager, creation)(uuid4().hex, user_id=RAW_OWNER, kwargs=supplied)
    row = await output_store.repository.get(record.run_id, user_id=RAW_OWNER)
    assert "inventory_evidence" not in row["kwargs"]
    assert "inventory_evidence" not in record.kwargs
    assert supplied["inventory_evidence"] == {"kind": "unavailable", "forged": "caller"}


class LegacyReceiptRepository(RunRepository):
    """Actual SQL store with the pre-inventory explicit keyword signature."""

    async def finalize_owned(self, run_id, *, owner_worker_id, expected_change_seq, expected_status, expected_cancel_action, require_lease, status, error=None, stop_reason=None, output_receipt=None):
        return await super().finalize_owned(
            run_id,
            owner_worker_id=owner_worker_id,
            expected_change_seq=expected_change_seq,
            expected_status=expected_status,
            expected_cancel_action=expected_cancel_action,
            require_lease=require_lease,
            status=status,
            error=error,
            stop_reason=stop_reason,
            output_receipt=output_receipt,
        )


@pytest.mark.asyncio
async def test_v1_custom_store_receives_no_inventory_keyword(output_store):
    manager = RunManager(LegacyReceiptRepository(output_store.sf))
    record = await manager.create_or_reject(uuid4().hex, user_id=RAW_OWNER, kwargs={"output_capture": {"version": 1}})
    await manager.try_start(record.run_id)
    receipt = UnavailableOutputReceipt(version=1, kind="unavailable", owner_user_id=RAW_OWNER, thread_id=record.thread_id, run_id=record.run_id, input_message_id=None, reason="failed")
    result = await manager.finalize_output(record.run_id, RunStatus.error, output_receipt=receipt)
    assert result.finalized and result.output_receipt_confirmed
    assert getattr(result, "inventory_evidence_confirmed", False) is False


@pytest.mark.asyncio
async def test_unsupported_memory_refuses_capture2_without_changing_ordinary_admission():
    from deerflow.runtime.runs.output import OutputEvidenceError
    from deerflow.runtime.runs.store.memory import MemoryRunStore

    store = MemoryRunStore()
    manager = RunManager(store)
    thread = uuid4().hex
    with pytest.raises(OutputEvidenceError):
        await manager.create_or_reject(thread, user_id=RAW_OWNER, kwargs={"output_capture": {"version": 2}})
    assert await store.list_by_thread(thread, user_id=RAW_OWNER) == []
    record = await manager.create_or_reject(thread, user_id=RAW_OWNER)
    assert (await store.get(record.run_id, user_id=RAW_OWNER))["status"] == "pending"


@pytest.mark.asyncio
async def test_host_attempt_confirms_exact_pair_without_callback_and_detaches_snapshot(output_store):
    from test_output_inventory_persistence import event_evidence, output_receipt

    manager, record, row = await admitted(output_store)
    output = output_receipt(row)
    inventory = event_evidence(output)
    attempt = await manager.begin_inventory_finalization(record, RunStatus.success, output_receipt=output, inventory_evidence=inventory)

    async def forbidden(_decision):
        pytest.fail("An exact success must not run cancellation cleanup")

    result = await manager.finalize_inventory(attempt, on_cancel=forbidden)
    assert result.finalized and result.output_receipt_confirmed and result.inventory_evidence_confirmed
    assert await manager.finalize_inventory(attempt, on_cancel=forbidden) is result
    snapshot = await manager.get_output_snapshot(record.run_id, user_id=RAW_OWNER, require_inventory=True)
    assert snapshot.status is RunStatus.success
    record.kwargs["inventory_evidence"]["inventory_sha256"] = "0" * 64
    assert snapshot.kwargs["inventory_evidence"]["inventory_sha256"] == "f" * 64


@pytest.mark.asyncio
@pytest.mark.parametrize("closure", ["failed", "cancelled"])
async def test_original_unstarted_closure_confirms_pair_without_source_io(output_store, closure):
    manager, record, _row = await admitted(output_store, start=False)
    if closure == "failed":
        assert await manager.fail_start_if_pending(record.run_id, error="Actual attachment failure")
    else:
        await manager.cancel(record.run_id)
    assert record.prestart_outcome.confirmed
    snapshot = await manager.get_output_snapshot(record.run_id, user_id=RAW_OWNER, require_inventory=True)
    assert snapshot.kwargs["output_evidence"]["reason"] == closure
    assert snapshot.kwargs["inventory_evidence"]["reason"] == "prestart_not_enumerated"
    assert snapshot.kwargs["inventory_evidence"]["input_message_id"] is None


@pytest.mark.asyncio
@pytest.mark.parametrize("action", ["interrupt", "rollback"])
@pytest.mark.parametrize("candidate", ["text", "failed", "cancelled"])
async def test_positive_active_decision_then_actual_cleanup_seals_fourth_pair(output_store, monkeypatch, action, candidate):
    from test_output_inventory_persistence import event_evidence, output_receipt

    from deerflow.runtime.runs.inventory_finalization import InventoryCleanupOutcome
    from deerflow.runtime.runs.output_inventory import output_receipt_digest

    manager, record, row = await admitted(output_store)
    output = output_receipt(row, reason=None if candidate == "text" else candidate)
    status = RunStatus.success if candidate == "text" else RunStatus.error if candidate == "failed" else RunStatus.interrupted
    inventory = event_evidence(output)
    attempt = await manager.begin_inventory_finalization(record, status, output_receipt=output, inventory_evidence=inventory)
    entered, release = asyncio.Event(), asyncio.Event()
    original = output_store.repository.finalize_owned
    calls, cleanup_calls = [], []

    async def held(*args, **kwargs):
        calls.append(kwargs)
        if len(calls) == 1:
            entered.set()
            await release.wait()
        return await original(*args, **kwargs)

    async def cleanup(decision):
        cleanup_calls.append((decision.action, asyncio.current_task()))
        await manager.set_status(record.run_id, RunStatus.interrupted if action == "interrupt" else RunStatus.error, persist=False)
        receipt = output_receipt(row, reason="cancelled")
        return InventoryCleanupOutcome.interrupt_completed(receipt) if action == "interrupt" else InventoryCleanupOutcome.rollback_completed(receipt)

    monkeypatch.setattr(output_store.repository, "finalize_owned", held)
    task = asyncio.create_task(manager.finalize_inventory(attempt, on_cancel=cleanup))
    try:
        await asyncio.wait_for(entered.wait(), 5)
        assert await output_store.repository.request_cancel(record.run_id, action=action) == action
        release.set()
        result = await asyncio.wait_for(task, 5)
        assert result.finalized and result.output_receipt_confirmed and result.inventory_evidence_confirmed
        assert cleanup_calls == [(action, task)]
        saved = await output_store.repository.get(record.run_id, user_id=RAW_OWNER)
        assert saved["kwargs"]["inventory_evidence"]["reason"] == "cancellation_after_inventory_not_enumerated"
        assert saved["kwargs"]["inventory_evidence"]["input_message_id"] == "genuine-human"
        assert saved["kwargs"]["inventory_evidence"]["output_receipt_sha256"] == output_receipt_digest(output_receipt(row, reason="cancelled"))
        assert calls[-1]["expected_cancel_action"] == action
        assert await manager.finalize_inventory(attempt, on_cancel=cleanup) is result
        assert len(cleanup_calls) == 1
        if candidate == "cancelled":
            assert output_receipt_digest(output) == saved["kwargs"]["inventory_evidence"]["output_receipt_sha256"]
            assert "inventory_event_seq" not in saved["kwargs"]["inventory_evidence"]
    finally:
        release.set()
        await asyncio.gather(task, return_exceptions=True)


@pytest.mark.asyncio
@pytest.mark.parametrize("fault", ["unconfirmed", "raised", "escalated", "version", "wrong_status", "input_changed"])
async def test_cleanup_uncertainty_and_changed_action_cannot_mint_fourth_proof(output_store, fault):
    from sqlalchemy import update
    from test_output_inventory_persistence import event_evidence, output_receipt

    from deerflow.persistence.run.model import RunRow
    from deerflow.runtime.runs.inventory_finalization import InventoryCleanupOutcome
    from deerflow.runtime.runs.output import OutputEvidenceError

    manager, record, row = await admitted(output_store)
    output = output_receipt(row)
    attempt = await manager.begin_inventory_finalization(record, RunStatus.success, output_receipt=output, inventory_evidence=event_evidence(output))
    await output_store.repository.request_cancel(record.run_id, action="interrupt")

    async def cleanup(_decision):
        if fault == "unconfirmed":
            return InventoryCleanupOutcome.unconfirmed()
        if fault == "raised":
            raise RuntimeError("private-cleanup-failure")
        await manager.set_status(record.run_id, RunStatus.error if fault == "wrong_status" else RunStatus.interrupted, persist=False)
        if fault == "escalated":
            async with output_store.sf.begin() as session:
                await session.execute(update(RunRow).where(RunRow.run_id == record.run_id).values(cancel_action="rollback", change_seq=RunRow.change_seq + 1))
        if fault == "version":
            async with output_store.sf.begin() as session:
                await session.execute(update(RunRow).where(RunRow.run_id == record.run_id).values(change_seq=RunRow.change_seq + 1))
        return InventoryCleanupOutcome.interrupt_completed(output_receipt(row, reason="cancelled", input_id="other-human" if fault == "input_changed" else "genuine-human"))

    try:
        result = await manager.finalize_inventory(attempt, on_cancel=cleanup)
        assert not result.inventory_evidence_confirmed
    except (OutputEvidenceError, RuntimeError):
        pass
    saved = await output_store.repository.get(record.run_id, user_id=RAW_OWNER)
    assert saved["status"] == "running"
    assert "output_evidence" not in saved["kwargs"] and "inventory_evidence" not in saved["kwargs"]


class LostPairCommit:
    def __init__(self, repository, *, desired_status=None):
        self.original = repository._sf
        self.writes = 0
        self.lost = False
        self.desired_status = desired_status
        repository._sf = self.session

    @asynccontextmanager
    async def session(self):
        async with self.original() as session:
            execute, commit = session.execute, session.commit
            terminal = False

            async def tracked(statement, *args, **kwargs):
                nonlocal terminal
                values = getattr(statement, "_values", None) or {}
                statuses = [getattr(value, "value", value) for key, value in values.items() if getattr(key, "name", key) == "status"]
                if getattr(statement, "is_update", False) and statuses and (self.desired_status is None or self.desired_status in statuses):
                    terminal = True
                    self.writes += 1
                return await execute(statement, *args, **kwargs)

            async def lost():
                await commit()
                if terminal and not self.lost:
                    self.lost = True
                    raise ConnectionError("private-commit-response")

            session.execute, session.commit = tracked, lost
            yield session


@pytest.mark.asyncio
async def test_actual_committed_pair_lost_response_confirms_both_without_cleanup(output_store):
    from test_output_inventory_persistence import event_evidence, output_receipt

    manager, record, row = await admitted(output_store)
    output = output_receipt(row)
    attempt = await manager.begin_inventory_finalization(record, RunStatus.success, output_receipt=output, inventory_evidence=event_evidence(output))
    fault = LostPairCommit(output_store.repository)

    async def forbidden(_decision):
        pytest.fail("Already committed exact pair is not an active cancellation")

    result = await manager.finalize_inventory(attempt, on_cancel=forbidden)
    assert result.output_receipt_confirmed and result.inventory_evidence_confirmed
    assert fault.writes == 1 and fault.lost


@pytest.mark.asyncio
async def test_unknown_cancelled_pair_commit_confirms_without_repeated_cleanup(output_store):
    from test_output_inventory_persistence import event_evidence, output_receipt

    from deerflow.runtime.runs.inventory_finalization import InventoryCleanupOutcome

    manager, record, row = await admitted(output_store)
    output = output_receipt(row)
    attempt = await manager.begin_inventory_finalization(record, RunStatus.success, output_receipt=output, inventory_evidence=event_evidence(output))
    await output_store.repository.request_cancel(record.run_id, action="interrupt")
    fault = LostPairCommit(output_store.repository, desired_status="interrupted")
    calls = []

    async def cleanup(decision):
        calls.append(decision.action)
        await manager.set_status(record.run_id, RunStatus.interrupted, persist=False)
        return InventoryCleanupOutcome.interrupt_completed(output_receipt(row, reason="cancelled"))

    result = await manager.finalize_inventory(attempt, on_cancel=cleanup)
    assert result.output_receipt_confirmed and result.inventory_evidence_confirmed
    assert calls == ["interrupt"] and fault.lost and fault.writes == 1
    assert await manager.finalize_inventory(attempt, on_cancel=cleanup) is result
    assert calls == ["interrupt"]


@pytest.mark.asyncio
async def test_initial_completed_cleanup_uses_its_post_cleanup_candidate_without_reexecution(output_store):
    from test_output_inventory_persistence import event_evidence, output_receipt

    from deerflow.runtime.runs.inventory_finalization import InventoryCleanupOutcome

    manager, record, row = await admitted(output_store)
    await manager.cancel(record.run_id)
    await manager.set_status(record.run_id, RunStatus.interrupted, persist=False)
    output = output_receipt(row, reason="cancelled")
    cleanup = InventoryCleanupOutcome.interrupt_completed(output)
    inventory = event_evidence(output)  # The candidate is constructed AFTER cleanup.
    attempt = await manager.begin_inventory_finalization(record, RunStatus.interrupted, output_receipt=output, inventory_evidence=inventory, cleanup_outcome=cleanup)

    async def forbidden(_decision):
        pytest.fail("Already completed exact-action cleanup must not repeat")

    result = await manager.finalize_inventory(attempt, on_cancel=forbidden)
    assert result.output_receipt_confirmed and result.inventory_evidence_confirmed
    assert record.kwargs["inventory_evidence"]["kind"] == "event"


@pytest.mark.asyncio
async def test_original_attempt_is_not_transferable_or_serialized_and_scope_copy_cannot_revive(output_store):
    from contextvars import copy_context
    from dataclasses import fields

    from test_output_inventory_persistence import event_evidence, output_receipt

    from deerflow.runtime.runs.inventory_finalization import _current_inventory_authority, _inventory_authority
    from deerflow.runtime.runs.output import OutputEvidenceError

    manager, record, row = await admitted(output_store)
    output = output_receipt(row)
    attempt = await manager.begin_inventory_finalization(record, RunStatus.success, output_receipt=output, inventory_evidence=event_evidence(output))
    peer = RunManager(output_store.repository)
    with pytest.raises(OutputEvidenceError):
        await peer.finalize_inventory(attempt, on_cancel=lambda _decision: None)
    with _inventory_authority(attempt):
        copied = copy_context()
        assert copied.run(_current_inventory_authority) is attempt
    assert copied.run(_current_inventory_authority) is None
    assert all("inventory" not in field.name for field in fields(record))
    assert all(value is not attempt for value in vars(record).values())
    assert "inventory_evidence" not in row["kwargs"]


@pytest.mark.asyncio
@pytest.mark.parametrize("half", ["output", "inventory", "terminal_empty"])
async def test_strict_snapshot_does_not_report_half_or_terminal_unsealed_as_pending(output_store, half):
    from sqlalchemy import update
    from test_output_inventory_persistence import event_evidence, output_receipt

    from deerflow.persistence.run.model import RunRow
    from deerflow.runtime.runs.output import OutputEvidenceError, receipt_to_dict
    from deerflow.runtime.runs.output_inventory import inventory_evidence_to_dict

    manager, record, row = await admitted(output_store)
    output = output_receipt(row)
    kwargs = {**row["kwargs"]}
    if half == "output":
        kwargs["output_evidence"] = receipt_to_dict(output)
    elif half == "inventory":
        kwargs["inventory_evidence"] = inventory_evidence_to_dict(event_evidence(output))
    async with output_store.sf.begin() as session:
        await session.execute(update(RunRow).where(RunRow.run_id == record.run_id).values(kwargs_json=kwargs, status="success" if half == "terminal_empty" else "running"))
    with pytest.raises(OutputEvidenceError):
        await manager.get_output_snapshot(record.run_id, user_id=RAW_OWNER, require_inventory=True)


@pytest.mark.asyncio
async def test_fixed_host_refusal_does_not_chain_handled_private_error():
    import traceback

    from deerflow.runtime.runs.output import OutputEvidenceError

    manager = RunManager()
    try:
        raise ValueError("private-host-continuation-marker")
    except ValueError:
        with pytest.raises(OutputEvidenceError) as caught:
            await manager.finalize_inventory(object(), on_cancel=lambda _decision: None)
    assert "private-host-continuation-marker" not in "".join(traceback.format_exception(caught.value))


@pytest.mark.asyncio
async def test_ordinary_terminal_recovery_preserves_historical_null_kwargs(output_store):
    from sqlalchemy import update

    from deerflow.persistence.run.model import RunRow

    manager = RunManager(output_store.repository)
    record = await manager.create_or_reject(uuid4().hex, user_id=RAW_OWNER)
    await manager.try_start(record.run_id)
    async with output_store.sf.begin() as session:
        await session.execute(update(RunRow).where(RunRow.run_id == record.run_id).values(kwargs_json=None))
    await manager.set_status_if_not_cancelled(record.run_id, RunStatus.success)
    row = await output_store.repository.get(record.run_id, user_id=RAW_OWNER)
    assert row["status"] == "success" and row["kwargs"] is None
