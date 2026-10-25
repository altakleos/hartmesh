"""Host-only checkpoint handoff from a confirmed finalizing capture.

These manually sealed unavailable fixtures test reservation authority, not
worker selection or model provenance; the worker suite supplies that evidence.
"""

import asyncio
from dataclasses import replace
from uuid import uuid4

import pytest
from _run_output_store_test_support import RAW_OWNER, capture_run
from _run_output_store_test_support import output_store as output_store
from sqlalchemy import func, select

from deerflow.persistence.run.model import RunRow
from deerflow.runtime.runs.manager import ConflictError, RunManager
from deerflow.runtime.runs.output import UnavailableOutputReceipt
from deerflow.runtime.runs.schemas import RunStatus, ThreadOperationKind


async def _finalizing(environment):
    manager, record = await capture_run(environment)
    await manager.set_status(record.run_id, RunStatus.success, persist=False)
    receipt = UnavailableOutputReceipt(version=1, kind="unavailable", reason="unsupported_output", owner_user_id=RAW_OWNER, thread_id=record.thread_id, run_id=record.run_id, input_message_id=None)
    result = await manager.finalize_output(record.run_id, RunStatus.success, output_receipt=receipt)
    assert result.output_receipt_confirmed
    record.task = asyncio.current_task()
    return manager, record


async def _count(environment):
    async with environment.sf() as session:
        return await session.scalar(select(func.count()).select_from(RunRow))


@pytest.mark.asyncio
@pytest.mark.parametrize("invalid", ["clone", "other_task", "unconfirmed", "aborted", "lost", "store_only", "operation", "worker", "thread", "owner", "status", "not_finalizing", "no_proof"])
async def test_finalizing_checkpoint_handoff_rejects_wrong_authority_before_sql_admission(output_store, invalid):
    manager, record = await _finalizing(output_store)
    supplied = replace(record) if invalid == "clone" else record
    thread, owner = record.thread_id, RAW_OWNER
    if invalid == "unconfirmed":
        record.terminal_confirmed = False
    elif invalid == "aborted":
        record.abort_event.set()
    elif invalid == "lost":
        record.ownership_lost = True
    elif invalid == "store_only":
        record.store_only = True
    elif invalid == "operation":
        record.operation_kind = ThreadOperationKind.checkpoint_write
    elif invalid == "worker":
        record.owner_worker_id = "another-worker"
    elif invalid == "thread":
        thread = uuid4().hex
    elif invalid == "owner":
        owner = "another-owner"
    elif invalid == "status":
        record.status = RunStatus.error
    elif invalid == "not_finalizing":
        record.finalizing = False
    elif invalid == "no_proof":
        record.kwargs.pop("output_evidence")

    async def attempt():
        with pytest.raises(ConflictError):
            async with manager.reserve_thread_operation(thread, kind=ThreadOperationKind.checkpoint_write, user_id=owner, finalizing_run=supplied):
                pytest.fail("Wrong finalizer authority admitted a checkpoint writer")

    before = await _count(output_store)
    if invalid == "other_task":
        await asyncio.create_task(attempt())
    else:
        await attempt()
    assert await _count(output_store) == before == 1


@pytest.mark.asyncio
async def test_finalizer_checkpoint_handoff_keeps_original_barrier_for_replacements(output_store):
    manager, record = await _finalizing(output_store)
    async with manager.reserve_thread_operation(record.thread_id, kind=ThreadOperationKind.checkpoint_write, user_id=RAW_OWNER, finalizing_run=record):
        assert record.finalizing and await _count(output_store) == 2
        with pytest.raises(ConflictError):
            await manager.create_or_reject(record.thread_id, user_id=RAW_OWNER)
    assert record.finalizing and await _count(output_store) == 1
    with pytest.raises(ConflictError):
        await manager.create_or_reject(record.thread_id, user_id=RAW_OWNER)
    await manager.set_finalizing(record.run_id, False)
    assert (await manager.create_or_reject(record.thread_id, user_id=RAW_OWNER)).status == RunStatus.pending


@pytest.mark.asyncio
async def test_exact_finalizer_cannot_bypass_an_actual_peer_checkpoint_reservation(output_store):
    manager, record = await _finalizing(output_store)
    peer = RunManager(output_store.repository)
    async with peer.reserve_thread_operation(record.thread_id, kind=ThreadOperationKind.checkpoint_write, user_id=RAW_OWNER):
        with pytest.raises(ConflictError):
            async with manager.reserve_thread_operation(record.thread_id, kind=ThreadOperationKind.checkpoint_write, user_id=RAW_OWNER, finalizing_run=record):
                pytest.fail("Finalizer displaced an admitted peer writer")
        assert await _count(output_store) == 2
    assert await _count(output_store) == 1 and record.finalizing


@pytest.mark.asyncio
async def test_exact_finalizer_cannot_bypass_another_local_finalizing_run(output_store):
    manager, record = await _finalizing(output_store)
    other = await manager.create(record.thread_id, user_id=RAW_OWNER)
    await manager.set_status(other.run_id, RunStatus.success, persist=False)
    assert await manager.persist_current_status(other.run_id)
    assert other.finalizing
    with pytest.raises(ConflictError):
        async with manager.reserve_thread_operation(record.thread_id, kind=ThreadOperationKind.checkpoint_write, user_id=RAW_OWNER, finalizing_run=record):
            pytest.fail("Finalizer displaced another local cleanup barrier")
    assert await _count(output_store) == 2
