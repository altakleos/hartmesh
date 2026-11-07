"""An ordinary Goal finalizer cannot turn staged success into checkpoint authority."""

import asyncio

import pytest

from deerflow.runtime.runs.manager import ConflictError, RunManager
from deerflow.runtime.runs.schemas import RunStatus, ThreadOperationKind
from deerflow.runtime.runs.store.memory import MemoryRunStore


@pytest.mark.asyncio
@pytest.mark.parametrize("store_backed", [False, True])
async def test_ordinary_finalizer_requires_current_terminal_ack(store_backed):
    manager = RunManager(store=MemoryRunStore() if store_backed else None)
    record = await manager.create_or_reject("goal-thread", "lead_agent", user_id="owner")
    record.task = asyncio.current_task()
    await manager.set_finalizing(record.run_id, True)

    async def reserve():
        async with manager.reserve_thread_operation(record.thread_id, kind=ThreadOperationKind.checkpoint_write, user_id="owner", finalizing_run=record):
            assert record.finalizing

    try:
        await manager.set_status(record.run_id, RunStatus.success, persist=False)
        with pytest.raises(ConflictError):
            await reserve()
        assert await manager.persist_current_status(record.run_id)
        await reserve()
        # A later outcome is not covered by the earlier acknowledgement.
        record.stop_reason = "unconfirmed-change"
        with pytest.raises(ConflictError):
            await reserve()
        record.stop_reason = None
        other = asyncio.create_task(reserve())
        with pytest.raises(ConflictError):
            await other
    finally:
        record.task = None
        await manager.set_finalizing(record.run_id, False)
        await manager.shutdown()
