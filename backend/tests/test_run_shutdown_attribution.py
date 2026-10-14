"""Shutdown attribution precedes cancellation and preserves staged outcomes."""

import asyncio
from contextlib import suppress

import pytest

from deerflow.runtime import RunManager, RunStatus
from deerflow.runtime.runs.manager import CancelOutcome
from deerflow.runtime.runs.store.memory import MemoryRunStore


async def close_task(task):
    if not task.done():
        task.cancel()
    with suppress(asyncio.CancelledError):
        await task


@pytest.mark.anyio
@pytest.mark.parametrize("existing_reason", [None, "storage-busy", ""])
async def test_shutdown_stamps_reason_before_workers_own_final_write(existing_reason):
    store = MemoryRunStore()
    manager = RunManager(store=store)
    record = await manager.create("conversation")
    await manager.set_status(record.run_id, RunStatus.running, stop_reason=existing_reason)
    started = asyncio.Event()
    seen = []

    async def worker():
        try:
            started.set()
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            seen.append(record.stop_reason)
            await manager.set_status(record.run_id, RunStatus.interrupted, persist=False)
            assert await manager.persist_current_status(record.run_id)
            raise

    record.task = asyncio.create_task(worker())
    try:
        await started.wait()
        await manager.shutdown(timeout=1)
        expected = "gateway_stopped" if existing_reason is None else existing_reason
        assert seen == [expected]
        row = await store.get(record.run_id)
        assert record.status == RunStatus.interrupted and row["status"] == "interrupted"
        assert record.stop_reason == row["stop_reason"] == expected
    finally:
        await close_task(record.task)


@pytest.mark.anyio
async def test_shutdown_force_persistence_carries_its_reason():
    store = MemoryRunStore()
    manager = RunManager(store=store)
    record = await manager.create("conversation")
    record.task = asyncio.create_task(asyncio.Event().wait())
    await asyncio.sleep(0)
    try:
        await manager.shutdown(timeout=1)
        row = await store.get(record.run_id)
        assert record.status == RunStatus.interrupted and row["status"] == "interrupted"
        assert row["stop_reason"] == record.stop_reason == "gateway_stopped"
    finally:
        await close_task(record.task)


@pytest.mark.anyio
@pytest.mark.parametrize("terminal", [RunStatus.success, RunStatus.error])
async def test_cancelled_cleanup_does_not_overwrite_staged_terminal_outcome(terminal):
    store = MemoryRunStore()
    manager = RunManager(store=store)
    record = await manager.create("conversation")
    await manager.set_status(record.run_id, RunStatus.running)
    started = asyncio.Event()

    async def worker():
        try:
            started.set()
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            await manager.set_status(record.run_id, terminal, persist=False)
            raise

    record.task = asyncio.create_task(worker())
    try:
        await started.wait()
        await manager.shutdown(timeout=1)
        assert record.status == terminal
        # Only the worker's confirmed finalization may publish its staged outcome.
        # Neither success nor interruption is fabricated after cancelled cleanup.
        assert (await store.get(record.run_id))["status"] == "running"
    finally:
        await close_task(record.task)


@pytest.mark.anyio
async def test_human_stop_is_not_attributed_to_gateway_shutdown():
    store = MemoryRunStore()
    manager = RunManager(store=store)
    record = await manager.create("conversation")
    record.task = asyncio.create_task(asyncio.Event().wait())
    await asyncio.sleep(0)
    try:
        assert await manager.cancel(record.run_id) == CancelOutcome.cancelled
        await manager.shutdown(timeout=1)
        assert record.stop_reason is None
        assert (await store.get(record.run_id)).get("stop_reason") is None
    finally:
        await close_task(record.task)
