"""Observe actual local execution tasks without reading another run ledger."""

import asyncio
from contextlib import suppress

import pytest

from deerflow.runtime import RunManager, RunStatus
from deerflow.runtime.runs.store.memory import MemoryRunStore


async def close_tasks(tasks):
    for task in tasks:
        task.cancel()
    for task in tasks:
        with suppress(asyncio.CancelledError):
            await task


@pytest.mark.anyio
async def test_active_task_snapshots_include_staged_outcomes_and_copy_metadata():
    manager = RunManager()
    tasks, active = [], []
    gate = asyncio.Event()
    try:
        for status in RunStatus:
            record = await manager.create("thread-" + status.value, user_id="human-reader", metadata={"instance": "river", "nested": ["retained"]})
            await manager.set_status(record.run_id, status, persist=False)
            record.task = asyncio.create_task(gate.wait())
            tasks.append(record.task)
            active.append(record)
        missing_task = await manager.create("no-task", metadata={"instance": "river"})
        done = await manager.create("done", metadata={"instance": "river"})
        done.task = asyncio.create_task(asyncio.sleep(0))
        await done.task
        missing_key = await manager.create("no-key")
        missing_key.task = asyncio.create_task(gate.wait())
        tasks.append(missing_key.task)
        # These synchronous reads cannot wait for the mutation lock or store IO.
        async with manager._lock:
            ids = manager.task_run_ids("instance", "river")
            facts = manager.task_runs("instance")
        assert ids == frozenset(record.run_id for record in active)
        assert list(facts) == [record.run_id for record in active]
        assert missing_task.run_id not in facts and done.run_id not in facts and missing_key.run_id not in facts
        for record in active:
            assert facts[record.run_id] == {"value": "river", "thread_id": record.thread_id, "user_id": "human-reader", "created_at": record.created_at, "metadata": record.metadata}
            facts[record.run_id]["metadata"]["instance"] = "edited-copy"
            assert record.metadata["instance"] == "river"
            assert facts[record.run_id]["metadata"]["nested"] is record.metadata["nested"]
        assert manager.task_run_ids("instance", "unknown") == frozenset()
    finally:
        await close_tasks(tasks)


@pytest.mark.anyio
@pytest.mark.parametrize("value", [None, "", 0, False])
async def test_metadata_key_presence_is_distinct_from_falsey_values(value):
    manager = RunManager()
    record = await manager.create("present", metadata={"instance": value})
    missing = await manager.create("absent")
    gate = asyncio.Event()
    tasks = [asyncio.create_task(gate.wait()), asyncio.create_task(gate.wait())]
    record.task, missing.task = tasks
    try:
        assert manager.task_run_ids("instance", value) == frozenset({record.run_id})
        assert list(manager.task_runs("instance")) == [record.run_id]
        assert manager.task_runs("instance")[record.run_id]["value"] == value
    finally:
        await close_tasks(tasks)


@pytest.mark.anyio
async def test_actual_peer_idempotent_reuse_has_no_local_execution_task():
    store = MemoryRunStore()
    owner = RunManager(store=store, worker_id="owner")
    peer = RunManager(store=store, worker_id="peer")
    metadata = {"instance": "river"}
    record = await owner.create_or_reject("conversation", user_id="human-reader", metadata=metadata, idempotency_key="activation-one")
    record.task = asyncio.create_task(asyncio.Event().wait())
    try:
        reused = await peer.create_or_reject("conversation", user_id="human-reader", metadata=metadata, idempotency_key="activation-one")
        assert reused.store_only and reused.idempotency_reused and reused.run_id == record.run_id
        assert peer.task_run_ids("instance", "river") == frozenset()
        assert peer.task_runs("instance") == {}
        assert owner.task_run_ids("instance", "river") == frozenset({record.run_id})
    finally:
        await close_tasks([record.task])
