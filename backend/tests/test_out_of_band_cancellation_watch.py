"""A cancellation written by another process must reach the worker that owns the run.

``accounts disable`` runs inside the deployment as its own process: it has the
database and nothing else, so the only way it can end a run is the durable
cancellation request the owner applies. In a multi-worker deployment the lease
heartbeat already observes that request on its next renewal. A single-worker
deployment -- which is what a tenant runs -- has no heartbeat, so nothing read
``cancel_action`` at all and the write sat in the row until the run ended on
its own. This watch is what closes that, and it runs only where the heartbeat
does not, so the two can never both signal one run.
"""

from __future__ import annotations

import asyncio
import os

import pytest

os.environ.setdefault("AUTH_JWT_SECRET", "test-secret-key-cancel-watch-min-32-chars")

from deerflow.config.run_ownership_config import RunOwnershipConfig
from deerflow.runtime.runs.manager import RunManager, RunStatus
from deerflow.runtime.runs.store.memory import MemoryRunStore


class _DurableMemoryRunStore(MemoryRunStore):
    """The memory store with the durable-lifecycle flag a SQL deployment has."""

    durable_lifecycle = True


@pytest.fixture
def anyio_backend():
    return "asyncio"


async def _manager(*, heartbeat: bool) -> RunManager:
    manager = RunManager(
        store=_DurableMemoryRunStore(),
        run_ownership_config=RunOwnershipConfig(heartbeat_enabled=heartbeat),
    )
    manager.out_of_band_cancellation_poll_seconds = 0.05
    return manager


async def _running_run(manager: RunManager, thread_id: str = "thread-1", *, user_id: str = "user-1") -> str:
    """One run this worker owns, running, with a task that never finishes on its own."""
    record = await manager.create(thread_id, user_id=user_id)
    await manager.set_status(record.run_id, RunStatus.running)
    record.task = asyncio.create_task(asyncio.Event().wait())
    return record.run_id


async def _wait_for_abort(manager: RunManager, run_id: str, *, timeout: float = 5.0) -> bool:
    deadline = asyncio.get_running_loop().time() + timeout
    while asyncio.get_running_loop().time() < deadline:
        record = manager._runs.get(run_id)
        if record is not None and record.abort_event.is_set():
            return True
        await asyncio.sleep(0.01)
    return False


@pytest.mark.anyio
async def test_a_cancellation_written_by_another_process_reaches_the_owning_run():
    manager = await _manager(heartbeat=False)
    await manager.start_cancellation_watch()
    try:
        run_id = await _running_run(manager)
        # Exactly what the deployer's command does: one durable write, no
        # in-process call on this manager.
        await manager._store.request_cancel_compat(run_id, action="interrupt", user_id="user-1")

        assert await _wait_for_abort(manager, run_id), "the durable cancellation was never observed"
    finally:
        await manager.stop_cancellation_watch()


@pytest.mark.anyio
async def test_the_watch_does_not_run_where_the_lease_heartbeat_already_observes_cancellations():
    """Two observers of one row would race to signal the same run."""
    manager = await _manager(heartbeat=True)
    await manager.start_cancellation_watch()
    try:
        assert manager._cancellation_watch_task is None
    finally:
        await manager.stop_cancellation_watch()


@pytest.mark.anyio
async def test_the_watch_leaves_an_uncancelled_run_alone():
    manager = await _manager(heartbeat=False)
    await manager.start_cancellation_watch()
    try:
        run_id = await _running_run(manager)
        await asyncio.sleep(0.3)

        record = manager._runs.get(run_id)
        assert record is not None and not record.abort_event.is_set()
        assert record.status is RunStatus.running
    finally:
        await manager.stop_cancellation_watch()


@pytest.mark.anyio
async def test_the_watch_survives_a_store_that_fails_one_tick():
    """A dead watch would silently stop honouring every later cancellation."""
    manager = await _manager(heartbeat=False)
    await manager.start_cancellation_watch()
    try:
        run_id = await _running_run(manager)
        original = manager._store.list_inflight
        failures = {"left": 2}

        async def failing_list_inflight(**kwargs):
            if failures["left"] > 0:
                failures["left"] -= 1
                raise RuntimeError("store unavailable")
            return await original(**kwargs)

        manager._store.list_inflight = failing_list_inflight
        await manager._store.request_cancel_compat(run_id, action="interrupt", user_id="user-1")

        assert await _wait_for_abort(manager, run_id)
    finally:
        await manager.stop_cancellation_watch()


@pytest.mark.anyio
async def test_the_watch_asks_the_store_nothing_while_no_run_is_active():
    """An idle Gateway must not poll the database on a timer forever."""
    manager = await _manager(heartbeat=False)
    queries = {"count": 0}
    original = manager._store.list_inflight

    async def counting_list_inflight(**kwargs):
        queries["count"] += 1
        return await original(**kwargs)

    manager._store.list_inflight = counting_list_inflight
    await manager.start_cancellation_watch()
    try:
        await asyncio.sleep(0.3)
        assert queries["count"] == 0
    finally:
        await manager.stop_cancellation_watch()


@pytest.mark.anyio
async def test_a_store_outside_this_repository_can_still_name_an_account_s_running_runs():
    """`list_active_by_user` is concrete on the base class on purpose.

    A store implemented elsewhere would break the deployer's command if the
    method were abstract, so the default filters `list_inflight`, which every
    store has. Only the SQL override is otherwise exercised.
    """
    store = _DurableMemoryRunStore()
    await store.put("theirs", thread_id="t1", user_id="user-1", status="running", created_at="2026-01-01T00:00:00+00:00")
    await store.put("also-theirs", thread_id="t2", user_id="user-1", status="pending", created_at="2026-01-01T00:00:01+00:00")
    await store.put("finished", thread_id="t3", user_id="user-1", status="success", created_at="2026-01-01T00:00:02+00:00")
    await store.put("someone-else", thread_id="t4", user_id="user-2", status="running", created_at="2026-01-01T00:00:03+00:00")

    active = await store.list_active_by_user("user-1")

    assert sorted(row["run_id"] for row in active) == ["also-theirs", "theirs"]


@pytest.mark.anyio
async def test_stopping_a_watch_that_will_not_finish_cancels_it():
    """A stuck tick must not hold Gateway shutdown open past its timeout."""
    manager = await _manager(heartbeat=False)
    await manager.start_cancellation_watch()
    task = manager._cancellation_watch_task
    assert task is not None

    await manager.stop_cancellation_watch(timeout=0.0)

    assert manager._cancellation_watch_task is None
    assert task.done()


async def _cancelled_out_of_band(manager: RunManager) -> tuple[str, int]:
    """A running run this worker owns, cancelled by another process; its epoch before."""
    run_id = await _running_run(manager)
    held = manager._runs[run_id].state_version
    await manager._store.request_cancel_compat(run_id, action="interrupt", user_id="user-1")
    return run_id, held


@pytest.mark.anyio
async def test_a_cancelled_call_adopts_the_epoch_one_past_the_one_it_held():
    """A tool call that started before the cancellation holds an epoch the store now refuses.

    Its terminal receipt is written at the epoch the cancellation produced,
    and only that one; this is the lookup that answers it, with or without the
    lease heartbeat.
    """
    manager = await _manager(heartbeat=False)
    run_id, held = await _cancelled_out_of_band(manager)
    record = manager._runs[run_id]

    epoch = await manager.adopt_cancellation_epoch(run_id, owner_id=record.owner_worker_id, held_epoch=held)

    assert epoch == held + 1
    assert record.state_version == held + 1


@pytest.mark.anyio
async def test_adopting_the_epoch_leaves_the_cancellation_to_the_watch():
    """The adoption signals nothing, or it would switch the watch off for this run.

    Two tools in one step: the quick one's receipt adopts the epoch before the
    watch's next tick. Had that set the run's abort, the watch -- which skips a
    run already signalled -- would never cancel the task the slow one is still
    running in, and its command would run to its own timeout.
    """
    manager = await _manager(heartbeat=False)
    await manager.start_cancellation_watch()
    try:
        run_id, held = await _cancelled_out_of_band(manager)
        record = manager._runs[run_id]
        task = record.task

        assert await manager.adopt_cancellation_epoch(run_id, owner_id=record.owner_worker_id, held_epoch=held) == held + 1
        assert not record.abort_event.is_set()

        assert await _wait_for_abort(manager, run_id)
        await asyncio.sleep(0)
        assert task.cancelled() or task.cancelling(), "the watch cancelled the task the slow tool runs in"
    finally:
        await manager.stop_cancellation_watch()


@pytest.mark.anyio
async def test_a_takeover_is_not_adopted():
    manager = await _manager(heartbeat=False)
    run_id, held = await _cancelled_out_of_band(manager)
    record = manager._runs[run_id]
    owner = record.owner_worker_id
    manager._store._runs[run_id]["owner_worker_id"] = "another-worker"

    assert await manager.adopt_cancellation_epoch(run_id, owner_id=owner, held_epoch=held) is None
    assert record.state_version == held
    assert not record.abort_event.is_set()


@pytest.mark.anyio
async def test_a_run_no_longer_running_is_not_adopted():
    manager = await _manager(heartbeat=False)
    run_id, held = await _cancelled_out_of_band(manager)
    record = manager._runs[run_id]
    manager._store._runs[run_id]["status"] = "interrupted"

    assert await manager.adopt_cancellation_epoch(run_id, owner_id=record.owner_worker_id, held_epoch=held) is None
    assert record.state_version == held


@pytest.mark.anyio
async def test_no_epoch_is_adopted_for_a_run_nobody_cancelled():
    manager = await _manager(heartbeat=False)
    run_id = await _running_run(manager)
    record = manager._runs[run_id]
    held = record.state_version
    manager._store._runs[run_id]["state_version"] = held + 1  # moved, but not by a cancellation

    assert await manager.adopt_cancellation_epoch(run_id, owner_id=record.owner_worker_id, held_epoch=held) is None
    assert record.state_version == held


@pytest.mark.anyio
async def test_only_the_epoch_one_past_the_held_one_is_adopted():
    manager = await _manager(heartbeat=False)
    run_id, held = await _cancelled_out_of_band(manager)
    record = manager._runs[run_id]

    assert await manager.adopt_cancellation_epoch(run_id, owner_id=record.owner_worker_id, held_epoch=held - 1) is None
    assert await manager.adopt_cancellation_epoch(run_id, owner_id=record.owner_worker_id, held_epoch=held + 1) is None
    assert record.state_version == held
