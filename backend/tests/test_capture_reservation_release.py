"""An exact preauthorized checkpoint reservation release never deletes a peer."""

from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest
from _run_output_store_test_support import RAW_OWNER
from _run_output_store_test_support import output_store as output_store
from sqlalchemy import update

from deerflow.persistence.run.model import RunRow


async def reservation(db):
    run_id, thread_id, worker_id = str(uuid4()), str(uuid4()), "original-worker"
    await db.repository.create_thread_operation_atomic(
        run_id=run_id, thread_id=thread_id, owner_worker_id=worker_id, lease_expires_at=(datetime.now(UTC) + timedelta(minutes=2)).isoformat(), operation_kind="checkpoint_write", user_id=RAW_OWNER, grace_seconds=10
    )
    row = await db.repository.get(run_id, user_id=RAW_OWNER)
    return row, {"user_id": RAW_OWNER, "thread_id": thread_id, "owner_worker_id": worker_id, "expected_change_seq": row["change_seq"]}


@pytest.mark.asyncio
async def test_exact_reservation_release_API_scaffold(output_store):
    assert callable(getattr(output_store.repository, "release_owned_checkpoint_reservation", None)), "New exact release API is absent; API-availability scaffold only"


@pytest.mark.asyncio
@pytest.mark.parametrize("change", ["unchanged", "heartbeat", "peer", "version", "cancel", "owner", "thread", "kind", "terminal", "missing"])
async def test_owned_reservation_release_uses_exact_current_SQL_scope(output_store, change):
    db = output_store
    row, scope = await reservation(db)
    if change == "heartbeat":
        renewal = await db.repository.renew_lease(row["run_id"], owner_worker_id=scope["owner_worker_id"], lease_expires_at=(datetime.now(UTC) + timedelta(minutes=4)).isoformat())
        assert renewal.renewed
    elif change == "missing":
        await db.repository.delete_thread_operation(row["run_id"], user_id=RAW_OWNER)
    elif change != "unchanged":
        values = {
            "peer": {"owner_worker_id": "peer-worker"},
            "version": {"change_seq": row["change_seq"] + 100},
            "cancel": {"cancel_action": "interrupt", "change_seq": row["change_seq"] + 100},
            "owner": {"user_id": "other/raw-owner"},
            "thread": {"thread_id": str(uuid4())},
            "kind": {"operation_kind": "artifact_write"},
            "terminal": {"status": "error"},
        }[change]
        async with db.sf() as session:
            await session.execute(update(RunRow).where(RunRow.run_id == row["run_id"]).values(**values))
            await session.commit()
    before = await db.repository.get(row["run_id"], user_id=None)
    released = await db.repository.release_owned_checkpoint_reservation(row["run_id"], **scope)
    assert released is (change in {"unchanged", "heartbeat"})
    after = await db.repository.get(row["run_id"], user_id=None)
    assert after is None if released else after == before


@pytest.mark.asyncio
async def test_exact_release_owns_native_delete_commit_through_repeated_cancel(output_store):
    import asyncio
    from contextvars import ContextVar

    from sqlalchemy import event
    from sqlalchemy.util import await_only

    db = output_store
    row, scope = await reservation(db)
    probe, _ = await reservation(db)
    entered, release = asyncio.Event(), asyncio.Event()
    role = ContextVar("exact_reservation_release", default=False)
    held = False

    def observe(_connection, _cursor, statement, _parameters, _context, _many):
        nonlocal held
        if held or not role.get() or not statement.lstrip().upper().startswith("DELETE FROM RUNS"):
            return
        held = True

        async def wait():
            entered.set()
            await release.wait()

        await_only(wait())

    async def deleting():
        token = role.set(True)
        try:
            return await db.repository.release_owned_checkpoint_reservation(row["run_id"], **scope)
        finally:
            role.reset(token)

    event.listen(db.engine.sync_engine, "after_cursor_execute", observe)
    task = asyncio.create_task(deleting())
    try:
        await asyncio.wait_for(entered.wait(), 3)
        task.cancel()
        await asyncio.sleep(0)
        task.cancel()
        done, _ = await asyncio.wait({task}, timeout=0.02)
        assert not done, "Public exact release detached its executed native DELETE before commit/exit"
    finally:
        release.set()
        await asyncio.gather(task, return_exceptions=True)
        event.remove(db.engine.sync_engine, "after_cursor_execute", observe)
    assert task.cancelled()
    assert await db.repository.get(row["run_id"], user_id=None) is None
    async with db.sf() as session:
        await session.execute(update(RunRow).where(RunRow.run_id == probe["run_id"]).values(model_name="independent-37"))
        await session.commit()
    assert (await db.repository.get(probe["run_id"], user_id=None))["model_name"] == "independent-37"
    assert db.engine.sync_engine.pool.checkedout() == 0
