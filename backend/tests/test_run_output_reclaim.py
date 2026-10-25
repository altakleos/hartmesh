"""Direct SQL reclaim authority; a cancellation request is not cleanup proof."""

import asyncio
from contextvars import ContextVar
from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest
from _run_output_store_test_support import RAW_OWNER
from _run_output_store_test_support import output_store as output_store
from sqlalchemy import event, select, update
from sqlalchemy.ext.asyncio import AsyncSession

from deerflow.persistence.run.model import RunRow
from deerflow.runtime.runs.output import OUTPUT_CAPTURE_DISCONNECT_KEY, OutputEvidenceError, UnavailableOutputReceipt, receipt_from_kwargs
from deerflow.runtime.runs.terminal import TerminalWriteFailure


def failed_receipt(run_id, thread_id):
    return UnavailableOutputReceipt(version=1, kind="unavailable", owner_user_id=RAW_OWNER, thread_id=thread_id, run_id=run_id, input_message_id=None, reason="failed")


async def seed_reclaim(e, *, lease="expired", capture=True, status="running", action=None):
    run_id, thread_id = uuid4().hex, uuid4().hex
    deadline = None if lease == "null" else (datetime.now(UTC) + timedelta(hours=1) * (1 if lease == "live" else -1)).isoformat()
    kwargs = {"retained": {"items": ["original", 7]}, "input": {"messages": []}}
    if capture:
        kwargs.update(output_capture={"version": 1}, **{OUTPUT_CAPTURE_DISCONNECT_KEY: "continue"})
    await e.repository.put(run_id, thread_id=thread_id, user_id=RAW_OWNER, owner_worker_id="original-worker", lease_expires_at=deadline, status=status, kwargs=kwargs)
    if action is not None:
        assert await e.repository.request_cancel(run_id, action=action) == action
    return run_id, thread_id, await e.repository.get(run_id, user_id=RAW_OWNER)


@pytest.mark.asyncio
@pytest.mark.parametrize("lease", ["expired", "null"])
@pytest.mark.parametrize("action", [None, "interrupt", "rollback"])
async def test_reclaim_atomically_seals_failed_without_claiming_cancellation(output_store, lease, action):
    e = output_store
    run_id, thread_id, before = await seed_reclaim(e, lease=lease, action=action)
    assert await e.repository.claim_for_takeover(run_id, grace_seconds=10, error="Owner unavailable", stop_reason="owner_lost")
    after = await e.repository.get(run_id, user_id=RAW_OWNER)
    assert after["status"] == "error" and after["error"] == "Owner unavailable"
    assert after["owner_worker_id"] == before["owner_worker_id"]
    assert after["cancel_action"] == action and after["stop_reason"] == "owner_lost"
    assert after["lease_expires_at"] == before["lease_expires_at"]
    assert after["change_seq"] > before["change_seq"]
    assert receipt_from_kwargs(after["kwargs"]) == failed_receipt(run_id, thread_id)
    expected = {**before["kwargs"], "output_evidence": failed_receipt(run_id, thread_id).model_dump(mode="json")}
    assert after["kwargs"] == expected
    assert await e.repository.claim_for_takeover(run_id, grace_seconds=10, error="Late different reason") is False
    assert await e.repository.get(run_id, user_id=RAW_OWNER) == after


@pytest.mark.asyncio
@pytest.mark.parametrize("action", [None, "interrupt", "rollback"])
async def test_ordinary_reclaim_preserves_existing_unsealed_behavior(output_store, action):
    run_id, _thread_id, before = await seed_reclaim(output_store, capture=False, action=action)
    assert await output_store.repository.claim_for_takeover(run_id, grace_seconds=10, error="Ordinary owner lost")
    after = await output_store.repository.get(run_id, user_id=RAW_OWNER)
    assert after["status"] == "error" and after["kwargs"] == before["kwargs"]
    assert after["owner_worker_id"] == before["owner_worker_id"] and after["cancel_action"] == action


@pytest.mark.asyncio
async def test_live_lease_refusal_has_no_row_mutation(output_store):
    run_id, _thread_id, before = await seed_reclaim(output_store, lease="live")
    assert await output_store.repository.claim_for_takeover(run_id, grace_seconds=10, error="Must refuse") is False
    assert await output_store.repository.get(run_id, user_id=RAW_OWNER) == before


@pytest.mark.asyncio
@pytest.mark.parametrize("status", ["success", "error", "interrupted"])
async def test_terminal_unsealed_capture_is_never_retrofitted(output_store, status):
    run_id, _thread_id, before = await seed_reclaim(output_store, status=status)
    assert await output_store.repository.claim_for_takeover(run_id, grace_seconds=10, error="Must not retrofit") is False
    assert await output_store.repository.get(run_id, user_id=RAW_OWNER) == before


@pytest.mark.asyncio
@pytest.mark.parametrize("bad", [{"version": True}, None, {"version": 2}])
async def test_malformed_capture_refuses_instead_of_ordinary_downgrade(output_store, bad):
    e = output_store
    run_id, _thread_id, _ = await seed_reclaim(e)
    async with e.sf.begin() as session:
        row = await session.get(RunRow, run_id)
        # ORM JSON dirty comparison treats True == 1. Force the actual stored
        # malformed primitive instead of accidentally retaining version=1.
        await session.execute(update(RunRow).where(RunRow.run_id == run_id).values(kwargs_json={**row.kwargs_json, "output_capture": bad}))
    before = await e.repository.get(run_id, user_id=RAW_OWNER)
    if bad is not None:
        assert type(before["kwargs"]["output_capture"]["version"]) is type(bad["version"])
    else:
        assert before["kwargs"]["output_capture"] is None
    with pytest.raises((OutputEvidenceError, TerminalWriteFailure)):
        await e.repository.claim_for_takeover(run_id, grace_seconds=10, error="Must refuse malformed")
    assert await e.repository.get(run_id, user_id=RAW_OWNER) == before


@pytest.mark.asyncio
async def test_renewal_committed_while_takeover_waits_wins(output_store):
    e = output_store
    run_id, _thread_id, _ = await seed_reclaim(e)
    dispatched = asyncio.Event()
    scope = ContextVar("takeover_clock_dispatch", default=False)

    def clock_dispatch(_connection, _cursor, statement, _parameters, _context, _many):
        sql = statement.upper().lstrip()
        if scope.get() and "RUN_CHANGE_CLOCK" in sql and sql.startswith(("INSERT", "UPDATE")):
            # SQLite can wait at the clock's INSERT before its UPDATE. This
            # records an actual driver dispatch, rather than task scheduling.
            dispatched.set()

    async def claim():
        token = scope.set(True)
        try:
            return await e.repository.claim_for_takeover(run_id, grace_seconds=10, error="Stale observer")
        finally:
            scope.reset(token)

    holder = e.sf()
    task = None
    event.listen(e.engine.sync_engine, "before_cursor_execute", clock_dispatch)
    try:
        await e.repository._next_change_seq(holder)
        await holder.execute(update(RunRow).where(RunRow.run_id == run_id).values(lease_expires_at=datetime.now(UTC) + timedelta(hours=1)))
        task = asyncio.create_task(claim())
        await asyncio.wait_for(dispatched.wait(), 5)
        done, _ = await asyncio.wait({task}, timeout=0.05)
        assert not done, "The actual dispatched clock operation must wait for the renewal transaction"
        await holder.commit()
        assert await asyncio.wait_for(task, 5) is False
    finally:
        await holder.close()
        if task is not None:
            await asyncio.gather(task, return_exceptions=True)
        event.remove(e.engine.sync_engine, "before_cursor_execute", clock_dispatch)
    after = await e.repository.get(run_id, user_id=RAW_OWNER)
    assert after["status"] == "running" and "output_evidence" not in after["kwargs"]


@pytest.mark.asyncio
async def test_late_owned_cas_cannot_replace_committed_reclaim(output_store):
    e = output_store
    # A NULL-lease owned CAS was eligible before reclaim. An already-expired
    # required lease must not mask the actual status/version/evidence fence.
    run_id, thread_id, before = await seed_reclaim(e, lease="null")
    assert await e.repository.claim_for_takeover(run_id, grace_seconds=10, error="Owner lost")
    reclaimed = await e.repository.get(run_id, user_id=RAW_OWNER)
    assert receipt_from_kwargs(reclaimed["kwargs"]) == failed_receipt(run_id, thread_id)
    assert (
        await e.repository.finalize_owned(
            run_id,
            owner_worker_id="original-worker",
            expected_change_seq=before["change_seq"],
            expected_status="running",
            expected_cancel_action=None,
            require_lease=False,
            status="interrupted",
            output_receipt=UnavailableOutputReceipt(version=1, kind="unavailable", owner_user_id=RAW_OWNER, thread_id=thread_id, run_id=run_id, input_message_id=None, reason="cancelled"),
        )
        is False
    )
    assert await e.repository.get(run_id, user_id=RAW_OWNER) == reclaimed


class CommitResponseLost(RuntimeError):
    pass


@pytest.mark.asyncio
async def test_actual_lost_commit_response_keeps_the_atomic_failed_seal(output_store, monkeypatch):
    e = output_store
    run_id, thread_id, before = await seed_reclaim(e)
    commit = AsyncSession.commit
    observed = []

    async def lose_response(session):
        await commit(session)
        if session.bind is e.engine and not observed:
            observed.append(True)
            raise CommitResponseLost("Fixed synthetic response loss")

    monkeypatch.setattr(AsyncSession, "commit", lose_response)
    with pytest.raises((CommitResponseLost, TerminalWriteFailure)):
        await e.repository.claim_for_takeover(run_id, grace_seconds=10, error="Owner lost")
    after = await e.repository.get(run_id, user_id=RAW_OWNER)
    assert observed and after["status"] == "error" and after["change_seq"] > before["change_seq"]
    assert receipt_from_kwargs(after["kwargs"]) == failed_receipt(run_id, thread_id)
    assert after["kwargs"]["retained"] == before["kwargs"]["retained"]


@pytest.mark.asyncio
async def test_cancelled_reclaim_waiter_drains_actual_committed_operation(output_store, monkeypatch):
    e = output_store
    run_id, _thread_id, _ = await seed_reclaim(e)
    entered, release, returned = asyncio.Event(), asyncio.Event(), asyncio.Event()
    commit = AsyncSession.commit

    async def hold_response(session):
        await commit(session)
        if session.bind is e.engine:
            entered.set()
            await release.wait()
            returned.set()

    monkeypatch.setattr(AsyncSession, "commit", hold_response)
    task = asyncio.create_task(e.repository.claim_for_takeover(run_id, grace_seconds=10, error="Owner lost"))
    premature = None
    try:
        await asyncio.wait_for(entered.wait(), 5)
        task.cancel()
        first_done, _ = await asyncio.wait({task}, timeout=0.02)
        task.cancel()
        done, _ = await asyncio.wait({task}, timeout=0.05)
        premature = task in first_done or task in done
        release.set()
        result = (await asyncio.gather(task, return_exceptions=True))[0]
    finally:
        release.set()
        await asyncio.gather(task, return_exceptions=True)
    # Actual independent transaction/readback must complete before assertions.
    async with e.sf.begin() as session:
        await session.execute(update(RunRow).where(RunRow.run_id == run_id).values(model_name="independent-writer"))
    async with e.sf() as session:
        assert await session.scalar(select(RunRow.model_name).where(RunRow.run_id == run_id)) == "independent-writer"
    assert not premature and returned.is_set()
    assert isinstance(result, asyncio.CancelledError)


@pytest.mark.asyncio
@pytest.mark.parametrize("proof", [None, "active-seal", "wrong-scope"])
async def test_invalid_existing_proof_cannot_be_replaced_by_reclaim(output_store, proof):
    e = output_store
    run_id, thread_id, before = await seed_reclaim(e)
    values = {**before["kwargs"], "output_evidence": None}
    if proof is not None:
        values["output_evidence"] = failed_receipt(run_id, "different-thread" if proof == "wrong-scope" else thread_id).model_dump(mode="json")
    async with e.sf.begin() as session:
        await session.execute(update(RunRow).where(RunRow.run_id == run_id).values(kwargs_json=values))
    before = await e.repository.get(run_id, user_id=RAW_OWNER)
    with pytest.raises((OutputEvidenceError, TerminalWriteFailure)):
        await e.repository.claim_for_takeover(run_id, grace_seconds=10, error="Must not replace existing proof")
    assert await e.repository.get(run_id, user_id=RAW_OWNER) == before


@pytest.mark.asyncio
@pytest.mark.parametrize("bad", [None, [], "unknown"])
async def test_corrupt_kwargs_cannot_prove_ordinary_reclaim(output_store, bad):
    e = output_store
    run_id, _thread_id, _ = await seed_reclaim(e)
    async with e.sf.begin() as session:
        await session.execute(update(RunRow).where(RunRow.run_id == run_id).values(kwargs_json=bad))
    before = await e.repository.get(run_id, user_id=RAW_OWNER)
    assert type(before["kwargs"]) is type(bad) and before["kwargs"] == bad
    with pytest.raises((OutputEvidenceError, TerminalWriteFailure)):
        await e.repository.claim_for_takeover(run_id, grace_seconds=10, error="Unknown capture authority")
    assert await e.repository.get(run_id, user_id=RAW_OWNER) == before


@pytest.mark.asyncio
async def test_actual_empty_dict_is_still_ordinary_reclaim(output_store):
    e = output_store
    run_id, _thread_id, _ = await seed_reclaim(e)
    async with e.sf.begin() as session:
        await session.execute(update(RunRow).where(RunRow.run_id == run_id).values(kwargs_json={}))
    assert await e.repository.claim_for_takeover(run_id, grace_seconds=10, error="Ordinary owner lost")
    assert (await e.repository.get(run_id, user_id=RAW_OWNER))["kwargs"] == {}


@pytest.mark.asyncio
@pytest.mark.parametrize("output_store", ["sqlite"], indirect=True)
@pytest.mark.parametrize("journal_mode", ["delete", "wal"])
async def test_native_reclaim_cursor_is_drained_before_independent_commit(output_store, monkeypatch, journal_mode, tmp_path):
    import gc
    import json
    import sqlite3
    import sys

    import aiosqlite
    from sqlalchemy import text
    from sqlalchemy.exc import OperationalError
    from test_run_repository_read_ownership import clear_error_frames

    from deerflow.utils.file_io import await_drained

    e = output_store
    run_id, _thread_id, _ = await seed_reclaim(e)
    async with e.engine.connect() as connection:
        assert (await connection.scalar(text("PRAGMA journal_mode=" + journal_mode))).lower() == journal_mode
    entered, release, closed, cancelled = (asyncio.Event() for _ in range(4))
    scope = ContextVar("reclaim_native_operation", default=False)
    execute = aiosqlite.Connection._execute
    cursors, connections = [], []
    facts = {"journal_mode": journal_mode, "phases": []}

    async def native(connection, function, *args, **kwargs):
        result = await execute(connection, function, *args, **kwargs)
        if cursors and getattr(function, "__name__", "") == "close" and getattr(function, "__self__", None) is cursors[0]:
            closed.set()
        statement = str(args[0]).upper() if args else ""
        if not cursors and scope.get() and getattr(function, "__name__", "") == "execute" and statement.lstrip().startswith("UPDATE RUNS SET"):
            cursors.append(function.__self__)
            connections.append(connection)
            entered.set()
            try:
                await release.wait()
            except asyncio.CancelledError:
                cancelled.set()
                raise
        return result

    async def reclaim():
        token = scope.set(True)
        try:
            return await e.repository.claim_for_takeover(run_id, grace_seconds=10, error="Owner lost")
        finally:
            scope.reset(token)

    async def writer(value):
        session = e.sf()
        try:
            await session.execute(text("PRAGMA busy_timeout=1000"))
            await session.execute(text("BEGIN IMMEDIATE"))
            facts["phases"].append("begin")
            await session.execute(update(RunRow).where(RunRow.run_id == run_id).values(model_name=str(value)))
            facts["phases"].append("update")
            await session.commit()
            facts["phases"].append("commit")
            async with e.sf() as check:
                return await check.scalar(select(RunRow.model_name).where(RunRow.run_id == run_id)) == str(value)
        except OperationalError as error:
            facts["writer_database_locked"] = isinstance(error.orig, sqlite3.OperationalError) and "locked" in str(error.orig).lower()
            await await_drained(session.rollback())
            return False
        finally:
            await await_drained(session.close())

    monkeypatch.setattr(aiosqlite.Connection, "_execute", native)
    task = asyncio.create_task(reclaim())
    result = None
    try:
        await asyncio.wait_for(entered.wait(), 5)
        task.cancel()
        first_done, _ = await asyncio.wait({task}, timeout=0.02)
        task.cancel()
        done, _ = await asyncio.wait({task}, timeout=0.05)
        premature = bool(first_done or done) or cancelled.is_set()
        release.set()
        (result,) = await asyncio.wait_for(asyncio.gather(task, return_exceptions=True), 5)
        committed = await asyncio.wait_for(writer(37), 5)
        facts.update(native_barrier_entered=True, committed_before_cleanup=committed, cursor_closed=closed.is_set(), premature=premature)
        await asyncio.to_thread((tmp_path / "reclaim-native-facts.json").write_text, json.dumps(facts), encoding="utf-8")
    finally:
        primary = sys.exception()
        release.set()
        if not task.done():
            task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        if cursors and not closed.is_set():
            try:
                await execute(connections[0], cursors[0].close)
            except ValueError:
                assert not connections[0]._running

                def close_stopped():
                    try:
                        cursors[0].close()
                    except sqlite3.ProgrammingError:
                        try:
                            cursors[0].connection.in_transaction
                        except sqlite3.ProgrammingError:
                            return
                        raise

                await asyncio.to_thread(close_stopped)
            try:
                task.exception()
            except BaseException as error:
                clear_error_frames(error, preserve=primary)
            if isinstance(result, BaseException):
                clear_error_frames(result, preserve=primary)
            cursors.clear()
            connections.clear()
            await asyncio.to_thread(gc.collect)
            assert await asyncio.wait_for(writer(41), 5)
        assert e.engine.sync_engine.pool.checkedout() == 0
    assert facts["committed_before_cleanup"] and facts["cursor_closed"] and not facts["premature"], facts
    assert isinstance(result, asyncio.CancelledError)
