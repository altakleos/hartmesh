"""SQL run reads retain their actual cursor and session through cancellation."""

import asyncio
import gc
import json
import sqlite3
import sys
import traceback
from contextvars import ContextVar
from types import SimpleNamespace
from uuid import uuid4

import aiosqlite
import pytest
from _run_output_store_test_support import RAW_OWNER, capture_run
from _run_output_store_test_support import output_store as output_store
from sqlalchemy import event, text, update
from sqlalchemy.exc import DBAPIError, OperationalError
from sqlalchemy.util import await_only

from deerflow.persistence.run.model import RunRow
from deerflow.runtime.runs.manager import RunManager
from deerflow.runtime.runs.schemas import RunStatus
from deerflow.runtime.runs.terminal import TerminalRecoveryPolicy
from deerflow.runtime.user_context import AUTO, get_current_user, reset_current_user, set_current_user
from deerflow.utils.file_io import await_drained


def clear_error_frames(error, *, preserve=None, seen=None):
    if error is preserve:
        return
    seen = set() if seen is None else seen
    if id(error) in seen:
        return
    seen.add(id(error))
    for linked in (error.__cause__, error.__context__):
        if linked is not None:
            clear_error_frames(linked, preserve=preserve, seen=seen)
    if error.__traceback__ is not None:
        traceback.clear_frames(error.__traceback__)
    error.__traceback__ = error.__cause__ = error.__context__ = None


@pytest.mark.asyncio
@pytest.mark.parametrize("output_store", ["sqlite"], indirect=True)
@pytest.mark.parametrize("journal_mode", ["delete", "wal"])
@pytest.mark.parametrize("entry", ["get", "terminal"])
@pytest.mark.parametrize("cancel", [False, True])
async def test_native_run_read_closes_before_cancellation_and_independent_commit(output_store, monkeypatch, journal_mode, entry, cancel):
    manager, record = await capture_run(output_store, capture=False)
    async with output_store.engine.connect() as connection:
        observed_mode = await connection.scalar(text("PRAGMA journal_mode=" + journal_mode))
        assert observed_mode.lower() == journal_mode
    original = await output_store.repository.get(record.run_id, user_id=RAW_OWNER)
    assert "output_capture" not in original["kwargs"] and "output_evidence" not in original["kwargs"]

    entered, release, cursor_cancelled, cursor_closed = (asyncio.Event() for _ in range(4))
    role = ContextVar("run_read_native_owner", default=False)
    cursors, connections = [], []
    execute = aiosqlite.Connection._execute
    facts = {"journal_mode": journal_mode, "entry": entry, "cancel": cancel, "writer_phases": []}

    async def native_execute(connection, function, *args, **kwargs):
        result = await execute(connection, function, *args, **kwargs)
        if cursors and getattr(function, "__name__", "") == "close" and getattr(function, "__self__", None) is cursors[0]:
            cursor_closed.set()
        statement = str(args[0]).lstrip().upper() if args else ""
        if not cursors and role.get() and getattr(function, "__name__", "") == "execute" and statement.startswith("SELECT") and "FROM RUNS" in statement:
            cursors.append(function.__self__)
            connections.append(connection)
            entered.set()
            try:
                await release.wait()
            except asyncio.CancelledError:
                cursor_cancelled.set()
                raise
        return result

    async def read():
        token = role.set(True)
        try:
            if entry == "terminal":
                return await manager.set_status_if_not_cancelled(record.run_id, RunStatus.success)
            return await output_store.repository.get(record.run_id, user_id=RAW_OWNER)
        finally:
            role.reset(token)

    async def independent_commit(value, *, diagnostic=False):
        session = output_store.sf()
        try:
            await session.execute(text("PRAGMA busy_timeout=1000"))
            await session.execute(text("BEGIN IMMEDIATE"))
            if diagnostic:
                facts["writer_phases"].append("begin")
            await session.execute(update(RunRow).where(RunRow.run_id == record.run_id).values(total_tokens=value))
            if diagnostic:
                facts["writer_phases"].append("update")
            await session.commit()
            if diagnostic:
                facts["writer_phases"].append("commit")
            row = await output_store.repository.get(record.run_id, user_id=RAW_OWNER)
            return row["total_tokens"] == value
        except OperationalError as error:
            if not diagnostic:
                raise
            facts["writer_database_locked"] = isinstance(error.orig, sqlite3.OperationalError) and "locked" in str(error.orig).lower()
            await await_drained(session.rollback())
            return False
        finally:
            await await_drained(session.close())

    monkeypatch.setattr(aiosqlite.Connection, "_execute", native_execute)
    operation = asyncio.create_task(read())
    watcher = asyncio.create_task(cursor_cancelled.wait())
    result = None
    try:
        await asyncio.wait_for(entered.wait(), 5)
        if cancel:
            operation.cancel()
            operation.cancel()
        await asyncio.wait({operation, watcher}, timeout=0.05, return_when=asyncio.FIRST_COMPLETED)
        premature = operation.done() or cursor_cancelled.is_set()
        release.set()
        done, _ = await asyncio.wait({operation}, timeout=5)
        facts["operation_finished"] = operation in done
        if operation in done:
            (result,) = await asyncio.gather(operation, return_exceptions=True)
        # Observe the physical write before any assertion or diagnostic cleanup.
        committed = await asyncio.wait_for(independent_commit(37, diagnostic=True), 5)
        assert facts["operation_finished"] and committed, json.dumps(facts, sort_keys=True)
        assert not premature and cursor_closed.is_set(), json.dumps(facts, sort_keys=True)
        if cancel:
            assert isinstance(result, asyncio.CancelledError)
        elif entry == "get":
            assert result == original
        else:
            assert result is None
        row = await output_store.repository.get(record.run_id, user_id=RAW_OWNER)
        assert row["status"] == ("success" if entry == "terminal" and not cancel else "running")
        assert row["total_tokens"] == 37 and row["kwargs"] == original["kwargs"]
    finally:
        primary_error = sys.exception()
        release.set()
        watcher.cancel()
        if not operation.done():
            operation.cancel()
        await asyncio.gather(operation, watcher, return_exceptions=True)
        # A failing baseline must retire its exact native handle and diagnostic
        # traceback references only AFTER the independent writer was observed.
        if cursors and not cursor_closed.is_set():
            try:
                await execute(connections[0], cursors[0].close)
            except ValueError:
                if connections[0]._running:
                    raise

                def close_stopped_cursor():
                    try:
                        cursors[0].close()
                    except sqlite3.ProgrammingError:
                        try:
                            cursors[0].connection.in_transaction
                        except sqlite3.ProgrammingError:
                            return
                        raise

                await asyncio.to_thread(close_stopped_cursor)
            try:
                operation.exception()
            except BaseException as owned_error:
                clear_error_frames(owned_error, preserve=primary_error)
            if isinstance(result, BaseException):
                clear_error_frames(result, preserve=primary_error)
            cursors.clear()
            connections.clear()
            await asyncio.to_thread(gc.collect)
            assert await asyncio.wait_for(independent_commit(41), 5)
        assert output_store.engine.sync_engine.pool.checkedout() == 0


@pytest.mark.asyncio
async def test_real_query_and_parent_context_remain_owned_until_release(output_store):
    owner = " raw owner/with\ttab "
    _manager, record = await capture_run(output_store, capture=False, owner=owner)
    parent = get_current_user()
    token = set_current_user(SimpleNamespace(id=owner))
    entered, release, cancelled = (asyncio.Event() for _ in range(3))
    seen = []

    def hold(_connection, _cursor, statement, _parameters, _context, _many):
        if statement.lstrip().upper().startswith("SELECT") and "FROM runs" in statement:

            async def wait():
                seen.append(get_current_user().id)
                entered.set()
                try:
                    await release.wait()
                except asyncio.CancelledError:
                    cancelled.set()
                    raise

            await_only(wait())

    event.listen(output_store.engine.sync_engine, "after_cursor_execute", hold)
    reading = asyncio.create_task(output_store.repository.get(record.run_id))
    watcher = asyncio.create_task(cancelled.wait())
    try:
        await asyncio.wait_for(entered.wait(), 5)
        reading.cancel()
        reading.cancel()
        done, _ = await asyncio.wait({reading, watcher}, timeout=0.05, return_when=asyncio.FIRST_COMPLETED)
        assert not done, "The actual query must settle before its caller leaves the owner scope"
        assert get_current_user().id == owner and seen == [owner]
        release.set()
        with pytest.raises(asyncio.CancelledError):
            await reading
        assert output_store.engine.sync_engine.pool.checkedout() == 0
    finally:
        release.set()
        watcher.cancel()
        await asyncio.gather(reading, watcher, return_exceptions=True)
        event.remove(output_store.engine.sync_engine, "after_cursor_execute", hold)
        reset_current_user(token)
    assert get_current_user() is parent


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ["auto", "explicit", "unfiltered", "foreign", "missing", "missing_context"])
async def test_get_retains_existing_raw_owner_and_absence_semantics(output_store, kind):
    owner = " raw owner/with\ttab "
    _manager, record = await capture_run(output_store, capture=False, owner=owner)
    parent = get_current_user()
    token = set_current_user(None if kind == "missing_context" else SimpleNamespace(id=owner))
    try:
        if kind == "missing_context":
            with pytest.raises(RuntimeError, match="RunRepository.get called with user_id=AUTO"):
                await output_store.repository.get(record.run_id)
            return
        supplied = {"auto": AUTO, "explicit": owner, "unfiltered": None, "foreign": "other-owner", "missing": owner}[kind]
        row = await output_store.repository.get("absent-row" if kind == "missing" else record.run_id, user_id=supplied)
        if kind in {"foreign", "missing"}:
            assert row is None
        else:
            assert row["user_id"] == owner and row["run_id"] == record.run_id and row["status"] == "running"
    finally:
        reset_current_user(token)
        assert get_current_user() is parent


@pytest.mark.asyncio
async def test_get_preserves_actual_database_errors(output_store):
    _manager, record = await capture_run(output_store, capture=False)
    async with output_store.sf() as session, session.begin():
        await session.execute(text("DROP TABLE runs"))
    with pytest.raises(DBAPIError):
        await output_store.repository.get(record.run_id, user_id=RAW_OWNER)
    assert output_store.engine.sync_engine.pool.checkedout() == 0


@pytest.mark.asyncio
@pytest.mark.parametrize("cancel_during_read", [False, True])
async def test_terminal_timeout_retries_only_after_held_read_and_keeps_cancel_fence(output_store, cancel_during_read):
    manager = RunManager(output_store.repository, terminal_recovery_policy=TerminalRecoveryPolicy(max_attempts=2, attempt_timeout=1, retry_delay=0))
    record = await manager.create_or_reject(uuid4().hex, user_id=RAW_OWNER)
    await manager.try_start(record.run_id)
    entered, release = asyncio.Event(), asyncio.Event()
    reads = []

    def hold(_connection, _cursor, statement, _parameters, _context, _many):
        if statement.lstrip().upper().startswith("SELECT") and "FROM runs" in statement:
            reads.append(statement)
            if len(reads) == 1:

                async def wait():
                    entered.set()
                    await release.wait()

                await_only(wait())

    event.listen(output_store.engine.sync_engine, "after_cursor_execute", hold)
    operation = asyncio.create_task(manager.set_status_if_not_cancelled(record.run_id, RunStatus.success))
    try:
        await asyncio.wait_for(entered.wait(), 5)
        await asyncio.sleep(1.05)
        assert len(reads) == 1 and not operation.done(), "A deadline cannot detach the first query to start another attempt"
        if cancel_during_read:
            await output_store.repository.request_cancel(record.run_id, action="interrupt")
        release.set()
        result = await asyncio.wait_for(operation, 5)
        assert result == ("interrupt" if cancel_during_read else None)
        assert len(reads) == 2
        row = await output_store.repository.get(record.run_id, user_id=RAW_OWNER)
        assert row["status"] == ("running" if cancel_during_read else "success")
        assert row["cancel_action"] == ("interrupt" if cancel_during_read else None)
        assert "output_capture" not in row["kwargs"] and "output_evidence" not in row["kwargs"]
    finally:
        release.set()
        await asyncio.gather(operation, return_exceptions=True)
        event.remove(output_store.engine.sync_engine, "after_cursor_execute", hold)
