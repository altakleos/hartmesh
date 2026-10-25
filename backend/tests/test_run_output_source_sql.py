"""Actual SQL source reads distinguish absence, changes, malformed data and ownership."""

import asyncio

import pytest
from _run_output_capture_test_support import capture_environment as capture_environment
from _run_output_capture_test_support import output_store as output_store
from _run_output_capture_test_support import put_messages
from langchain_core.messages import AIMessage, HumanMessage
from sqlalchemy import delete, update

from deerflow.persistence.models.run_event import RunEventRow
from deerflow.runtime.events.store.memory import MemoryRunEventStore
from deerflow.runtime.runs.output import OutputEvidenceError, TextOutputReceipt, canonical_text_digest
from deerflow.runtime.runs.output_source import read_durable_output_source

TEXT = "  SQL exact\nCafé\u2028text  "
OWNER = "human:output/owner"


async def seeded(environment, *, input=True):
    human = HumanMessage(id="actual-human__user", content="input").model_dump()
    ai = AIMessage(id="actual-ai", content=TEXT).model_dump()
    rows = await put_messages(environment, [human, ai] if input else [ai])
    return TextOutputReceipt(
        version=1, kind="text", owner_user_id=OWNER, thread_id="thread", run_id="run", input_message_id="actual-human" if input else None, assistant_message_id="actual-ai", event_seq=rows[-1]["seq"], text_sha256=canonical_text_digest(TEXT)
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("input", [False, True])
async def test_actual_exact_sql_source_and_nullable_genuine_input(capture_environment, input):
    receipt = await seeded(capture_environment, input=input)
    audit = await read_durable_output_source(capture_environment.event_store, receipt=receipt)
    assert (audit.state, audit.text) == ("matched", TEXT)
    if input:
        assert audit.input_message["id"] == "actual-human__user"
        with pytest.raises(TypeError):
            audit.input_message["content"] = "changed"
    else:
        assert audit.input_message is None


@pytest.mark.asyncio
@pytest.mark.parametrize("missing", ["assistant", "human"])
async def test_successful_absence_is_missing_not_uncertainty(capture_environment, missing):
    receipt = await seeded(capture_environment)
    async with capture_environment.sf.begin() as session:
        await session.execute(delete(RunEventRow).where(RunEventRow.seq == (receipt.event_seq if missing == "assistant" else 1)))
    assert (await read_durable_output_source(capture_environment.event_store, receipt=receipt)).state == "missing"


@pytest.mark.asyncio
async def test_valid_changed_source_offers_no_alternative(capture_environment):
    receipt = await seeded(capture_environment)
    changed = AIMessage(id="actual-ai", content="different").model_dump()
    async with capture_environment.sf.begin() as session:
        import json

        await session.execute(update(RunEventRow).where(RunEventRow.seq == receipt.event_seq).values(content=json.dumps(changed)))
    audit = await read_durable_output_source(capture_environment.event_store, receipt=receipt)
    assert (audit.state, audit.text) == ("changed", None)


@pytest.mark.asyncio
async def test_failed_json_decode_is_uncertainty_not_source_changed(capture_environment):
    receipt = await seeded(capture_environment)
    async with capture_environment.sf.begin() as session:
        await session.execute(update(RunEventRow).where(RunEventRow.seq == receipt.event_seq).values(content="{broken"))
    with pytest.raises(OutputEvidenceError):
        await read_durable_output_source(capture_environment.event_store, receipt=receipt)


@pytest.mark.asyncio
async def test_foreign_store_response_is_never_accepted(capture_environment, monkeypatch):
    receipt = await seeded(capture_environment)
    original = capture_environment.event_store.list_messages_by_run

    async def foreign(*args, **kwargs):
        rows = await original(*args, **kwargs)
        rows[0]["user_id"] = "foreign"
        return rows

    monkeypatch.setattr(capture_environment.event_store, "list_messages_by_run", foreign)
    with pytest.raises(OutputEvidenceError):
        await read_durable_output_source(capture_environment.event_store, receipt=receipt)


@pytest.mark.asyncio
async def test_whole_read_is_retained_through_repeated_cancellation(capture_environment, monkeypatch):
    receipt = await seeded(capture_environment)
    entered, release, finished = asyncio.Event(), asyncio.Event(), asyncio.Event()
    original = capture_environment.event_store.list_messages_by_run

    async def held(*args, **kwargs):
        entered.set()
        await release.wait()
        try:
            return await original(*args, **kwargs)
        finally:
            finished.set()

    monkeypatch.setattr(capture_environment.event_store, "list_messages_by_run", held)
    task = asyncio.create_task(read_durable_output_source(capture_environment.event_store, receipt=receipt))
    try:
        await asyncio.wait_for(entered.wait(), 3)
        task.cancel()
        task.cancel()
        await asyncio.sleep(0)
        assert not task.done() and not finished.is_set()
        release.set()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert finished.is_set()
    finally:
        release.set()
        await asyncio.gather(task, return_exceptions=True)


@pytest.mark.asyncio
async def test_unsupported_memory_never_reads_even_for_missing_source(capture_environment, monkeypatch):
    receipt = await seeded(capture_environment)
    store = MemoryRunEventStore()

    async def forbidden(*args, **kwargs):
        pytest.fail("unsupported store read")

    monkeypatch.setattr(store, "list_messages_by_run", forbidden)
    with pytest.raises(OutputEvidenceError):
        await read_durable_output_source(store, receipt=receipt)


@pytest.mark.asyncio
async def test_many_messages_use_progressing_bounded_run_prefix(capture_environment, monkeypatch):
    await put_messages(capture_environment, [HumanMessage(id="actual-human", content="input").model_dump()])
    await put_messages(capture_environment, [AIMessage(id=f"intermediate-{i}", content="earlier").model_dump() for i in range(260)])
    rows = await put_messages(capture_environment, [AIMessage(id="actual-ai", content=TEXT).model_dump()])
    receipt = TextOutputReceipt(
        version=1, kind="text", owner_user_id=OWNER, thread_id="thread", run_id="run", input_message_id="actual-human", assistant_message_id="actual-ai", event_seq=rows[-1]["seq"], text_sha256=canonical_text_digest(TEXT)
    )
    calls = []
    original = capture_environment.event_store.list_messages_by_run

    async def observed(*args, **kwargs):
        calls.append(kwargs.copy())
        return await original(*args, **kwargs)

    monkeypatch.setattr(capture_environment.event_store, "list_messages_by_run", observed)
    assert (await read_durable_output_source(capture_environment.event_store, receipt=receipt)).state == "matched"
    assert [c["after_seq"] for c in calls] == [0, 256]
    assert all(c["user_id"] == OWNER and c["before_seq"] == receipt.event_seq + 1 for c in calls)


@pytest.mark.asyncio
async def test_changed_human_caller_cannot_prove_the_root_input(capture_environment):
    receipt = await seeded(capture_environment)
    async with capture_environment.sf.begin() as session:
        await session.execute(update(RunEventRow).where(RunEventRow.seq == 1).values(event_metadata={"caller": "subagent:other", "content_is_json": True, "content_is_dict": True}))
    assert (await read_durable_output_source(capture_environment.event_store, receipt=receipt)).state == "changed"


@pytest.mark.asyncio
@pytest.mark.parametrize("bad", ["reverse", "repeat", "raw-content", "wrong-run", "failed"])
async def test_bad_complete_page_never_becomes_a_successful_absence(capture_environment, monkeypatch, bad):
    receipt = await seeded(capture_environment)
    original = capture_environment.event_store.list_messages_by_run

    async def broken(*args, **kwargs):
        rows = await original(*args, **kwargs)
        if bad == "failed":
            raise RuntimeError("private synthetic details")
        if bad == "reverse":
            return list(reversed(rows))
        if bad == "repeat":
            return [rows[0], rows[0]]
        rows[0]["content" if bad == "raw-content" else "run_id"] = "broken"
        return rows

    monkeypatch.setattr(capture_environment.event_store, "list_messages_by_run", broken)
    with pytest.raises(OutputEvidenceError, match="^Run output evidence is unavailable$"):
        await read_durable_output_source(capture_environment.event_store, receipt=receipt)


@pytest.mark.asyncio
@pytest.mark.parametrize("output_store", ["sqlite"], indirect=True)
@pytest.mark.parametrize("journal_mode", ["delete", "wal"])
async def test_native_source_cursor_is_owned_before_independent_commit(capture_environment, monkeypatch, journal_mode, tmp_path):
    import gc
    import json
    import sqlite3
    import sys
    from contextvars import ContextVar

    import aiosqlite
    from sqlalchemy import select, text
    from sqlalchemy.exc import OperationalError
    from test_run_repository_read_ownership import clear_error_frames

    from deerflow.utils.file_io import await_drained

    e = capture_environment
    receipt = await seeded(e)
    async with e.engine.connect() as connection:
        assert (await connection.scalar(text("PRAGMA journal_mode=" + journal_mode))).lower() == journal_mode
    entered, release, closed, cancelled = (asyncio.Event() for _ in range(4))
    scope = ContextVar("output_source_native_read", default=False)
    execute = aiosqlite.Connection._execute
    cursors, connections = [], []
    facts = {"journal_mode": journal_mode, "phases": []}

    async def native(connection, function, *args, **kwargs):
        result = await execute(connection, function, *args, **kwargs)
        if cursors and getattr(function, "__name__", "") == "close" and getattr(function, "__self__", None) is cursors[0]:
            closed.set()
        statement = str(args[0]).upper() if args else ""
        if not cursors and scope.get() and getattr(function, "__name__", "") == "execute" and statement.lstrip().startswith("SELECT") and "FROM RUN_EVENTS" in statement:
            cursors.append(function.__self__)
            connections.append(connection)
            entered.set()
            try:
                await release.wait()
            except asyncio.CancelledError:
                cancelled.set()
                raise
        return result

    async def read():
        token = scope.set(True)
        try:
            return await read_durable_output_source(e.event_store, receipt=receipt)
        finally:
            scope.reset(token)

    async def writer(value):
        session = e.sf()
        try:
            await session.execute(text("PRAGMA busy_timeout=1000"))
            await session.execute(text("BEGIN IMMEDIATE"))
            facts["phases"].append("begin")
            await session.execute(update(RunEventRow).where(RunEventRow.seq == receipt.event_seq).values(event_metadata={"caller": "lead_agent", "probe": value}))
            facts["phases"].append("update")
            await session.commit()
            facts["phases"].append("commit")
            async with e.sf() as check:
                result = await check.scalar(select(RunEventRow.event_metadata).where(RunEventRow.seq == receipt.event_seq))
            return result["probe"] == value
        except OperationalError as error:
            facts["writer_database_locked"] = isinstance(error.orig, sqlite3.OperationalError) and "locked" in str(error.orig).lower()
            await await_drained(session.rollback())
            return False
        finally:
            await await_drained(session.close())

    monkeypatch.setattr(aiosqlite.Connection, "_execute", native)
    task = asyncio.create_task(read())
    result = None
    try:
        await asyncio.wait_for(entered.wait(), 5)
        task.cancel()
        task.cancel()
        await asyncio.sleep(0)
        premature = task.done() or cancelled.is_set()
        release.set()
        done, _ = await asyncio.wait({task}, timeout=5)
        if task in done:
            (result,) = await asyncio.gather(task, return_exceptions=True)
        committed = await asyncio.wait_for(writer(37), 5)
        facts.update(committed_before_cleanup=committed, cursor_closed=closed.is_set(), premature=premature, caller_done=task in done)
        await asyncio.to_thread((tmp_path / "source-native-facts.json").write_text, json.dumps(facts), encoding="utf-8")
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
