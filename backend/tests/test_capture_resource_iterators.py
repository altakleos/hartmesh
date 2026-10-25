"""Real saver iterators retain native resources across yields and close them."""

from __future__ import annotations

import asyncio
import sqlite3
from contextlib import aclosing
from contextvars import ContextVar

import pytest
from test_capture_resource_facades import config_for, seed_checkpoint
from test_capture_resource_facades import native_resources as native_resources
from test_capture_resource_facades import provided_resources as provided_resources
from test_capture_resource_facades import provided_saver as provided_saver
from test_capture_resource_facades import resource_backend as resource_backend


async def independent_commit(env, value):
    """Use another actual connection; return only after COMMIT and read-back."""
    if env.kind == "sqlite":
        import aiosqlite

        async with aiosqlite.connect(env.path, timeout=1) as connection:
            await connection.execute("CREATE TABLE IF NOT EXISTS capture_writer_health (id INTEGER PRIMARY KEY, value INTEGER NOT NULL)")
            await connection.execute("INSERT OR REPLACE INTO capture_writer_health(id, value) VALUES (1, ?)", (value,))
            await connection.commit()
            async with connection.execute("SELECT value FROM capture_writer_health WHERE id=1") as cursor:
                return (await cursor.fetchone())[0]
    import psycopg

    async with await psycopg.AsyncConnection.connect(env.dsn) as connection:
        await connection.execute("CREATE TABLE IF NOT EXISTS capture_writer_health (id INTEGER PRIMARY KEY, value INTEGER NOT NULL)")
        await connection.execute("INSERT INTO capture_writer_health(id,value) VALUES (1,%s) ON CONFLICT (id) DO UPDATE SET value=excluded.value", (value,))
        await connection.commit()
        async with connection.cursor() as cursor:
            await cursor.execute("SELECT value FROM capture_writer_health WHERE id=1")
            return (await cursor.fetchone())[0]


@pytest.mark.asyncio
async def test_raw_native_iterator_closure_releases_lock_cursor_and_writer(native_resources, monkeypatch):
    """Baseline physical control: explicit close, not GC, releases a real cursor."""
    env = native_resources
    first = await seed_checkpoint(env.saver, "native-history", value="first")
    await seed_checkpoint(env.saver, "native-history", value="second", step=1)
    closed = []
    if env.kind == "sqlite":
        import aiosqlite

        original = aiosqlite.Cursor.close

        async def close(cursor):
            await original(cursor)
            if cursor._conn is env.saver.conn:
                closed.append(True)

        monkeypatch.setattr(aiosqlite.Cursor, "close", close)
    elif env.kind == "postgres":
        import psycopg

        original = psycopg.AsyncCursor.close

        async def close(cursor):
            await original(cursor)
            assert cursor.closed
            closed.append(True)

        monkeypatch.setattr(psycopg.AsyncCursor, "close", close)
    history = env.saver.alist(config_for("native-history"))
    blocked = None
    try:
        row = await anext(history)
        assert row.checkpoint["channel_values"] == {"value": "second"}
        if env.kind != "memory":
            assert env.saver.lock.locked()
            blocked = asyncio.create_task(env.saver.aget_tuple(first))
            done, _ = await asyncio.wait({blocked}, timeout=0.02)
            assert not done, "same-saver operation must wait for the live history cursor"
        await history.aclose()
        if env.kind != "memory":
            assert not env.saver.lock.locked()
            assert len(closed) >= (2 if env.kind == "sqlite" else 1)
            assert (await asyncio.wait_for(blocked, 3)).checkpoint["channel_values"] == {"value": "first"}
            assert await asyncio.wait_for(independent_commit(env, 37), 3) == 37
    finally:
        await history.aclose()
        if blocked is not None:
            await asyncio.gather(blocked, return_exceptions=True)


@pytest.mark.asyncio
async def test_explicit_early_break_closes_actual_iterator(native_resources):
    env = native_resources
    saved = await seed_checkpoint(env.saver, "early-break")
    async with aclosing(env.saver.alist(saved)) as history:
        async for row in history:
            assert row.config == saved
            break
    if env.kind != "memory":
        assert not env.saver.lock.locked()
        assert await asyncio.wait_for(independent_commit(env, 41), 3) == 41


@pytest.mark.asyncio
async def test_retirement_forces_idle_native_history_closed_without_consumer(provided_resources, monkeypatch):
    from deerflow.runtime.runs.capture_io import CaptureIOClosed, CaptureIOOwner, capture_io_scope

    env = provided_resources
    await seed_checkpoint(env.saver, "idle-capture", value="first")
    await seed_checkpoint(env.saver, "idle-capture", value="second", step=1)
    owner = CaptureIOOwner()
    cursor_closed = asyncio.Event()
    handles = []
    if env.kind == "sqlite":
        import aiosqlite

        execute = aiosqlite.Connection._execute

        async def native(connection, function, *args, **kwargs):
            result = await execute(connection, function, *args, **kwargs)
            handle = getattr(function, "__self__", None)
            statement = str(args[0]).upper().lstrip() if args else ""
            if connection is env.native_saver.conn and not handles and getattr(function, "__name__", "") == "execute" and statement.startswith("SELECT") and "FROM CHECKPOINTS" in statement:
                # alist uses Connection.execute; its native result is the
                # cursor, while function.__self__ is the connection itself.
                assert isinstance(result, sqlite3.Cursor)
                handles.append(result)
            if handles and getattr(function, "__name__", "") == "close" and handle is handles[0]:
                cursor_closed.set()
            return result

        monkeypatch.setattr(aiosqlite.Connection, "_execute", native)
    elif env.kind == "postgres":
        import psycopg

        execute, close = psycopg.AsyncCursor.execute, psycopg.AsyncCursor.close

        async def native(cursor, query, *args, **kwargs):
            result = await execute(cursor, query, *args, **kwargs)
            if not handles and "FROM CHECKPOINTS" in str(query).upper():
                handles.append(cursor)
            return result

        async def closed_cursor(cursor):
            await close(cursor)
            if handles and cursor is handles[0]:
                assert cursor.closed
                cursor_closed.set()

        monkeypatch.setattr(psycopg.AsyncCursor, "execute", native)
        monkeypatch.setattr(psycopg.AsyncCursor, "close", closed_cursor)
    history = None
    try:
        with capture_io_scope(owner):
            history = env.saver.alist(config_for("idle-capture"))
            assert (await anext(history)).checkpoint["channel_values"] == {"value": "second"}
        if env.kind != "memory":
            assert env.native_saver.lock.locked()
        owner.retire()
        await asyncio.wait_for(owner.drain(), 3)
        # This happens BEFORE diagnostic/finally close: retirement owns closure.
        if env.kind != "memory":
            assert handles, "actual native history query must enter under both implementations"
            committed = await asyncio.wait_for(independent_commit(env, 53), 3) == 53
            assert committed and cursor_closed.is_set() and not env.native_saver.lock.locked(), {"independent_commit": committed, "awaited_cursor_close": cursor_closed.is_set(), "saver_lock_held": env.native_saver.lock.locked()}
        with pytest.raises(CaptureIOClosed):
            await anext(history)
    finally:
        if history is not None:
            await history.aclose()
        if env.kind != "memory":
            assert cursor_closed.is_set(), "fixture cleanup must complete the actual native cursor close"
        owner.retire()
        await owner.drain()


@pytest.mark.asyncio
@pytest.mark.parametrize("resource_backend", ["sqlite"], indirect=True)
@pytest.mark.parametrize("journal_mode", ["DELETE", "WAL"])
async def test_captured_native_select_drains_before_cancel_and_writer(provided_saver, monkeypatch, journal_mode):
    """Actual executed SELECT; measure COMMIT before red-only handle cleanup."""
    import aiosqlite

    from deerflow.runtime.runs.capture_io import CaptureIOOwner, capture_io_scope

    env = provided_saver
    saved = await seed_checkpoint(env.saver, "native-cancel")
    async with env.native_saver.conn.execute("PRAGMA journal_mode=" + journal_mode) as cursor:
        assert (await cursor.fetchone())[0].upper() == journal_mode
    owner = CaptureIOOwner()
    entered, release, cancelled, closed = (asyncio.Event() for _ in range(4))
    role = ContextVar("capture_saver_native_select", default=False)
    execute = aiosqlite.Connection._execute
    handles = []

    async def native(connection, function, *args, **kwargs):
        result = await execute(connection, function, *args, **kwargs)
        handle = getattr(function, "__self__", None)
        if handles and getattr(function, "__name__", "") == "close" and handle is handles[0]:
            closed.set()
        statement = str(args[0]).upper().lstrip() if args else ""
        if connection is env.native_saver.conn and role.get() and not handles and getattr(function, "__name__", "") == "execute" and statement.startswith("SELECT") and "FROM CHECKPOINTS" in statement:
            assert isinstance(result, sqlite3.Cursor)
            handles.append(result)
            entered.set()
            try:
                await release.wait()
            except asyncio.CancelledError:
                cancelled.set()
                raise
        return result

    async def read():
        token = role.set(True)
        try:
            with capture_io_scope(owner):
                return await env.saver.aget_tuple(saved)
        finally:
            role.reset(token)

    monkeypatch.setattr(aiosqlite.Connection, "_execute", native)
    operation = asyncio.create_task(read())
    watcher = asyncio.create_task(cancelled.wait())
    facts = {"journal_mode": journal_mode}
    try:
        await asyncio.wait_for(entered.wait(), 3)
        owner.retire()
        operation.cancel()
        await asyncio.wait({operation, watcher}, timeout=0.05, return_when=asyncio.FIRST_COMPLETED)
        facts["premature_after_first_cancel"] = operation.done() or cancelled.is_set()
        operation.cancel()
        await asyncio.wait({operation, watcher}, timeout=0.05, return_when=asyncio.FIRST_COMPLETED)
        facts["premature"] = facts["premature_after_first_cancel"] or operation.done() or cancelled.is_set()
        release.set()
        result = (await asyncio.wait_for(asyncio.gather(operation, return_exceptions=True), 3))[0]
        try:
            facts["independent_commit"] = await asyncio.wait_for(independent_commit(env, 67), 3) == 67
        except aiosqlite.OperationalError:
            facts["independent_commit"] = False
        facts["cursor_closed"] = closed.is_set()
        # Assertions follow the actual independent COMMIT attempt, never precede it.
        assert facts["independent_commit"] and facts["cursor_closed"] and not facts["premature"], facts
        assert isinstance(result, asyncio.CancelledError)
    finally:
        release.set()
        watcher.cancel()
        await asyncio.gather(operation, watcher, return_exceptions=True)
        if handles and not closed.is_set():
            await execute(env.native_saver.conn, handles[0].close)
            closed.set()
        owner.retire()
        await owner.drain()
        assert await asyncio.wait_for(independent_commit(env, 71), 3) == 71


@pytest.mark.asyncio
@pytest.mark.parametrize("resource_backend", ["postgres"], indirect=True)
async def test_captured_postgres_native_read_retains_complete_cursor_call(provided_resources, monkeypatch):
    import psycopg

    from deerflow.runtime.runs.capture_io import CaptureIOOwner, capture_io_scope

    env = provided_resources
    saved = await seed_checkpoint(env.saver, "postgres-native-cancel")
    owner = CaptureIOOwner()
    entered, release, cancelled, closed = (asyncio.Event() for _ in range(4))
    role = ContextVar("capture_postgres_native_read", default=False)
    execute, close = psycopg.AsyncCursor.execute, psycopg.AsyncCursor.close
    handles = []

    async def native(cursor, query, *args, **kwargs):
        result = await execute(cursor, query, *args, **kwargs)
        if role.get() and not handles and "FROM CHECKPOINTS" in str(query).upper():
            handles.append(cursor)
            entered.set()
            try:
                await release.wait()
            except asyncio.CancelledError:
                cancelled.set()
                raise
        return result

    async def closed_cursor(cursor):
        await close(cursor)
        if handles and cursor is handles[0]:
            assert cursor.closed
            closed.set()

    async def read():
        token = role.set(True)
        try:
            with capture_io_scope(owner):
                return await env.saver.aget_tuple(saved)
        finally:
            role.reset(token)

    monkeypatch.setattr(psycopg.AsyncCursor, "execute", native)
    monkeypatch.setattr(psycopg.AsyncCursor, "close", closed_cursor)
    operation = asyncio.create_task(read())
    watcher = asyncio.create_task(cancelled.wait())
    try:
        await asyncio.wait_for(entered.wait(), 3)
        owner.retire()
        operation.cancel()
        await asyncio.wait({operation, watcher}, timeout=0.05, return_when=asyncio.FIRST_COMPLETED)
        premature_first = operation.done() or cancelled.is_set()
        operation.cancel()
        await asyncio.wait({operation, watcher}, timeout=0.05, return_when=asyncio.FIRST_COMPLETED)
        premature = premature_first or operation.done() or cancelled.is_set()
        release.set()
        result = (await asyncio.wait_for(asyncio.gather(operation, return_exceptions=True), 3))[0]
        committed = await asyncio.wait_for(independent_commit(env, 73), 3) == 73
        assert committed and closed.is_set() and not premature, {"independent_commit": committed, "cursor_closed": closed.is_set(), "premature": premature}
        assert isinstance(result, asyncio.CancelledError)
    finally:
        release.set()
        watcher.cancel()
        await asyncio.gather(operation, watcher, return_exceptions=True)
        if handles and not closed.is_set():
            await close(handles[0])
        owner.retire()
        await owner.drain()
        assert await asyncio.wait_for(independent_commit(env, 79), 3) == 79


@pytest.mark.asyncio
@pytest.mark.parametrize("resource_backend", ["memory"], indirect=True)
@pytest.mark.parametrize("mode", ["async", "sync"])
async def test_ordinary_history_closed_before_first_step_stays_exhausted(provided_saver, mode):
    env = provided_saver
    saved = await seed_checkpoint(env.saver, "closed-before-start")
    if mode == "async":
        history = env.saver.alist(saved)
        await history.aclose()
        with pytest.raises(StopAsyncIteration):
            await anext(history)
    else:
        history = env.saver.list(saved)
        history.close()
        with pytest.raises(StopIteration):
            next(history)


@pytest.mark.asyncio
@pytest.mark.parametrize("resource_backend", ["sqlite", "postgres"], indirect=True)
async def test_captured_active_history_cancellation_drains_then_propagates(provided_saver, monkeypatch):
    from deerflow.runtime.runs.capture_io import CaptureIOOwner, capture_io_scope

    env = provided_saver
    saved = await seed_checkpoint(env.saver, "cancel-active-history")
    entered, release = asyncio.Event(), asyncio.Event()
    original = type(env.native_saver).alist

    async def paused(saver, *args, **kwargs):
        async with aclosing(original(saver, *args, **kwargs)) as native:
            async for row in native:
                entered.set()  # The real native iterator has yielded with its cursor held.
                await release.wait()
                yield row

    monkeypatch.setattr(type(env.native_saver), "alist", paused)
    owner = CaptureIOOwner()
    history = env.saver.alist(saved)

    async def advance():
        with capture_io_scope(owner):
            return await anext(history)

    pending = asyncio.create_task(advance())
    try:
        await asyncio.wait_for(entered.wait(), 3)
        assert env.native_saver.lock.locked()
        owner.retire()
        pending.cancel()
        done_first, _ = await asyncio.wait({pending}, timeout=0.05)
        pending.cancel()
        done_second, _ = await asyncio.wait({pending}, timeout=0.05)
        release.set()
        result = (await asyncio.wait_for(asyncio.gather(pending, return_exceptions=True), 3))[0]
        await owner.drain()
        assert not done_first and not done_second
        assert not env.native_saver.lock.locked()
        assert await asyncio.wait_for(independent_commit(env, 83), 3) == 83
        assert isinstance(result, asyncio.CancelledError), "lifecycle retirement must not replace the caller's cancellation"
    finally:
        release.set()
        await asyncio.gather(pending, return_exceptions=True)
        await history.aclose()
        owner.retire()
        await owner.drain()


@pytest.mark.asyncio
@pytest.mark.parametrize("resource_backend", ["memory"], indirect=True)
@pytest.mark.parametrize("mode", ["sync", "async"])
async def test_failed_iterator_close_is_one_retained_fixed_outcome(provided_saver, monkeypatch, mode):
    import traceback

    from deerflow.runtime.runs.capture_io import CaptureIOCleanupError, CaptureIOOwner, capture_io_scope

    env = provided_saver
    saved = await seed_checkpoint(env.saver, "failed-history-close")
    closes = []
    marker = "private test-only close detail"
    native = type(env.native_saver)
    original = getattr(native, "list" if mode == "sync" else "alist")

    class FailingClose:
        def __init__(self, inner):
            self.inner = inner

        def __iter__(self):
            return self

        def __next__(self):
            return next(self.inner)

        def __aiter__(self):
            return self

        async def __anext__(self):
            return await anext(self.inner)

        def close(self):
            closes.append(True)
            self.inner.close()
            raise RuntimeError(marker)

        async def aclose(self):
            closes.append(True)
            await self.inner.aclose()
            raise RuntimeError(marker)

    monkeypatch.setattr(native, "list" if mode == "sync" else "alist", lambda inner, *args, **kwargs: FailingClose(original(inner, *args, **kwargs)))
    owner = CaptureIOOwner()
    with capture_io_scope(owner):
        history = env.saver.list(saved) if mode == "sync" else env.saver.alist(saved)
        assert (next(history) if mode == "sync" else await anext(history)).checkpoint["channel_values"] == {"value": "retained"}
    owner.retire()
    for _ in range(2):
        with pytest.raises(CaptureIOCleanupError) as raised:
            await owner.drain()
        assert marker not in "".join(traceback.format_exception(raised.value))
    assert closes == [True], "a completed failed native close must never be retried"


@pytest.mark.asyncio
@pytest.mark.parametrize("resource_backend", ["memory"], indirect=True)
@pytest.mark.parametrize("mode", ["sync", "async"])
async def test_primary_iterator_error_survives_secondary_cleanup_failure(provided_saver, monkeypatch, mode):
    from deerflow.runtime.runs.capture_io import CaptureIOCleanupError, CaptureIOOwner, capture_io_scope

    env = provided_saver
    saved = await seed_checkpoint(env.saver, "primary-history-error")
    native = type(env.native_saver)
    original = getattr(native, "list" if mode == "sync" else "alist")

    class Source:
        def __init__(self, inner):
            self.inner, self.first = inner, True

        def __iter__(self):
            return self

        def __next__(self):
            if not self.first:
                raise ValueError("test primary delegate failure")
            self.first = False
            return next(self.inner)

        def __aiter__(self):
            return self

        async def __anext__(self):
            if not self.first:
                raise ValueError("test primary delegate failure")
            self.first = False
            return await anext(self.inner)

        def close(self):
            self.inner.close()
            raise RuntimeError("test secondary close failure")

        async def aclose(self):
            await self.inner.aclose()
            raise RuntimeError("test secondary close failure")

    monkeypatch.setattr(native, "list" if mode == "sync" else "alist", lambda inner, *args, **kwargs: Source(original(inner, *args, **kwargs)))
    owner = CaptureIOOwner()
    try:
        with capture_io_scope(owner):
            history = env.saver.list(saved) if mode == "sync" else env.saver.alist(saved)
            assert (next(history) if mode == "sync" else await anext(history)).checkpoint["channel_values"] == {"value": "retained"}
            with pytest.raises(ValueError, match="test primary delegate failure"):
                if mode == "sync":
                    next(history)
                else:
                    await anext(history)
    finally:
        owner.retire()
        with pytest.raises(CaptureIOCleanupError):
            await owner.drain()


@pytest.mark.asyncio
@pytest.mark.parametrize("resource_backend", ["memory"], indirect=True)
@pytest.mark.parametrize("mode", ["sync", "async"])
async def test_history_cannot_borrow_original_owner_from_a_different_capture(provided_saver, mode):
    from deerflow.runtime.runs.capture_io import CaptureIOOwner, CaptureIOUnsupported, capture_io_scope

    env = provided_saver
    first = await seed_checkpoint(env.saver, "original-history-owner", value="first")
    await seed_checkpoint(env.saver, "original-history-owner", value="second", step=1)
    original, foreign = CaptureIOOwner(), CaptureIOOwner()
    history = env.saver.list(config_for("original-history-owner")) if mode == "sync" else env.saver.alist(config_for("original-history-owner"))
    try:
        with capture_io_scope(original):
            assert (next(history) if mode == "sync" else await anext(history)).checkpoint["channel_values"] == {"value": "second"}
        foreign.retire()
        with capture_io_scope(foreign):
            with pytest.raises(CaptureIOUnsupported):
                if mode == "sync":
                    next(history)
                else:
                    await anext(history)
        # Refusing another owner neither advances nor retires the original.
        with capture_io_scope(original):
            row = next(history) if mode == "sync" else await anext(history)
            assert row.config == first and row.checkpoint["channel_values"] == {"value": "first"}
    finally:
        if mode == "sync":
            history.close()
        else:
            await history.aclose()
        original.retire()
        foreign.retire()
        await original.drain()
        await foreign.drain()


@pytest.mark.asyncio
@pytest.mark.parametrize("resource_backend", ["sqlite", "postgres"], indirect=True)
async def test_idle_history_retirement_unblocks_already_admitted_native_write(provided_saver, monkeypatch):
    from deerflow.runtime.runs.capture_io import CaptureIOOwner, capture_io_scope

    env = provided_saver
    saved = await seed_checkpoint(env.saver, "idle-lock-progress")
    owner = CaptureIOOwner()
    with capture_io_scope(owner):
        history = env.saver.alist(saved)
        await anext(history)
    entered = asyncio.Event()
    actual = type(env.native_saver).aput

    async def write(saver, *args, **kwargs):
        if saver is env.native_saver:
            entered.set()
        return await actual(saver, *args, **kwargs)

    monkeypatch.setattr(type(env.native_saver), "aput", write)

    async def accepted():
        with capture_io_scope(owner):
            return await seed_checkpoint(env.saver, "idle-lock-progress", value="accepted-before-retirement", step=1)

    operation, draining = asyncio.create_task(accepted()), None
    try:
        await asyncio.wait_for(entered.wait(), 3)
        assert env.native_saver.lock.locked()
        done, _ = await asyncio.wait({operation}, timeout=0.02)
        assert not done, "the actual admitted SDK call must wait behind native history"
        owner.retire()  # Iterator close starts independently of owner.drain ordering.
        draining = asyncio.create_task(owner.drain())
        done, _ = await asyncio.wait({operation, draining}, timeout=3)
        assert len(done) == 2, "retirement must close idle history before admitted operations can drain"
        result = operation.result()
        assert (await env.saver.aget_tuple(result)).checkpoint["channel_values"] == {"value": "accepted-before-retirement"}
        assert not env.native_saver.lock.locked()
        assert await asyncio.wait_for(independent_commit(env, 89), 3) == 89
    finally:
        # A failed ordering assertion must explicitly free its real lock before
        # joining mandatory drains; timeout never cancels/detaches native work.
        await history.aclose()
        owner.retire()
        await asyncio.gather(operation, *(task for task in [draining] if task is not None), return_exceptions=True)
        await owner.drain()


@pytest.mark.asyncio
@pytest.mark.parametrize("resource_backend", ["memory"], indirect=True)
@pytest.mark.parametrize("mode", ["sync", "async"])
async def test_ordinary_valid_iterator_without_close_keeps_protocol_compatibility(provided_saver, monkeypatch, mode):
    from app.gateway.services import _RawCheckpointReadAccessor

    env = provided_saver
    await seed_checkpoint(env.saver, "ordinary-no-close", value="first")
    await seed_checkpoint(env.saver, "ordinary-no-close", value="second", step=1)
    actual = [row async for row in env.native_saver.alist(config_for("ordinary-no-close"))]

    class NoClose:
        def __init__(self):
            self.rows = iter(actual)

        def __iter__(self):
            return self

        def __next__(self):
            return next(self.rows)

        def __aiter__(self):
            return self

        async def __anext__(self):
            try:
                return next(self.rows)
            except StopIteration:
                raise StopAsyncIteration from None

    monkeypatch.setattr(type(env.native_saver), "list" if mode == "sync" else "alist", lambda *_args, **_kwargs: NoClose())
    if mode == "async":
        rows = await _RawCheckpointReadAccessor(env.saver, "full").ahistory(config_for("ordinary-no-close"), limit=1)
        assert len(rows) == 1 and rows[0].values == {"value": "second"}
    else:
        history = env.saver.list(config_for("ordinary-no-close"))
        assert next(history).checkpoint["channel_values"] == {"value": "second"}
        history.close()  # The outer optional method must not invent inner support.


@pytest.mark.asyncio
@pytest.mark.parametrize("resource_backend", ["memory"], indirect=True)
@pytest.mark.parametrize("mode", ["sync", "async"])
async def test_ordinary_iterator_close_preserves_actual_delegate_error(provided_saver, monkeypatch, mode):
    env = provided_saver
    saved = await seed_checkpoint(env.saver, "ordinary-close-error")
    row = await env.native_saver.aget_tuple(saved)

    class Source:
        def __iter__(self):
            return self

        def __next__(self):
            return row

        def __aiter__(self):
            return self

        async def __anext__(self):
            return row

        def close(self):
            raise ValueError("ordinary delegate close failed")

        async def aclose(self):
            raise ValueError("ordinary delegate close failed")

    monkeypatch.setattr(type(env.native_saver), "list" if mode == "sync" else "alist", lambda *_args, **_kwargs: Source())
    history = env.saver.list(saved) if mode == "sync" else env.saver.alist(saved)
    assert (next(history) if mode == "sync" else await anext(history)).config == saved
    with pytest.raises(ValueError, match="ordinary delegate close failed"):
        if mode == "sync":
            history.close()
        else:
            await history.aclose()
