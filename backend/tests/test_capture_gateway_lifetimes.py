"""Gateway consumers close actual native resources before returning or raising."""

import asyncio
import sqlite3
from types import SimpleNamespace

import aiosqlite
import pytest
from langgraph.checkpoint.base import CheckpointTuple, empty_checkpoint
from langgraph.checkpoint.sqlite.aio import AsyncSqliteSaver

from app.gateway.services import _RawCheckpointReadAccessor
from deerflow.runtime.checkpoint_mode import CHECKPOINT_MODE_METADATA_KEY, CheckpointModeMismatchError


class RetainedNativeHistory:
    """Keep the real iterator reachable, as an operation registry must do.

    Resource ownership cannot depend on a consumer's reference counting or a
    later asynchronous-generator garbage-collection hook.
    """

    def __init__(self, inner):
        self.inner = inner
        self.iterator = None

    def alist(self, *args, **kwargs):
        self.iterator = self.inner.alist(*args, **kwargs)
        return self.iterator

    async def aget_tuple(self, *args, **kwargs):
        return await self.inner.aget_tuple(*args, **kwargs)


@pytest.mark.asyncio
async def test_ordinary_history_supports_public_async_iterator_without_close():
    config = {"configurable": {"thread_id": "ordinary-history", "checkpoint_ns": ""}}
    checkpoint = empty_checkpoint()
    item = CheckpointTuple(config, checkpoint, {}, None, [])

    class PublicIterator:
        def __init__(self):
            self.items = iter([item])

        def __aiter__(self):
            return self

        async def __anext__(self):
            try:
                return next(self.items)
            except StopIteration:
                raise StopAsyncIteration from None

    saver = SimpleNamespace(alist=lambda *args, **kwargs: PublicIterator())
    rows = await _RawCheckpointReadAccessor(saver, "full").ahistory(config, limit=1)
    assert len(rows) == 1 and rows[0].config == config and rows[0].created_at == checkpoint["ts"]


@pytest.mark.asyncio
@pytest.mark.parametrize("journal_mode", ["DELETE", "WAL"])
@pytest.mark.parametrize("exit_kind", ["limit", "mode_error", "exhaustion", "zero"])
async def test_raw_history_closes_native_iterator_at_its_return_boundary(tmp_path, monkeypatch, journal_mode, exit_kind):
    path = tmp_path / "history.sqlite"
    config = {"configurable": {"thread_id": "history-owner", "checkpoint_ns": ""}}
    async with AsyncSqliteSaver.from_conn_string(str(path)) as saver:
        await saver.setup()
        for number in range(2):
            checkpoint = empty_checkpoint()
            checkpoint["id"] = f"checkpoint-{number}"
            metadata = {"source": "input", "step": number, "writes": {}}
            if exit_kind == "mode_error":
                metadata[CHECKPOINT_MODE_METADATA_KEY] = "delta"
            await saver.aput(config, checkpoint, metadata, {})
        async with saver.conn.execute("PRAGMA journal_mode=" + journal_mode) as cursor:
            assert (await cursor.fetchone())[0].upper() == journal_mode
        await saver.conn.commit()

        created, closed = [], []
        original_init, original_close = aiosqlite.Cursor.__init__, aiosqlite.Cursor.close

        def observe_init(cursor, connection, native_cursor):
            original_init(cursor, connection, native_cursor)
            if connection is saver.conn:
                created.append(cursor)

        async def observe_close(cursor):
            # The real driver's close has completed on its worker thread.
            await original_close(cursor)
            if cursor in created:
                closed.append(cursor)

        monkeypatch.setattr(aiosqlite.Cursor, "__init__", observe_init)
        monkeypatch.setattr(aiosqlite.Cursor, "close", observe_close)
        retained = RetainedNativeHistory(saver)
        accessor = _RawCheckpointReadAccessor(retained, "full")
        try:
            if exit_kind == "mode_error":
                with pytest.raises(CheckpointModeMismatchError):
                    await accessor.ahistory(config)
            else:
                limit = 1 if exit_kind == "limit" else 0 if exit_kind == "zero" else None
                rows = await accessor.ahistory(config, limit=limit)
                assert len(rows) == {"limit": 1, "exhaustion": 2, "zero": 0}[exit_kind]

            def independent_commit():
                with sqlite3.connect(path, timeout=1) as writer:
                    writer.execute("CREATE TABLE writer_health (value INTEGER)")
                    writer.execute("INSERT INTO writer_health VALUES (1)")
                    writer.commit()
                    return writer.execute("SELECT value FROM writer_health").fetchone()[0]

            # This is a real COMMIT/readback health control in both modes,
            # not a claim that the retained cursors block an external writer.
            assert await asyncio.to_thread(independent_commit) == 1
            expected = 0 if exit_kind == "zero" else 2
            assert len(created) == len(closed) == expected, f"Native cursors: {len(created)} created, {len(closed)} completed closes"
            assert not saver.lock.locked(), "Returned history must release the actual saver lock"
        finally:
            if retained.iterator is not None:
                await retained.iterator.aclose()
