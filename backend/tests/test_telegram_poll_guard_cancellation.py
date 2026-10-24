"""Retirement drains the poller's real SQLite guard before closing its owner."""

import asyncio
from types import SimpleNamespace

import aiosqlite
import pytest
import pytest_asyncio
from _telegram_bot_test_support import update_payload
from _telegram_repository_test_support import TABLES, bind_link
from sqlalchemy import event, text
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from test_telegram_copy_guard_supervisor import environment, owned_link, stop_environment, wait_until
from test_telegram_supervisor import _supervisor_logging as _supervisor_logging

from app.telegram_bots.config import TelegramBotsConfig
from app.telegram_bots.links import BotLinkRepository
from app.telegram_bots.settings import SettingsRepository
from deerflow.persistence.base import Base
from deerflow.persistence.channel_connections.sql import ChannelConnectionRepository

pytestmark = pytest.mark.asyncio


@pytest_asyncio.fixture
async def sqlite_links(tmp_path):
    engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path / 'poll-guard.sqlite'}")

    @event.listens_for(engine.sync_engine, "connect")
    def pragmas(connection, record):
        connection.execute("PRAGMA foreign_keys=ON")
        connection.execute("PRAGMA busy_timeout=1000")

    try:
        async with engine.begin() as connection:
            await connection.execute(text("BEGIN"))
            await connection.run_sync(lambda sync: Base.metadata.create_all(sync, tables=TABLES))
        sessions = async_sessionmaker(engine, expire_on_commit=False)
        config = TelegramBotsConfig()
        connections = ChannelConnectionRepository(sessions)
        yield SimpleNamespace(engine=engine, sessions=sessions, connections=connections, config=config, settings=SettingsRepository(sessions, config), links=BotLinkRepository(sessions, connections, config))
    finally:
        await engine.dispose()


@pytest.mark.parametrize("transactional", [False, True], ids=["owner-read", "locked-read"])
async def test_retirement_drains_native_select_cursor_before_independent_writer(sqlite_links, monkeypatch, transactional):
    db = sqlite_links
    first_reply, release_sql, entered_sql, cursor_cancelled, cursor_closed = (asyncio.Event() for _ in range(5))
    accepted = []

    def script(link, normal, updates):
        updates.enqueue("getUpdates", result=[update_payload(83)], release=first_reply)
        updates.enqueue("getUpdates", result=[], release=asyncio.Event())

    async def handler(link, update):
        accepted.append(update.update_id)
        return True

    e = environment(db, monkeypatch, script=script, handler=handler)
    link, token = await owned_link(db, e, bot=9_000_003_001)
    link = await bind_link(db, link)
    retiring = None
    try:
        await e.supervisor.check()
        await e.supervisor.start()
        transport = e.channels.created[link.id]
        await wait_until(lambda: len(transport.updates.calls) == 1)
        poll = e.supervisor.poll_task(link.id)
        runner = e.supervisor._runners[link.id]
        guarded_tasks, native_cursor = set(), []
        refresh = transport.channel.refresh_link

        async def guarded_refresh():
            task = asyncio.current_task()
            guarded_tasks.add(task)
            try:
                return await refresh()
            finally:
                guarded_tasks.discard(task)

        monkeypatch.setattr(transport.channel, "refresh_link", guarded_refresh)
        execute = aiosqlite.Connection._execute

        async def native_execute(connection, function, *args, **kwargs):
            result = await execute(connection, function, *args, **kwargs)
            if native_cursor and getattr(function, "__name__", "") == "close" and getattr(function, "__self__", None) is native_cursor[0]:
                cursor_closed.set()
            if not native_cursor and asyncio.current_task() in guarded_tasks and getattr(function, "__name__", "") == "execute" and args and str(args[0]).lstrip().upper().startswith("SELECT") and connection.in_transaction is transactional:
                native_cursor.append(function.__self__)
                entered_sql.set()
                try:
                    await release_sql.wait()
                except asyncio.CancelledError:
                    cursor_cancelled.set()
                    raise
            return result

        monkeypatch.setattr(aiosqlite.Connection, "_execute", native_execute)
        first_reply.set()
        await asyncio.wait_for(entered_sql.wait(), 3)
        counts = (poll.record.polls, poll.record.successes, poll.record.conflicts)
        retiring = asyncio.create_task(e.supervisor._retire(link.id, reason="guard-test"))
        await wait_until(lambda: runner.cancelling() > 0)
        runner.cancel()
        await asyncio.sleep(0)
        assert not cursor_cancelled.is_set(), "Retirement interrupted the live native SELECT cursor"
        assert not retiring.done()
        assert not cursor_closed.is_set()
        release_sql.set()
        await asyncio.wait_for(retiring, 3)
        assert cursor_closed.is_set()
        # Acquisition alone does not prove release of a SQLite read statement:
        # a real independent write must also COMMIT after retirement returns.
        saved = await asyncio.wait_for(db.links.update_if_current(link.id, token_revision=link.token_revision, last_error="GuardDrained"), 3)
        assert saved.last_error == "GuardDrained"
        assert (await db.links.get(link.id)).last_error == "GuardDrained"
        assert (poll.record.polls, poll.record.successes, poll.record.conflicts) == counts
        assert poll.offset is None and accepted == []
        assert len(transport.updates.calls) == 1
        assert transport.normal.closed and transport.updates.closed
    finally:
        first_reply.set()
        release_sql.set()
        if retiring is not None:
            await asyncio.gather(retiring, return_exceptions=True)
        await stop_environment(e)
