"""Shutdown retains real reader sessions until their SQL cleanup completes."""

import asyncio

import pytest
from _telegram_repository_test_support import db as db
from sqlalchemy.ext.asyncio import AsyncSession
from test_telegram_supervisor import _supervisor_logging as _supervisor_logging
from test_telegram_supervisor import build

pytestmark = pytest.mark.asyncio


@pytest.mark.parametrize("reader", ["sender", "snapshot", "links", "settings"])
@pytest.mark.parametrize("phase", ["body", "close"])
async def test_shutdown_waits_for_reader_close(db, monkeypatch, reader, phase):
    host = build(db, monkeypatch).supervisor
    await host.check()
    entered, release = asyncio.Event(), asyncio.Event()
    queried, release_query = asyncio.Event(), asyncio.Event()
    original = AsyncSession.close
    retained = []

    async def held_close(session):
        if session.bind is db.engine and session.in_transaction() and not retained:
            retained.append(asyncio.current_task())
            entered.set()
            await release.wait()
        await original(session)

    monkeypatch.setattr(AsyncSession, "close", held_close)
    if phase == "body":

        def hold_query(original_query):
            async def query(session, *args, **kwargs):
                result = await original_query(session, *args, **kwargs)
                if session.bind is db.engine and not queried.is_set():
                    queried.set()
                    await release_query.wait()
                return result

            return query

        for method in ("get", "execute"):
            monkeypatch.setattr(AsyncSession, method, hold_query(getattr(AsyncSession, method)))
    operation = {
        "sender": host.sender.tick,
        "snapshot": lambda: host.sender._snapshot("absent-link", "absent-delivery"),
        "links": host.links.list_all,
        "settings": lambda: host.settings.get("absent-owner"),
    }[reader]
    owner = asyncio.create_task(operation(), name="telegram-retained-reader")
    host._background.add(owner)
    owner.add_done_callback(host._background.discard)
    stopping = None
    try:
        await asyncio.wait_for((queried if phase == "body" else entered).wait(), 10)
        assert db.engine.sync_engine.pool.checkedout() == 1
        stopping = asyncio.create_task(host.begin_shutdown())
        await asyncio.wait_for(entered.wait(), 10)
        done, _ = await asyncio.wait({stopping}, timeout=0.1)
        assert owner.cancelling(), "shutdown did not cancel its reader owner"
        assert not done, "shutdown abandoned a reader's unfinished SQL close"
        owner.cancel()
        done, _ = await asyncio.wait({stopping}, timeout=0.1)
        assert not done, "repeated cancellation abandoned the same SQL close"
        release.set()
        await asyncio.wait_for(stopping, 10)
        assert owner.cancelled()
        assert db.engine.sync_engine.pool.checkedout() == 0
    finally:
        release.set()
        release_query.set()
        await asyncio.gather(owner, *([stopping] if stopping is not None else []), return_exceptions=True)
        # Join the exact close task on the unfixed path as well, so the red
        # regression never abandons its own connection during fixture teardown.
        if retained:
            await asyncio.gather(*retained, return_exceptions=True)
        await host.stop()
