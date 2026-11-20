"""Gateway shutdown retains a committed writer until its pool check-in."""

import asyncio
from contextvars import ContextVar

import pytest
from _telegram_repository_test_support import add_link, bind_link
from _telegram_repository_test_support import db as db
from sqlalchemy import event, select
from sqlalchemy.orm import Session
from sqlalchemy.util import await_only
from test_telegram_supervisor import _supervisor_logging as _supervisor_logging
from test_telegram_supervisor import build

from deerflow.persistence.telegram_bots.model import TelegramOutboxRow

pytestmark = pytest.mark.asyncio


@pytest.mark.parametrize("job", ["reconcile", "sender"])
async def test_shutdown_owns_transaction_pool_reset(db, monkeypatch, job):
    host = build(db, monkeypatch).supervisor
    await host.check()
    link = await bind_link(db, await add_link(db))
    async with db.sessions.begin() as writer:
        delivery = await writer.scalar(select(TelegramOutboxRow).where(TelegramOutboxRow.link_id == link.id))
        delivery.state, delivery.claim_owner, delivery.claim_id = "sending", "earlier-process", "earlier-claim"
    entered, release = asyncio.Event(), asyncio.Event()
    inside_job = ContextVar("telegram_writer_job", default=False)
    committed = False

    def after_commit(session):
        nonlocal committed
        if inside_job.get() and session.bind is db.engine.sync_engine:
            committed = True

    event.listen(Session, "after_commit", after_commit)
    dialect = db.engine.sync_engine.dialect
    original = dialect.do_rollback

    async def wait_for_release():
        entered.set()
        await release.wait()

    def rollback(connection):
        # Even after a commit the pool resets the DBAPI connection before
        # checking it in. Hold this reset while the supervisor stops its job.
        if inside_job.get() and committed and not entered.is_set():
            await_only(wait_for_release())
        return original(connection)

    monkeypatch.setattr(dialect, "do_rollback", rollback)

    async def operation():
        inside_job.set(True)
        if job == "reconcile":
            return await host.links.set_health(link.id, "agent_unavailable", holds=True, expected_token_revision=link.token_revision)
        return await host.sender.recover_prior_claims()

    owner = asyncio.create_task(operation(), name="telegram-" + job)
    host._background.add(owner)
    owner.add_done_callback(host._background.discard)
    stopping = None
    try:
        await asyncio.wait_for(entered.wait(), 10)
        assert db.engine.sync_engine.pool.checkedout() == 1
        stopping = asyncio.create_task(host.begin_shutdown())
        await asyncio.sleep(0)
        assert owner.cancelling()
        owner.cancel()
        done, _ = await asyncio.wait({stopping}, timeout=0.05)
        assert not done, "the writer abandoned its post-commit pool reset"
        release.set()
        await asyncio.wait_for(stopping, 10)
        assert owner.cancelled()
        assert db.engine.sync_engine.pool.checkedout() == 0
        if job == "reconcile":
            assert (await host.links.get(link.id)).health == "agent_unavailable"
        else:
            async with db.sessions() as session:
                recovered = await session.get(TelegramOutboxRow, delivery.id)
                assert recovered.state == "pending" and recovered.claim_owner is None
    finally:
        release.set()
        event.remove(Session, "after_commit", after_commit)
        await asyncio.gather(owner, *([stopping] if stopping else []), return_exceptions=True)
        await host.stop()


async def test_cancelled_writer_rolls_back_and_a_caught_error_does_not_undo_a_later_write(db):
    from app.telegram_bots.sql_writers import writer_session
    from deerflow.persistence.telegram_bots.model import TelegramBotLinkRow

    link = await add_link(db)
    began = asyncio.Event()

    async def write():
        async with writer_session(db.sessions) as session:
            row = await session.get(TelegramBotLinkRow, link.id)
            row.health = "agent_unavailable"
            await session.flush()
            began.set()
            await asyncio.Event().wait()

    task = asyncio.create_task(write())
    try:
        await asyncio.wait_for(began.wait(), 10)
        task.cancel()
        await task
    except asyncio.CancelledError:
        assert (await db.links.get(link.id)).health == "ok"
        async with writer_session(db.sessions) as session:
            row = await session.get(TelegramBotLinkRow, link.id)
            row.health = "agent_unavailable"
    finally:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
    assert (await db.links.get(link.id)).health == "agent_unavailable"
    assert db.engine.sync_engine.pool.checkedout() == 0
