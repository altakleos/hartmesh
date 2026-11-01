"""Targeted lifecycle retirement preserves other link workers and SQL fences."""

import asyncio
from datetime import UTC, datetime

import pytest
from _telegram_repository_test_support import add_link, bind_link
from _telegram_repository_test_support import db as db
from sqlalchemy import event, select
from sqlalchemy.exc import OperationalError
from sqlalchemy.orm import Session

from app.telegram_bots.producer import DeliveryEligibility, Outbox, OutboxSender
from deerflow.persistence.telegram_bots.model import TelegramOutboxRow

pytestmark = pytest.mark.asyncio


# Liveness, not speed: SQLite workers can be slow to start on a loaded runner.
BOUND = 15


async def build(db, *, swallow_cancel=False):
    first = await bind_link(db, await add_link(db, bot=9_000_000_001))
    other = await bind_link(db, await add_link(db, bot=9_000_000_002), account=7_000_000_002)
    async with db.sessions.begin() as session:
        for row in await session.scalars(select(TelegramOutboxRow)):
            row.state = "sent"
    outbox = Outbox(db.sessions, clock=lambda: datetime(2030, 1, 1, tzinfo=UTC))
    identities = {link.id: await outbox.plan_product(link, source_id="retirement", parts=[{"kind": "text", "source_ref": {"key": "hello.privacy", "values": {}}}]) for link in (first, other)}
    entered = {link.id: asyncio.Event() for link in (first, other)}
    releases = {link.id: asyncio.Event() for link in (first, other)}
    cancelled = {link.id: asyncio.Event() for link in (first, other)}

    async def raw(*, link_id, **scope):
        entered[link_id].set()
        try:
            await releases[link_id].wait()
        except asyncio.CancelledError:
            cancelled[link_id].set()
            if not swallow_cancel:
                raise
            await releases[link_id].wait()
        return [501]

    async def allowed(link, row, part):
        return DeliveryEligibility.allow()

    sender = OutboxSender(outbox, run_id="retirement-process", deliver_part=raw, eligibility=allowed)
    await sender.tick()
    await asyncio.wait_for(asyncio.gather(*(value.wait() for value in entered.values())), BOUND)
    return first, other, identities, entered, releases, cancelled, sender


async def saved(db, identity):
    async with db.sessions() as session:
        return await session.get(TelegramOutboxRow, identity)


async def test_retire_one_link_drains_its_worker_without_waiting_for_other_link(db):
    first, other, identities, entered, releases, cancelled, sender = await build(db)
    try:
        await asyncio.wait_for(sender.retire_link(first.id), BOUND)
        a, b = await saved(db, identities[first.id]), await saved(db, identities[other.id])
        assert a.state == "pending" and a.claim_owner is None and a.claim_id is None
        assert a.parts[0]["state"] == "pending" and a.parts[0]["message_ids"] == []
        assert cancelled[first.id].is_set() and not cancelled[other.id].is_set()
        assert b.state == "sending" and b.claim_owner == "retirement-process"
        releases[other.id].set()
        await sender.drain()
        assert (await saved(db, identities[other.id])).state == "sent"
        await sender.retire_link(first.id)  # idempotent without a worker
    finally:
        for release in releases.values():
            release.set()
        await sender.stop()


async def test_retirement_invalidates_receipt_before_transport_swallows_cancellation(db):
    first, other, identities, entered, releases, cancelled, sender = await build(db, swallow_cancel=True)
    task = asyncio.create_task(sender.retire_link(first.id))
    try:
        await asyncio.wait_for(cancelled[first.id].wait(), BOUND)
        row = await saved(db, identities[first.id])
        assert row.state == "pending" and row.claim_owner is None and row.claim_id is None
        assert not task.done()
        # While the old transport remains live, neither tick nor retirement may
        # admit a replacement worker for its pending row.
        assert await sender.tick() == 0
        releases[first.id].set()
        await asyncio.wait_for(task, BOUND)
        row = await saved(db, identities[first.id])
        assert row.state == "pending" and row.parts[0]["message_ids"] == [] and row.attempts == 0
        assert not cancelled[other.id].is_set()
    finally:
        for release in releases.values():
            release.set()
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        await sender.stop()


async def test_cancelled_retirement_caller_keeps_owned_cleanup_until_sender_stop(db):
    first, other, identities, entered, releases, cancelled, sender = await build(db, swallow_cancel=True)
    task = asyncio.create_task(sender.retire_link(first.id))
    stopping = None
    try:
        await asyncio.wait_for(cancelled[first.id].wait(), BOUND)
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        assert task.cancelled()
        stopping = asyncio.create_task(sender.stop())
        await asyncio.wait_for(cancelled[other.id].wait(), BOUND)
        assert not stopping.done()
        for release in releases.values():
            release.set()
        await asyncio.wait_for(stopping, BOUND)
        for identity in identities.values():
            row = await saved(db, identity)
            assert row.state == "pending" and row.claim_id is None and row.parts[0]["message_ids"] == []
    finally:
        for release in releases.values():
            release.set()
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        if stopping is not None:
            await asyncio.gather(stopping, return_exceptions=True)
        await sender.stop()


async def test_failed_retirement_commit_keeps_live_claim_until_retry_commits(db):
    first, other, identities, entered, releases, cancelled, sender = await build(db)
    before = await saved(db, identities[first.id])
    failures = []

    def fail_fence(session):
        if session.get_bind() is not db.engine.sync_engine:
            return
        row = next((row for row in session.identity_map.values() if isinstance(row, TelegramOutboxRow) and row.id == identities[first.id]), None)
        if row is not None and row.state == "pending":
            failures.append(row.id)
            raise OperationalError("synthetic retirement fence unavailable", {}, None)

    event.listen(Session, "before_commit", fail_fence)
    try:
        with pytest.raises(OperationalError):
            await sender.retire_link(first.id)
        row = await saved(db, identities[first.id])
        assert failures == [row.id]
        assert row.state == "sending" and row.claim_id == before.claim_id
        assert not cancelled[first.id].is_set() and not cancelled[other.id].is_set()
        assert await sender.tick() == 0
    finally:
        event.remove(Session, "before_commit", fail_fence)
    try:
        # Concurrent lifecycle callers share one retained retirement task.
        await asyncio.wait_for(asyncio.gather(sender.retire_link(first.id), sender.retire_link(first.id)), BOUND)
        row = await saved(db, identities[first.id])
        assert row.state == "pending" and row.claim_id is None
        assert cancelled[first.id].is_set() and not cancelled[other.id].is_set()
    finally:
        for release in releases.values():
            release.set()
        await sender.stop()


@pytest.mark.parametrize("identity", ["", None, True])
async def test_retire_link_requires_exact_nonempty_identity(db, identity):
    async def raw(**scope):
        raise AssertionError("No dispatch expected")

    async def allowed(link, row, part):
        return DeliveryEligibility.allow()

    sender = OutboxSender(Outbox(db.sessions), run_id="validation-process", deliver_part=raw, eligibility=allowed)
    with pytest.raises(ValueError, match="link"):
        await sender.retire_link(identity)
    await sender.stop()


@pytest.mark.parametrize("cancel_stop_caller", [False, True])
async def test_stop_fence_failure_still_drains_live_transport_before_raising(db, cancel_stop_caller):
    link = await bind_link(db, await add_link(db))
    async with db.sessions.begin() as session:
        for row in await session.scalars(select(TelegramOutboxRow)):
            row.state = "sent"
    now = datetime(2030, 1, 1, tzinfo=UTC)
    outbox = Outbox(db.sessions, clock=lambda: now)
    identity = await outbox.plan_product(link, source_id="failed-stop", parts=[{"kind": "text", "source_ref": {"key": "hello.privacy", "values": {}}} for _ in range(2)])
    entered, released, cancelled, faulted = asyncio.Event(), asyncio.Event(), asyncio.Event(), asyncio.Event()
    calls = []

    async def raw(**scope):
        nonlocal now
        calls.append(scope["part_id"])
        entered.set()
        try:
            await released.wait()
        except asyncio.CancelledError:
            cancelled.set()
            await released.wait()  # a dispatched SDK call may still return its ACK
        now = now.replace(second=now.second + 1)
        return [501 + len(calls)]

    async def allowed(link, row, part):
        return DeliveryEligibility.allow()

    def fail_fence(session):
        if session.get_bind() is not db.engine.sync_engine:
            return
        row = next((row for row in session.identity_map.values() if isinstance(row, TelegramOutboxRow) and row.id == identity), None)
        if row is not None and row.state == "pending":
            faulted.set()
            raise OperationalError("synthetic stop fence unavailable", {}, None)

    sender = OutboxSender(outbox, run_id="failed-stop", deliver_part=raw, eligibility=allowed)
    stopping = None
    try:
        await sender.tick()
        await asyncio.wait_for(entered.wait(), BOUND)
        event.listen(Session, "before_commit", fail_fence)
        stopping = asyncio.create_task(sender.stop())
        await asyncio.wait_for(faulted.wait(), BOUND)
        await asyncio.wait_for(cancelled.wait(), BOUND)
        assert not stopping.done()  # failure cannot abandon the live SDK waiter
        if cancel_stop_caller:
            stopping.cancel()
            await asyncio.sleep(0)
            stopping.cancel()
            await asyncio.sleep(0)
            assert not stopping.done()
        released.set()
        with pytest.raises(asyncio.CancelledError if cancel_stop_caller else OperationalError):
            await asyncio.wait_for(stopping, BOUND)
        assert len(calls) == 1  # no next part after stop, even if the first ACK returns
        row = await saved(db, identity)
        assert row.parts[0]["message_ids"] == [502] and row.parts[1]["message_ids"] == []
        assert await sender.tick() == 0
    finally:
        event.remove(Session, "before_commit", fail_fence)
        released.set()
        if stopping is not None:
            await asyncio.gather(stopping, return_exceptions=True)
        await sender.stop()


async def test_one_stop_fence_failure_does_not_skip_cancelling_other_link_workers(db):
    first, other, identities, entered, releases, cancelled, sender = await build(db)

    def fail_first_fence(session):
        if session.get_bind() is not db.engine.sync_engine:
            return
        row = next((row for row in session.identity_map.values() if isinstance(row, TelegramOutboxRow) and row.id == identities[first.id]), None)
        if row is not None and row.state == "pending":
            raise OperationalError("synthetic first-link stop fence unavailable", {}, None)

    event.listen(Session, "before_commit", fail_first_fence)
    try:
        with pytest.raises(OperationalError):
            await asyncio.wait_for(sender.stop(), BOUND)
        assert cancelled[first.id].is_set() and cancelled[other.id].is_set()
        assert await sender.tick() == 0
        for identity in identities.values():
            assert (await saved(db, identity)).parts[0]["message_ids"] == []
    finally:
        event.remove(Session, "before_commit", fail_first_fence)
        for release in releases.values():
            release.set()
        await sender.stop()
