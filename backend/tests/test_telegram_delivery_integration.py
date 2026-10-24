"""Durable inputs and independently scheduled deliveries share one completion truth."""

import asyncio
from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest
from _telegram_repository_test_support import add_link, bind_link, rows
from _telegram_repository_test_support import db as db
from sqlalchemy import select, text
from telegram import Chat, Message, Update, User

from app.telegram_bots.inbox import InboxRepository
from app.telegram_bots.links import BotLink
from app.telegram_bots.producer import DeliveryEligibility, Outbox, OutboxSender
from deerflow.persistence.telegram_bots.model import TelegramBotLinkRow, TelegramInboundMarkRow, TelegramOutboxRow

pytestmark = pytest.mark.asyncio


class Clock:
    def __init__(self):
        self.now = datetime(2030, 1, 1, tzinfo=UTC)

    def __call__(self):
        return self.now

    def advance(self, seconds=2):
        self.now += timedelta(seconds=seconds)


async def eligible(link, item, part):
    return DeliveryEligibility.allow()


def part():
    return {"kind": "text", "source_ref": {"key": "chat.restarted", "values": {"product": "HartMesh"}}}


async def snapshot(db, cls, identity):
    async with db.sessions() as session:
        return await session.get(cls, identity)


async def receiver(db, calls):
    async def deliver(*, link_id, outbox_id, part_id, claim_owner, claim_id):
        item = await snapshot(db, TelegramOutboxRow, outbox_id)
        saved = next(value for value in item.parts if value["part_id"] == part_id)
        assert item.state == "sending" and saved["state"] == "sending"
        assert (item.link_id, item.claim_owner, item.claim_id) == (link_id, claim_owner, claim_id)
        calls.append((outbox_id, part_id))
        return [1000 + len(calls)]

    return deliver


async def flush(db, sender, clock):
    for _ in range(12):
        await sender.tick()
        await sender.drain()
        pending = [item for item in await rows(db, TelegramOutboxRow) if item.state not in {"sent", "skipped"}]
        if not pending:
            return
        clock.advance()
    pytest.fail("saved deliveries did not settle within bounded scheduler advances")


async def setup(db):
    link = await bind_link(db, await add_link(db))
    clock, calls = Clock(), []
    inbox = InboxRepository(db.sessions)
    outbox = Outbox(db.sessions, inbox=inbox, clock=clock)
    sender = OutboxSender(outbox, run_id=uuid4().hex, deliver_part=await receiver(db, calls), eligibility=eligible, jitter=lambda: 0)
    return link, clock, calls, inbox, outbox, sender


async def turn(inbox, link):
    value = Update(202, message=Message(102, datetime(2030, 1, 1, tzinfo=UTC), Chat(link.chat_id, "private"), from_user=User(link.telegram_user_id, "Fixture", False), text="synthetic input"))
    mark, _ = await inbox.accept(link, value, sanitized_input={"text": "synthetic input"})
    mark = await inbox.route(mark, kind="chat", target_id="original-thread", basis={"thread_id": "original-thread"}, operation_ids=[uuid4().hex])
    turn_id = uuid4().hex
    await inbox.bind_turn([mark], turn_id=turn_id, thread_id="original-thread")
    mark, _ = await inbox.accept(link, value, sanitized_input={"text": "synthetic input"})
    publication = mark.route_basis["current_publication_id"]
    await inbox.bind_run(turn_id=turn_id, publication_id=publication, run_id="original-run")
    return mark, turn_id, {"thread_id": "original-thread", "run_id": "original-run", "publication_id": publication, "assistant_message_id": "exact-assistant"}


async def writer(db, link):
    session = db.sessions()
    await session.begin()
    if db.dialect == "sqlite":
        await session.execute(text("BEGIN IMMEDIATE"))
    await session.scalar(select(TelegramBotLinkRow).where(TelegramBotLinkRow.id == link.id).with_for_update())
    return session


async def test_existing_binding_welcome_plan_is_consumed_unchanged(db):
    link, clock, calls, inbox, outbox, sender = await setup(db)
    welcome = (await rows(db, TelegramOutboxRow))[0]
    part_ids = [value["part_id"] for value in welcome.parts]
    assert (await rows(db, TelegramInboundMarkRow))[0].state == "reply_pending"
    await flush(db, sender, clock)
    saved = await snapshot(db, TelegramOutboxRow, welcome.id)
    mark = (await rows(db, TelegramInboundMarkRow))[0]
    assert saved.state == "sent" and [value["part_id"] for value in saved.parts] == part_ids
    assert calls == [(welcome.id, identity) for identity in part_ids]
    assert all(value["message_ids"] for value in saved.parts)
    assert mark.route_complete and mark.state == "done" and mark.closed_at is not None


async def test_delivery_uses_public_detached_link_snapshot_without_credentials(db):
    link = await add_link(db)
    async with db.sessions() as session:
        persisted = await session.get(TelegramBotLinkRow, link.id)
        persisted.notices = {"nested": {"ids": [1]}}
        captured = BotLink.from_row(persisted)
        assert captured.id == link.id and captured.created_at == link.created_at
        assert "encrypted_token" not in vars(captured)
        captured.notices["nested"]["ids"].append(2)
        assert persisted.notices == {"nested": {"ids": [1]}}


async def test_primary_reply_cannot_complete_an_input_with_a_pending_auxiliary_plan(db):
    link, clock, calls, inbox, outbox, sender = await setup(db)
    await flush(db, sender, clock)
    mark, turn_id, source = await turn(inbox, link)
    # Deliberately retain this snapshot across the auxiliary plan. Registration
    # of the final reply must merge the current SQL references, not overwrite it.
    async with await writer(db, link) as session:
        assert await outbox.add(link, dedupe_key="notice:auxiliary", class_="reply", payload={"parts": [part()]}, not_before=clock() + timedelta(days=1), session=session)
        auxiliary = await session.scalar(select(TelegramOutboxRow).where(TelegramOutboxRow.dedupe_key == "notice:auxiliary"))
        auxiliary_id = auxiliary.id
        await inbox.register_delivery([mark], outbox_id=auxiliary_id, session=session)
        await session.commit()
    primary_id = await outbox.plan_reply(link, turn_id=turn_id, source=source, parts=[part()], inbox_rows=[mark])
    clock.advance()  # Respect the preceding welcome's per-link pacing window.
    await sender.tick()
    await sender.drain()
    current = await snapshot(db, TelegramInboundMarkRow, (link.id, mark.message_id, mark.chat_id))
    assert current.route_complete and current.state == "reply_pending" and current.closed_at is None
    assert set(current.delivery_ids) == {auxiliary_id, primary_id}
    assert (await snapshot(db, TelegramOutboxRow, primary_id)).state == "sent"
    clock.advance(86_400)
    await flush(db, sender, clock)
    assert (await snapshot(db, TelegramInboundMarkRow, (link.id, mark.message_id, mark.chat_id))).state == "done"


async def test_caller_rollback_reverts_final_plan_and_input_completion_together(db):
    link, clock, calls, inbox, outbox, sender = await setup(db)
    await flush(db, sender, clock)
    mark, turn_id, source = await turn(inbox, link)
    async with await writer(db, link) as session:
        delivery_id = await outbox.plan_reply(link, turn_id=turn_id, source=source, parts=[part()], inbox_rows=[mark], session=session)
        assert (await session.get(TelegramInboundMarkRow, (link.id, mark.message_id, mark.chat_id))).route_complete
        await session.rollback()
    assert await snapshot(db, TelegramOutboxRow, delivery_id) is None
    current = await snapshot(db, TelegramInboundMarkRow, (link.id, mark.message_id, mark.chat_id))
    assert not current.route_complete and current.outbox_id is None and current.delivery_ids == []


async def test_conflict_allows_dispatched_receipt_but_holds_further_parts(db):
    link, clock, calls, inbox, outbox, sender = await setup(db)
    await flush(db, sender, clock)
    await outbox.add(link, dedupe_key="notice:conflict", class_="reply", payload={"parts": [part(), part()]})
    clock.advance()
    entered, release = asyncio.Event(), asyncio.Event()
    delivered = []

    async def delayed(**scope):
        delivered.append(scope["part_id"])
        entered.set()
        await release.wait()
        return [2000 + len(delivered)]

    sender = OutboxSender(outbox, run_id=uuid4().hex, deliver_part=delayed, eligibility=eligible, jitter=lambda: 0)
    assert await sender.tick() == 1
    await asyncio.wait_for(entered.wait(), timeout=5)
    try:
        await db.links.set_health(link.id, "conflict", holds=True, transition={"conflict_since": clock(), "probe_until": clock() + timedelta(days=1), "webhook_host": None})
    finally:
        release.set()
        await sender.drain()
    saved = next(item for item in await rows(db, TelegramOutboxRow) if item.dedupe_key == "notice:conflict")
    assert saved.parts[0]["state"] == "sent" and saved.parts[1]["state"] == "pending"
    clock.advance()
    assert await sender.tick() == 0 and len(delivered) == 1
    await db.links.set_health(link.id, "conflict", holds=False)
    await sender.tick()
    await sender.drain()
    assert len(delivered) == 2
    assert (await snapshot(db, TelegramOutboxRow, saved.id)).state == "sent"
