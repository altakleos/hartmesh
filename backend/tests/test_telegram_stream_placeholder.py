"""Actual SQL placeholder retirement and live sender claim fences."""

import asyncio

import pytest
from _telegram_repository_test_support import db as db  # noqa: F401
from sqlalchemy import select
from test_telegram_keyboard_delivery import keyboard_environment
from test_telegram_publication_custody import publication_environment as publication_environment  # noqa: F401

from app.telegram_bots.producer import DeliveryEligibility, Outbox, OutboxSender
from deerflow.persistence.telegram_bots.model import TelegramCallbackRow, TelegramOutboxRow


async def placeholder(db, monkeypatch):
    link, callbacks, guard = await keyboard_environment(db, monkeypatch)
    box = Outbox(db.sessions)
    identity = await box.plan_product_keyboard(
        link,
        source_id="chat-stream:" + "a" * 32 + ":0",
        parts=[{"kind": "text", "source_ref": {"key": "answer.thinking", "values": {}}, "reply_to_message_id": 301, "protect_content": False}],
        keyboard=[[{"label": "label.stop", "kind": "stop_answer", "target_kind": "draft", "target_id": "301", "payload": {}}]],
        callbacks=callbacks,
        guard=guard,
    )
    return link, callbacks, guard, box, identity


@pytest.mark.asyncio
async def test_unclaimed_placeholder_is_retired_with_its_original_group(db, monkeypatch):
    link, _callbacks, guard, box, identity = await placeholder(db, monkeypatch)
    assert await box.retire_stream_placeholder(link, outbox_id=identity, guard=guard) == "retired"
    assert await box.retire_stream_placeholder(link, outbox_id=identity, guard=guard) == "retired"
    async with db.sessions() as session:
        row = await session.get(TelegramOutboxRow, identity)
        buttons = list(await session.scalars(select(TelegramCallbackRow).where(TelegramCallbackRow.payload["anchor_outbox_id"].as_string() == identity)))
        assert row.state == "skipped" and row.skip_reason == "superseded"
        assert buttons and all(button.consumed_at is not None for button in buttons)


@pytest.mark.asyncio
async def test_live_placeholder_claim_is_held_until_actual_sender_settles(db, monkeypatch):
    link, _callbacks, guard, box, identity = await placeholder(db, monkeypatch)
    entered, release = asyncio.Event(), asyncio.Event()

    async def eligible(_link, item, _part):
        return DeliveryEligibility.allow() if item.id == identity else DeliveryEligibility.wait()

    async def deliver(**_capture):
        entered.set()
        await release.wait()
        return [701]

    sender = OutboxSender(box, run_id="placeholder-test", deliver_part=deliver, eligibility=eligible)
    try:
        assert await sender.tick() == 1
        await asyncio.wait_for(entered.wait(), 3)
        assert await box.retire_stream_placeholder(link, outbox_id=identity, guard=guard) == "sending"
        release.set()
        await sender.drain()
        assert await box.retire_stream_placeholder(link, outbox_id=identity, guard=guard) == "sent"
    finally:
        release.set()
        await sender.stop()


@pytest.mark.asyncio
async def test_expired_placeholder_cannot_block_a_later_real_reply(db, monkeypatch):
    from datetime import timedelta

    from app.telegram_bots.producer import PreviewExpired

    link, _callbacks, _guard, box, identity = await placeholder(db, monkeypatch)
    real = await box.plan_product(link, source_id="later-real-reply", parts=[{"kind": "text", "source_ref": {"key": "error.generic", "values": {}}}])
    calls = []

    async def eligible(_link, item, _part):
        return DeliveryEligibility.allow() if item.id in {identity, real} else DeliveryEligibility.wait()

    async def deliver(**capture):
        calls.append(capture["outbox_id"])
        if capture["outbox_id"] == identity:
            raise PreviewExpired()
        return [702]

    sender = OutboxSender(box, run_id="expired-placeholder", deliver_part=deliver, eligibility=eligible)
    try:
        await sender.tick()
        await sender.drain()
        async with db.sessions() as session:
            assert (await session.get(TelegramOutboxRow, identity)).skip_reason == "superseded"
        original_clock = box.clock
        box.clock = lambda: original_clock() + timedelta(seconds=2)
        await sender.tick()
        await sender.drain()
        async with db.sessions() as session:
            assert (await session.get(TelegramOutboxRow, real)).state == "sent"
        assert calls == [identity, real]
    finally:
        await sender.stop()


@pytest.mark.asyncio
@pytest.mark.parametrize("expired", [False, True])
async def test_last_auxiliary_retirement_settles_already_delivered_input(publication_environment, expired):
    from datetime import timedelta

    from app.telegram_bots.callbacks import CallbackRepository
    from app.telegram_bots.producer import PreviewExpired
    from deerflow.persistence.telegram_bots.model import TelegramInboundMarkRow

    e = publication_environment
    box, callbacks = Outbox(e.sessions), CallbackRepository(e.sessions)
    identity = await box.plan_product_keyboard(
        e.link,
        source_id="chat-stream:" + "b" * 32 + ":0",
        parts=[{"kind": "text", "source_ref": {"key": "answer.thinking", "values": {}}, "reply_to_message_id": e.origins[0].message_id}],
        keyboard=[[{"label": "label.stop", "kind": "stop_answer", "target_kind": "draft", "target_id": str(e.origins[0].message_id), "payload": {}}]],
        callbacks=callbacks,
        guard=e.guard_factory(),
        inbox_rows=e.origins,
    )
    final = await box.plan_input_outcome(e.link, source_id="actual-final-input", source_ref={"key": "error.generic", "values": {}}, keyboard=[], callbacks=callbacks, guard=e.guard_factory(), inbox_rows=e.origins)
    allowed = {final}

    async def eligible(_link, item, _part):
        return DeliveryEligibility.allow() if item.id in allowed else DeliveryEligibility.wait()

    async def deliver(**capture):
        if capture["outbox_id"] == identity:
            raise PreviewExpired()
        return [701]

    async def states():
        async with e.sessions() as session:
            return list(await session.scalars(select(TelegramInboundMarkRow.state).where(TelegramInboundMarkRow.link_id == e.link.id, TelegramInboundMarkRow.message_id.in_([row.message_id for row in e.origins]))))

    sender = OutboxSender(box, run_id="settle-placeholder", deliver_part=deliver, eligibility=eligible)
    try:
        assert await sender.tick() == 1
        await sender.drain()
        assert await states() == ["reply_pending"] * len(e.origins)
        if expired:
            allowed.clear()
            allowed.add(identity)
            clock = box.clock
            box.clock = lambda: clock() + timedelta(seconds=2)
            assert await sender.tick() == 1
            await sender.drain()
        else:
            assert await box.retire_stream_placeholder(e.link, outbox_id=identity, guard=e.guard_factory()) == "retired"
        assert await states() == ["done"] * len(e.origins)
    finally:
        await sender.stop()
