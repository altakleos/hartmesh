"""Fixed navigation fallback and reconnect scheduling retain delivery ownership."""

from copy import deepcopy
from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest
from _telegram_repository_test_support import add_link, bind_link, rows
from _telegram_repository_test_support import db as db
from telegram.error import Forbidden

from app.telegram_bots.producer import DeliveryEligibility, Outbox, OutboxSender
from deerflow.persistence.telegram_bots.model import TelegramBotLinkRow, TelegramOutboxRow


class Clock:
    def __init__(self):
        self.now = datetime(2030, 1, 1, tzinfo=UTC)

    def __call__(self):
        return self.now

    def advance(self, seconds=2):
        self.now += timedelta(seconds=seconds)


def memory_part():
    return {
        "kind": "text",
        "source_ref": {"key": "web.memory", "values": {"name": "Fixture", "product": "HartMesh"}},
        "url_button": {"page": "web.memory", "url": "https://example.test/workspace?settings=memory&from=chat"},
    }


@pytest.mark.parametrize(
    "button",
    [
        {},
        {"page": "web.memory", "url": "javascript:alert(1)"},
        {"page": "web.memory", "url": "https://user:password@example.test/workspace"},
        {"page": "web.memory", "url": "https://example.test/\nunsafe"},
        {"page": "web.memory", "url": "https://example.test:invalid/path"},
        {"page": "web.memory", "url": "https:///missing-host"},
        {"page": "web.memory", "url": 1},
        {"page": "web.account", "url": "https://example.test/workspace"},
        {"page": "web.memory", "url": "https://example.test", "callback_data": "untrusted"},
        [{"page": "web.memory", "url": "https://example.test"}],
    ],
)
def test_navigation_parts_reject_unsupported_or_unsafe_controls(button):
    supplied = memory_part()
    supplied["url_button"] = button
    with pytest.raises(ValueError):
        Outbox._parts([supplied], 0)


def test_navigation_fallback_receipt_cannot_be_supplied_by_a_new_plan():
    supplied = memory_part()
    supplied["url_button_fallback"] = True
    frozen = Outbox._parts([supplied], 0)[0]
    assert "url_button_fallback" not in frozen
    assert frozen["url_button"] == supplied["url_button"]


@pytest.mark.parametrize("kind,key", [("document", "web.memory"), ("text", "cmd.unknown")])
def test_memory_navigation_is_a_closed_fixed_text_part(kind, key):
    supplied = memory_part()
    supplied["kind"], supplied["source_ref"]["key"] = kind, key
    with pytest.raises(ValueError):
        Outbox._parts([supplied], 0)


async def claimed(db):
    link = await bind_link(db, await add_link(db))
    clock = Clock()
    outbox = Outbox(db.sessions, clock=clock)
    supplied = memory_part()
    supplied["url_button_fallback"] = True
    identity = await outbox.plan_product(link, source_id="memory-input", parts=[supplied])
    async with db.sessions() as session, session.begin():
        saved = await session.get(TelegramOutboxRow, identity, with_for_update=True)
        saved.state, saved.claim_owner, saved.claim_id, saved.claimed_at = "sending", "test-owner", uuid4().hex, clock()
        parts = deepcopy(saved.parts)
        parts[0]["state"] = "sending"
        saved.parts = parts
        claim = saved.claim_id
        part_id = parts[0]["part_id"]
    captured = await outbox.capture_sending_part(link_id=link.id, outbox_id=identity, part_id=part_id, claim_owner="test-owner", claim_id=claim, token_revision=link.token_revision)
    return link, clock, outbox, captured


async def saved(db, identity):
    async with db.sessions() as session:
        return await session.get(TelegramOutboxRow, identity)


@pytest.mark.asyncio
async def test_url_refusal_persists_one_same_source_fallback_and_releases_claim(db):
    link, clock, outbox, captured = await claimed(db)
    original = deepcopy(captured.part)
    before = await saved(db, captured.item.id)
    assert "url_button_fallback" not in before.payload["product_basis"]["parts"][0]
    assert await outbox.fallback_url_button(captured)
    current = await saved(db, captured.item.id)
    expected = {**original, "state": "pending", "url_button_fallback": True}
    assert current.parts == [expected]
    assert current.state == "pending" and current.not_before.replace(tzinfo=UTC) == clock()
    assert current.claim_owner is current.claim_id is current.claimed_at is None
    assert current.payload == before.payload and current.attempts == before.attempts == 0
    assert not await outbox.fallback_url_button(captured)
    assert (await saved(db, captured.item.id)).parts == [expected]


@pytest.mark.asyncio
@pytest.mark.parametrize("change", ["claim", "generation", "token", "source", "button", "version", "part", "target", "connection", "account", "topic", "reply"])
async def test_url_fallback_cannot_replace_a_successor_claim_or_changed_source(db, change):
    link, clock, outbox, captured = await claimed(db)
    async with db.sessions() as session, session.begin():
        row = await session.get(TelegramOutboxRow, captured.item.id, with_for_update=True)
        binding = await session.get(TelegramBotLinkRow, link.id, with_for_update=True)
        parts = deepcopy(row.parts)
        if change == "claim":
            row.claim_id = uuid4().hex
        elif change == "generation":
            binding.binding_generation += 1
        elif change == "token":
            binding.token_revision += 1
        elif change == "source":
            parts[0]["source_ref"]["values"]["name"] = "Successor"
        elif change == "button":
            parts[0]["url_button"]["url"] = "https://example.test/successor"
        elif change == "version":
            parts[0]["presentation_version"] += 1
            row.payload = {**row.payload, "presentation_version": parts[0]["presentation_version"]}
        elif change == "target":
            binding.agent_ref = "successor-target"
        elif change == "connection":
            binding.connection_id = None
        elif change == "account":
            binding.telegram_user_id += 1
        elif change == "topic":
            row.message_thread_id = 2
        elif change == "reply":
            parts[0]["reply_to_message_id"] = 999
        else:
            parts[0]["part_id"] = uuid4().hex
        row.parts = parts
    before = await saved(db, captured.item.id)
    assert not await outbox.fallback_url_button(captured)
    after = await saved(db, captured.item.id)
    assert (after.parts, after.payload, after.state, after.claim_owner, after.claim_id, after.not_before) == (before.parts, before.payload, before.state, before.claim_owner, before.claim_id, before.not_before)


@pytest.mark.asyncio
@pytest.mark.parametrize("first", ["plain", "url"])
async def test_url_and_plain_fallbacks_compose_without_replacing_source_or_part(db, first):
    link, clock, outbox, captured = await claimed(db)
    initial = outbox.fallback_to_plain if first == "plain" else outbox.fallback_url_button
    subsequent = outbox.fallback_url_button if first == "plain" else outbox.fallback_to_plain
    assert await initial(captured)
    async with db.sessions() as session, session.begin():
        row = await session.get(TelegramOutboxRow, captured.item.id, with_for_update=True)
        row.state, row.claim_owner, row.claim_id, row.claimed_at = "sending", "test-owner", uuid4().hex, clock()
        parts = deepcopy(row.parts)
        parts[0]["state"] = "sending"
        row.parts = parts
        claim = row.claim_id
    latest = await outbox.capture_sending_part(link_id=link.id, outbox_id=captured.item.id, part_id=captured.part["part_id"], claim_owner="test-owner", claim_id=claim, token_revision=link.token_revision)
    assert await subsequent(latest)
    current = (await saved(db, captured.item.id)).parts[0]
    assert current["plain_fallback"] and current["url_button_fallback"]
    assert current["part_id"] == captured.part["part_id"] and current["source_ref"] == captured.part["source_ref"] and current["url_button"] == captured.part["url_button"]


@pytest.mark.asyncio
async def test_memory_plan_replay_keeps_original_navigation_and_never_borrows_input_objects(db):
    link = await bind_link(db, await add_link(db))
    outbox = Outbox(db.sessions)
    supplied = memory_part()
    original = deepcopy(supplied)
    identity = await outbox.plan_product(link, source_id="memory-replay", parts=[supplied])
    supplied["url_button"]["url"] = "https://example.test/successor"
    supplied["source_ref"]["values"]["name"] = "Successor"
    before = await saved(db, identity)
    assert before.parts[0]["url_button"] == original["url_button"] and before.parts[0]["source_ref"] == original["source_ref"]
    assert await outbox.plan_product(link, source_id="memory-replay", parts=[original]) == identity
    with pytest.raises(ValueError, match="product-source-conflict"):
        await outbox.plan_product(link, source_id="memory-replay", parts=[supplied])
    after = await saved(db, identity)
    assert after.parts == before.parts and after.payload == before.payload


@pytest.mark.asyncio
async def test_forbidden_without_a_committed_current_block_retains_transient_backoff(db):
    link = await bind_link(db, await add_link(db))
    clock = Clock()
    outbox = Outbox(db.sessions, clock=clock)

    async def allow(*args):
        return DeliveryEligibility.allow()

    async def refused(**scope):
        raise Forbidden("synthetic other refusal without current block")

    sender = OutboxSender(outbox, run_id=uuid4().hex, deliver_part=refused, eligibility=allow, jitter=lambda: 0)
    try:
        welcome = (await rows(db, TelegramOutboxRow))[0]
        assert await sender.tick() == 1
        await sender.drain()
        current = await saved(db, welcome.id)
        assert (await db.links.get(link.id)).state == "connected"
        assert current.state == "pending" and current.attempts == 1
        assert current.not_before.replace(tzinfo=UTC) == clock() + timedelta(seconds=5)
        assert await sender.tick() == 0
    finally:
        await sender.stop()


@pytest.mark.asyncio
async def test_current_blocked_delivery_waits_due_without_charging_or_resetting_other_deadlines(db):
    link = await bind_link(db, await add_link(db))
    clock, calls, fail = Clock(), [], False
    outbox = Outbox(db.sessions, clock=clock)

    async def allow(*args):
        return DeliveryEligibility.allow()

    async def deliver(**scope):
        nonlocal fail
        capture = await outbox.capture_sending_part(**scope, token_revision=link.token_revision)
        if fail and capture.part["ordinal"] == 1:
            assert await db.links.mark_blocked_if_current(link_id=link.id, chat_id=link.chat_id, binding_generation=link.binding_generation, token_revision=link.token_revision)
            raise Forbidden("Forbidden: bot was blocked by the user")
        calls.append(capture.part["part_id"])
        return [1000 + len(calls)]

    sender = OutboxSender(outbox, run_id=uuid4().hex, deliver_part=deliver, eligibility=allow, jitter=lambda: 0)
    try:
        for _ in range(3):
            await sender.tick()
            await sender.drain()
            clock.advance()
        first = {"kind": "text", "source_ref": {"key": "cmd.unknown", "values": {}}}
        identity = await outbox.plan_product(link, source_id="blocked-input", parts=[first, memory_part()])
        floor = clock() + timedelta(hours=1)
        assert await outbox.add(link, dedupe_key="notice:server-wait", class_="reply", payload={"parts": [first]}, not_before=floor)
        fail = True
        await sender.tick()
        await sender.drain()
        clock.advance()
        await sender.tick()
        await sender.drain()
        current = await saved(db, identity)
        assert current.state == "pending" and current.parts[0]["state"] == "sent" and current.parts[1]["state"] == "pending"
        assert current.attempts == 0 and current.degraded_at is None
        assert current.not_before.replace(tzinfo=UTC) <= clock()
        future = next(row for row in await rows(db, TelegramOutboxRow) if row.dedupe_key == "notice:server-wait")
        assert future.not_before.replace(tzinfo=UTC) == floor
        assert await sender.tick() == 0
        assert calls.count(current.parts[0]["part_id"]) == 1
    finally:
        await sender.stop()
