"""Final authority and context fences use real SQL and the actual SDK."""

import asyncio
import secrets
from types import SimpleNamespace

import pytest
from _telegram_bot_test_support import ManualClock, RecordingRequest
from _telegram_repository_test_support import add_link, bind_link
from _telegram_repository_test_support import db as db
from sqlalchemy import select

from app.telegram_bots.adapter import SendContext, TelegramBotChannel
from app.telegram_bots.producer import DeliveryEligibility, Outbox, OutboxSender
from deerflow.persistence.telegram_bots.model import TelegramOutboxRow

pytestmark = pytest.mark.asyncio


async def build(db, *, parts=1):
    link = await bind_link(db, await add_link(db))
    async with db.sessions.begin() as session:
        for row in await session.scalars(select(TelegramOutboxRow)):
            row.state = "sent"
    request, updates, clock = RecordingRequest(), RecordingRequest(), ManualClock()
    policy = SimpleNamespace(decision=DeliveryEligibility.allow(), calls=[], notice_entered=None, notice_release=None)
    channels = {}

    async def eligibility(current, item, part):
        policy.calls.append((current, item, part))
        return policy.decision

    async def protect_notice(context):
        if policy.notice_entered is not None:
            policy.notice_entered.set()
            await policy.notice_release.wait()
        return False

    services = SimpleNamespace(
        send_allowed=lambda: True,
        config=db.config,
        links=db.links,
        settings=db.settings,
        outbox=Outbox(db.sessions, clock=clock.now),
        tls_context=object(),
        request_factory=lambda **kw: request if kw["connection_pool_size"] == 8 else updates,
        eligibility=eligibility,
        protect_notice=protect_notice,
        channel=channels.get,
    )
    token = str(link.bot_id) + ":" + secrets.token_urlsafe(30)
    channel = await asyncio.to_thread(TelegramBotChannel, link=link, token=token, supervisor=services, monotonic=clock.monotonic, sleep=clock.sleep, now=clock.now)
    channels[link.id] = channel
    request.enqueue_get_me(bot_id=link.bot_id)
    await channel.start()
    identity = await services.outbox.plan_product(link, source_id="final-authority", parts=[{"kind": "text", "source_ref": {"key": "hello.privacy", "values": {}}} for _ in range(parts)])

    async def raw(*, link_id, **scope):
        assert link_id == link.id
        return await channel.send_part(**scope)

    sender = OutboxSender(services.outbox, run_id="final-authority", deliver_part=raw, eligibility=eligibility, jitter=lambda: 0)
    return SimpleNamespace(link=link, request=request, updates=updates, clock=clock, channel=channel, channels=channels, services=services, policy=policy, sender=sender, identity=identity)


async def saved(db, identity):
    async with db.sessions() as session:
        return await session.get(TelegramOutboxRow, identity)


def enqueue_message(fixture, *, release=None, message=501):
    return fixture.request.enqueue("sendMessage", result={"message_id": message, "date": 1_893_456_000, "chat": {"id": fixture.link.chat_id, "type": "private"}, "text": "Fixed text"}, release=release)


def stall_render(fixture):
    entered, release = asyncio.Event(), asyncio.Event()
    original = fixture.channel._render_source

    async def render(*args, **kwargs):
        entered.set()
        await release.wait()
        return await original(*args, **kwargs)

    fixture.channel._render_source = render
    return entered, release


@pytest.mark.parametrize("withdrawal", ["disabled", "target-unavailable", "binding", "replacement"])
async def test_authority_withdrawn_during_source_preparation_sends_nothing(db, withdrawal):
    fixture = await build(db)
    entered, release = stall_render(fixture)
    enqueue_message(fixture)
    try:
        await fixture.sender.tick()
        await asyncio.wait_for(entered.wait(), 2)
        if withdrawal == "disabled":
            fixture.policy.decision = DeliveryEligibility.skip("disabled")
        elif withdrawal == "target-unavailable":
            fixture.policy.decision = DeliveryEligibility.wait()
        elif withdrawal == "binding":
            await db.links.unbind(fixture.link, actor_id=fixture.link.owner_user_id)
        else:
            fixture.channels[fixture.link.id] = object()  # same token revision, new process-owned adapter
        release.set()
        await fixture.sender.drain()
        row = await saved(db, fixture.identity)
        assert [call.endpoint for call in fixture.request.calls] == ["getMe"]
        assert row.state != "sent" and row.attempts == 0
        assert row.parts[0]["message_ids"] == []
    finally:
        release.set()
        await fixture.sender.stop()
        await fixture.channel.stop()


async def test_protection_strengthened_during_preparation_requires_new_persisted_attempt(db):
    fixture = await build(db)
    entered, release = stall_render(fixture)
    enqueue_message(fixture)
    try:
        await fixture.sender.tick()
        await asyncio.wait_for(entered.wait(), 2)
        fixture.policy.decision = DeliveryEligibility.allow(protect_content=True)
        release.set()
        await fixture.sender.drain()
        row = await saved(db, fixture.identity)
        assert row.state == "pending" and row.attempts == 0
        assert row.parts[0]["protect_content"] is False
        assert [call.endpoint for call in fixture.request.calls] == ["getMe"]
        fixture.clock.advance(1)
        await fixture.sender.tick()
        await fixture.sender.drain()
        row = await saved(db, fixture.identity)
        assert row.state == "sent" and row.parts[0]["protect_content"] is True
        assert row.parts[0]["message_ids"] == [501]
        assert fixture.request.calls[-1].parameters["protect_content"] is True
        assert sum(part is not None and part["state"] == "sending" for _, _, part in fixture.policy.calls) == 2
    finally:
        release.set()
        await fixture.sender.stop()
        await fixture.channel.stop()


async def test_binding_changes_while_final_authority_awaits_are_recaptured_before_sdk(db):
    fixture = await build(db)
    entered, release = asyncio.Event(), asyncio.Event()
    original = fixture.services.eligibility

    async def gated(link, item, part):
        decision = await original(link, item, part)
        if part is not None and part["state"] == "sending":
            entered.set()
            await release.wait()
        return decision

    fixture.services.eligibility = gated
    enqueue_message(fixture)
    try:
        await fixture.sender.tick()
        await asyncio.wait_for(entered.wait(), 2)
        await db.links.unbind(fixture.link, actor_id=fixture.link.owner_user_id)
        release.set()
        await fixture.sender.drain()
        assert [call.endpoint for call in fixture.request.calls] == ["getMe"]
        assert (await saved(db, fixture.identity)).state != "sent"
    finally:
        release.set()
        await fixture.sender.stop()
        await fixture.channel.stop()


async def test_disabled_owner_between_parts_keeps_only_first_acknowledgement(db):
    fixture = await build(db, parts=2)
    release = asyncio.Event()
    first = enqueue_message(fixture, release=release)
    enqueue_message(fixture, message=502)
    try:
        await fixture.sender.tick()
        await asyncio.wait_for(first.started.wait(), 2)
        fixture.policy.decision = DeliveryEligibility.skip("disabled")
        release.set()
        await fixture.sender.drain()
        row = await saved(db, fixture.identity)
        assert row.state == "skipped" and row.skip_reason == "disabled"
        assert row.parts[0]["message_ids"] == [501] and row.parts[1]["message_ids"] == []
        assert [call.endpoint for call in fixture.request.calls] == ["getMe", "sendMessage"]
    finally:
        release.set()
        await fixture.sender.stop()
        await fixture.channel.stop()


@pytest.mark.parametrize("change", ["generation", "replacement", "token"])
async def test_notice_rechecks_original_context_after_protection_preparation(db, change):
    fixture = await build(db)
    entered, release = asyncio.Event(), asyncio.Event()
    fixture.policy.notice_entered, fixture.policy.notice_release = entered, release
    context = SendContext(fixture.link.id, fixture.link.chat_id, fixture.link.binding_generation, fixture.link.token_revision, "bound_message")
    enqueue_message(fixture)
    task = asyncio.create_task(fixture.channel.send_notice("hello.privacy", context=context))
    try:
        await asyncio.wait_for(entered.wait(), 2)
        if change == "generation":
            await db.links.unbind(fixture.link, actor_id=fixture.link.owner_user_id)
        elif change == "replacement":
            fixture.channels[fixture.link.id] = object()
        else:
            await db.links.store_token(fixture.link.id, "wrapped-replacement", actor_id=fixture.link.owner_user_id, expected_revision=fixture.link.token_revision, token_changed=True)
        release.set()
        assert await task is None
        assert [call.endpoint for call in fixture.request.calls] == ["getMe"]
    finally:
        release.set()
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        await fixture.sender.stop()
        await fixture.channel.stop()


async def test_authorized_access_off_notice_is_independent_of_ordinary_delivery_refusal(db):
    fixture = await build(db)
    fixture.policy.decision = DeliveryEligibility.skip("disabled")
    context = SendContext(fixture.link.id, fixture.link.chat_id, fixture.link.binding_generation, fixture.link.token_revision, "bound_message")
    enqueue_message(fixture)
    try:
        assert await fixture.channel.send_notice("access.off", context=context, product="HartMesh") == 501
        assert fixture.policy.calls == []
        assert [call.endpoint for call in fixture.request.calls] == ["getMe", "sendMessage"]
    finally:
        await fixture.sender.stop()
        await fixture.channel.stop()


async def test_optional_bound_lifecycle_notice_cannot_use_a_different_chat(db):
    fixture = await build(db)
    context = SendContext(fixture.link.id, fixture.link.chat_id + 1, fixture.link.binding_generation, fixture.link.token_revision, "optional")
    enqueue_message(fixture)
    try:
        assert await fixture.channel.send_notice("access.off", context=context, product="HartMesh") is None
        assert [call.endpoint for call in fixture.request.calls] == ["getMe"]
    finally:
        await fixture.sender.stop()
        await fixture.channel.stop()
