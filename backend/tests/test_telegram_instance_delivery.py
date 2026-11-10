"""Current employee policy at actual SQL/SDK delivery boundaries."""

import asyncio
from datetime import UTC, datetime

import pytest
import pytest_asyncio
from _telegram_repository_test_support import db as db
from sqlalchemy import select, update
from test_telegram_bot_adapter import message
from test_telegram_instance import instance_environment
from test_telegram_threads import close

from app.telegram_bots.adapter import SendContext
from app.telegram_bots.producer import OutboxItem, OutboxSender
from deerflow.persistence.agent_instances.model import AgentInstanceGrantRow
from deerflow.persistence.telegram_bots.model import TelegramInstancePolicyRow, TelegramOutboxRow

pytestmark = pytest.mark.asyncio


@pytest_asyncio.fixture
async def instance(db, monkeypatch):
    e = await instance_environment(db, monkeypatch)
    e.db = db
    e.channel._services.protect_notice = e.live.authority.protect_notice
    e.channel._services.prepare_live_owner = e.live.authority.prepare_live_owner
    try:
        yield e
    finally:
        await close(e)


async def policy(e, value):
    async with e.db.sessions.begin() as session:
        row = await session.get(TelegramInstancePolicyRow, e.link.agent_ref)
        if row is None:
            row = TelegramInstancePolicyRow(instance_id=e.link.agent_ref, updated_by=e.link.owner_user_id)
            session.add(row)
        row.policy, row.updated_at = value, datetime.now(UTC)


async def welcome(e):
    async with e.db.sessions() as session:
        row = await session.scalar(select(TelegramOutboxRow).where(TelegramOutboxRow.link_id == e.link.id))
        return OutboxItem(**{name: getattr(row, name) for name in OutboxItem.__dataclass_fields__})


async def test_current_policy_governs_saved_parts_before_reconcile(instance):
    e = instance
    item = await welcome(e)
    assert (await e.live.authority.delivery(e.link, item)).decision == "allow"
    await policy(e, "protected")
    decision = await e.live.authority.delivery(e.link, item)
    assert decision.decision == "allow" and decision.protect_content is True
    await policy(e, "off")
    assert (await e.live.authority.delivery(e.link, item)).reason == "policy-off"
    assert await e.live.authority.target(e.link) == "policy_off"
    await policy(e, "allowed")
    assert (await e.live.authority.delivery(e.link, item)).protect_content is False


async def test_protected_notice_is_protected_in_the_actual_sdk_request(instance):
    e = instance
    await policy(e, "protected")
    request = e.channel.bot.request
    message(request)
    context = SendContext(e.link.id, e.link.chat_id, e.link.binding_generation, e.link.token_revision, "optional")
    assert await e.channel.send_notice("hello.privacy", context=context) == 501
    assert request.calls[-1].parameters["protect_content"] is True


async def test_policy_strengthened_during_limiter_wait_prevents_unprotected_notice(instance):
    e = instance
    limiter = e.channel.bot.rate_limiter

    async def changed(delay):
        await policy(e, "protected")
        limiter._short_hold_until = 0

    limiter._sleep = changed
    limiter._short_hold_until = limiter._time() + 1
    request = e.channel.bot.request
    message(request)
    context = SendContext(e.link.id, e.link.chat_id, e.link.binding_generation, e.link.token_revision, "optional")
    assert await asyncio.wait_for(e.channel.send_notice("hello.privacy", context=context), 3) is None
    assert [call.endpoint for call in request.calls] == ["getMe"]


async def test_protected_policy_never_sends_disposable_voice_drafts(instance):
    e = instance
    await policy(e, "protected")
    request = e.channel.bot.request
    request.enqueue("sendMessageDraft", result=True)
    assert await e.channel.live_request(e.link, "send_message_draft", is_current=lambda: True, draft_id=123, text="Fixture transcript preparation") is None
    assert [call.endpoint for call in request.calls] == ["getMe"]


async def test_live_preview_requires_current_use_but_lifecycle_notice_does_not(instance):
    e = instance
    await policy(e, "protected")
    async with e.db.sessions.begin() as session:
        await session.execute(update(AgentInstanceGrantRow).values(permissions=2))
    request = e.channel.bot.request
    request.enqueue("sendMessageDraft", result=True)
    assert await e.channel.live_request(e.link, "send_message_draft", is_current=lambda: True, protected=True, draft_id=123, text="Private preview") is None
    message(request)
    context = SendContext(e.link.id, e.link.chat_id, e.link.binding_generation, e.link.token_revision, "optional")
    assert await e.channel.send_notice("hello.privacy", context=context) == 501
    assert [call.endpoint for call in request.calls] == ["getMe", "sendMessage"]


async def test_sender_records_protection_before_actual_instance_send(instance):
    e = instance
    await policy(e, "protected")
    await e.db.links.end_pending(e.link.id, skip_reason="superseded")
    e.channel._services.eligibility = e.live.authority.delivery
    outbox = e.channel._services.outbox
    identity = await e.channel.send_text("hello.privacy", source_id="protected-fixture")

    async def raw(*, link_id, **kwargs):
        assert link_id == e.link.id
        return await e.channel.send_part(**kwargs)

    sender = OutboxSender(outbox, run_id="protected-fixture", deliver_part=raw, eligibility=e.live.authority.delivery)
    message(e.channel.bot.request)
    try:
        assert await sender.tick() == 1
        await sender.drain()
        async with e.db.sessions() as session:
            row = await session.get(TelegramOutboxRow, identity)
            assert row.state == "sent" and row.parts[0]["protect_content"] is True
            assert row.parts[0]["message_ids"] == [501]
        assert e.channel.bot.request.calls[-1].parameters["protect_content"] is True
    finally:
        await sender.stop()


@pytest.mark.parametrize("failed_restore", [False, True])
async def test_policy_off_stops_polling_and_restoration_drops_pending(instance, monkeypatch, failed_restore):
    from _telegram_bot_test_support import RecordingRequest
    from test_telegram_supervisor import build

    from deerflow.persistence.telegram_bots.model import TelegramBotLinkEventRow, TelegramCallbackRow

    e = instance
    await e.channel.stop()
    await e.db.links.end_pending(e.link.id, skip_reason="superseded")
    live = build(e.db, monkeypatch)
    live.supervisor.authority = e.live.authority
    live.supervisor._app_config = e.live.config
    live.normal.enqueue_get_me(bot_id=e.link.bot_id)
    held = live.updates.enqueue("getUpdates", result=[], release=asyncio.Event())
    token = str(e.link.bot_id) + ":" + "A" * 35
    await e.db.links.store_token(e.link.id, await live.supervisor.encrypt_token(e.link.bot_id, token), actor_id=e.link.owner_user_id)
    await live.supervisor.check()
    await live.supervisor.start()
    await asyncio.wait_for(held.started.wait(), 3)
    try:
        async with e.db.sessions.begin() as session:
            session.add(TelegramCallbackRow(id="a" * 32, group_id="b" * 32, link_id=e.link.id, chat_id=e.link.chat_id, binding_generation=e.link.binding_generation, kind="fixture", created_at=datetime.now(UTC)))
        await live.supervisor.outbox.add(
            await e.db.links.get(e.link.id), dedupe_key="policy-pending-fixture", class_="reply", payload={"parts": [{"kind": "text", "source_ref": {"key": "hello.privacy", "values": {}}}]}, not_before=live.clock.now()
        )
        await policy(e, "off")
        message(live.normal)
        await live.supervisor.reconcile()
        assert (await e.db.links.get(e.link.id)).health == "policy_off"
        assert live.supervisor.poll_task(e.link.id) is None
        assert "Telegram is turned off" in live.normal.calls[-1].parameters["text"]
        assert live.normal.calls[-1].parameters["protect_content"] is True
        async with e.db.sessions() as session:
            assert (await session.get(TelegramCallbackRow, "a" * 32)).consumed_at is None
            pending = await session.scalar(select(TelegramOutboxRow).where(TelegramOutboxRow.dedupe_key == "policy-pending-fixture"))
            assert pending.state == "skipped" and pending.skip_reason == "policy-off"
        await live.supervisor.reconcile()
        assert sum(call.endpoint == "sendMessage" for call in live.normal.calls) == 1
        normal, updates = RecordingRequest(), RecordingRequest()
        live.supervisor.request_factory = lambda **kw: normal if kw["connection_pool_size"] == 8 else updates
        normal.enqueue_get_me(bot_id=e.link.bot_id)
        updates.enqueue("getUpdates", result=[])
        next_poll = updates.enqueue("getUpdates", result=[], release=asyncio.Event())
        message(normal)
        live.clock.advance(1)
        await policy(e, "protected")
        if failed_restore:
            from deerflow.persistence.telegram_bots.model import TelegramBotLinkRow

            # An off observation need not have successfully sent a notice.
            async with e.db.sessions.begin() as session:
                row = await session.get(TelegramBotLinkRow, e.link.id)
                row.notices = {key: value for key, value in row.notices.items() if key != "policy"}
            factory = live.supervisor._channel_factory

            def failed(**kwargs):
                raise RuntimeError("Fixture constructor failure")

            live.supervisor._channel_factory = failed
            await live.supervisor.reconcile()
            assert live.supervisor.channel(e.link.id) is None
            assert (await e.db.links.get(e.link.id)).notices.get("policy_discard")
            live.supervisor._channel_factory = factory
        await live.supervisor.reconcile()
        await asyncio.wait_for(next_poll.started.wait(), 3)
        assert updates.calls[0].parameters["offset"] == -1
        assert (await e.db.links.get(e.link.id)).health == "ok"
        assert "Telegram is on again" in normal.calls[-1].parameters["text"]
        assert normal.calls[-1].parameters["protect_content"] is True
        async with e.db.sessions() as session:
            actions = list(await session.scalars(select(TelegramBotLinkEventRow.action).where(TelegramBotLinkEventRow.link_id == e.link.id)))
        assert actions.count("policy_off") == actions.count("policy_on") == 1
        message(normal, identity=502)
        live.clock.advance(1)
        await policy(e, "allowed")
        await live.supervisor.reconcile()
        assert "Forwarding and saving are back" in normal.calls[-1].parameters["text"]
        await live.supervisor.reconcile()
        assert sum(call.endpoint == "sendMessage" for call in normal.calls) == 2
    finally:
        await live.supervisor.stop()


async def test_live_preview_revocation_during_final_limiter_refresh_is_refused(instance, monkeypatch):
    e = instance
    refresh, calls = e.channel.refresh_link, 0

    async def withdraw():
        nonlocal calls
        calls += 1
        result = await refresh()
        if calls == 4:  # second check, inside the real SDK rate limiter
            async with e.db.sessions.begin() as session:
                await session.execute(update(AgentInstanceGrantRow).values(permissions=2))
        return result

    monkeypatch.setattr(e.channel, "refresh_link", withdraw)
    request = e.channel.bot.request
    request.enqueue("sendMessageDraft", result=True)
    assert await e.channel.live_request(e.link, "send_message_draft", is_current=lambda: True, draft_id=123, text="Private preview") is None
    assert calls == 4
    assert [call.endpoint for call in request.calls] == ["getMe"]


async def test_delivery_uses_policy_observed_after_target_validation(instance, monkeypatch):
    e = instance
    target = e.live.authority._supported_target

    async def strengthen(link):
        await policy(e, "protected")
        return await target(link)

    monkeypatch.setattr(e.live.authority, "_supported_target", strengthen)
    result = await e.live.authority.delivery(e.link, await welcome(e))
    assert result.decision == "allow" and result.protect_content is True


@pytest.mark.parametrize("value", ["protected", "off"])
async def test_fixed_outbox_part_rechecks_policy_inside_sdk_limiter(instance, value):
    e = instance
    await e.db.links.end_pending(e.link.id, skip_reason="superseded")
    e.channel._services.eligibility = e.live.authority.delivery
    outbox = e.channel._services.outbox
    identity = await e.channel.send_text("hello.privacy", source_id="limiter-fixture")
    limiter = e.channel.bot.rate_limiter

    async def changed(delay):
        await policy(e, value)
        limiter._short_hold_until = 0

    limiter._sleep = changed
    limiter._short_hold_until = limiter._time() + 1

    async def raw(*, link_id, **kwargs):
        return await e.channel.send_part(**kwargs)

    sender = OutboxSender(outbox, run_id="limiter-fixture", deliver_part=raw, eligibility=e.live.authority.delivery)
    message(e.channel.bot.request)
    try:
        assert await sender.tick() == 1
        await asyncio.wait_for(sender.drain(), 3)
        assert [call.endpoint for call in e.channel.bot.request.calls] == ["getMe"]
        async with e.db.sessions() as session:
            assert (await session.get(TelegramOutboxRow, identity)).state != "sent"
    finally:
        await sender.stop()


async def test_unobserved_off_on_is_discarded_on_next_start(instance, monkeypatch):
    from test_telegram_supervisor import build

    from app.telegram_bots.policy import InstancePolicy

    e = instance
    await e.channel.stop()
    store = InstancePolicy(e.db.sessions)

    async def checked(session):
        # Production API authority is exercised separately; this fixture writes
        # the host-owned policy repository against the actual instance/link.
        assert session.in_transaction()

    await store.set(e.link.agent_ref, "off", updated_by=e.link.owner_user_id, check_current=checked)
    await store.set(e.link.agent_ref, "allowed", updated_by=e.link.owner_user_id, check_current=checked)
    assert (await e.db.links.get(e.link.id)).health == "ok"
    assert (await e.live.authority.delivery(e.link, await welcome(e))).decision == "wait"
    live = build(e.db, monkeypatch)
    live.supervisor.authority, live.supervisor._app_config = e.live.authority, e.live.config
    token = str(e.link.bot_id) + ":" + "A" * 35
    await e.db.links.store_token(e.link.id, await live.supervisor.encrypt_token(e.link.bot_id, token), actor_id=e.link.owner_user_id)
    live.normal.enqueue_get_me(bot_id=e.link.bot_id)
    live.updates.enqueue("getUpdates", result=[])
    next_poll = live.updates.enqueue("getUpdates", result=[], release=asyncio.Event())
    await live.supervisor.check()
    await live.supervisor.start()
    try:
        await asyncio.wait_for(next_poll.started.wait(), 3)
        assert live.updates.calls[0].parameters["offset"] == -1
        assert "policy_discard" not in (await e.db.links.get(e.link.id)).notices
    finally:
        await live.supervisor.stop()


async def test_discard_ack_cannot_clear_new_off_epoch_or_replaced_transport(instance):
    from app.telegram_bots.policy import InstancePolicy

    e, store = instance, InstancePolicy(instance.db.sessions)

    async def checked(session):
        assert session.in_transaction()

    async def set_value(value):
        await store.set(e.link.agent_ref, value, updated_by=e.link.owner_user_id, check_current=checked)

    await set_value("off")
    captured = await e.db.links.get(e.link.id)
    intent = captured.notices["policy_discard"]
    assert not await store.complete_discard(captured, intent, check_current=lambda: True)
    await set_value("allowed")
    assert not await store.complete_discard(captured, intent, check_current=lambda: False)
    await set_value("off")
    await set_value("allowed")
    assert not await store.complete_discard(captured, intent, check_current=lambda: True)
    captured = await e.db.links.get(e.link.id)
    intent = captured.notices["policy_discard"]
    await e.db.links.store_token(e.link.id, "replacement-fixture", actor_id=e.link.owner_user_id)
    assert not await store.complete_discard(captured, intent, check_current=lambda: True)
    current = await e.db.links.get(e.link.id)
    assert current.notices["policy_discard"] == intent
    assert await store.complete_discard(current, intent, check_current=lambda: True)
    assert "policy_discard" not in (await e.db.links.get(e.link.id)).notices


@pytest.mark.parametrize("value", ["protected", "off"])
@pytest.mark.parametrize("surface", ["delivery", "live-preview"])
async def test_policy_strengthened_during_last_use_read_cannot_release_stale_send(instance, monkeypatch, value, surface):
    e = instance
    view, calls = e.live.authority.instance_view, 0

    async def strengthen(link):
        nonlocal calls
        calls += 1
        result = await view(link)
        if calls == 2:  # last authority read, including the SDK limiter check
            await policy(e, value)
        return result

    monkeypatch.setattr(e.live.authority, "instance_view", strengthen)
    if surface == "delivery":
        verdict = await e.live.authority.delivery(e.link, await welcome(e))
        assert verdict.decision != "allow" or verdict.protect_content is True
    else:
        e.channel.bot.request.enqueue("sendMessageDraft", result=True)
        assert await e.channel.live_request(e.link, "send_message_draft", is_current=lambda: True, draft_id=123, text="Private preview") is None
        assert [call.endpoint for call in e.channel.bot.request.calls] == ["getMe"]
