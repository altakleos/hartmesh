"""Digest item presses open full source-owned previews without consuming navigation."""

import pytest
from _telegram_repository_test_support import db as db
from sqlalchemy import select
from test_telegram_bot_adapter import message
from test_telegram_work_navigation_origin import navigation
from test_telegram_work_source_delivery import work_delivery as work_delivery
from test_work_attention_pages import work_pages as work_pages

from deerflow.persistence.telegram_bots.model import TelegramCallbackRow, TelegramOutboxRow


@pytest.mark.asyncio
@pytest.mark.parametrize("protected", [False, True])
async def test_digest_item_opens_full_question_and_keeps_navigation_reusable(work_delivery, protected):
    from app.telegram_bots.work_digest_items import DigestItems
    from app.telegram_bots.work_request_source import WorkRequestSources

    e = work_delivery
    digests, anchor, callback, target = await navigation(e, protected)
    items = DigestItems(digests=digests, requests=e.plans)
    e.channel._services.work_request_sources = WorkRequestSources(reader=e.reader, outbox=e.sender.outbox, callbacks=e.plans.callbacks, navigation=items.navigation)
    owner = await e.live.authority.prepare_live_owner(e.link.owner_user_id)
    guard = e.db.links.publication_guard(e.link, check_live_owner=owner, check_attempt_current=lambda: True)
    options = dict(callback_id=callback, query_id="item-query-one", chat_id=e.link.chat_id, message_id=5801, message_thread_id=None, guard=guard)
    identity = await items.open(e.link, **options)
    assert await items.open(e.link, **options) == identity
    message(e.channel.bot.request, identity=6101)
    e.delivery_clock.advance(2)
    assert await e.sender.tick() == 1
    await e.sender.drain()
    assert not e.dispatch_errors
    calls = [call for call in e.channel.bot.request.calls if call.endpoint == "sendMessage"]
    assert "Which source?" in calls[-1].parameters["text"]
    assert calls[-1].parameters["protect_content"] is protected
    async with e.db.sessions() as reader:
        row = await reader.get(TelegramOutboxRow, identity)
        assert row.state == "sent" and row.class_ == "reply"
        origin = row.payload["work_request_basis"]["navigation_origin"]
        assert origin["anchor_outbox_id"] == anchor and origin["member_id"] == target
        assert row.payload["work_request_basis"]["concealed"] is False
        assert (await reader.get(TelegramCallbackRow, callback)).consumed_at is None
        assert (await reader.get(TelegramOutboxRow, target)).skip_reason == "in-digest"
    later = await items.open(e.link, **{**options, "query_id": "item-query-two"})
    assert later != identity


async def pending(e, *, protected=True):
    from app.telegram_bots.work_digest_items import DigestItems
    from app.telegram_bots.work_request_source import WorkRequestSources

    digests, anchor, callback, target = await navigation(e, protected)
    items = DigestItems(digests=digests, requests=e.plans)
    e.channel._services.work_request_sources = WorkRequestSources(reader=e.reader, outbox=e.sender.outbox, callbacks=e.plans.callbacks, navigation=items.navigation)
    owner = await e.live.authority.prepare_live_owner(e.link.owner_user_id)
    guard = e.db.links.publication_guard(e.link, check_live_owner=owner, check_attempt_current=lambda: True)
    identity = await items.open(e.link, callback_id=callback, query_id="item-query", chat_id=e.link.chat_id, message_id=5801, message_thread_id=None, guard=guard)
    return items, identity, anchor, callback, target


@pytest.mark.asyncio
@pytest.mark.parametrize("change", ["anchor", "callback", "member", "claim"])
async def test_digest_preview_final_source_read_refuses_changed_origin(work_delivery, monkeypatch, change):
    from app.gateway import authz
    from app.telegram_bots import work_request_source as module
    from app.telegram_bots.work_digest_projection import DigestProjection

    e = work_delivery
    _items, identity, anchor, callback, target = await pending(e)
    original_read, original_provider = module.current_attention, authz.resolve_route_permissions
    active, changed = [], []

    async def read(*args, **kwargs):
        if isinstance(kwargs.get("projection"), DigestProjection):
            active.append(True)
        try:
            return await original_read(*args, **kwargs)
        finally:
            active.clear()

    async def provider(*args, **kwargs):
        held = await original_provider(*args, **kwargs)
        if active and not changed:
            async with e.db.sessions.begin() as writer:
                if change == "anchor":
                    row = await writer.get(TelegramOutboxRow, anchor)
                    row.parts = [{**row.parts[-1], "message_ids": [5802]}]
                elif change == "callback":
                    (await writer.get(TelegramCallbackRow, callback)).target_id = "f" * 32
                elif change == "member":
                    (await writer.get(TelegramOutboxRow, target)).skip_reason = "superseded"
                else:
                    (await writer.get(TelegramOutboxRow, identity)).claim_id = "foreign"
            changed.append(True)
        return held

    monkeypatch.setattr(module, "current_attention", read)
    monkeypatch.setattr(authz, "resolve_route_permissions", provider)
    before = len([call for call in e.channel.bot.request.calls if call.endpoint == "sendMessage"])
    message(e.channel.bot.request, identity=6101)
    e.delivery_clock.advance(2)
    assert await e.sender.tick() == 1
    await e.sender.drain()
    assert changed
    assert len([call for call in e.channel.bot.request.calls if call.endpoint == "sendMessage"]) == before


@pytest.mark.asyncio
@pytest.mark.parametrize("change", ["control", "claim"])
async def test_digest_preview_publication_rechecks_new_controls_after_provider(work_delivery, monkeypatch, change):
    from app.gateway import authz
    from app.telegram_bots import work_digest_items as module
    from app.telegram_bots.producer import ProductSourceRetired

    e = work_delivery
    digests, _anchor, callback, _target = await navigation(e, True)
    items = module.DigestItems(digests=digests, requests=e.plans)
    owner = await e.live.authority.prepare_live_owner(e.link.owner_user_id)
    guard = e.db.links.publication_guard(e.link, check_live_owner=owner, check_attempt_current=lambda: True)
    original_prepare, original_provider = module.prepare_attention, authz.resolve_route_permissions
    active, changed = [], []

    async def prepare(*args, **kwargs):
        admission = await original_prepare(*args, **kwargs)

        async def check(**options):
            if isinstance(options.get("projection"), module._PreviewProjection):
                active.append((options["session"], options["projection"].guard.preview))
            try:
                return await admission(**options)
            finally:
                active.clear()

        return check

    async def provider(*args, **kwargs):
        held = await original_provider(*args, **kwargs)
        if active and not changed:
            writer, row = active[-1]
            if change == "control":
                control = await writer.scalar(select(TelegramCallbackRow).where(TelegramCallbackRow.group_id == row.parts[-1]["callback_group_id"]))
                control.target_id = "f" * 32
            else:
                (await writer.get(TelegramOutboxRow, row.id)).claim_id = "foreign"
            await writer.flush()
            changed.append(True)
        return held

    monkeypatch.setattr(module, "prepare_attention", prepare)
    monkeypatch.setattr(authz, "resolve_route_permissions", provider)
    with pytest.raises(ProductSourceRetired):
        await items.open(e.link, callback_id=callback, query_id="item-query", chat_id=e.link.chat_id, message_id=5801, message_thread_id=None, guard=guard)
    assert changed
    async with e.db.sessions() as reader:
        assert await reader.scalar(select(TelegramOutboxRow.id).where(TelegramOutboxRow.dedupe_key.like("notice:work-navigation:%"))) is None
        assert (await reader.get(TelegramCallbackRow, callback)).consumed_at is None


@pytest.mark.asyncio
async def test_pending_digest_preview_pins_original_sources_after_control_expiry(work_delivery):
    from datetime import timedelta

    from app.telegram_bots.retention import _pinned_deliveries

    e = work_delivery
    _items, identity, anchor, _callback, target = await pending(e)
    async with e.db.sessions() as reader:
        pinned = await _pinned_deliveries(reader, e.delivery_clock.now() + timedelta(days=31), 30)
        assert {identity, anchor, target} <= pinned


@pytest.mark.asyncio
async def test_ordinary_recovery_cannot_strip_navigation_origin(work_delivery):
    from app.telegram_bots.producer import ProductSourceRetired
    from app.telegram_bots.work_request_recovery import WorkRequestRecovery

    e = work_delivery
    _items, identity, _anchor, _callback, _target = await pending(e)
    async with e.db.sessions.begin() as writer:
        row = await writer.get(TelegramOutboxRow, identity)
        source = await e.reader.read_request(e.link, row.request_id)
        settings = await e.plans.settings.get(e.link.owner_user_id)
        with pytest.raises(ProductSourceRetired):
            await WorkRequestRecovery(e.plans)._replace(writer, e.link, row, source, settings)


@pytest.mark.asyncio
async def test_decision_navigation_preview_remains_available_with_notifications_off(work_delivery, monkeypatch):
    import test_telegram_work_event_intents as intents

    from deerflow.persistence.telegram_bots.model import TelegramPersonSettingsRow

    original = intents.request

    async def decision(e, peer):
        return await original(e, peer, purpose="decision")

    monkeypatch.setattr(intents, "request", decision)
    e = work_delivery
    _items, identity, _anchor, _callback, _target = await pending(e)
    async with e.db.sessions.begin() as writer:
        (await writer.get(TelegramPersonSettingsRow, e.link.owner_user_id)).notify = "none"
    message(e.channel.bot.request, identity=6102)
    e.delivery_clock.advance(2)
    assert await e.sender.tick() == 1
    await e.sender.drain()
    async with e.db.sessions() as reader:
        row = await reader.get(TelegramOutboxRow, identity)
        assert row.state == "sent" and row.payload["work_request_basis"]["source"]["request"]["purpose"] == "decision"
        controls = list(await reader.scalars(select(TelegramCallbackRow).where(TelegramCallbackRow.group_id == row.parts[-1]["callback_group_id"])))
        assert {control.kind for control in controls} == {"decide_own"}


@pytest.mark.asyncio
async def test_digest_query_replay_retains_pending_publication_after_transient_send(work_delivery):
    e = work_delivery
    items, identity, _anchor, callback, _target = await pending(e, protected=False)
    e.channel.bot.request.enqueue("sendMessage", status=503, body={"ok": False, "error_code": 503, "description": "Service unavailable"})
    e.delivery_clock.advance(2)
    assert await e.sender.tick() == 1
    await e.sender.drain()
    async with e.db.sessions() as reader:
        row = await reader.get(TelegramOutboxRow, identity)
        assert row.state == "pending" and row.parts[-1].get("dispatched_at")
        attempts, parts = row.attempts, row.parts
    owner = await e.live.authority.prepare_live_owner(e.link.owner_user_id)
    guard = e.db.links.publication_guard(e.link, check_live_owner=owner, check_attempt_current=lambda: True)
    assert await items.open(e.link, callback_id=callback, query_id="item-query", chat_id=e.link.chat_id, message_id=5801, message_thread_id=None, guard=guard) == identity
    async with e.db.sessions() as reader:
        row = await reader.get(TelegramOutboxRow, identity)
        assert row.state == "pending" and row.attempts == attempts and row.parts == parts


@pytest.mark.asyncio
@pytest.mark.parametrize("damage", [None, "naive", "multiple_ids", "duplicate"])
async def test_digest_query_replay_qualifies_actual_multipart_ack_prefix(work_delivery, monkeypatch, damage):
    import test_telegram_work_event_intents as intents

    from app.telegram_bots.producer import PermanentDeliveryError, ProductSourceRetired

    create = intents.CreateRequest
    monkeypatch.setattr(intents, "CreateRequest", lambda **kwargs: create(**{**kwargs, "question": "Q" * 4096, "reason": "R" * 4096}))
    e = work_delivery
    items, identity, _anchor, callback, _target = await pending(e, protected=False)
    async with e.db.sessions() as reader:
        assert len((await reader.get(TelegramOutboxRow, identity)).parts) >= 3
    count = 2 if damage == "duplicate" else 1
    for ordinal in range(count):
        message(e.channel.bot.request, identity=6101 + ordinal)
        e.delivery_clock.advance(2)
        assert await e.sender.tick() == 1
        await e.sender.drain()
    e.channel.bot.request.enqueue("sendMessage", status=503, body={"ok": False, "error_code": 503, "description": "Service unavailable"})
    e.delivery_clock.advance(2)
    assert await e.sender.tick() == 1
    await e.sender.drain()
    async with e.db.sessions.begin() as writer:
        row = await writer.get(TelegramOutboxRow, identity)
        assert row.state == "pending" and row.parts[count]["state"] == "pending" and row.parts[count].get("dispatched_at")
        if damage:
            change = {"sent_at": "2030-01-01T00:00:00"} if damage == "naive" else {"message_ids": [6101, 6102]} if damage == "multiple_ids" else {"message_ids": [6102]}
            row.parts = [{**row.parts[0], **change}, *row.parts[1:]]
    owner = await e.live.authority.prepare_live_owner(e.link.owner_user_id)
    guard = e.db.links.publication_guard(e.link, check_live_owner=owner, check_attempt_current=lambda: True)

    async def replay():
        return await items.open(e.link, callback_id=callback, query_id="item-query", chat_id=e.link.chat_id, message_id=5801, message_thread_id=None, guard=guard)

    if damage:
        with pytest.raises((PermanentDeliveryError, ProductSourceRetired)):
            await replay()
    else:
        assert await replay() == identity
