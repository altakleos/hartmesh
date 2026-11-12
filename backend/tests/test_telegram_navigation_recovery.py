"""Navigation previews refresh without discarding their reusable origin."""

from copy import deepcopy

import pytest
from _telegram_repository_test_support import db as db
from test_telegram_bot_adapter import message
from test_telegram_digest_items import pending
from test_telegram_work_source_delivery import work_delivery as work_delivery
from test_work_attention_pages import work_pages as work_pages

from deerflow.persistence.agent_instances.work import AgentWorkRow
from deerflow.persistence.telegram_bots.model import TelegramCallbackRow, TelegramOutboxRow, TelegramPersonSettingsRow


@pytest.mark.asyncio
@pytest.mark.parametrize("change", ["locale", "revision"])
async def test_navigation_preview_refresh_preserves_full_source_and_original_gesture(work_delivery, change):
    from app.telegram_bots.producer import DeliveryClaimRetired
    from app.telegram_bots.work_navigation_recovery import WorkNavigationRecovery

    e = work_delivery
    items, identity, anchor, callback, target = await pending(e)
    e.channel._services.work_request_sources.recovery = WorkNavigationRecovery(e.plans, navigation=items.navigation)
    async with e.db.sessions.begin() as writer:
        row = await writer.get(TelegramOutboxRow, identity)
        origin = deepcopy(row.payload["work_request_basis"]["navigation_origin"])
        old_part = deepcopy(row.parts[-1])
        if change == "locale":
            (await writer.get(TelegramPersonSettingsRow, e.link.owner_user_id)).locale = "zh-CN"
        else:
            (await writer.get(AgentWorkRow, row.work_id)).revision += 1
    e.delivery_clock.advance(2)
    before = len(e.channel.bot.request.calls)
    assert await e.sender.tick() == 1
    await e.sender.drain()
    assert len(e.channel.bot.request.calls) == before
    async with e.db.sessions() as reader:
        row = await reader.get(TelegramOutboxRow, identity)
        assert row.state == "pending" and row.payload["presentation_version"] == 1
        basis = row.payload["work_request_basis"]
        assert basis["navigation_origin"] == origin and basis["concealed"] is False
        assert row.class_ == "reply" and row.parts[0]["part_id"] == old_part["part_id"]
        assert row.parts[0]["state"] == "retired" and row.parts[-1]["protect_content"] is True
        assert (await reader.get(TelegramCallbackRow, callback)).consumed_at is None
        assert (await reader.get(TelegramOutboxRow, target)).skip_reason == "in-digest"
        assert (await reader.get(TelegramOutboxRow, anchor)).state == "sent"
    message(e.channel.bot.request, identity=6201)
    e.delivery_clock.advance(2)
    assert await e.sender.tick() == 1
    await e.sender.drain()
    async with e.db.sessions() as reader:
        assert (await reader.get(TelegramOutboxRow, identity)).state == "sent"
    assert len(e.dispatch_errors) == 1 and isinstance(e.dispatch_errors[0], DeliveryClaimRetired)


@pytest.mark.asyncio
@pytest.mark.parametrize("change", ["target", "age", "anchor", "pending"])
async def test_navigation_refresh_rechecks_new_group_after_provider(work_delivery, monkeypatch, change):
    from datetime import timedelta

    from sqlalchemy import select

    from app.gateway import authz
    from app.telegram_bots import work_navigation_recovery as module

    e = work_delivery
    items, identity, _anchor, _callback, _target = await pending(e)
    e.channel._services.work_request_sources.recovery = module.WorkNavigationRecovery(e.plans, navigation=items.navigation)
    async with e.db.sessions.begin() as writer:
        (await writer.get(TelegramPersonSettingsRow, e.link.owner_user_id)).locale = "zh-CN"
    original_prepare, original_provider = module.prepare_attention, authz.resolve_route_permissions
    active, changed = [], []

    async def prepare(*args, **kwargs):
        admission = await original_prepare(*args, **kwargs)

        async def check(**options):
            if options.get("session") is not None:
                active.append(options["session"])
            try:
                return await admission(**options)
            finally:
                active.clear()

        return check

    async def provider(*args, **kwargs):
        held = await original_provider(*args, **kwargs)
        if active and not changed:
            writer = active[-1]
            row = await writer.get(TelegramOutboxRow, identity)
            if row.payload["presentation_version"] == 1:
                control = await writer.scalar(select(TelegramCallbackRow).where(TelegramCallbackRow.group_id == row.parts[-1]["callback_group_id"]))
                if change == "target":
                    control.target_id = "f" * 32
                elif change == "age":
                    control.created_at += timedelta(days=1)
                elif change == "anchor":
                    control.message_id = 9999
                else:
                    control.payload = {**control.payload, "presentation_pending": False}
                await writer.flush()
                changed.append(True)
        return held

    monkeypatch.setattr(module, "prepare_attention", prepare)
    monkeypatch.setattr(authz, "resolve_route_permissions", provider)
    before = len(e.channel.bot.request.calls)
    e.delivery_clock.advance(2)
    assert await e.sender.tick() == 1
    await e.sender.drain()
    assert changed and len(e.channel.bot.request.calls) == before
    async with e.db.sessions() as reader:
        row = await reader.get(TelegramOutboxRow, identity)
        assert row.payload["presentation_version"] == 0


@pytest.mark.asyncio
async def test_navigation_acknowledged_preview_refresh_keeps_original_button_age(work_delivery):
    from sqlalchemy import select

    from app.telegram_bots.work_navigation_recovery import WorkNavigationRecovery

    e = work_delivery
    items, identity, _anchor, callback, _target = await pending(e)
    recovery = WorkNavigationRecovery(e.plans, navigation=items.navigation)
    message(e.channel.bot.request, identity=6201)
    e.delivery_clock.advance(2)
    assert await e.sender.tick() == 1
    await e.sender.drain()
    async with e.db.sessions.begin() as writer:
        original = (await writer.get(TelegramCallbackRow, callback)).created_at
        (await writer.get(TelegramPersonSettingsRow, e.link.owner_user_id)).locale = "zh-CN"
    owner = await e.live.authority.prepare_live_owner(e.link.owner_user_id)
    guard = e.db.links.publication_guard(e.link, check_live_owner=owner, check_attempt_current=lambda: True)
    assert await recovery.refresh_anchor(e.link, identity, expected_version=0, guard=guard) == identity
    message(e.channel.bot.request, "editMessageText", identity=6201)
    e.delivery_clock.advance(2)
    assert await e.sender.tick() == 1
    await e.sender.drain()
    async with e.db.sessions() as reader:
        row = await reader.get(TelegramOutboxRow, identity)
        assert row.state == "sent" and row.parts[-1]["message_ids"] == [6201]
        controls = list(await reader.scalars(select(TelegramCallbackRow).where(TelegramCallbackRow.group_id == row.parts[-1]["callback_group_id"])))
        assert controls and all(control.created_at == original for control in controls)
        assert (await reader.get(TelegramCallbackRow, callback)).consumed_at is None


@pytest.mark.asyncio
async def test_navigation_refresh_rejects_malformed_earlier_text_receipt(work_delivery, monkeypatch):
    import test_telegram_work_event_intents as intents

    from app.telegram_bots.producer import PermanentDeliveryError, ProductSourceRetired
    from app.telegram_bots.work_navigation_recovery import WorkNavigationRecovery

    create = intents.CreateRequest
    monkeypatch.setattr(intents, "CreateRequest", lambda **kwargs: create(**{**kwargs, "question": "Q" * 4096, "reason": "R" * 4096}))
    e = work_delivery
    items, identity, _anchor, _callback, _target = await pending(e, protected=False)
    async with e.db.sessions() as reader:
        count = len((await reader.get(TelegramOutboxRow, identity)).parts)
        assert count >= 3
    for index in range(count):
        message(e.channel.bot.request, identity=6301 + index)
        e.delivery_clock.advance(2)
        assert await e.sender.tick() == 1
        await e.sender.drain()
    async with e.db.sessions.begin() as writer:
        row = await writer.get(TelegramOutboxRow, identity)
        assert row.state == "sent"
        row.parts = [{**row.parts[0], "message_ids": [9999, 6301]}, *row.parts[1:]]
        (await writer.get(TelegramPersonSettingsRow, e.link.owner_user_id)).locale = "zh-CN"
    owner = await e.live.authority.prepare_live_owner(e.link.owner_user_id)
    guard = e.db.links.publication_guard(e.link, check_live_owner=owner, check_attempt_current=lambda: True)
    with pytest.raises((PermanentDeliveryError, ProductSourceRetired)):
        await WorkNavigationRecovery(e.plans, navigation=items.navigation).refresh_anchor(e.link, identity, expected_version=0, guard=guard)
    async with e.db.sessions() as reader:
        assert (await reader.get(TelegramOutboxRow, identity)).payload["presentation_version"] == 0


@pytest.mark.asyncio
@pytest.mark.parametrize("protected", [False, True])
async def test_pending_navigation_keeps_expired_origin_bookkeeping_without_rearming(work_delivery, protected):
    from app.telegram_bots.callbacks import CallbackRefused

    e = work_delivery
    _items, identity, _anchor, callback, _target = await pending(e, protected=protected)
    async with e.db.sessions() as reader:
        original = (await reader.get(TelegramCallbackRow, callback)).created_at
    e.delivery_clock.advance((e.plans.callbacks.config.callbacks.max_age_days + 1) * 86400)
    e.plans.callbacks.clock = e.delivery_clock.now
    await e.plans.callbacks.sweep(e.delivery_clock.now())
    async with e.db.sessions() as reader:
        row = await reader.get(TelegramCallbackRow, callback)
        assert row is not None and row.created_at == original
        with pytest.raises(CallbackRefused):
            e.plans.callbacks._usable(row)
        assert (await reader.get(TelegramOutboxRow, identity)).state == "pending"
