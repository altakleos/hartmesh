"""A missing protected digest target requires an explicit current-source repair."""

import pytest
from _telegram_repository_test_support import db as db
from test_telegram_bot_adapter import message
from test_telegram_digest_views import concealed
from test_telegram_work_source_delivery import work_delivery as work_delivery
from test_work_attention_pages import work_pages as work_pages

from app.telegram_bots.work_digest_views import DigestViews, DigestViewSources
from deerflow.persistence.telegram_bots.model import TelegramOutboxRow


async def missing(e):
    plans, identity, members, callback, guard = await concealed(e)
    views = DigestViews(plans)
    e.channel._services.work_digest_view_sources = DigestViewSources(views)
    result = await views.reveal(e.link, callback, chat_id=e.link.chat_id, message_id=5801, message_thread_id=None, guard=guard)
    e.channel.bot.request.enqueue("editMessageText", status=400, body={"ok": False, "error_code": 400, "description": "Bad Request: message to edit not found"})
    e.delivery_clock.advance(2)
    assert await e.sender.tick() == 1
    await e.sender.drain()
    async with e.db.sessions() as reader:
        row = await reader.get(TelegramOutboxRow, result)
        assert (row.state, row.failed_reason) == ("failed", "digest-view-refresh-required")
    return views, identity, members, result, guard


@pytest.mark.asyncio
async def test_digest_missing_target_repair_sends_one_new_protected_card(work_delivery):
    e = work_delivery
    views, identity, members, result, guard = await missing(e)
    assert await views.repair(e.link, result, guard=guard) == result
    assert await views.repair(e.link, result, guard=guard) is None
    message(e.channel.bot.request, identity=5901)
    e.delivery_clock.advance(2)
    assert await e.sender.tick() == 1
    await e.sender.drain()
    calls = [call for call in e.channel.bot.request.calls if call.endpoint == "sendMessage"]
    assert len(calls) == 2 and calls[-1].parameters["protect_content"] is True
    async with e.db.sessions() as reader:
        row = await reader.get(TelegramOutboxRow, result)
        assert row.state == "sent" and row.message_id == 5901
        assert row.parts[0]["state"] == "retired" and row.parts[0]["message_ids"] == []
        assert row.parts[-1]["message_ids"] == [5901]
        assert (await reader.get(TelegramOutboxRow, identity)).message_id == 5801
        assert all([(await reader.get(TelegramOutboxRow, child)).skip_reason == "in-digest" for child in members])


@pytest.mark.asyncio
@pytest.mark.parametrize("field", ["claim_id", "claim_owner", "claimed_at"])
async def test_digest_repair_never_reclaims_an_active_delivery(work_delivery, field):
    e = work_delivery
    views, _identity, _members, result, guard = await missing(e)
    async with e.db.sessions.begin() as writer:
        row = await writer.get(TelegramOutboxRow, result)
        setattr(row, field, e.delivery_clock.now() if field == "claimed_at" else "another-owner")
    assert await views.repair(e.link, result, guard=guard) is None


@pytest.mark.asyncio
async def test_digest_show_refreshes_changed_locale_without_losing_consumed_show(work_delivery):
    from deerflow.persistence.telegram_bots.model import TelegramPersonSettingsRow

    e = work_delivery
    plans, identity, _members, callback, guard = await concealed(e)
    views = DigestViews(plans)
    e.channel._services.work_digest_view_sources = DigestViewSources(views)
    result = await views.reveal(e.link, callback, chat_id=e.link.chat_id, message_id=5801, message_thread_id=None, guard=guard)
    async with e.db.sessions.begin() as writer:
        (await writer.get(TelegramPersonSettingsRow, e.link.owner_user_id)).locale = "zh-CN"
    e.delivery_clock.advance(2)
    assert await e.sender.tick() == 1
    await e.sender.drain()
    async with e.db.sessions() as reader:
        row = await reader.get(TelegramOutboxRow, result)
        assert row.state == "pending" and row.payload["presentation_version"] == 1
        assert row.payload["work_digest_view_basis"]["locale"] == "zh-CN"
    e.channel.bot.request.enqueue("editMessageText", result={"message_id": 5801, "date": 1893456000, "chat": {"id": e.link.chat_id, "type": "private"}, "text": "Full digest"})
    e.delivery_clock.advance(2)
    assert await e.sender.tick() == 1
    await e.sender.drain()
    async with e.db.sessions() as reader:
        assert (await reader.get(TelegramOutboxRow, result)).state == "sent"
        assert (await reader.get(TelegramOutboxRow, identity)).state == "sent"


@pytest.mark.asyncio
async def test_digest_show_retains_multiple_refreshes_before_one_sdk_edit(work_delivery):
    from deerflow.persistence.telegram_bots.model import TelegramPersonSettingsRow

    e = work_delivery
    plans, _identity, _members, callback, guard = await concealed(e)
    views = DigestViews(plans)
    e.channel._services.work_digest_view_sources = DigestViewSources(views)
    result = await views.reveal(e.link, callback, chat_id=e.link.chat_id, message_id=5801, message_thread_id=None, guard=guard)
    for version, locale in enumerate(("zh-CN", "en-US"), 1):
        async with e.db.sessions.begin() as writer:
            (await writer.get(TelegramPersonSettingsRow, e.link.owner_user_id)).locale = locale
        e.delivery_clock.advance(2)
        assert await e.sender.tick() == 1
        await e.sender.drain()
        async with e.db.sessions() as reader:
            row = await reader.get(TelegramOutboxRow, result)
            assert row.state == "pending" and row.payload["presentation_version"] == version
            assert len(row.payload["work_digest_view_basis"]["history"]) == version
    e.channel.bot.request.enqueue("editMessageText", result={"message_id": 5801, "date": 1893456000, "chat": {"id": e.link.chat_id, "type": "private"}, "text": "Full digest"})
    e.delivery_clock.advance(2)
    assert await e.sender.tick() == 1
    await e.sender.drain()
    assert len([call for call in e.channel.bot.request.calls if call.endpoint == "editMessageText"]) == 1
    async with e.db.sessions() as reader:
        assert (await reader.get(TelegramOutboxRow, result)).state == "sent"


@pytest.mark.asyncio
async def test_digest_repair_final_provider_rechecks_new_callback_group(work_delivery, monkeypatch):
    from sqlalchemy import select

    from app.gateway import authz
    from app.telegram_bots import work_digest_view_recovery as module
    from app.telegram_bots.producer import ProductSourceRetired
    from deerflow.persistence.telegram_bots.model import TelegramCallbackRow

    e = work_delivery
    views, _identity, _members, result, guard = await missing(e)
    original_prepare, original_provider = module.prepare_attention, authz.resolve_route_permissions
    active, changed = [], []

    async def prepare(*args, **kwargs):
        admission = await original_prepare(*args, **kwargs)

        async def check(**options):
            projection = options.get("projection")
            if isinstance(projection, module._RepairProjection) and projection.item.state == "pending":
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
            row = await writer.get(TelegramOutboxRow, result)
            control = await writer.scalar(select(TelegramCallbackRow).where(TelegramCallbackRow.group_id == row.parts[-1]["callback_group_id"]))
            control.target_id = "f" * 32
            await writer.flush()
            changed.append(True)
        return held

    monkeypatch.setattr(module, "prepare_attention", prepare)
    monkeypatch.setattr(authz, "resolve_route_permissions", provider)
    with pytest.raises(ProductSourceRetired):
        await views.repair(e.link, result, guard=guard)
    assert changed
    async with e.db.sessions() as reader:
        assert (await reader.get(TelegramOutboxRow, result)).state == "failed"


@pytest.mark.asyncio
async def test_digest_repair_reconciles_retired_unacknowledged_show(work_delivery, monkeypatch):
    from app.telegram_bots.producer import ProductSourceRetired

    e = work_delivery
    plans, _identity, _members, callback, guard = await concealed(e)
    views = DigestViews(plans)
    source = DigestViewSources(views)
    e.channel._services.work_digest_view_sources = source
    result = await views.reveal(e.link, callback, chat_id=e.link.chat_id, message_id=5801, message_thread_id=None, guard=guard)

    async def retired(_captured):
        raise ProductSourceRetired()

    with monkeypatch.context() as context:
        context.setattr(source, "read_part", retired)
        e.delivery_clock.advance(2)
        assert await e.sender.tick() == 1
        await e.sender.drain()
    async with e.db.sessions() as reader:
        row = await reader.get(TelegramOutboxRow, result)
        assert row.state == "skipped" and row.parts[-1]["state"] == "sending" and row.claim_id is None
    assert await views.repair(e.link, result, guard=guard) == result
    e.channel.bot.request.enqueue("editMessageText", result={"message_id": 5801, "date": 1893456000, "chat": {"id": e.link.chat_id, "type": "private"}, "text": "Full digest"})
    e.delivery_clock.advance(2)
    assert await e.sender.tick() == 1
    await e.sender.drain()
    async with e.db.sessions() as reader:
        assert (await reader.get(TelegramOutboxRow, result)).state == "sent"


@pytest.mark.asyncio
@pytest.mark.parametrize("field", ["part_id", "callback_group_id"])
async def test_digest_refresh_cannot_reuse_a_retired_presentation_identity(work_delivery, field):
    from types import SimpleNamespace

    from app.telegram_bots.producer import PermanentDeliveryError, _item
    from app.telegram_bots.work_digest_views import frozen_view

    e = work_delivery
    views, _identity, _members, result, guard = await missing(e)
    await views.repair(e.link, result, guard=guard)
    async with e.db.sessions.begin() as writer:
        row = await writer.get(TelegramOutboxRow, result)
        row.parts = [*row.parts[:-1], {**row.parts[-1], field: row.parts[0][field]}]
        item = _item(row)
    with pytest.raises(PermanentDeliveryError):
        frozen_view(SimpleNamespace(link=e.link, item=item, part=None), pending=True)
