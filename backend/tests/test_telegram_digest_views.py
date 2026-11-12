"""Explicit Show preserves the acknowledged batch and replaces its controls."""

from datetime import UTC, datetime

import pytest
from _telegram_repository_test_support import db as db
from sqlalchemy import select
from test_telegram_digest_navigation import acknowledged
from test_telegram_work_source_delivery import work_delivery as work_delivery
from test_work_attention_pages import work_pages as work_pages

from deerflow.persistence.telegram_bots.model import TelegramCallbackRow, TelegramInstancePolicyRow, TelegramOutboxRow


async def concealed(e):
    async with e.db.sessions.begin() as writer:
        writer.add(TelegramInstancePolicyRow(instance_id=e.link.agent_ref, policy="protected", updated_by=e.actor.subject_id, updated_at=datetime.now(UTC)))
    plans, identity, members, callback, _ = await acknowledged(e)
    owner = await e.live.authority.prepare_live_owner(e.link.owner_user_id)
    guard = e.db.links.publication_guard(e.link, check_live_owner=owner, check_attempt_current=lambda: True)
    return plans, identity, members, callback, guard


@pytest.mark.asyncio
async def test_digest_show_edits_original_card_and_keeps_original_batch_ack(work_delivery):
    from datetime import timedelta

    from app.telegram_bots.retention import _pinned_deliveries
    from app.telegram_bots.work_digest_views import DigestViews, DigestViewSources

    e = work_delivery
    plans, identity, members, callback, guard = await concealed(e)
    views = DigestViews(plans)
    e.channel._services.work_digest_view_sources = DigestViewSources(views)
    options = dict(chat_id=e.link.chat_id, message_id=5801, message_thread_id=None, guard=guard)
    result = await views.reveal(e.link, callback, **options)
    assert result != identity
    assert await views.reveal(e.link, callback, **options) == result
    e.channel.bot.request.enqueue("editMessageText", result={"message_id": 5801, "date": 1893456000, "chat": {"id": e.link.chat_id, "type": "private"}, "text": "Full digest"})
    e.delivery_clock.advance(2)
    assert await e.sender.tick() == 1
    await e.sender.drain()
    assert not e.dispatch_errors
    edits = [call for call in e.channel.bot.request.calls if call.endpoint == "editMessageText"]
    assert len(edits) == 1 and edits[0].parameters["message_id"] == 5801
    assert len(edits[0].parameters["reply_markup"]["inline_keyboard"]) == 3
    async with e.db.sessions() as reader:
        parent, view = await reader.get(TelegramOutboxRow, identity), await reader.get(TelegramOutboxRow, result)
        assert parent.state == view.state == "sent"
        assert parent.message_id == view.message_id == 5801
        assert parent.payload["work_digest_basis"]["concealed"] is True
        assert all([(await reader.get(TelegramOutboxRow, child)).skip_reason == "in-digest" for child in members])
        show = await reader.get(TelegramCallbackRow, callback)
        assert show.consumed_at is not None and show.closed_reason == "revealed"
        controls = list(await reader.scalars(select(TelegramCallbackRow).where(TelegramCallbackRow.group_id == view.parts[0]["callback_group_id"])))
        assert len(controls) == 3 and all(control.kind == "digest_item" and control.message_id == 5801 and not control.payload["presentation_pending"] for control in controls)
        assert {identity, result, *members} <= await _pinned_deliveries(reader, e.delivery_clock.now() + timedelta(days=29), 30)


@pytest.mark.asyncio
async def test_digest_show_shutdown_drains_admitted_edit(work_delivery):
    import asyncio

    from app.telegram_bots.work_digest_views import DigestViews, DigestViewSources

    e = work_delivery
    plans, identity, members, callback, guard = await concealed(e)
    views = DigestViews(plans)
    e.channel._services.work_digest_view_sources = DigestViewSources(views)
    result = await views.reveal(e.link, callback, chat_id=e.link.chat_id, message_id=5801, message_thread_id=None, guard=guard)
    release = asyncio.Event()
    call = e.channel.bot.request.enqueue("editMessageText", release=release, result={"message_id": 5801, "date": 1893456000, "chat": {"id": e.link.chat_id, "type": "private"}, "text": "Full digest"})
    e.delivery_clock.advance(2)
    assert await e.sender.tick() == 1
    await asyncio.wait_for(call.started.wait(), 5)
    stopping = asyncio.create_task(e.sender.stop())
    try:
        await asyncio.sleep(0.05)
        assert not stopping.done() and not call.cancelled.is_set()
    finally:
        release.set()
        await asyncio.wait_for(stopping, 5)
    assert not call.cancelled.is_set()
    async with e.db.sessions() as reader:
        assert (await reader.get(TelegramOutboxRow, identity)).state == "sent"
        assert (await reader.get(TelegramOutboxRow, result)).state == "pending"
        assert all([(await reader.get(TelegramOutboxRow, child)).skip_reason == "in-digest" for child in members])


@pytest.mark.asyncio
async def test_pending_digest_show_keeps_consumed_origin_after_callback_sweep(work_delivery):
    from app.telegram_bots.work_digest_views import DigestViews, DigestViewSources

    e = work_delivery
    plans, identity, _members, callback, guard = await concealed(e)
    views = DigestViews(plans)
    e.channel._services.work_digest_view_sources = DigestViewSources(views)
    result = await views.reveal(e.link, callback, chat_id=e.link.chat_id, message_id=5801, message_thread_id=None, guard=guard)
    e.delivery_clock.advance(8 * 86400)
    plans.callbacks.clock = e.delivery_clock.now
    await plans.callbacks.sweep(e.delivery_clock.now())
    async with e.db.sessions() as reader:
        assert await reader.get(TelegramCallbackRow, callback) is not None
    e.channel.bot.request.enqueue("editMessageText", result={"message_id": 5801, "date": 1893456000, "chat": {"id": e.link.chat_id, "type": "private"}, "text": "Full digest"})
    assert await e.sender.tick() == 1
    await e.sender.drain()
    async with e.db.sessions() as reader:
        assert (await reader.get(TelegramOutboxRow, result)).state == "sent"


@pytest.mark.asyncio
@pytest.mark.parametrize("change", ["receipt", "show", "member", "view_claim", "view_controls", "locale"])
async def test_digest_show_final_provider_rejects_changed_origin_or_presentation(work_delivery, monkeypatch, change):
    from app.gateway import authz
    from app.telegram_bots import work_digest_views as module
    from deerflow.persistence.agent_instances.human_input import HumanInputRequestRow
    from deerflow.persistence.telegram_bots.model import TelegramPersonSettingsRow

    e = work_delivery
    plans, identity, members, callback, guard = await concealed(e)
    views = module.DigestViews(plans)
    e.channel._services.work_digest_view_sources = module.DigestViewSources(views)
    result = await views.reveal(e.link, callback, chat_id=e.link.chat_id, message_id=5801, message_thread_id=None, guard=guard)
    original_prepare, original_provider = module.prepare_attention, authz.resolve_route_permissions
    active, changed = [], []

    async def prepare(*args, **kwargs):
        admission = await original_prepare(*args, **kwargs)

        async def check(**options):
            if isinstance(options.get("projection"), module._ViewProjection):
                active.append(True)
            try:
                return await admission(**options)
            finally:
                active.clear()

        return check

    async def provider(*args, **kwargs):
        held = await original_provider(*args, **kwargs)
        if active and not changed:
            async with e.db.sessions.begin() as writer:
                parent = await writer.get(TelegramOutboxRow, identity)
                view = await writer.get(TelegramOutboxRow, result)
                if change == "receipt":
                    parent.parts = [{**parent.parts[0], "message_ids": [5802]}]
                elif change == "show":
                    (await writer.get(TelegramCallbackRow, callback)).closed_reason = "superseded"
                elif change == "member":
                    child = await writer.get(TelegramOutboxRow, members[-1])
                    request = await writer.get(HumanInputRequestRow, child.request_id)
                    request.state, request.closed_reason = "closed", "resolved"
                elif change == "view_claim":
                    view.claim_id = "foreign-claim"
                elif change == "view_controls":
                    control = await writer.scalar(select(TelegramCallbackRow).where(TelegramCallbackRow.group_id == view.parts[0]["callback_group_id"]))
                    control.target_id = "f" * 32
                else:
                    (await writer.get(TelegramPersonSettingsRow, e.link.owner_user_id)).locale = "zh-CN"
            changed.append(True)
        return held

    monkeypatch.setattr(module, "prepare_attention", prepare)
    monkeypatch.setattr(authz, "resolve_route_permissions", provider)
    e.channel.bot.request.enqueue("editMessageText", result={"message_id": 5801, "date": 1893456000, "chat": {"id": e.link.chat_id, "type": "private"}, "text": "Full digest"})
    e.delivery_clock.advance(2)
    assert await e.sender.tick() == 1
    await e.sender.drain()
    assert changed and not [call for call in e.channel.bot.request.calls if call.endpoint == "editMessageText"]
