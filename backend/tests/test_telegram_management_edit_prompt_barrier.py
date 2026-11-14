from datetime import timedelta

import pytest
from _telegram_repository_test_support import db as db
from test_telegram_work_interaction_delivery import guard_for, selected
from test_telegram_work_management_delivery import open_card, prepare_management
from test_telegram_work_source_delivery import work_delivery as work_delivery
from test_work_attention_pages import work_pages as work_pages

from app.telegram_bots.assignment_typing import shown_prompt
from app.telegram_bots.producer import _item
from deerflow.persistence.telegram_bots.model import TelegramCallbackRow, TelegramOutboxRow


async def open_editor(e):
    from app.telegram_bots.work_management_editing import WorkManagementEdits
    from app.telegram_bots.work_news_delivery import WorkNewsSources

    cards, button = await prepare_management(e, "work_change")
    await open_card(e, cards, button)
    parent_id = e.identity
    edit = await selected(e, "assign_edit")
    edits = WorkManagementEdits(cards)
    sources = WorkNewsSources(edits.prompts)
    e.channel._services.work_management_prompt_sources = sources
    identity = await edits.open(e.link, edit.id, query_id="review-prompt", chat_id=e.link.chat_id, message_id=edit.message_id, message_thread_id=None, guard=await guard_for(e))
    async with e.db.sessions() as reader:
        row = await reader.get(TelegramOutboxRow, identity)
        cleanup_id = row.payload["management_prompt_cleanup_id"]
        prompt_id = row.payload["management_prompt_intent"]["prompt_id"]
    return sources, parent_id, identity, cleanup_id, prompt_id, edit


async def acknowledge_cleanup(e, cleanup_id):
    e.channel.bot.request.enqueue("editMessageReplyMarkup", body={"ok": True, "result": True})
    e.clock.advance(10)
    for _ in range(8):
        async with e.db.sessions() as reader:
            row = await reader.get(TelegramOutboxRow, cleanup_id)
            if row.state == "sent":
                break
        claimed = await e.sender.tick()
        if claimed == 0:
            e.clock.advance(1)
            continue
        assert claimed == 1
        await e.sender.drain()
    async with e.db.sessions() as reader:
        row = await reader.get(TelegramOutboxRow, cleanup_id)
        assert row.state == "sent" and row.parts[0]["state"] == "sent", (row.state, row.failed_reason, e.dispatch_errors)
        assert row.parts[0]["message_ids"] == [6103]
    from app.telegram_bots.producer import DeliveryRetry

    assert all(isinstance(exc, DeliveryRetry) for exc in e.dispatch_errors), e.dispatch_errors
    e.dispatch_errors.clear()
    assert any(call.endpoint == "editMessageReplyMarkup" and call.parameters.get("message_id") == 6103 for call in e.channel.bot.request.calls)


@pytest.mark.asyncio
async def test_prompt_waits_for_real_keyboard_ack_then_arms_from_actual_sdk_ack(work_delivery):
    e = work_delivery
    sources, parent_id, identity, cleanup_id, prompt_id, edit = await open_editor(e)
    async with e.db.sessions.begin() as writer:
        cleanup = await writer.get(TelegramOutboxRow, cleanup_id)
        cleanup.not_before = e.clock.now() + timedelta(minutes=10)
    before = len([call for call in e.channel.bot.request.calls if call.endpoint == "sendMessage"])
    e.clock.advance(10)
    assert await e.sender.tick() == 1
    await e.sender.drain()
    assert len([call for call in e.channel.bot.request.calls if call.endpoint == "sendMessage"]) == before
    async with e.db.sessions() as reader:
        prompt = await reader.get(TelegramCallbackRow, prompt_id)
        assert prompt.payload["prompt_visibility"]["state"] == "pending" and prompt.message_id is None and prompt.expires_at is None
    e.dispatch_errors.clear()
    async with e.db.sessions.begin() as writer:
        (await writer.get(TelegramOutboxRow, cleanup_id)).not_before = e.clock.now()
    await acknowledge_cleanup(e, cleanup_id)
    e.channel.bot.request.enqueue("sendMessage", body={"ok": True, "result": {"message_id": 6301, "date": 1, "chat": {"id": e.link.chat_id, "type": "private"}, "text": "Correction prompt"}})
    e.clock.advance(10)
    assert await e.sender.tick() == 1
    await e.sender.drain()
    assert not e.dispatch_errors, e.dispatch_errors
    async with e.db.sessions() as reader:
        row = await reader.get(TelegramOutboxRow, identity)
        prompt = await reader.get(TelegramCallbackRow, prompt_id)
        assert row.state == "sent" and row.parts[-1]["message_ids"] == [6301]
        shown_prompt(_item(row), prompt)
        assert prompt.message_id == 6301 and prompt.payload["prompt_visibility"]["state"] == "visible" and prompt.expires_at is not None
    sent = [call for call in e.channel.bot.request.calls if call.endpoint in {"editMessageReplyMarkup", "sendMessage"}]
    assert sent[-2].endpoint == "editMessageReplyMarkup" and sent[-1].endpoint == "sendMessage"
