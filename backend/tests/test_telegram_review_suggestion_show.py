"""A protected reviewer result retains its original Show and cleanup proof."""

import json

import pytest
from _telegram_repository_test_support import db as db
from sqlalchemy import select
from test_telegram_bot_adapter import message
from test_telegram_callback_commands import press
from test_telegram_digest_item_families import protect
from test_telegram_notification_runtime import runtime
from test_telegram_work_file_exports import work_exports as work_exports
from test_telegram_work_review_event_delivery import queued
from test_telegram_work_source_delivery import work_delivery as work_delivery
from test_work_attention_pages import work_pages as work_pages

from app.telegram_bots.assignment_retention import assignment_dependencies
from app.telegram_bots.work_review_event_delivery import WorkReviewEventDelivery
from deerflow.persistence.telegram_bots.model import TelegramActRow, TelegramCallbackRow, TelegramOutboxRow


@pytest.mark.asyncio
@pytest.mark.parametrize("work_exports", ["review_suggestion_event"], indirect=True)
@pytest.mark.parametrize("damage", ["none", "show", "cleanup"])
async def test_review_suggestion_keeps_its_real_show_and_cleanup_through_drafting(work_exports, monkeypatch, damage):
    e, calls = work_exports, []
    owner = await runtime(e)

    async def model(**kwargs):
        calls.append(kwargs)
        return json.dumps({"objective": "Review the older evidence next.", "success_criteria": "List verified references"})

    monkeypatch.setattr("app.telegram_bots.assign.run_oneshot_llm", model)
    monkeypatch.setattr("app.telegram_bots.assign.get_app_config", lambda: e.live.config)
    try:
        await protect(e)
        plans, identity = await queued(e)
        assert await WorkReviewEventDelivery(plans).materialize(e.link, identity)
        message(e.channel.bot.request, identity=991)
        assert await e.sender.tick() == 1
        await e.sender.drain()
        e.delivery_clock.advance(2)
        async with e.db.sessions() as reader:
            row = await reader.get(TelegramOutboxRow, identity)
            show = await reader.scalar(select(TelegramCallbackRow).where(TelegramCallbackRow.group_id == row.parts[-1]["callback_group_id"]))
            assert show.kind == "reveal" and not e.native_reads
        e.channel.bot.request.enqueue("answerCallbackQuery", result=True)
        assert await e.channel.handle_update(press(e, 994, show.id, message_id=991))
        await e.channel._drain_ingress()
        for endpoint, message_id in (("editMessageReplyMarkup", 991), ("sendDocument", 992), ("sendMessage", 993)):
            message(e.channel.bot.request, endpoint, identity=message_id)
            assert await e.sender.tick() == 1
            await e.sender.drain()
            e.delivery_clock.advance(2)
        assert not e.dispatch_errors, e.dispatch_errors
        async with e.db.sessions.begin() as writer:
            row = await writer.get(TelegramOutboxRow, identity)
            selected = await writer.scalar(select(TelegramCallbackRow).where(TelegramCallbackRow.group_id == row.parts[-1]["callback_group_id"], TelegramCallbackRow.kind == "assign_from_suggestion"))
            cleanup_id = row.payload["work_request_basis"]["cleanup_id"]
            assert selected is not None and selected.message_id == 993 and not calls
            if damage == "show":
                await writer.delete(await writer.get(TelegramCallbackRow, show.id))
            elif damage == "cleanup":
                cleanup = await writer.get(TelegramOutboxRow, cleanup_id)
                cleanup.payload = {**cleanup.payload, "target_message_id": 9999}
        e.channel.bot.request.enqueue("answerCallbackQuery", result=True)
        assert await e.channel.handle_update(press(e, 995, selected.id, message_id=993))
        await e.channel._drain_ingress()
        assert len(calls) == int(damage == "none")
        async with e.db.sessions() as reader:
            assert len(list(await reader.scalars(select(TelegramActRow)))) == int(damage == "none")
            if damage == "none":
                retained = await assignment_dependencies(reader, e.sender.outbox.clock(), max_age_days=7)
                assert {identity, cleanup_id} <= retained.deliveries
                assert {selected.group_id, show.group_id} <= retained.groups
    finally:
        await owner.stop()
