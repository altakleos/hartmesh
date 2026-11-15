"""Protected suggestion drafting retains the actual consumed Show and cleanup."""

import json

import pytest
from _telegram_repository_test_support import db as db
from sqlalchemy import select
from test_telegram_bot_adapter import message
from test_telegram_callback_commands import press
from test_telegram_digest_item_families import protect
from test_telegram_notification_runtime import runtime
from test_telegram_result_notice_delivery import prepare
from test_telegram_work_file_exports import work_exports as work_exports
from test_telegram_work_source_delivery import work_delivery as work_delivery
from test_work_attention_pages import work_pages as work_pages

from deerflow.persistence.telegram_bots.model import TelegramActRow, TelegramCallbackRow, TelegramOutboxRow


@pytest.mark.asyncio
@pytest.mark.parametrize("work_exports", ["done_suggestion_event"], indirect=True)
@pytest.mark.parametrize("damage", ["none", "show", "cleanup", "cleanup-shape"])
async def test_suggestion_press_requires_its_actual_protected_show_receipt(work_exports, monkeypatch, damage):
    e, calls = work_exports, []
    owner = await runtime(e)

    async def model(**kwargs):
        calls.append(kwargs)
        return json.dumps({"objective": "Review the older evidence next.", "success_criteria": "List verified references"})

    monkeypatch.setattr("app.telegram_bots.assign.run_oneshot_llm", model)
    monkeypatch.setattr("app.telegram_bots.assign.get_app_config", lambda: e.live.config)
    try:
        await protect(e)
        _plans, identity = await prepare(e)
        message(e.channel.bot.request, identity=921)
        assert await e.sender.tick() == 1
        await e.sender.drain()
        e.delivery_clock.advance(2)
        async with e.db.sessions() as reader:
            row = await reader.get(TelegramOutboxRow, identity)
            show = await reader.scalar(select(TelegramCallbackRow).where(TelegramCallbackRow.group_id == row.parts[-1]["callback_group_id"]))
            assert show.kind == "reveal" and not calls
        e.channel.bot.request.enqueue("answerCallbackQuery", result=True)
        assert await e.channel.handle_update(press(e, 922, show.id, message_id=921))
        await e.channel._drain_ingress()
        for endpoint, message_id in (("editMessageReplyMarkup", 921), ("sendDocument", 923), ("sendMessage", 924)):
            message(e.channel.bot.request, endpoint, identity=message_id)
            assert await e.sender.tick() == 1
            await e.sender.drain()
            e.delivery_clock.advance(2)
        assert not e.dispatch_errors, e.dispatch_errors
        async with e.db.sessions.begin() as writer:
            row = await writer.get(TelegramOutboxRow, identity)
            selected = await writer.scalar(select(TelegramCallbackRow).where(TelegramCallbackRow.group_id == row.parts[-1]["callback_group_id"], TelegramCallbackRow.kind == "assign_from_suggestion"))
            assert selected is not None and selected.message_id == 924 and row.payload["result_notice_basis"]["suggestion_controls"] == 1
            if damage == "show":
                await writer.delete(await writer.get(TelegramCallbackRow, show.id))
            elif damage == "cleanup":
                cleanup = await writer.get(TelegramOutboxRow, row.payload["result_notice_basis"]["cleanup_id"])
                cleanup.payload = {**cleanup.payload, "target_message_id": 9999}
        e.channel.bot.request.enqueue("answerCallbackQuery", result=True)
        assert await e.channel.handle_update(press(e, 925, selected.id, message_id=924))
        await e.channel._drain_ingress()
        expected = int(damage in {"none", "cleanup-shape"})
        assert len(calls) == expected
        if damage == "cleanup-shape":
            async with e.db.sessions.begin() as writer:
                parent = await writer.get(TelegramOutboxRow, identity)
                cleanup = await writer.get(TelegramOutboxRow, parent.payload["result_notice_basis"]["cleanup_id"])
                cleanup.payload = []
        async with e.db.sessions() as reader:
            roots = list(await reader.scalars(select(TelegramActRow)))
            assert len(roots) == expected
            if expected:
                from app.telegram_bots.assignment_retention import assignment_dependencies

                retained = await assignment_dependencies(reader, e.sender.outbox.clock(), max_age_days=7)
                assert identity in retained.deliveries
                if damage == "none":
                    assert row.payload["result_notice_basis"]["cleanup_id"] in retained.deliveries
                assert show.group_id in retained.groups
    finally:
        await owner.stop()
