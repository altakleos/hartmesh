"""A valid canonical suggestion above the extraction limit has an owned reply."""

import pytest
from _telegram_repository_test_support import db as db
from sqlalchemy import select
from test_telegram_bot_adapter import message
from test_telegram_callback_commands import press
from test_telegram_notification_runtime import runtime
from test_telegram_result_notice_delivery import prepare
from test_telegram_work_file_exports import work_exports as work_exports
from test_telegram_work_source_delivery import work_delivery as work_delivery
from test_work_attention_pages import work_pages as work_pages

from deerflow.persistence.telegram_bots.model import TelegramActRow, TelegramCallbackRow, TelegramOutboxRow


@pytest.mark.asyncio
@pytest.mark.parametrize("work_exports", ["done_long_suggestion_event", "done_suggestion_event"], indirect=True)
async def test_real_suggestion_over_drafting_limit_gets_one_manual_assign_reply(work_exports, monkeypatch):
    e, calls = work_exports, []
    owner = await runtime(e)

    async def forbidden(**kwargs):
        calls.append(kwargs)
        raise AssertionError("oversized suggestion must not enter extraction")

    monkeypatch.setattr("app.telegram_bots.assign.run_oneshot_llm", forbidden)
    if len(e.result["suggestion"]["statement"]) < 4000:
        owner.host.outbox.config.assign.max_chars = 10
    try:
        _plans, identity = await prepare(e)
        async with e.db.sessions() as reader:
            row = await reader.get(TelegramOutboxRow, identity)
            kinds = [part["kind"] for part in row.parts]
        for index, kind in enumerate(kinds):
            message(e.channel.bot.request, "sendDocument" if kind == "document" else "sendMessage", identity=940 + index)
            assert await e.sender.tick() == 1
            await e.sender.drain()
            e.delivery_clock.advance(2)
        async with e.db.sessions() as reader:
            row = await reader.get(TelegramOutboxRow, identity)
            selected = await reader.scalar(select(TelegramCallbackRow).where(TelegramCallbackRow.group_id == row.parts[-1]["callback_group_id"]))
            assert row.payload["result_notice_basis"]["source"]["work"]["suggestion"] == e.result["suggestion"]
        for update in (950, 951):
            e.channel.bot.request.enqueue("answerCallbackQuery", result=True)
            assert await e.channel.handle_update(press(e, update, selected.id, message_id=939 + len(kinds)))
            await e.channel._drain_ingress()
        async with e.db.sessions() as reader:
            notices = [item for item in await reader.scalars(select(TelegramOutboxRow)) if any(part.get("source_ref", {}).get("key") == "assign.suggestion_too_long" for part in item.parts)]
            assert len(notices) == 1
            assert not list(await reader.scalars(select(TelegramActRow)))
        message(e.channel.bot.request, identity=952)
        assert await e.sender.tick() == 1
        await e.sender.drain()
        text = [call.parameters["text"] for call in e.channel.bot.request.calls if call.endpoint == "sendMessage"][-1]
        assert "/assign" in text and str(owner.host.outbox.config.assign.max_chars) in text
        assert not calls and not e.dispatch_errors
    finally:
        await owner.stop()
