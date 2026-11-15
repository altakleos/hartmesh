"""Original suggestion proof reaches real Gateway admission, with offline launch."""

import json

import pytest
from _telegram_repository_test_support import db as db
from sqlalchemy import func, select
from test_telegram_bot_adapter import message
from test_telegram_callback_commands import press
from test_telegram_notification_runtime import runtime
from test_telegram_result_suggestion_drafts import ready_suggestion
from test_telegram_result_suggestion_fences import damage_source
from test_telegram_work_file_exports import work_exports as work_exports
from test_telegram_work_interaction_delivery import guard_for
from test_telegram_work_source_delivery import work_delivery as work_delivery
from test_telegram_work_start import prepare_startable
from test_work_attention_pages import work_pages as work_pages

from deerflow.persistence.agent_instances.work import AgentWorkRow
from deerflow.persistence.telegram_bots.model import TelegramActRow, TelegramCallbackRow, TelegramOutboxRow


@pytest.mark.asyncio
@pytest.mark.parametrize("work_exports", ["done_suggestion_event"], indirect=True)
@pytest.mark.parametrize("damage", ["none", "event"])
async def test_suggestion_start_retains_original_receipts_through_gateway_admission(work_exports, monkeypatch, damage):
    e = work_exports
    owner = await runtime(e)
    e.acts = owner.host.acts
    await prepare_startable(e, monkeypatch)

    async def model(**kwargs):
        return json.dumps({"objective": "Review the older evidence next.", "success_criteria": "List verified references"})

    monkeypatch.setattr("app.telegram_bots.assign.run_oneshot_llm", model)
    monkeypatch.setattr("app.telegram_bots.assign.get_app_config", lambda: e.live.config)
    try:
        _plans, identity, selected = await ready_suggestion(e)
        e.channel.bot.request.enqueue("answerCallbackQuery", result=True)
        assert await e.channel.handle_update(press(e, 970, selected.id, message_id=911))
        await e.channel._drain_ingress()
        message(e.channel.bot.request, identity=971)
        assert await e.sender.tick() == 1
        await e.sender.drain()
        e.delivery_clock.advance(2)
        async with e.db.sessions() as reader:
            root = await reader.scalar(select(TelegramActRow))
            card = await reader.scalar(select(TelegramOutboxRow).where(TelegramOutboxRow.payload["assignment_intent"]["draft_id"].as_string() == root.id))
            start = await reader.scalar(select(TelegramCallbackRow).where(TelegramCallbackRow.group_id == card.parts[-1]["callback_group_id"], TelegramCallbackRow.kind == "assign_start"))
            before = await reader.scalar(select(func.count()).select_from(AgentWorkRow))
        staged = await owner.chat.assignment_admissions.stage(e.link, start.id, chat_id=e.link.chat_id, message_id=971, message_thread_id=None, guard=await guard_for(e))
        if damage != "none":
            await damage_source(e, identity, selected, damage)
        result = await e.acts.check(staged.id)
        if damage == "none":
            assert result.state == "done", (result.state, result.reason)
            await e.acts.resume_continuation(result.id)
            assert len(e.launches) == 1
            await e.acts.resume_continuation(result.id)
            assert len(e.launches) == 1
        else:
            assert result.state != "done" and not e.launches
        async with e.db.sessions() as reader:
            assert await reader.scalar(select(func.count()).select_from(AgentWorkRow)) == before + int(damage == "none")
    finally:
        await owner.stop()
