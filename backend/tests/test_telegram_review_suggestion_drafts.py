"""Review result suggestions retain their own canonical event and SDK carrier."""

import json

import pytest
from _telegram_repository_test_support import db as db
from sqlalchemy import select
from test_telegram_bot_adapter import message
from test_telegram_callback_commands import press
from test_telegram_notification_runtime import runtime
from test_telegram_work_file_exports import work_exports as work_exports
from test_telegram_work_review_event_delivery import queued
from test_telegram_work_source_delivery import work_delivery as work_delivery
from test_work_attention_pages import work_pages as work_pages

from app.telegram_bots.work_review_event_delivery import WorkReviewEventDelivery
from deerflow.persistence.agent_instances.model import AgentInstanceGrantRow
from deerflow.persistence.telegram_bots.model import TelegramActRow, TelegramCallbackRow, TelegramOutboxRow


@pytest.mark.asyncio
@pytest.mark.parametrize("work_exports", ["review_suggestion_event"], indirect=True)
@pytest.mark.parametrize("permissions", [7, 3, 1])
async def test_actual_review_result_suggestion_drafts_from_its_final_sdk_control(work_exports, monkeypatch, permissions):
    e, calls = work_exports, []
    owner = await runtime(e)

    async def model(**kwargs):
        calls.append(kwargs)
        return json.dumps({"objective": "Review the older evidence next.", "success_criteria": "List verified references"})

    monkeypatch.setattr("app.telegram_bots.assign.run_oneshot_llm", model)
    monkeypatch.setattr("app.telegram_bots.assign.get_app_config", lambda: e.live.config)
    try:
        plans, identity = await queued(e)
        assert await WorkReviewEventDelivery(plans).materialize(e.link, identity)
        async with e.db.sessions() as reader:
            row = await reader.get(TelegramOutboxRow, identity)
            controls = list(await reader.scalars(select(TelegramCallbackRow).where(TelegramCallbackRow.group_id == row.parts[-1]["callback_group_id"])))
            assert {control.kind for control in controls} == {"accept_open", "changes_open", "assign_from_suggestion"}
            selected = next(control for control in controls if control.kind == "assign_from_suggestion")
            assert selected.message_id is None and selected.payload["presentation_pending"]
        for endpoint, message_id in (("sendDocument", 980), ("sendMessage", 981)):
            message(e.channel.bot.request, endpoint, identity=message_id)
            assert await e.sender.tick() == 1
            await e.sender.drain()
            e.delivery_clock.advance(2)
        assert not calls and not e.dispatch_errors, e.dispatch_errors
        sent = [call for call in e.channel.bot.request.calls if call.endpoint == "sendMessage"][-1]
        assert "Review the older evidence next." in sent.parameters["text"]
        async with e.db.sessions() as reader:
            control = await reader.get(TelegramCallbackRow, selected.id)
            assert control.message_id == 981
        async with e.db.sessions.begin() as writer:
            (await writer.get(AgentInstanceGrantRow, (e.link.agent_ref, e.link.owner_user_id))).permissions = permissions
        e.channel.bot.request.enqueue("answerCallbackQuery", result=True)
        assert await e.channel.handle_update(press(e, 982, selected.id, message_id=981))
        await e.channel._drain_ingress()
        assert len(calls) == int(permissions != 1)
        async with e.db.sessions() as reader:
            roots = list(await reader.scalars(select(TelegramActRow)))
            assert len(roots) == int(permissions != 1)
            if permissions == 1:
                return
            assert roots[0].body["source"] == "suggestion" and not roots[0].operation_ids
            assert roots[0].body["origin"]["anchor_outbox_id"] == identity
            card = await reader.scalar(select(TelegramOutboxRow).where(TelegramOutboxRow.payload["assignment_intent"]["draft_id"].as_string() == roots[0].id))
            assert card is not None and card.parts
    finally:
        await owner.stop()
