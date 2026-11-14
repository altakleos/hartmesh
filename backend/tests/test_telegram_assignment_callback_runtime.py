"""Installed card gestures preserve original saved words and two-step starts."""

from hashlib import sha256

import pytest
from _telegram_bot_test_support import make_update
from _telegram_repository_test_support import db as db
from sqlalchemy import func, select
from test_telegram_assignment_entrypoints import installed_prompt
from test_telegram_callback_commands import press
from test_telegram_work_interaction_delivery import selected
from test_telegram_work_preview_recovery import dispatch
from test_telegram_work_source_delivery import work_delivery as work_delivery
from test_telegram_work_start import prepare_startable
from test_work_attention_pages import work_pages as work_pages

from deerflow.persistence.agent_instances.work import AgentWorkEventRow, AgentWorkRow
from deerflow.persistence.telegram_bots.model import TelegramActRow, TelegramInboundMarkRow, TelegramOutboxRow


@pytest.mark.asyncio
@pytest.mark.parametrize("action", ["assign_save", "assign_start"])
async def test_installed_assignment_card_press_saves_once_and_only_start_launches(work_delivery, monkeypatch, action):
    e = work_delivery
    owner, calls = await installed_prompt(e, monkeypatch)
    e.acts = owner.host.acts
    await prepare_startable(e, monkeypatch)
    e.clock.advance(1)
    try:
        assert await e.channel.handle_update(make_update(6141, account=e.link.chat_id, text="Review the handbook"))
        await e.channel._drain_ingress()
        async with e.db.sessions() as reader:
            mark = await reader.get(TelegramInboundMarkRow, (e.link.id, 6141, e.link.chat_id))
            root_id = mark.route_target_id
            card = await reader.scalar(select(TelegramOutboxRow).where(TelegramOutboxRow.payload["assignment_intent"]["draft_id"].as_string() == root_id))
            e.identity = card.id
            before = await reader.scalar(select(func.count()).select_from(AgentWorkRow))
        await dispatch(e, 6142)
        assert not e.dispatch_errors, e.dispatch_errors
        callback = await selected(e, action)
        e.channel.bot.request.enqueue("answerCallbackQuery", result=True)
        update = press(e, 6143, callback.id, message_id=6142)
        assert await e.channel.handle_update(update)
        await e.channel._drain_ingress()
        async with e.db.sessions() as reader:
            primary = await reader.get(TelegramActRow, callback.id)
            assert primary is not None and primary.kind == "delegate" and primary.state == "done"
            root = await reader.get(TelegramActRow, root_id)
            assert root.operation_ids == [primary.id] and primary.body["objective"] == root.body["draft"]["objective"]
            assert await reader.scalar(select(func.count()).select_from(AgentWorkRow)) == before + 1
            assert await reader.scalar(select(func.count()).select_from(AgentWorkEventRow).where(AgentWorkEventRow.operation_id == primary.id)) == 1
            child = await reader.get(TelegramActRow, sha256((callback.id + ":activate").encode()).hexdigest()[:32])
            if action == "assign_start":
                assert child is not None and child.kind == "activate" and child.state == "done"
                assert child.result["activation_receipt"]["operation_id"] == child.id
            else:
                assert child is None and primary.continuation["delegate_source"]["start"] is False
                assert "activation_continuation" not in primary.continuation
        assert len(e.launches) == int(action == "assign_start")
        assert calls == ["Review the handbook"]
        assert await e.channel.handle_update(update)
        await e.channel._drain_ingress()
        assert len(e.launches) == int(action == "assign_start") and calls == ["Review the handbook"]
    finally:
        await owner.stop()
