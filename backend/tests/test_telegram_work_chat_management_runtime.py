"""Installed management buttons preserve review and explicit confirmation."""

import pytest
from _telegram_bot_test_support import make_update
from _telegram_repository_test_support import db as db
from sqlalchemy import func, select
from test_telegram_callback_commands import press
from test_telegram_work_chat_runtime import installed_work_runtime
from test_telegram_work_interaction_delivery import selected
from test_telegram_work_preview_recovery import dispatch
from test_telegram_work_source_delivery import work_delivery as work_delivery
from test_work_attention_pages import work_pages as work_pages

from deerflow.persistence.agent_instances.work import AgentWorkEventRow, AgentWorkRow
from deerflow.persistence.telegram_bots.model import TelegramActRow, TelegramCallbackRow, TelegramOutboxRow


async def open_detail(e, *, first=6171, topic=None):
    assert await e.channel.handle_update(make_update(first, account=e.link.chat_id, text="/work", topic=topic))
    await e.channel._drain_ingress()
    async with e.db.sessions() as reader:
        listing = await reader.scalar(select(TelegramOutboxRow).where(TelegramOutboxRow.payload["work_view_intent"]["key"].as_string() == "view.list"))
        e.identity = listing.id
    await dispatch(e, first + 1)
    async with e.db.sessions() as reader:
        detail = await reader.scalar(select(TelegramCallbackRow).where(TelegramCallbackRow.payload["anchor_outbox_id"].as_string() == e.identity, TelegramCallbackRow.kind == "work_details", TelegramCallbackRow.target_id == e.work["id"]))
    e.channel.bot.request.enqueue("answerCallbackQuery", result=True)
    update = press(e, first + 2, detail.id, message_id=first + 1)
    if topic is not None:
        from telegram import Update

        data = update.to_dict()
        data["callback_query"]["message"]["message_thread_id"] = topic
        update = Update.de_json(data, e.channel.bot)
    assert await e.channel.handle_update(update)
    await e.channel._drain_ingress()
    async with e.db.sessions() as reader:
        row = await reader.scalar(select(TelegramOutboxRow).where(TelegramOutboxRow.payload["work_view_intent"]["key"].as_string() == "view.detail", TelegramOutboxRow.work_id == e.work["id"]))
        e.identity = row.id
    await dispatch(e, first + 3)
    assert not e.dispatch_errors, e.dispatch_errors


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ["work_change", "work_withdraw"])
async def test_installed_management_open_is_private_and_withdraw_waits_for_second_press(work_delivery, kind):
    e = work_delivery
    owner = await installed_work_runtime(e)
    try:
        await open_detail(e)
        button = await selected(e, kind)
        async with e.db.sessions() as reader:
            before = (await reader.get(AgentWorkRow, e.work["id"])).revision
        e.channel.bot.request.enqueue("answerCallbackQuery", result=True)
        assert await e.channel.handle_update(press(e, 6175, button.id, message_id=6174))
        await e.channel._drain_ingress()
        async with e.db.sessions() as reader:
            row = await reader.scalar(select(TelegramOutboxRow).where(TelegramOutboxRow.payload["work_management_intent"]["kind"].as_string() == ("edit" if kind == "work_change" else "cancel")))
            assert row is not None and row.state == "pending" and row.parts
            e.identity = row.id
            assert (await reader.get(AgentWorkRow, e.work["id"])).revision == before
            assert not list(await reader.scalars(select(TelegramActRow).where(TelegramActRow.state == "done")))
        await dispatch(e, 6176)
        assert not e.dispatch_errors, e.dispatch_errors
        if kind == "work_change":
            assert await selected(e, "assign_edit") and await selected(e, "assign_discard")
        else:
            confirm = await selected(e, "work_withdraw_confirm")
            e.channel.bot.request.enqueue("answerCallbackQuery", result=True)
            assert await e.channel.handle_update(press(e, 6177, confirm.id, message_id=6176))
            await e.channel._drain_ingress()
            async with e.db.sessions() as reader:
                act = await reader.get(TelegramActRow, confirm.id)
                assert act is not None and act.kind == "cancel" and act.state == "done"
                assert (await reader.get(AgentWorkRow, e.work["id"])).status == "cancelled"
                assert await reader.scalar(select(func.count()).select_from(AgentWorkEventRow).where(AgentWorkEventRow.operation_id == act.id, AgentWorkEventRow.action == "cancel")) == 1
    finally:
        await owner.stop()


@pytest.mark.asyncio
async def test_installed_saved_edit_typing_is_private_until_fresh_save_changes(work_delivery, monkeypatch):
    from types import SimpleNamespace
    from unittest.mock import AsyncMock, Mock

    from test_telegram_work_start import prepare_startable

    from app.telegram_bots.assign import DraftAssignment

    e = work_delivery
    owner = await installed_work_runtime(e)
    await prepare_startable(e, monkeypatch)
    e.clock.advance(1)
    owner.host.turns = SimpleNamespace(accept=AsyncMock(), retire=AsyncMock(), message_sent=Mock(), typing_allowed=lambda *args, **kwargs: False)
    calls = []

    async def proposal(words, **kwargs):
        calls.append((words, kwargs["previous"]))
        return DraftAssignment(objective="Review the expanded guide", success_criteria="Record verified corrections")

    monkeypatch.setattr("app.telegram_bots.work_management_editing.draft_assignment", proposal)
    try:
        await open_detail(e)
        change = await selected(e, "work_change")
        e.channel.bot.request.enqueue("answerCallbackQuery", result=True)
        assert await e.channel.handle_update(press(e, 6175, change.id, message_id=6174))
        await e.channel._drain_ingress()
        async with e.db.sessions() as reader:
            parent = await reader.scalar(select(TelegramOutboxRow).where(TelegramOutboxRow.payload["work_management_intent"]["kind"].as_string() == "edit"))
            parent_id, e.identity = parent.id, parent.id
            before = (await reader.get(AgentWorkRow, e.work["id"])).revision
        await dispatch(e, 6176)
        edit = await selected(e, "assign_edit")
        e.channel.bot.request.enqueue("answerCallbackQuery", result=True)
        assert await e.channel.handle_update(press(e, 6177, edit.id, message_id=6176))
        await e.channel._drain_ingress()
        async with e.db.sessions() as reader:
            prompt = await reader.scalar(select(TelegramOutboxRow).where(TelegramOutboxRow.payload["management_prompt_intent"]["purpose"].as_string() == "work_edit"))
            assert prompt is not None
            cleanup_id = prompt.payload["management_prompt_cleanup_id"]
        e.channel.bot.request.enqueue("editMessageReplyMarkup", body={"ok": True, "result": True})
        e.clock.advance(10)
        for _ in range(8):
            async with e.db.sessions() as reader:
                cleanup = await reader.get(TelegramOutboxRow, cleanup_id)
                if cleanup.state == "sent":
                    assert cleanup.parts[0]["message_ids"] == [6176]
                    break
            assert await e.sender.tick() in {0, 1}
            await e.sender.drain()
            e.clock.advance(1)
        else:
            pytest.fail("The original management keyboard cleanup was not acknowledged")
        from app.telegram_bots.producer import DeliveryRetry

        assert all(isinstance(error, DeliveryRetry) for error in e.dispatch_errors), e.dispatch_errors
        e.dispatch_errors.clear()
        await dispatch(e, 6180)
        update = make_update(6181, account=e.link.chat_id, text="Include the additional chapter")
        assert await e.channel.handle_update(update)
        await e.channel._drain_ingress()
        assert len(calls) == 1 and calls[0][0] == "Include the additional chapter"
        assert owner.host.turns.accept.await_count == 0
        async with e.db.sessions() as reader:
            assert (await reader.get(AgentWorkRow, e.work["id"])).revision == before
            parent = await reader.get(TelegramOutboxRow, parent_id)
            assert parent.payload["work_news_basis"]["source"]["work"]["draft"]["objective"] == "Review the expanded guide"
        e.identity = parent_id
        e.clock.advance(10)
        e.channel.bot.request.enqueue("editMessageText", body={"ok": True, "result": {"message_id": 6176, "date": 1, "chat": {"id": e.link.chat_id, "type": "private"}, "text": "Corrected draft"}})
        assert await e.sender.tick() == 1
        await e.sender.drain()
        assert not e.dispatch_errors, e.dispatch_errors
        save = await selected(e, "work_change_save")
        e.channel.bot.request.enqueue("answerCallbackQuery", result=True)
        assert await e.channel.handle_update(press(e, 6182, save.id, message_id=6176))
        await e.channel._drain_ingress()
        async with e.db.sessions() as reader:
            act = await reader.get(TelegramActRow, save.id)
            work = await reader.get(AgentWorkRow, e.work["id"])
            assert act is not None and act.state == "done" and act.kind == "edit"
            assert work.objective == "Review the expanded guide" and work.success_criteria == "Record verified corrections"
        assert len(calls) == 1
        e.clock.advance(10)
        e.channel.bot.request.enqueue("editMessageText", body={"ok": True, "result": {"message_id": 6176, "date": 1, "chat": {"id": e.link.chat_id, "type": "private"}, "text": "Saved changes"}})
        assert await e.sender.tick() == 1
        await e.sender.drain()
        assert not e.dispatch_errors, e.dispatch_errors
        start = await selected(e, "work_start")
        assert start is not None and not e.launches
        e.channel.bot.request.enqueue("answerCallbackQuery", result=True)
        assert await e.channel.handle_update(press(e, 6183, start.id, message_id=6176))
        await e.channel._drain_ingress()
        async with e.db.sessions() as reader:
            act = await reader.get(TelegramActRow, start.id)
            assert act is not None and act.kind == "activate" and act.state == "done"
            assert len(e.launches) == 1
    finally:
        await owner.stop()
