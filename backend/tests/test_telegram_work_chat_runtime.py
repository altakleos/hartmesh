"""Installed Work commands use real ingress and the supervisor's durable sender."""

import pytest
from _telegram_bot_test_support import ManualClock, make_update
from _telegram_repository_test_support import db as db
from sqlalchemy import select
from test_telegram_notification_runtime import runtime
from test_telegram_work_preview_recovery import dispatch
from test_telegram_work_source_delivery import work_delivery as work_delivery
from test_work_attention_pages import work_pages as work_pages

from deerflow.persistence.telegram_bots.model import TelegramInboundMarkRow, TelegramOutboxRow


async def installed_work_runtime(e):
    e.clock = ManualClock(e.sender.outbox.clock())
    e.sender.outbox.clock = e.clock.now
    e.channel._now = e.clock.now
    e.channel._services.callbacks.clock = e.clock.now
    return await runtime(e)


@pytest.mark.asyncio
@pytest.mark.parametrize("word", ["work", "status"])
async def test_installed_agent_work_commands_reach_one_full_acknowledged_list(work_delivery, word):
    e = work_delivery
    owner = await installed_work_runtime(e)
    try:
        update = make_update(6101, account=e.link.chat_id, text="/" + word)
        assert await e.channel.handle_update(update)
        await e.channel._drain_ingress()
        async with e.db.sessions() as reader:
            mark = await reader.get(TelegramInboundMarkRow, (e.link.id, 6101, e.link.chat_id))
            assert mark is not None and mark.route_kind == "work_view"
            rows = list(await reader.scalars(select(TelegramOutboxRow).where(TelegramOutboxRow.payload["work_view_intent"]["key"].as_string() == "view.list")))
            assert len(rows) == 1 and rows[0].state == "pending" and rows[0].parts
            e.identity = rows[0].id
        await dispatch(e, 6102)
        assert not e.dispatch_errors, e.dispatch_errors
        async with e.db.sessions() as reader:
            row = await reader.get(TelegramOutboxRow, e.identity)
            assert row.state == "sent" and row.message_id == 6102
        sent = [call.parameters for call in e.channel.bot.request.calls if call.endpoint == "sendMessage"]
        assert len(sent) == 1 and e.work["objective"] in sent[0]["text"] and sent[0]["reply_markup"]
        assert await e.channel.handle_update(update)
        await e.channel._drain_ingress()
        async with e.db.sessions() as reader:
            assert len(list(await reader.scalars(select(TelegramOutboxRow).where(TelegramOutboxRow.payload["work_view_intent"]["key"].as_string() == "view.list")))) == 1
    finally:
        await owner.stop()


@pytest.mark.asyncio
async def test_installed_list_detail_start_uses_original_sdk_controls_and_separate_conversation(work_delivery, monkeypatch):
    from test_telegram_callback_commands import press
    from test_telegram_work_interaction_delivery import selected
    from test_telegram_work_start import prepare_startable

    from deerflow.persistence.telegram_bots.model import TelegramActRow, TelegramCallbackRow

    e = work_delivery
    owner = await installed_work_runtime(e)
    e.acts = owner.host.acts
    await prepare_startable(e, monkeypatch)
    e.clock.advance(1)  # The fresh record must precede the list's frozen horizon.
    try:
        assert await e.channel.handle_update(make_update(6111, account=e.link.chat_id, text="/work"))
        await e.channel._drain_ingress()
        async with e.db.sessions() as reader:
            row = await reader.scalar(select(TelegramOutboxRow).where(TelegramOutboxRow.payload["work_view_intent"]["key"].as_string() == "view.list"))
            e.identity = row.id
        await dispatch(e, 6112)
        async with e.db.sessions() as reader:
            detail = await reader.scalar(
                select(TelegramCallbackRow).where(TelegramCallbackRow.payload["anchor_outbox_id"].as_string() == e.identity, TelegramCallbackRow.kind == "work_details", TelegramCallbackRow.target_id == e.work["id"])
            )
        e.channel.bot.request.enqueue("answerCallbackQuery", result=True)
        assert await e.channel.handle_update(press(e, 6113, detail.id, message_id=6112))
        await e.channel._drain_ingress()
        async with e.db.sessions() as reader:
            row = await reader.scalar(select(TelegramOutboxRow).where(TelegramOutboxRow.payload["work_view_intent"]["key"].as_string() == "view.detail", TelegramOutboxRow.work_id == e.work["id"]))
            assert row is not None and row.state == "pending"
            e.identity = row.id
        await dispatch(e, 6114)
        start = await selected(e, "work_start")
        assert not e.launches
        e.channel.bot.request.enqueue("answerCallbackQuery", result=True)
        assert await e.channel.handle_update(press(e, 6115, start.id, message_id=6114))
        await e.channel._drain_ingress()
        chat_thread = await e.db.connections.get_thread_id(e.link.connection_id, str(e.link.chat_id), None)
        async with e.db.sessions() as reader:
            act = await reader.get(TelegramActRow, start.id)
            assert act is not None and act.kind == "activate" and act.state == "done"
            assert act.body["thread_id"] != chat_thread
        assert len(e.launches) == 1
        assert not e.dispatch_errors, e.dispatch_errors
    finally:
        await owner.stop()


@pytest.mark.asyncio
@pytest.mark.parametrize("entry", ["inline", "prompt"])
async def test_installed_assign_words_reach_one_private_review_card(work_delivery, monkeypatch, entry):
    from sqlalchemy import func

    from app.telegram_bots.assign import DraftAssignment
    from deerflow.persistence.agent_instances.work import AgentWorkRow
    from deerflow.persistence.telegram_bots.model import TelegramActRow

    e = work_delivery
    owner = await installed_work_runtime(e)
    words, calls = "Review the handbook", []

    async def proposal(value, **kwargs):
        calls.append(value)
        return DraftAssignment(objective=value, success_criteria="Record verified corrections")

    monkeypatch.setattr("app.telegram_bots.assignment_requests.draft_assignment", proposal)
    async with e.db.sessions() as reader:
        before = await reader.scalar(select(func.count()).select_from(AgentWorkRow))
    try:
        first = make_update(6121, account=e.link.chat_id, text="/assign" + (" " + words if entry == "inline" else ""))
        assert await e.channel.handle_update(first)
        await e.channel._drain_ingress()
        final = first
        if entry == "prompt":
            assert calls == []
            async with e.db.sessions() as reader:
                prompt = await reader.scalar(select(TelegramOutboxRow).where(TelegramOutboxRow.payload["assignment_prompt_intent"]["purpose"].as_string() == "assign_words"))
                assert prompt is not None and prompt.parts
                e.identity = prompt.id
            await dispatch(e, 6122)
            final = make_update(6123, account=e.link.chat_id, text=words)
            assert await e.channel.handle_update(final)
            await e.channel._drain_ingress()
        assert calls == [words]
        async with e.db.sessions() as reader:
            mark = await reader.get(TelegramInboundMarkRow, (e.link.id, final.message.message_id, e.link.chat_id))
            assert mark.route_kind == "assignment_draft"
            root = await reader.get(TelegramActRow, mark.route_target_id)
            assert root.state == "draft" and not root.operation_ids and root.body["draft"]["objective"] == words
            card = await reader.scalar(select(TelegramOutboxRow).where(TelegramOutboxRow.payload["assignment_intent"]["draft_id"].as_string() == root.id))
            assert card is not None and card.state == "pending" and card.parts[-1]["stored_keyboard"]
            assert await reader.scalar(select(func.count()).select_from(AgentWorkRow)) == before
            assert not list(await reader.scalars(select(TelegramActRow).where(TelegramActRow.kind == "activate")))
        assert await e.channel.handle_update(final)
        await e.channel._drain_ingress()
        assert calls == [words]
    finally:
        await owner.stop()
