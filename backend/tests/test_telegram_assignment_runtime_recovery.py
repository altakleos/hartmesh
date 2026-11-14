"""Restarted owners recover actual private claims without another extraction."""

import pytest
from _telegram_bot_test_support import make_update
from _telegram_repository_test_support import db as db
from sqlalchemy import select
from test_telegram_notification_runtime import runtime
from test_telegram_work_chat_runtime import installed_work_runtime
from test_telegram_work_preview_recovery import dispatch
from test_telegram_work_source_delivery import work_delivery as work_delivery
from test_work_attention_pages import work_pages as work_pages

from app.telegram_bots.assign import DraftAssignment
from deerflow.persistence.telegram_bots.model import TelegramActRow, TelegramInboundMarkRow, TelegramOutboxRow


@pytest.mark.asyncio
@pytest.mark.parametrize("entry", ["inline", "prompt"])
async def test_runtime_recovers_abandoned_private_extraction_with_manual_criteria(work_delivery, monkeypatch, entry):
    e = work_delivery
    owner = await installed_work_runtime(e)
    restarted, calls = None, []

    async def proposal(words, **kwargs):
        calls.append(words)
        return DraftAssignment(objective=words, success_criteria="Record verified corrections")

    async def interrupted(*args, **kwargs):
        raise RuntimeError("private-result-interrupted")

    monkeypatch.setattr("app.telegram_bots.assignment_requests.draft_assignment", proposal)
    try:
        if entry == "prompt":
            assert await e.channel.handle_update(make_update(6191, account=e.link.chat_id, text="/assign"))
            await e.channel._drain_ingress()
            await dispatch(e, 6192)
        update = make_update(6193, account=e.link.chat_id, text=("/assign " if entry == "inline" else "") + "Review the handbook")
        with monkeypatch.context() as fault:
            fault.setattr(owner.host.work_chat.assignment_inputs.requests, "_complete", interrupted)
            with pytest.raises(RuntimeError, match="private-result-interrupted"):
                await e.channel.handle_update(update)
        async with e.db.sessions() as reader:
            mark = await reader.get(TelegramInboundMarkRow, (e.link.id, 6193, e.link.chat_id))
            assert mark.route_kind == "assignment_draft" and not mark.route_complete
            identity = mark.route_target_id
            root = await reader.get(TelegramActRow, identity)
            assert root.body["version"] == 2 and root.body["request"]["phase"] == "extracting"
        await owner.stop()
        restarted = await runtime(e, preserve=True)
        await restarted.recover_inputs(lambda: True, link_id=e.link.id)
        await restarted.recover_inputs(lambda: True, link_id=e.link.id)
        async with e.db.sessions() as reader:
            root = await reader.get(TelegramActRow, identity)
            assert root.body["version"] == 2 and root.body["request"]["phase"] == "awaiting_done"
            assert root.body["request"]["words"] == "Review the handbook"
            rows = list(await reader.scalars(select(TelegramOutboxRow).where(TelegramOutboxRow.payload["assignment_prompt_intent"]["purpose"].as_string() == "assign_done")))
            assert len(rows) == 1 and rows[0].parts and rows[0].state == "pending"
            assert not root.operation_ids
        assert calls == ["Review the handbook"]
    finally:
        await owner.stop()
        if restarted is not None:
            await restarted.stop()


@pytest.mark.asyncio
@pytest.mark.parametrize("word,intent", [("work", "work_view_intent"), ("assign", "assignment_prompt_intent")])
async def test_runtime_materializes_original_pending_command_reply_after_restart(work_delivery, monkeypatch, word, intent):
    e = work_delivery
    owner = await installed_work_runtime(e)
    restarted = None

    async def interrupted(*args, **kwargs):
        raise RuntimeError("command-plan-interrupted")

    plans = owner.host.work_chat.views if word == "work" else owner.host.work_chat.assignment_prompts
    try:
        with monkeypatch.context() as fault:
            fault.setattr(plans, "materialize", interrupted)
            with pytest.raises(RuntimeError, match="command-plan-interrupted"):
                await e.channel.handle_update(make_update(6211, account=e.link.chat_id, text="/" + word))
        async with e.db.sessions() as reader:
            mark = await reader.get(TelegramInboundMarkRow, (e.link.id, 6211, e.link.chat_id))
            identity = mark.outbox_id
            pending = await reader.get(TelegramOutboxRow, identity)
            assert pending.state == "pending" and not pending.parts and intent in pending.payload
        await owner.stop()
        restarted = await runtime(e, preserve=True)
        assert await restarted.prepare_deliveries() == 1
        async with e.db.sessions() as reader:
            pending = await reader.get(TelegramOutboxRow, identity)
            assert pending.state == "pending" and pending.parts
        await dispatch(e, 6212)
        assert not e.dispatch_errors, e.dispatch_errors
        async with e.db.sessions() as reader:
            assert (await reader.get(TelegramOutboxRow, identity)).state == "sent"
            assert not list(await reader.scalars(select(TelegramActRow).where(TelegramActRow.kind == "activate")))
    finally:
        await owner.stop()
        if restarted is not None:
            await restarted.stop()


@pytest.mark.asyncio
async def test_maintenance_refreshes_saved_card_after_postcommit_observation_failure(work_delivery, monkeypatch):
    from test_telegram_assignment_entrypoints import installed_prompt
    from test_telegram_callback_commands import press
    from test_telegram_work_interaction_delivery import selected

    from deerflow.persistence.agent_instances.work import AgentWorkEventRow

    e = work_delivery
    owner, calls = await installed_prompt(e, monkeypatch)
    restarted = None

    async def interrupted(*args, **kwargs):
        raise RuntimeError("saved-card-observation-interrupted")

    try:
        assert await e.channel.handle_update(make_update(6221, account=e.link.chat_id, text="Review the handbook"))
        await e.channel._drain_ingress()
        async with e.db.sessions() as reader:
            mark = await reader.get(TelegramInboundMarkRow, (e.link.id, 6221, e.link.chat_id))
            card = await reader.scalar(select(TelegramOutboxRow).where(TelegramOutboxRow.payload["assignment_intent"]["draft_id"].as_string() == mark.route_target_id))
            identity, e.identity = card.id, card.id
        await dispatch(e, 6222)
        save = await selected(e, "assign_save")
        e.channel.bot.request.enqueue("answerCallbackQuery", result=True)
        with monkeypatch.context() as fault:
            fault.setattr(owner.host.work_chat.assignments, "observe", interrupted)
            assert await e.channel.handle_update(press(e, 6223, save.id, message_id=6222))
            await e.channel._drain_ingress()
        async with e.db.sessions() as reader:
            assert (await reader.get(TelegramActRow, save.id)).state == "done"
            assert (await reader.get(TelegramOutboxRow, identity)).payload["work_news_basis"]["source"]["work"]["phase"] == "draft"
        await owner.stop()
        restarted = await runtime(e, preserve=True)
        await restarted.tick_maintenance()
        await restarted.tick_maintenance()
        async with e.db.sessions() as reader:
            current = await reader.get(TelegramOutboxRow, identity)
            assert current.payload["work_news_basis"]["source"]["work"]["phase"] == "saved"
            events = list(await reader.scalars(select(AgentWorkEventRow).where(AgentWorkEventRow.operation_id == save.id)))
            assert len(events) == 1 and events[0].action == "delegate"
        assert calls == ["Review the handbook"]
    finally:
        await owner.stop()
        if restarted is not None:
            await restarted.stop()


@pytest.mark.asyncio
async def test_retired_recovery_guard_cannot_write_an_abandoned_model_fallback(work_delivery, monkeypatch):
    e = work_delivery
    owner = await installed_work_runtime(e)
    restarted, calls = None, []

    async def proposal(words, **kwargs):
        calls.append(words)
        return DraftAssignment(objective=words, success_criteria="Record verified corrections")

    async def interrupted(*args, **kwargs):
        raise RuntimeError("private-result-interrupted")

    monkeypatch.setattr("app.telegram_bots.assignment_requests.draft_assignment", proposal)
    try:
        with monkeypatch.context() as fault:
            fault.setattr(owner.host.work_chat.assignment_inputs.requests, "_complete", interrupted)
            with pytest.raises(RuntimeError, match="private-result-interrupted"):
                await e.channel.handle_update(make_update(6241, account=e.link.chat_id, text="/assign Review the handbook"))
        await owner.stop()
        restarted = await runtime(e, preserve=True)
        active = [True]
        requests = restarted.host.work_chat.assignment_inputs.requests
        read = requests._read

        async def retire(*args, **kwargs):
            result = await read(*args, **kwargs)
            active[0] = False
            return result

        with monkeypatch.context() as fault:
            fault.setattr(requests, "_read", retire)
            await restarted.recover_inputs(lambda: active[0], link_id=e.link.id)
        async with e.db.sessions() as reader:
            mark = await reader.get(TelegramInboundMarkRow, (e.link.id, 6241, e.link.chat_id))
            root = await reader.get(TelegramActRow, mark.route_target_id)
            assert root.body["request"]["phase"] == "extracting"
        active[0] = True
        await restarted.recover_inputs(lambda: active[0], link_id=e.link.id)
        async with e.db.sessions() as reader:
            root = await reader.get(TelegramActRow, mark.route_target_id)
            assert root.body["request"]["phase"] == "awaiting_done"
        assert calls == ["Review the handbook"]
    finally:
        await owner.stop()
        if restarted is not None:
            await restarted.stop()
