"""A canonical result suggestion becomes a private draft only on its own press."""

import pytest
from _telegram_repository_test_support import db as db
from sqlalchemy import select
from test_telegram_bot_adapter import message
from test_telegram_result_notice_delivery import prepare
from test_telegram_work_file_exports import work_exports as work_exports
from test_telegram_work_source_delivery import work_delivery as work_delivery
from test_work_attention_pages import work_pages as work_pages

from deerflow.persistence.telegram_bots.model import TelegramActRow, TelegramCallbackRow, TelegramOutboxRow


async def ready_suggestion(e):
    plans, identity = await prepare(e)
    message(e.channel.bot.request, "sendDocument", identity=910)
    message(e.channel.bot.request, identity=911)
    for _ in range(2):
        assert await e.sender.tick() == 1
        await e.sender.drain()
        e.delivery_clock.advance(2)
    async with e.db.sessions() as reader:
        control = await reader.scalar(select(TelegramCallbackRow).where(TelegramCallbackRow.payload["anchor_outbox_id"].as_string() == identity, TelegramCallbackRow.kind == "assign_from_suggestion"))
        assert control is not None and control.message_id == 911, e.dispatch_errors
    return plans, identity, control


@pytest.mark.asyncio
@pytest.mark.parametrize("work_exports", ["done_suggestion_event"], indirect=True)
async def test_actual_result_suggestion_gets_one_control_after_its_full_sdk_ack(work_exports, monkeypatch):
    e = work_exports

    async def forbidden(**_kwargs):
        raise AssertionError("rendering a result cannot extract or save an assignment")

    monkeypatch.setattr("app.telegram_bots.assign.run_oneshot_llm", forbidden)
    _plans, identity = await prepare(e)
    async with e.db.sessions() as reader:
        row = await reader.get(TelegramOutboxRow, identity)
        controls = list(await reader.scalars(select(TelegramCallbackRow).where(TelegramCallbackRow.group_id == row.parts[-1].get("callback_group_id"))))
        assert [control.kind for control in controls] == ["assign_from_suggestion"]
        assert controls[0].message_id is None and controls[0].payload["presentation_pending"]
    message(e.channel.bot.request, "sendDocument", identity=870)
    message(e.channel.bot.request, identity=871)
    for index in range(2):
        assert await e.sender.tick() == 1
        await e.sender.drain()
        e.delivery_clock.advance(2)
        async with e.db.sessions() as reader:
            control = await reader.get(TelegramCallbackRow, controls[0].id)
            assert control.message_id == (871 if index == 1 else None)
    async with e.db.sessions() as reader:
        row = await reader.get(TelegramOutboxRow, identity)
        assert row.state == "sent", e.dispatch_errors
        suggestion = row.payload["result_notice_basis"]["source"]["work"]["suggestion"]
        assert (control.target_kind, control.target_id, control.payload["work_id"], control.payload["event_id"]) == ("work", row.work_id, row.work_id, suggestion["id"])
        assert not list(await reader.scalars(select(TelegramActRow)))


@pytest.mark.asyncio
@pytest.mark.parametrize("work_exports", ["done_suggestion_event"], indirect=True)
async def test_installed_result_press_drafts_once_without_saving_work_or_inventing_inbox(work_exports, monkeypatch):
    import json

    from sqlalchemy import func
    from test_telegram_callback_commands import press
    from test_telegram_notification_runtime import runtime

    from deerflow.persistence.agent_instances.work import AgentWorkRow
    from deerflow.persistence.telegram_bots.model import TelegramInboundMarkRow

    e, calls = work_exports, []
    owner = await runtime(e)

    async def model(**kwargs):
        calls.append(kwargs)
        return json.dumps({"objective": "Review the older evidence next.", "success_criteria": "List verified references"})

    monkeypatch.setattr("app.telegram_bots.assign.run_oneshot_llm", model)
    monkeypatch.setattr("app.telegram_bots.assign.get_app_config", lambda: e.live.config)
    try:
        _plans, identity = await prepare(e)
        message(e.channel.bot.request, "sendDocument", identity=880)
        message(e.channel.bot.request, identity=881)
        for _ in range(2):
            assert await e.sender.tick() == 1
            await e.sender.drain()
            e.delivery_clock.advance(2)
        async with e.db.sessions() as reader:
            selected = await reader.scalar(select(TelegramCallbackRow).where(TelegramCallbackRow.payload["anchor_outbox_id"].as_string() == identity, TelegramCallbackRow.kind == "assign_from_suggestion"))
            before = await reader.scalar(select(func.count()).select_from(AgentWorkRow))
            before_inbox = await reader.scalar(select(func.count()).select_from(TelegramInboundMarkRow))
        assert not calls
        for update in (882, 883):
            e.channel.bot.request.enqueue("answerCallbackQuery", result=True)
            assert await e.channel.handle_update(press(e, update, selected.id, message_id=881))
            await e.channel._drain_ingress()
        assert len(calls) == 1
        assert "Review the older evidence next." in str(calls[0])
        async with e.db.sessions() as reader:
            roots = list(await reader.scalars(select(TelegramActRow)))
            assert len(roots) == 1 and roots[0].state == "draft" and roots[0].attempts == 0 and not roots[0].operation_ids
            assert roots[0].body["source"] == "suggestion"
            assert roots[0].body["origin"]["anchor_outbox_id"] == identity
            assert roots[0].body["origin"]["message_id"] == 881
            assert await reader.scalar(select(func.count()).select_from(AgentWorkRow)) == before
            assert await reader.scalar(select(func.count()).select_from(TelegramInboundMarkRow)) == before_inbox
            cards = list(await reader.scalars(select(TelegramOutboxRow).where(TelegramOutboxRow.payload["assignment_intent"]["draft_id"].as_string() == roots[0].id)))
            assert len(cards) == 1 and cards[0].parts
        message(e.channel.bot.request, identity=884)
        assert await e.sender.tick() == 1
        await e.sender.drain()
        assert not e.dispatch_errors, e.dispatch_errors
        from app.telegram_bots.assignment_retention import assignment_dependencies

        async with e.db.sessions() as reader:
            retained = await assignment_dependencies(reader, e.sender.outbox.clock(), max_age_days=7)
            assert identity in retained.deliveries and selected.group_id in retained.groups
            assert (e.link.id, 881, e.link.chat_id) not in retained.inputs
        async with e.db.sessions() as reader:
            save = await reader.scalar(select(TelegramCallbackRow).where(TelegramCallbackRow.payload["anchor_outbox_id"].as_string() == cards[0].id, TelegramCallbackRow.kind == "assign_save"))
        e.channel.bot.request.enqueue("answerCallbackQuery", result=True)
        assert await e.channel.handle_update(press(e, 885, save.id, message_id=884))
        await e.channel._drain_ingress()
        async with e.db.sessions() as reader:
            saved = await reader.get(TelegramActRow, save.id)
            assert saved is not None and saved.state == "done" and saved.kind == "delegate"
            assert saved.body["objective"] == "Review the older evidence next."
            assert await reader.scalar(select(func.count()).select_from(AgentWorkRow)) == before + 1
            assert not list(await reader.scalars(select(TelegramActRow).where(TelegramActRow.kind == "activate")))
        assert len(calls) == 1
        e.channel.bot.request.enqueue("answerCallbackQuery", result=True)
        assert await e.channel.handle_update(press(e, 886, selected.id, message_id=881))
        await e.channel._drain_ingress()
        assert len(calls) == 1
        async with e.db.sessions() as reader:
            assert await reader.scalar(select(func.count()).select_from(AgentWorkRow)) == before + 1
    finally:
        await owner.stop()


@pytest.mark.asyncio
@pytest.mark.parametrize("work_exports", ["done_suggestion_event"], indirect=True)
@pytest.mark.parametrize("mode", ["fallback", "edit"])
async def test_suggestion_private_draft_keeps_real_manual_criteria_and_edit_inputs(work_exports, monkeypatch, mode):
    import json

    from _telegram_bot_test_support import update_payload
    from sqlalchemy import func
    from telegram import Update
    from test_telegram_callback_commands import press
    from test_telegram_notification_runtime import runtime

    from deerflow.persistence.agent_instances.work import AgentWorkRow
    from deerflow.persistence.telegram_bots.model import TelegramInboundMarkRow

    e, calls = work_exports, []
    owner = await runtime(e)

    async def model(**kwargs):
        calls.append(kwargs)
        if mode == "fallback":
            return "invalid extraction"
        return json.dumps({"objective": "Review the older evidence next." if len(calls) == 1 else "Review both sources", "success_criteria": "List verified references"})

    monkeypatch.setattr("app.telegram_bots.assign.run_oneshot_llm", model)
    monkeypatch.setattr("app.telegram_bots.assign.get_app_config", lambda: e.live.config)
    try:
        _plans, identity = await prepare(e)
        message(e.channel.bot.request, "sendDocument", identity=890)
        message(e.channel.bot.request, identity=891)
        for _ in range(2):
            assert await e.sender.tick() == 1
            await e.sender.drain()
            e.delivery_clock.advance(2)
        async with e.db.sessions() as reader:
            selected = await reader.scalar(select(TelegramCallbackRow).where(TelegramCallbackRow.payload["anchor_outbox_id"].as_string() == identity, TelegramCallbackRow.kind == "assign_from_suggestion"))
            before = await reader.scalar(select(func.count()).select_from(AgentWorkRow))
        e.channel.bot.request.enqueue("answerCallbackQuery", result=True)
        assert await e.channel.handle_update(press(e, 892, selected.id, message_id=891))
        await e.channel._drain_ingress()
        async with e.db.sessions() as reader:
            root = await reader.scalar(select(TelegramActRow))
            assert root is not None and root.body["origin"]["message_id"] == 891
            root_id = root.id
        if mode == "edit":
            message(e.channel.bot.request, identity=893)
            assert await e.sender.tick() == 1
            await e.sender.drain()
            e.delivery_clock.advance(2)
            async with e.db.sessions() as reader:
                edit = await reader.scalar(select(TelegramCallbackRow).where(TelegramCallbackRow.target_id == root_id, TelegramCallbackRow.kind == "assign_edit"))
            e.channel.bot.request.enqueue("answerCallbackQuery", result=True)
            assert await e.channel.handle_update(press(e, 894, edit.id, message_id=893))
            await e.channel._drain_ingress()
        async with e.db.sessions() as reader:
            prompt = await reader.scalar(select(TelegramOutboxRow).where(TelegramOutboxRow.payload["assignment_prompt_intent"]["draft_id"].as_string() == root_id))
            assert prompt is not None and prompt.parts, e.dispatch_errors
        message(e.channel.bot.request, identity=895)
        assert await e.sender.tick() == 1
        await e.sender.drain()
        e.delivery_clock.advance(2)
        text = "List every verified correction" if mode == "fallback" else "Include both sources"
        raw = update_payload(896, account=e.link.chat_id, text=text)
        raw["message"]["reply_to_message"] = {"message_id": 895, "date": raw["message"]["date"], "chat": raw["message"]["chat"], "from": {"id": e.link.bot_id, "is_bot": True, "first_name": "Fixture bot"}, "text": "Assignment prompt"}
        update = Update.de_json(raw, e.channel.bot)
        assert await e.channel.handle_update(update)
        await e.channel._drain_ingress()
        async with e.db.sessions() as reader:
            mark = await reader.get(TelegramInboundMarkRow, (e.link.id, 896, e.link.chat_id))
            assert mark is not None and mark.route_kind == ("assignment_criteria" if mode == "fallback" else "assignment_draft")
            completed = await reader.get(TelegramActRow, mark.route_target_id)
            assert completed.body["version"] == 1 and not completed.operation_ids
            if mode == "fallback":
                assert completed.id == root_id and completed.body["source"] == "fallback"
                assert completed.body["criteria_origin"]["message_id"] == 896
                assert completed.body["draft"]["success_criteria"] == text
            else:
                assert completed.id != root_id and completed.body["source"] == "edit" and completed.body["origin"]["message_id"] == 896
                original = await reader.get(TelegramActRow, root_id)
                assert original.state == "refused" and original.reason == "replaced"
            assert await reader.scalar(select(func.count()).select_from(AgentWorkRow)) == before
            assert not list(await reader.scalars(select(TelegramInboundMarkRow).where(TelegramInboundMarkRow.message_id == 891)))
            card = await reader.scalar(select(TelegramOutboxRow).where(TelegramOutboxRow.payload["assignment_intent"]["draft_id"].as_string() == completed.id))
            assert card.parts
        assert len(calls) == (1 if mode == "fallback" else 2)
        assert await e.channel.handle_update(update)
        await e.channel._drain_ingress()
        assert len(calls) == (1 if mode == "fallback" else 2)
        assert not e.dispatch_errors, e.dispatch_errors
    finally:
        await owner.stop()
