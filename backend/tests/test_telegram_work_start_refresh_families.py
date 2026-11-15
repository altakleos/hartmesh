"""Refresh each actual Start carrier without replacing its original evidence."""

from unittest.mock import AsyncMock

import pytest
from _telegram_repository_test_support import db as db
from sqlalchemy import select
from test_agent_work_records import command
from test_telegram_callback_commands import press
from test_telegram_notification_runtime import runtime
from test_telegram_work_interaction_delivery import guard_for, selected
from test_telegram_work_preview_recovery import dispatch
from test_telegram_work_source_delivery import work_delivery as work_delivery
from test_work_attention_pages import work_pages as work_pages

from deerflow.agent_instances.contract import Refusal
from deerflow.persistence.agent_instances.human_input import HumanInputEventRow
from deerflow.persistence.agent_instances.work import AgentWorkEventRow
from deerflow.persistence.telegram_bots.model import TelegramActRow, TelegramCallbackRow, TelegramOutboxRow


async def family_control(e, monkeypatch, family):
    if family == "assignment":
        from test_telegram_assignment_card_delivery import prepare_card, save_card
        from test_telegram_work_execution_outcomes import dispatch_edit

        plans, draft, _inbox = await prepare_card(e)
        await dispatch(e, 4810)
        _button, admitted = await save_card(e, plans)
        saved = await e.acts.check(admitted.id)
        assert saved.state == "done"
        await plans.observe(e.link, draft.id)
        await dispatch_edit(e, 4810)
        proof = (AgentWorkEventRow, admitted.id)
    elif family == "outcome":
        from test_telegram_work_outcome_controls import saved_answer

        _plans, primary, e.identity = await saved_answer(e, "answer")
        await dispatch(e, 804)
        proof = (HumanInputEventRow, primary.id)
    elif family == "execution":
        from test_telegram_work_execution_outcomes import execution_plans
        from test_telegram_work_start_controls import stage, start_control

        _views, controller, first = await start_control(e, monkeypatch)
        act = await stage(e, controller, first)
        monkeypatch.setattr("app.gateway.work_execution.activate_work", AsyncMock(side_effect=Refusal("restarting", reason="restarting")))
        act = await e.acts.check(act.id)
        assert act.state == "refused"
        plans = execution_plans(e)
        e.identity = await plans.observe(e.link, act.id)
        await dispatch(e, 3870)
        proof = (TelegramActRow, act.id)
    elif family == "status":
        from test_telegram_status_controls import stopped_status

        button = await stopped_status(e)
        async with e.db.sessions() as reader:
            parent = await reader.get(TelegramOutboxRow, button.payload["anchor_outbox_id"])
            event = await reader.get(AgentWorkEventRow, parent.event_id)
            assert event is not None
        return button, (AgentWorkEventRow, event.operation_id)
    elif family == "management-saved":
        from test_telegram_assignment_prompt_inputs import accept_reply
        from test_telegram_work_execution_outcomes import dispatch_edit
        from test_telegram_work_management_editing import editing
        from test_telegram_work_start import prepare_startable

        from app.telegram_bots.assign import DraftAssignment

        await prepare_startable(e, monkeypatch)
        cards, edits, parent_id, _edit = await editing(e, work_id=e.work["id"])

        async def proposal(words, **kwargs):
            return DraftAssignment(objective="Review the expanded guide", success_criteria="List verified corrections")

        monkeypatch.setattr("app.telegram_bots.work_management_editing.draft_assignment", proposal)
        incoming = await accept_reply(e, 6302, "Include the additional chapter", 6301)
        assert await edits.answer(e.link, incoming, guard=await guard_for(e)) == parent_id
        e.identity = parent_id
        await dispatch_edit(e, 6103)
        save = await selected(e, "work_change_save")
        act = await cards.confirm(e.link, save.id, chat_id=e.link.chat_id, message_id=save.message_id, message_thread_id=None, guard=await guard_for(e))
        assert (await e.acts.check(act.id)).state == "done"
        await cards.observe(e.link, parent_id)
        await dispatch_edit(e, 6103)
        proof = (AgentWorkEventRow, act.id)
    else:
        from test_telegram_work_management_delivery import open_card, prepare_management
        from test_telegram_work_management_navigation import publish_edit
        from test_telegram_work_start import prepare_startable

        await prepare_startable(e, monkeypatch)
        plans, control = await prepare_management(e, "work_withdraw", work_id=e.work["id"])
        await open_card(e, plans, control)
        back = await selected(e, "back")
        await plans.back(e.link, back.id, chat_id=e.link.chat_id, message_id=back.message_id, message_thread_id=None, guard=await guard_for(e))
        await publish_edit(e)
        proof = (TelegramCallbackRow, back.id)
    button = await selected(e, "work_start")
    assert button is not None
    return button, proof


@pytest.mark.asyncio
@pytest.mark.parametrize("family", ["assignment", "outcome", "execution", "management-back", "status"])
@pytest.mark.parametrize("missing", [False, True])
async def test_refreshed_start_keeps_each_real_carrier_proof(work_delivery, monkeypatch, family, missing):
    e = work_delivery
    owner = await runtime(e, preserve=True)
    view_sources = owner.host.work_view_sources
    try:
        old, (model, operation) = await family_control(e, monkeypatch, family)
        owner.host.work_view_sources = view_sources
        current = await e.human.work.get(actor=e.actor, instance_id=e.link.agent_ref, work_id=old.target_id)
        current = await e.human.work.command(actor=e.actor, instance_id=e.link.agent_ref, work_id=old.target_id, request=command(current, "edit", objective="Current assignment after the original card"))
        if missing:
            async with e.db.sessions.begin() as writer:
                if model in {AgentWorkEventRow, HumanInputEventRow}:
                    source = await writer.scalar(select(model).where(model.operation_id == operation))
                else:
                    source = await writer.get(model, operation)
                assert source is not None
                await writer.delete(source)
        e.channel.bot.request.enqueue("answerCallbackQuery", result=True)
        assert await e.channel.handle_update(press(e, 3570, old.id, message_id=old.message_id))
        await e.channel._drain_ingress()
        async with e.db.sessions() as reader:
            views = [row for row in await reader.scalars(select(TelegramOutboxRow)) if row.dedupe_key.startswith("notice:work-start-refresh:") and row.parts]
            assert bool(views) is not missing
            if views:
                assert current["objective"] in "".join(part["visible_text"] for part in views[0].parts)
            assert await reader.get(TelegramActRow, old.id) is None
    finally:
        await owner.stop()


@pytest.mark.asyncio
async def test_management_origin_allows_current_read_after_manage_is_revoked(work_delivery, monkeypatch):
    from deerflow.persistence.agent_instances.model import AgentInstanceGrantRow

    e = work_delivery
    owner = await runtime(e, preserve=True)
    view_sources = owner.host.work_view_sources
    try:
        old, _proof = await family_control(e, monkeypatch, "management-back")
        owner.host.work_view_sources = view_sources
        current = await e.human.work.get(actor=e.actor, instance_id=e.link.agent_ref, work_id=old.target_id)
        current = await e.human.work.command(actor=e.actor, instance_id=e.link.agent_ref, work_id=old.target_id, request=command(current, "edit", objective="Current assignment for a read-only person"))
        async with e.db.sessions.begin() as writer:
            grant = await writer.get(AgentInstanceGrantRow, (e.link.agent_ref, e.link.owner_user_id))
            grant.permissions = 3
        e.channel.bot.request.enqueue("answerCallbackQuery", result=True)
        assert await e.channel.handle_update(press(e, 3598, old.id, message_id=old.message_id))
        await e.channel._drain_ingress()
        async with e.db.sessions() as reader:
            views = [row for row in await reader.scalars(select(TelegramOutboxRow)) if row.dedupe_key.startswith("notice:work-start-refresh:") and row.parts]
            assert len(views) == 1
            assert current["objective"] in "".join(part["visible_text"] for part in views[0].parts)
            kinds = {spec["kind"] for line in views[0].payload["product_keyboard_basis"] for spec in line}
            assert "work_start" in kinds and not kinds.intersection({"work_change", "work_withdraw"})
            assert await reader.get(TelegramActRow, old.id) is None
    finally:
        await owner.stop()


@pytest.mark.asyncio
@pytest.mark.parametrize("family,legacy", [("outcome", "row"), ("status", "row"), ("status", "stored")])
async def test_legacy_specialized_carrier_start_only_refreshes_a_current_preview(work_delivery, monkeypatch, family, legacy):
    from copy import deepcopy

    e = work_delivery
    owner = await runtime(e, preserve=True)
    view_sources = owner.host.work_view_sources
    try:
        old, _proof = await family_control(e, monkeypatch, family)
        owner.host.work_view_sources = view_sources
        async with e.db.sessions.begin() as writer:
            callback = await writer.get(TelegramCallbackRow, old.id)
            callback.payload = {key: value for key, value in callback.payload.items() if key != "assignment_revision"}
            if legacy == "stored":
                parent = await writer.get(TelegramOutboxRow, old.payload["anchor_outbox_id"])
                payload = deepcopy(parent.payload)
                for line in payload["product_keyboard_basis"]:
                    for spec in line:
                        if spec["kind"] == "work_start":
                            spec["payload"].pop("assignment_revision", None)
                parent.payload = payload
        current = await e.human.work.get(actor=e.actor, instance_id=e.link.agent_ref, work_id=old.target_id)
        current = await e.human.work.command(actor=e.actor, instance_id=e.link.agent_ref, work_id=old.target_id, request=command(current, "edit", objective="Current version after an older Start"))
        e.channel.bot.request.enqueue("answerCallbackQuery", result=True)
        assert await e.channel.handle_update(press(e, 3595, old.id, message_id=old.message_id))
        await e.channel._drain_ingress()
        async with e.db.sessions() as reader:
            views = [row for row in await reader.scalars(select(TelegramOutboxRow)) if row.dedupe_key.startswith("notice:work-start-refresh:") and row.parts]
            assert len(views) == 1
            assert current["objective"] in "".join(part["visible_text"] for part in views[0].parts)
            assert await reader.get(TelegramActRow, old.id) is None
    finally:
        await owner.stop()


@pytest.mark.asyncio
@pytest.mark.parametrize("family", ["assignment", "execution", "management-back"])
async def test_current_timezone_and_agent_name_do_not_replace_immutable_origin_proof(work_delivery, monkeypatch, family):
    from deerflow.persistence.agent_instances.model import AgentInstanceRow
    from deerflow.persistence.telegram_bots.model import TelegramPersonSettingsRow

    e = work_delivery
    owner = await runtime(e, preserve=True)
    view_sources = owner.host.work_view_sources
    try:
        old, _proof = await family_control(e, monkeypatch, family)
        owner.host.work_view_sources = view_sources
        current = await e.human.work.get(actor=e.actor, instance_id=e.link.agent_ref, work_id=old.target_id)
        current = await e.human.work.command(actor=e.actor, instance_id=e.link.agent_ref, work_id=old.target_id, request=command(current, "edit", objective="Current assignment across display changes"))
        async with e.db.sessions.begin() as writer:
            (await writer.get(TelegramPersonSettingsRow, e.link.owner_user_id)).timezone = "UTC"
            (await writer.get(AgentInstanceRow, e.link.agent_ref)).name = "Renamed agent"
        e.channel.bot.request.enqueue("answerCallbackQuery", result=True)
        assert await e.channel.handle_update(press(e, 3590, old.id, message_id=old.message_id))
        await e.channel._drain_ingress()
        async with e.db.sessions() as reader:
            views = [row for row in await reader.scalars(select(TelegramOutboxRow)) if row.dedupe_key.startswith("notice:work-start-refresh:") and row.parts]
            assert len(views) == 1
            assert current["objective"] in "".join(part["visible_text"] for part in views[0].parts)
            assert views[0].payload["work_news_basis"]["source"]["work"]["timezone"] == "UTC"
            assert await reader.get(TelegramActRow, old.id) is None
    finally:
        await owner.stop()
