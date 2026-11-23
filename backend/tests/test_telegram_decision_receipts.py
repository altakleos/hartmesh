"""The two acknowledged decision cards become historical receipts after a restart."""

from datetime import UTC, datetime

import pytest
from _telegram_repository_test_support import db as db
from sqlalchemy import select
from test_telegram_request_continuations import decision_confirmation
from test_telegram_work_callback_dispatch import install_dispatch, press_work
from test_telegram_work_control_admission import admissions
from test_telegram_work_execution_outcomes import dispatch_edit
from test_telegram_work_interaction_delivery import guard_for, selected
from test_telegram_work_preview_recovery import dispatch
from test_telegram_work_source_delivery import work_delivery as work_delivery
from test_work_attention_pages import work_pages as work_pages

from app.telegram_bots.work_event_intents import WorkEventIntents
from app.telegram_bots.work_outcome_delivery import WorkOutcomePlans, WorkOutcomeSources
from app.telegram_bots.work_question_edits import WorkQuestionEdits
from deerflow.persistence.agent_instances.work import AgentWorkEventRow
from deerflow.persistence.telegram_bots.model import TelegramOutboxRow, TelegramPersonSettingsRow


@pytest.mark.asyncio
@pytest.mark.parametrize("combined", [False, True])
@pytest.mark.parametrize(("locale", "prefix"), [("en-US", "Decided by you"), ("zh-CN", "你于")])
async def test_decision_resolves_both_historical_cards(work_delivery, monkeypatch, combined, locale, prefix):
    e = work_delivery
    async with e.db.sessions.begin() as writer:
        settings = await writer.get(TelegramPersonSettingsRow, e.link.owner_user_id)
        settings.locale, settings.timezone = locale, "Asia/Shanghai"
    _, request, confirm_resume = await decision_confirmation(e, monkeypatch)
    callback = confirm_resume if combined else await selected(e, "decide_confirm")
    # Opening another copy cannot change the destination of the original press.
    e.identity = await e.plans.plan(e.link, request_id=request["id"], dedupe_key="another-copy")
    await dispatch(e, 804)
    controls = admissions(e)
    callbacks = install_dispatch(e, WorkOutcomePlans(e.plans, controls.acts))

    async def interrupted(*args, **kwargs):
        raise ConnectionError("observer interrupted after action commit")

    monkeypatch.setattr(callbacks.outcomes, "plan", interrupted)
    await press_work(e, callback, message_id=802)
    primary = await controls.acts._work_record(callback.id)
    assert primary.state == "done"
    async with e.db.sessions.begin() as writer:
        event = await writer.scalar(select(AgentWorkEventRow).where(AgentWorkEventRow.operation_id == primary.id))
        event.created_at = datetime(2025, 3, 1, 12, 34, tzinfo=UTC)
    # Reconstruct the observer and sender source from persisted state.
    plans = WorkOutcomePlans(e.plans, admissions(e).acts)
    e.channel._services.work_outcome_sources = WorkOutcomeSources(plans)
    identity = await plans.plan(e.link, primary.id, guard=await guard_for(e))
    e.identity = identity
    await dispatch_edit(e, 802)
    calls = [call for call in e.channel.bot.request.calls if call.endpoint == "editMessageText"]
    assert calls and calls[-1].parameters["message_id"] == 802
    receipt = calls[-1].parameters["text"]
    assert prefix in receipt and "2025-03-01 20:34" in receipt and primary.body["note"] in receipt
    assert await plans.plan(e.link, primary.id, guard=await guard_for(e)) == identity
    async with e.db.sessions.begin() as writer:
        event = await writer.scalar(select(AgentWorkEventRow).where(AgentWorkEventRow.operation_id == primary.id))
        projector = WorkEventIntents(links=e.db.links, human=e.human)
        await projector.project(writer, e.link, event)
        await projector.project(writer, e.link, event)
        edit = (await writer.scalars(select(TelegramOutboxRow).where(TelegramOutboxRow.dedupe_key == "edit:" + event.id))).one()
        edit_id = edit.id
    edits = WorkQuestionEdits(e.plans)
    e.channel._services.work_question_edit_sources = edits
    assert await edits.materialize(e.link, edit_id)
    e.identity = edit_id
    await dispatch_edit(e, 801)
    original = [call for call in e.channel.bot.request.calls if call.endpoint == "editMessageText"][-1].parameters
    assert original["message_id"] == 801 and prefix in original["text"]
    assert "20:34" in original["text"] and primary.body["note"] in original["text"]
    assert not original.get("reply_markup", {}).get("inline_keyboard")
    async with e.db.sessions() as reader:
        assert (await reader.get(TelegramOutboxRow, identity)).state == "sent"
        assert (await reader.get(TelegramOutboxRow, edit_id)).state == "sent"


@pytest.mark.asyncio
async def test_incomplete_decision_history_skips_only_original_edit(work_delivery, monkeypatch):
    e = work_delivery
    _, _, callback = await decision_confirmation(e, monkeypatch)
    controls = admissions(e)
    from test_telegram_work_control_admission import stage

    primary = await controls.acts.check((await stage(e, controls, callback)).id)
    assert primary.state == "done"
    projector = WorkEventIntents(links=e.db.links, human=e.human)
    async with e.db.sessions.begin() as writer:
        event = await writer.scalar(select(AgentWorkEventRow).where(AgentWorkEventRow.operation_id == primary.id))
        confirm = await writer.get(TelegramOutboxRow, callback.payload["anchor_outbox_id"])
        original = await writer.get(TelegramOutboxRow, confirm.payload["work_request_basis"]["origin"]["anchor_outbox_id"])
        # Independent damaged histories must leave the event reader able to
        # process the same event for other recipients and advance its cursor.
        for damage in ("empty_parts", "missing_origin_field", "wrong_ack", "invalid_ack_time"):
            savepoint = await writer.begin_nested()
            if damage == "empty_parts":
                original.parts = []
            elif damage == "missing_origin_field":
                basis = confirm.payload["work_request_basis"]
                confirm.payload = {**confirm.payload, "work_request_basis": {**basis, "origin": {"message_id": 801}}}
            elif damage == "wrong_ack":
                original.parts = [{**part, "message_ids": [999]} for part in original.parts]
            else:
                original.parts = [{**part, "sent_at": "invalid"} for part in original.parts]
            await writer.flush()
            await projector.project(writer, e.link, event)
            assert await writer.scalar(select(TelegramOutboxRow.id).where(TelegramOutboxRow.dedupe_key == "edit:" + event.id)) is None
            await savepoint.rollback()
            await writer.refresh(original)
            await writer.refresh(confirm)
        await projector.project(writer, e.link, event)
        assert await writer.scalar(select(TelegramOutboxRow.id).where(TelegramOutboxRow.dedupe_key == "edit:" + event.id))
