"""Actual assignment SDK consent remains required at the child service boundary."""

import pytest
from _telegram_repository_test_support import db as db
from test_telegram_assignment_card_delivery import prepare_card
from test_telegram_work_interaction_delivery import guard_for, selected
from test_telegram_work_preview_recovery import dispatch
from test_telegram_work_source_delivery import work_delivery as work_delivery
from test_telegram_work_start import prepare_startable
from test_work_attention_pages import work_pages as work_pages

from app.telegram_bots.assignment_controls import AssignmentAdmissions
from deerflow.persistence.telegram_bots.model import TelegramActRow, TelegramCallbackRow


@pytest.mark.asyncio
@pytest.mark.parametrize("damage", [None, "group", "origin", "draft"])
async def test_confirmed_start_keeps_actual_original_card_custody(work_delivery, monkeypatch, damage):
    from sqlalchemy import delete

    from app.gateway import work_execution
    from app.telegram_bots.work_actions import thaw

    e = work_delivery
    plans, draft, _inbox = await prepare_card(e)
    await prepare_startable(e, monkeypatch)
    await dispatch(e, 7811)
    assert not e.dispatch_errors, e.dispatch_errors
    button = await selected(e, "assign_start")
    staged = await AssignmentAdmissions(plans, e.acts).stage(e.link, button.id, chat_id=e.link.chat_id, message_id=7811, message_thread_id=None, guard=await guard_for(e))
    primary = await e.acts.check(staged.id)
    assert primary.state == "done" and not e.launches
    original, entered = work_execution.activate_work, []

    async def enter(*args, **kwargs):
        entered.append(True)
        if damage:
            async with e.db.sessions.begin() as writer:
                if damage == "group":
                    await writer.execute(delete(TelegramCallbackRow).where(TelegramCallbackRow.group_id == button.group_id))
                elif damage == "origin":
                    parent = await writer.get(TelegramActRow, primary.id)
                    value = thaw(parent.continuation)
                    value.pop("delegate_origin")
                    parent.continuation = value
                else:
                    await writer.delete(await writer.get(TelegramActRow, draft.id))
        return await original(*args, **kwargs)

    monkeypatch.setattr(work_execution, "activate_work", enter)
    try:
        await e.acts.resume_continuation(primary.id)
    except ValueError:
        pass
    assert entered
    assert len(e.launches) == (0 if damage else 1)


@pytest.mark.asyncio
@pytest.mark.parametrize("retained", ["root", "card", "controls"])
async def test_callback_admission_cannot_be_relabeled_internal(work_delivery, monkeypatch, retained):
    from sqlalchemy import delete

    from app.telegram_bots.work_actions import thaw
    from deerflow.persistence.telegram_bots.model import TelegramOutboxRow

    e = work_delivery
    plans, draft, _inbox = await prepare_card(e)
    await prepare_startable(e, monkeypatch)
    await dispatch(e, 7831)
    assert not e.dispatch_errors, e.dispatch_errors
    button = await selected(e, "assign_start")
    staged = await AssignmentAdmissions(plans, e.acts).stage(e.link, button.id, chat_id=e.link.chat_id, message_id=7831, message_thread_id=None, guard=await guard_for(e))
    parent = await e.acts.check(staged.id)
    assert parent.state == "done" and not e.launches
    async with e.db.sessions.begin() as writer:
        root = await writer.get(TelegramActRow, draft.id)
        assert root.continuation["assignment_admission"]
        if retained != "root":
            root.continuation = None
        row = await writer.get(TelegramActRow, parent.id)
        value = thaw(row.continuation)
        value["delegate_source"]["origin_kind"] = "internal"
        value.pop("delegate_origin")
        row.continuation = value
        if retained == "controls":
            await writer.delete(await writer.get(TelegramOutboxRow, e.identity))
        else:
            await writer.execute(delete(TelegramCallbackRow).where(TelegramCallbackRow.group_id == button.group_id))
    try:
        await e.acts.resume_continuation(parent.id)
    except ValueError:
        pass
    assert not e.launches
