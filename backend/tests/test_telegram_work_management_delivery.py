"""Saved-work previews and second-tap commands use actual recorded SDK ACKs."""

import pytest
from _telegram_repository_test_support import db as db
from sqlalchemy import func, select
from test_telegram_bot_adapter import message
from test_telegram_work_interaction_delivery import guard_for, selected
from test_telegram_work_preview_recovery import dispatch
from test_telegram_work_source_delivery import work_delivery as work_delivery
from test_telegram_work_view_delivery import prepare_view
from test_work_attention_pages import work_pages as work_pages

from app.telegram_bots.acts import Acts
from deerflow.persistence.agent_instances.work import AgentWorkEventRow
from deerflow.persistence.telegram_bots.model import TelegramActRow, TelegramOutboxRow


async def prepare_management(e, kind):
    from app.telegram_bots.work_management_delivery import WorkManagementPlans
    from app.telegram_bots.work_management_sources import WorkManagementReader
    from app.telegram_bots.work_news_delivery import WorkNewsSources

    e.acts = Acts(e.db.sessions, settings=e.db.settings, callbacks=e.channel._services.callbacks, outbox=e.sender.outbox, work=e.human.work, authority=e.live.authority, links=e.db.links)
    e.management_before = await e.human.work.get(actor=e.actor, instance_id=e.instance.id, work_id=e.work["id"])
    views, _ = await prepare_view(e)
    await dispatch(e, 6101)
    detail = await selected(e, "work_details")
    e.identity = await views.open(e.link, detail.id, query_id="management-detail", chat_id=e.link.chat_id, message_id=detail.message_id, message_thread_id=None, guard=await guard_for(e))
    await dispatch(e, 6102)
    control = await selected(e, kind)
    plans = WorkManagementPlans(reader=WorkManagementReader(e.acts), outbox=e.sender.outbox, callbacks=e.channel._services.callbacks)
    e.channel._services.work_management_sources = WorkNewsSources(plans)
    return plans, control


async def open_card(e, plans, control):
    args = dict(query_id="management-card", chat_id=e.link.chat_id, message_id=control.message_id, message_thread_id=None)
    identity = await plans.open(e.link, control.id, **args, guard=await guard_for(e))
    assert await plans.open(e.link, control.id, **args, guard=await guard_for(e)) == identity
    e.identity = identity
    await dispatch(e, 6103)
    assert not e.dispatch_errors, e.dispatch_errors
    return identity


@pytest.mark.asyncio
async def test_change_opens_a_full_private_copy_without_mutating_work(work_delivery):
    e = work_delivery
    plans, button = await prepare_management(e, "work_change")
    identity = await open_card(e, plans, button)
    save = await selected(e, "work_change_save")
    async with e.db.sessions() as reader:
        draft = await reader.get(TelegramActRow, save.target_id)
        assert draft.kind == "edit" and draft.state == "draft" and not draft.operation_ids
        assert draft.body["draft"]["objective"] == e.work["objective"]
        assert draft.body["draft"]["success_criteria"] == e.work["success_criteria"]
        assert (draft.body["expected_revision"], draft.body["expected_assignment_revision"]) == (e.management_before["revision"], e.management_before["assignment_revision"])
        row = await reader.get(TelegramOutboxRow, identity)
        assert row.state == "sent" and all(part["state"] == "sent" for part in row.parts)
        assert await reader.scalar(select(func.count()).select_from(AgentWorkEventRow).where(AgentWorkEventRow.action == "edit")) == 0


@pytest.mark.asyncio
async def test_withdraw_first_tap_never_mutates_and_second_tap_owns_one_command(work_delivery):
    e = work_delivery
    plans, button = await prepare_management(e, "work_withdraw")
    await open_card(e, plans, button)
    confirm = await selected(e, "work_withdraw_confirm")
    async with e.db.sessions() as reader:
        assert await reader.get(TelegramActRow, confirm.id) is None
        assert await reader.scalar(select(func.count()).select_from(AgentWorkEventRow).where(AgentWorkEventRow.action == "cancel")) == 0
    args = dict(chat_id=e.link.chat_id, message_id=confirm.message_id, message_thread_id=None)
    act = await plans.confirm(e.link, confirm.id, **args, guard=await guard_for(e))
    assert act.id == confirm.id and act.kind == "cancel" and act.attempts == 0
    assert (await plans.confirm(e.link, confirm.id, **args, guard=await guard_for(e))).body == act.body
    done = await e.acts.check(act.id)
    assert done.state == "done"
    await plans.observe(e.link, e.identity)
    e.clock.advance(10)
    message(e.channel.bot.request, endpoint="editMessageText", identity=6103)
    assert await e.sender.tick() == 1
    await e.sender.drain()
    assert not e.dispatch_errors, e.dispatch_errors
    async with e.db.sessions() as reader:
        assert await reader.scalar(select(func.count()).select_from(AgentWorkEventRow).where(AgentWorkEventRow.operation_id == act.id)) == 1
    messages = [call.parameters.get("text", "") for call in e.channel.bot.request.calls if call.endpoint in {"sendMessage", "editMessageText"}]
    assert any("Withdrawn:" in text and e.work["objective"] in text for text in messages)


@pytest.mark.asyncio
async def test_save_before_edit_preserves_questions_and_work_revisions(work_delivery):
    e = work_delivery
    plans, button = await prepare_management(e, "work_change")
    await open_card(e, plans, button)
    save = await selected(e, "work_change_save")
    act = await plans.confirm(e.link, save.id, chat_id=e.link.chat_id, message_id=save.message_id, message_thread_id=None, guard=await guard_for(e))
    assert act.state == "refused" and act.reason == "unchanged" and act.attempts == 0
    current = await e.human.work.get(actor=e.actor, instance_id=e.instance.id, work_id=e.work["id"])
    assert (current["revision"], current["assignment_revision"]) == (e.management_before["revision"], e.management_before["assignment_revision"])
