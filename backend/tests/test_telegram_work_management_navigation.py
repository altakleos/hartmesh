"""Leaving a saved-work preview changes only its private presentation."""

import pytest
from _telegram_repository_test_support import db as db
from sqlalchemy import func, select
from test_telegram_bot_adapter import message
from test_telegram_work_interaction_delivery import guard_for, selected
from test_telegram_work_management_delivery import open_card, prepare_management
from test_telegram_work_source_delivery import work_delivery as work_delivery
from test_work_attention_pages import work_pages as work_pages

from deerflow.persistence.agent_instances.work import AgentWorkEventRow
from deerflow.persistence.telegram_bots.model import TelegramActRow


async def publish_edit(e):
    e.clock.advance(10)
    message(e.channel.bot.request, endpoint="editMessageText", identity=6103)
    assert await e.sender.tick() == 1
    await e.sender.drain()
    assert not e.dispatch_errors, e.dispatch_errors


@pytest.mark.asyncio
async def test_withdraw_back_returns_current_work_without_a_command(work_delivery):
    e = work_delivery
    plans, control = await prepare_management(e, "work_withdraw")
    await open_card(e, plans, control)
    back = await selected(e, "back")
    args = dict(chat_id=e.link.chat_id, message_id=back.message_id, message_thread_id=None)
    assert await plans.back(e.link, back.id, **args, guard=await guard_for(e)) == e.identity
    assert await plans.back(e.link, back.id, **args, guard=await guard_for(e)) == e.identity
    await publish_edit(e)
    assert await selected(e, "work_change")
    async with e.db.sessions() as reader:
        assert await reader.scalar(select(func.count()).select_from(AgentWorkEventRow).where(AgentWorkEventRow.action == "cancel")) == 0


@pytest.mark.asyncio
async def test_discard_closes_only_the_private_edit_copy(work_delivery):
    e = work_delivery
    plans, control = await prepare_management(e, "work_change")
    await open_card(e, plans, control)
    discard = await selected(e, "assign_discard")
    args = dict(chat_id=e.link.chat_id, message_id=discard.message_id, message_thread_id=None)
    assert await plans.discard(e.link, discard.id, **args, guard=await guard_for(e)) == e.identity
    assert await plans.discard(e.link, discard.id, **args, guard=await guard_for(e)) == e.identity
    await publish_edit(e)
    async with e.db.sessions() as reader:
        root = await reader.get(TelegramActRow, discard.target_id)
        assert root.state == "refused" and root.reason == "discarded" and not root.operation_ids
        assert await reader.scalar(select(func.count()).select_from(AgentWorkEventRow).where(AgentWorkEventRow.action == "edit")) == 0
    current = await e.human.work.get(actor=e.actor, instance_id=e.instance.id, work_id=e.work["id"])
    assert (current["revision"], current["assignment_revision"]) == (e.management_before["revision"], e.management_before["assignment_revision"])
