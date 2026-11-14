"""Refused composite recovery keeps final cancellation and origin fences."""

import pytest
from sqlalchemy import select
from test_telegram_composite_execution_outcomes import db as db
from test_telegram_composite_execution_outcomes import uncertain_child
from test_telegram_composite_execution_outcomes import work_delivery as work_delivery
from test_telegram_composite_execution_outcomes import work_pages as work_pages
from test_telegram_work_execution_outcomes import dispatch_edit
from test_telegram_work_interaction_delivery import guard_for, selected
from test_telegram_work_preview_recovery import dispatch

from app.gateway import services, work_execution
from app.telegram_bots.producer import ProductSourceRetired
from app.telegram_bots.work_actions import thaw
from deerflow.agent_instances.contract import AgentDenied
from deerflow.persistence.agent_instances.work import AgentWorkAttemptRow
from deerflow.persistence.telegram_bots.model import TelegramActRow


@pytest.mark.asyncio
@pytest.mark.parametrize("damage", ["parent-cancel", "origin-downgrade"])
async def test_refused_parent_runless_exception_retains_final_consent(work_delivery, monkeypatch, damage):
    e = work_delivery
    plans, parent_id, child_id = await uncertain_child(e, monkeypatch, runless=True)
    e.identity = await plans.observe(e.link, child_id)
    await dispatch(e, 7965)
    retry = await selected(e, "act_check")
    result = await plans.retry(e.link, retry.id, chat_id=e.link.chat_id, message_id=7965, message_thread_id=None, guard=await guard_for(e))
    assert result.state == "refused" and result.reason == "restarting"
    await e.acts.resume_continuation(parent_id)
    assert (await e.acts._work_record(parent_id)).continuation["activation_continuation"]["phase"] == "refused"
    await dispatch_edit(e, 7965)
    retry = await selected(e, "act_check")
    assert retry is not None
    async with e.db.sessions() as reader:
        original = await reader.scalar(select(AgentWorkAttemptRow).where(AgentWorkAttemptRow.activation_id == child_id))
        original_id = original.id
        assert original.run_id is None
    entered = False

    async def retire(*args, **kwargs):
        nonlocal entered
        entered = True
        async with e.db.sessions.begin() as writer:
            parent = await writer.get(TelegramActRow, parent_id)
            if damage == "parent-cancel":
                parent.cancel_requested_at = e.clock.now()
            else:
                continuation = thaw(parent.continuation)
                continuation["delegate_source"]["origin_kind"] = "internal"
                continuation.pop("delegate_origin")
                parent.continuation = continuation
        return await e.actual_activation(*args, **kwargs)

    monkeypatch.setattr(work_execution, "activate_work", retire)
    monkeypatch.setattr(services, "start_run", e.actual_launcher)
    try:
        await plans.retry(e.link, retry.id, chat_id=e.link.chat_id, message_id=7965, message_thread_id=None, guard=await guard_for(e))
    except (ValueError, AgentDenied, ProductSourceRetired):
        pass
    assert entered
    assert not e.launches
    async with e.db.sessions() as reader:
        original = await reader.scalar(select(AgentWorkAttemptRow).where(AgentWorkAttemptRow.activation_id == child_id))
        assert original.id == original_id and original.run_id is None
