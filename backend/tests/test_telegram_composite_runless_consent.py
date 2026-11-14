"""Actual SDK Retry must retain original consent at runless host recovery."""

import pytest
from sqlalchemy import delete
from test_telegram_composite_execution_outcomes import db as db
from test_telegram_composite_execution_outcomes import uncertain_child
from test_telegram_composite_execution_outcomes import work_delivery as work_delivery
from test_telegram_composite_execution_outcomes import work_pages as work_pages
from test_telegram_work_interaction_delivery import guard_for, selected
from test_telegram_work_preview_recovery import dispatch

from app.gateway import services, work_execution
from deerflow.agent_instances.contract import AgentDenied
from deerflow.persistence.telegram_bots.model import TelegramCallbackRow


@pytest.mark.asyncio
async def test_runless_composite_retry_keeps_original_consent_at_launch(work_delivery, monkeypatch):
    e = work_delivery
    plans, parent_id, child_id = await uncertain_child(e, monkeypatch, runless=True)
    e.identity = await plans.observe(e.link, child_id)
    await dispatch(e, 7961)
    assert not e.dispatch_errors, e.dispatch_errors
    retry = await selected(e, "act_check")
    assert retry is not None and not e.launches
    entered = False

    async def retire(*args, **kwargs):
        nonlocal entered
        entered = True
        async with e.db.sessions.begin() as writer:
            original = await writer.get(TelegramCallbackRow, parent_id)
            await writer.execute(delete(TelegramCallbackRow).where(TelegramCallbackRow.group_id == original.group_id))
        return await e.actual_activation(*args, **kwargs)

    monkeypatch.setattr(work_execution, "activate_work", retire)
    monkeypatch.setattr(services, "start_run", e.actual_launcher)
    try:
        await plans.retry(e.link, retry.id, chat_id=e.link.chat_id, message_id=7961, message_thread_id=None, guard=await guard_for(e))
    except (ValueError, AgentDenied):
        pass
    assert entered
    assert not e.launches, "runless recovery still requires current original composite consent at final launch admission"
