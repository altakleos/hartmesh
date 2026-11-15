"""ACKed Retry belongs to the exact effective child and original admission."""

import pytest
from _telegram_repository_test_support import db as db
from sqlalchemy import select
from test_telegram_start_context_ownership import arm_conversation_change
from test_telegram_work_interaction_delivery import guard_for, selected
from test_telegram_work_preview_recovery import dispatch
from test_telegram_work_source_delivery import work_delivery as work_delivery
from test_telegram_work_start_controls import stage, start_control
from test_work_attention_pages import work_pages as work_pages

from app.telegram_bots.work_actions import same_body, thaw
from app.telegram_bots.work_execution_delivery import WorkExecutionPlans
from app.telegram_bots.work_execution_sources import WorkExecutionReader
from app.telegram_bots.work_news_delivery import WorkNewsSources
from app.telegram_bots.work_start_conversations import context_child_id
from deerflow.agent_instances.contract import Refusal
from deerflow.persistence.agent_instances.work import AgentWorkAttemptRow


@pytest.mark.asyncio
@pytest.mark.parametrize("mode", ["uncertain", "refused", "stale", "late-stale", "expired-promise", "expired-unknown", "expired-old-control"])
async def test_actual_retry_qualifies_its_effective_child_and_original_deadline(work_delivery, monkeypatch, mode):
    from app.gateway import services, work_execution

    e = work_delivery
    _views, controller, button = await start_control(e, monkeypatch)
    root = await stage(e, controller, button)
    arm_conversation_change(e, monkeypatch)
    native, launcher = work_execution.activate_work, services.start_run
    e.acts.outbox.config.acts.max_attempts = 1

    async def unavailable(*args, **kwargs):
        raise RuntimeError("launcher unavailable after actual native reservation")

    async def unknown_child(request, **kwargs):
        if kwargs["body"].operation_id != root.id:
            raise RuntimeError("child admission response unavailable")
        return await native(request, **kwargs)

    monkeypatch.setattr(services, "start_run", unavailable)
    if mode in {"expired-unknown", "expired-old-control"}:
        monkeypatch.setattr(work_execution, "activate_work", unknown_child)
    root = await e.acts.check(root.id)
    child = await e.acts._work_record(context_child_id(root.id))
    assert child.state == "uncertain" and not e.launches
    original_body = thaw(child.body)

    async def restarting(*args, **kwargs):
        raise Refusal("host restarting", reason="restarting")

    if mode == "refused":
        monkeypatch.setattr(work_execution, "activate_work", restarting)
        child = await e.acts.check(child.id)
        assert child.state == "refused" and child.reason == "restarting"
        await e.acts.check(root.id)
    monkeypatch.setattr(work_execution, "activate_work", native)
    if mode in {"expired-promise", "expired-unknown"}:
        e.clock.advance(3601)
    plans = WorkExecutionPlans(reader=WorkExecutionReader(work=e.human.work, authority=e.live.authority, links=e.db.links, clock=e.clock.now), outbox=e.sender.outbox, callbacks=e.channel._services.callbacks, acts=e.acts)
    e.channel._services.work_execution_sources = WorkNewsSources(plans)
    e.identity = await plans.observe(e.link, root.id)
    assert e.identity is not None
    await dispatch(e, 8601)
    assert not e.dispatch_errors, e.dispatch_errors
    retry = await selected(e, "act_check")
    if mode == "expired-unknown":
        assert retry is None
        assert not e.launches
        return
    assert retry is not None and retry.payload["act_id"] == root.id
    if mode == "expired-old-control":
        e.clock.advance(3601)
    shown = await e.acts._work_record(child.id)
    if mode == "stale":
        monkeypatch.setattr(work_execution, "activate_work", restarting)
        advanced = await e.acts.check(child.id)
        assert advanced.attempts > shown.attempts
        monkeypatch.setattr(work_execution, "activate_work", native)
    if mode == "late-stale":
        from app.telegram_bots import work_start_conversations

        receipt = work_start_conversations.context_receipt
        changed = False

        async def advance_during_receipt(acts, current):
            nonlocal changed
            if not changed:
                changed = True
                monkeypatch.setattr(work_execution, "activate_work", restarting)
                advanced = await e.acts.check(child.id)
                assert advanced.attempts > shown.attempts
                monkeypatch.setattr(work_execution, "activate_work", native)
            return await receipt(acts, current)

        monkeypatch.setattr(work_start_conversations, "context_receipt", advance_during_receipt)
    monkeypatch.setattr(services, "start_run", launcher)
    result = await plans.retry(e.link, retry.id, chat_id=e.link.chat_id, message_id=8601, message_thread_id=None, guard=await guard_for(e))
    if mode in {"stale", "late-stale", "expired-old-control"}:
        assert result.state != "done" and not e.launches
    else:
        assert result.state == "done" and len(e.launches) == 1
    async with e.db.sessions() as reader:
        attempts = list(await reader.scalars(select(AgentWorkAttemptRow).where(AgentWorkAttemptRow.activation_id == child.id)))
        if mode == "expired-old-control":
            assert attempts == []
        else:
            assert len(attempts) == 1 and same_body(attempts[0].request, original_body)
    if mode == "expired-old-control":
        from test_telegram_work_execution_outcomes import dispatch_edit

        await dispatch_edit(e, 8601)
        assert await selected(e, "act_check") is None
