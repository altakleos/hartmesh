"""Real SDK ancestry qualifies context children and their own native promises."""

import pytest
from _telegram_repository_test_support import db as db
from sqlalchemy import delete, select
from test_telegram_assignment_card_delivery import prepare_card
from test_telegram_start_context_ownership import arm_conversation_change
from test_telegram_work_carriers import stopped_carrier
from test_telegram_work_interaction_delivery import guard_for, selected
from test_telegram_work_preview_recovery import dispatch
from test_telegram_work_source_delivery import work_delivery as work_delivery
from test_telegram_work_start import prepare_startable
from test_telegram_work_view_delivery import prepare_view
from test_work_attention_pages import work_pages as work_pages

from app.telegram_bots.acts import Acts
from app.telegram_bots.assignment_controls import AssignmentAdmissions
from app.telegram_bots.work_actions import thaw
from app.telegram_bots.work_start_controls import WorkStartAdmissions
from app.telegram_bots.work_start_conversations import context_child_id
from deerflow.agent_instances.work_execution import WorkExecution
from deerflow.persistence.agent_instances.work import AgentWorkAttemptRow
from deerflow.persistence.telegram_bots.model import TelegramActRow, TelegramCallbackRow


async def consent(e, monkeypatch, origin):
    if origin == "composite":
        assignments, _draft, _inbox = await prepare_card(e)
        await prepare_startable(e, monkeypatch)
        await dispatch(e, 8401)
        button = await selected(e, "assign_start")
        parent = await AssignmentAdmissions(assignments, e.acts).stage(e.link, button.id, chat_id=e.link.chat_id, message_id=button.message_id, message_thread_id=None, guard=await guard_for(e))
        await e.acts.check(parent.id)
        return parent, button
    button = await stopped_carrier(e, "stop")
    target = await e.human.work.get(actor=e.actor, instance_id=e.instance.id, work_id=button.target_id)
    await prepare_startable(e, monkeypatch)
    e.work = target
    views, _ = await prepare_view(e)
    e.acts = Acts(e.db.sessions, settings=e.db.settings, callbacks=e.channel._services.callbacks, outbox=e.sender.outbox, work=e.human.work, authority=e.live.authority, links=e.db.links)
    root = await WorkStartAdmissions(views, e.acts).stage(e.link, button.id, chat_id=e.link.chat_id, message_id=button.message_id, message_thread_id=None, guard=await guard_for(e))
    return root, button


async def start(e, owner, origin):
    if origin == "composite":
        parent = await e.acts.resume_continuation(owner.id)
        return await e.acts._work_record(parent.continuation["activation_continuation"]["child_id"])
    return await e.acts.check(owner.id)


@pytest.mark.asyncio
@pytest.mark.parametrize("origin", ["news", "composite"])
@pytest.mark.parametrize("damage", [None, "gesture", "child-marker"])
async def test_actual_original_source_reaches_the_context_child_final_sql(work_delivery, monkeypatch, origin, damage):
    e = work_delivery
    owner, button = await consent(e, monkeypatch, origin)
    calls = arm_conversation_change(e, monkeypatch)
    locked = WorkExecution._locked

    async def before_final(service, session, execution, work_id, **kwargs):
        if len(calls) == 2 and damage is not None:
            async with e.db.sessions.begin() as writer:
                if damage == "gesture":
                    await writer.execute(delete(TelegramCallbackRow).where(TelegramCallbackRow.group_id == button.group_id))
                else:
                    child = await writer.get(TelegramActRow, calls[-1]["operation_id"])
                    child.continuation = {key: value for key, value in child.continuation.items() if key != "activation_context_parent"}
        return await locked(service, session, execution, work_id, **kwargs)

    monkeypatch.setattr(WorkExecution, "_locked", before_final)
    root = await start(e, owner, origin)
    assert len(calls) == 2
    assert (root.state == "done") is (damage is None)
    assert len(e.launches) == (1 if damage is None else 0)
    async with e.db.sessions() as reader:
        assert await reader.get(TelegramActRow, context_child_id(root.id)) is not None
        attempts = list(await reader.scalars(select(AgentWorkAttemptRow).where(AgentWorkAttemptRow.activation_id == context_child_id(root.id))))
        assert len(attempts) == (1 if damage is None else 0)
        if damage is None:
            assert attempts[0].request == calls[1]
            assert await reader.get(TelegramCallbackRow, button.id) is not None
    assert not e.dispatch_errors, e.dispatch_errors


@pytest.mark.asyncio
@pytest.mark.parametrize("origin", ["news", "composite"])
async def test_context_child_recovers_only_its_own_real_runless_promise(work_delivery, monkeypatch, origin):
    from app.gateway import services

    e = work_delivery
    owner, _button = await consent(e, monkeypatch, origin)
    calls = arm_conversation_change(e, monkeypatch)
    launch = services.start_run

    async def unavailable(*args, **kwargs):
        raise RuntimeError("actual native reservation lost its launcher response")

    monkeypatch.setattr(services, "start_run", unavailable)
    root = await start(e, owner, origin)
    child_id = context_child_id(root.id)
    async with e.db.sessions() as reader:
        attempt = await reader.scalar(select(AgentWorkAttemptRow).where(AgentWorkAttemptRow.activation_id == child_id))
        assert attempt is not None and attempt.run_id is None
        original = thaw(attempt.request)
    monkeypatch.setattr(services, "start_run", launch)
    e.clock.advance(e.acts.outbox.config.acts.retry_seconds + 1)
    recovered = await e.acts.check(root.id)
    assert recovered.state == "done" and len(calls) == 3 and len(e.launches) == 1
    assert calls[-1] == original
    async with e.db.sessions() as reader:
        assert len(list(await reader.scalars(select(AgentWorkAttemptRow).where(AgentWorkAttemptRow.activation_id == child_id)))) == 1
