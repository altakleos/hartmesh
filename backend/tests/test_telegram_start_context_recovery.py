"""Known context refusals recover once; unknown admissions keep their body."""

import pytest
from _telegram_repository_test_support import db as db
from sqlalchemy import select
from test_telegram_work_actions import actions as actions
from test_telegram_work_start import start_args
from test_telegram_work_start import startable as startable

from app.telegram_bots.work_actions import same_body
from deerflow.agent_instances.work_execution import WorkExecution
from deerflow.persistence.agent_instances.work import AgentWorkAttemptRow
from deerflow.persistence.telegram_bots.model import TelegramActRow


@pytest.mark.asyncio
@pytest.mark.parametrize("repeated", [False, True])
async def test_actual_generation_race_gets_one_fresh_execution_with_the_same_body(startable, monkeypatch, repeated):
    e = startable
    locked, calls = WorkExecution._locked, []

    async def changed_before_native_lock(service, session, execution, work_id, **kwargs):
        calls.append(execution.instance.generation)
        if repeated or len(calls) == 1:
            current = await e.human.agents.get(actor=e.actor, instance_id=e.instance.id)
            await e.human.agents.rename(actor=e.actor, instance_id=e.instance.id, expected_generation=current.generation, name="Renamed during admission " + str(len(calls)))
        return await locked(service, session, execution, work_id, **kwargs)

    monkeypatch.setattr(WorkExecution, "_locked", changed_before_native_lock)
    act = await e.acts.activate(e.link, **start_args(e))
    assert len(calls) == 2
    assert calls[1] > calls[0]
    assert act.state == ("refused" if repeated else "done")
    assert len(e.launches) == (0 if repeated else 1)
    original_body = dict(act.body)
    again = await e.acts.check(act.id)
    assert dict(again.body) == original_body and len(calls) == 2
    async with e.db.sessions() as reader:
        attempts = list(await reader.scalars(select(AgentWorkAttemptRow).where(AgentWorkAttemptRow.activation_id == act.id)))
        assert len(attempts) == (0 if repeated else 1)
        if attempts:
            assert attempts[0].request == original_body


@pytest.mark.asyncio
@pytest.mark.parametrize("retire", ["owner", "deadline"])
async def test_execution_refresh_cannot_commit_after_its_last_owner_wait(startable, monkeypatch, retire):
    from datetime import timedelta

    from app.telegram_bots import work_start_context
    from app.telegram_bots.authority import AuthorityUnavailable

    e = startable
    locked, calls = WorkExecution._locked, []
    owner_factory = work_start_context.prepare_work_owner
    retired = False

    async def changed_before_native_lock(service, session, execution, work_id, **kwargs):
        calls.append(execution.instance.generation)
        if len(calls) == 1:
            current = await e.human.agents.get(actor=e.actor, instance_id=e.instance.id)
            await e.human.agents.rename(actor=e.actor, instance_id=e.instance.id, expected_generation=current.generation, name="Renamed before refresh")
        return await locked(service, session, execution, work_id, **kwargs)

    async def owner_wait(*args):
        owner = await owner_factory(*args)

        async def check(*values):
            nonlocal retired
            await owner(*values)
            if retire == "owner":
                retired = True
            else:
                later = e.acts.outbox.clock() + timedelta(hours=2)
                monkeypatch.setattr(e.acts.outbox, "clock", lambda: later)

        return check

    monkeypatch.setattr(WorkExecution, "_locked", changed_before_native_lock)
    monkeypatch.setattr(work_start_context, "prepare_work_owner", owner_wait)
    act = await e.acts.stage_activation(e.link, **start_args(e))
    try:
        await e.acts._attempt_activation(act, explicit=True, attempt_current=lambda: not retired)
    except AuthorityUnavailable:
        pass
    assert len(calls) == 1 and not e.launches
    async with e.db.sessions() as reader:
        saved = await reader.get(TelegramActRow, act.id)
        assert "start_context_recovery" not in (saved.continuation or {})


@pytest.mark.asyncio
@pytest.mark.parametrize("reason", ["conversation-changed", "space-changed", "conversation-mismatch"])
@pytest.mark.parametrize("repeated", [False, True])
async def test_actual_conversation_refusal_uses_one_distinct_child_and_keeps_original_cas(startable, monkeypatch, reason, repeated):
    from uuid import uuid4

    from app.gateway import agent_conversations, work_execution
    from deerflow.persistence.agent_instances.model import AgentConversationRow
    from deerflow.persistence.agent_instances.work import AgentWorkEventRow
    from deerflow.persistence.spaces.model import SpaceRow
    from deerflow.persistence.thread_meta.model import ThreadMetaRow

    e = startable
    native, locked = work_execution.activate_work, WorkExecution._locked
    calls, changed = [], False

    async def call(request, **kwargs):
        calls.append(kwargs["body"].model_dump(mode="json"))
        return await native(request, **kwargs)

    async def native_lock(service, session, execution, work_id, **kwargs):
        nonlocal changed
        if (repeated or not changed) and reason != "conversation-mismatch":
            changed = True
            async with e.db.sessions.begin() as writer:
                if reason == "conversation-changed":
                    await writer.delete(await writer.get(ThreadMetaRow, execution.thread_id))
                else:
                    (await writer.get(SpaceRow, e.instance.home_id)).generation += 1
        return await locked(service, session, execution, work_id, **kwargs)

    if reason == "conversation-mismatch":
        other = await e.http_app.state.agent_conversations.create(actor=e.actor, instance_id=e.instance.id, creation_id=uuid4().hex)
        execution_for_run = agent_conversations.execution_for_run

        async def misrouted_once(request, thread_id, **kwargs):
            nonlocal changed
            selected = other["thread_id"] if repeated or not changed else thread_id
            changed = True
            return await execution_for_run(request, selected, **kwargs)

        monkeypatch.setattr(agent_conversations, "execution_for_run", misrouted_once)
    monkeypatch.setattr(work_execution, "activate_work", call)
    monkeypatch.setattr(WorkExecution, "_locked", native_lock)
    parent = await e.acts.activate(e.link, **start_args(e))
    if repeated:
        assert parent.state == "refused" and len(calls) == 2 and not e.launches
        assert (await e.acts.check(parent.id)).state == "refused"
        assert len(calls) == 2
        async with e.db.sessions() as reader:
            assert len(list(await reader.scalars(select(TelegramActRow).where(TelegramActRow.kind == "activate_context")))) == 1
            assert await reader.scalar(select(AgentWorkAttemptRow.id)) is None
        return
    assert parent.state == "done" and len(calls) == 2 and len(e.launches) == 1
    assert calls[0] == dict(parent.body)
    async with e.db.sessions() as reader:
        child = await reader.scalar(select(TelegramActRow).where(TelegramActRow.kind == "activate_context"))
        assert child is not None and child.id != parent.id and child.state == "done"
        assert child.body == calls[1]
        assert {key: value for key, value in child.body.items() if key not in {"operation_id", "thread_id"}} == {key: value for key, value in parent.body.items() if key not in {"operation_id", "thread_id"}}
        assert child.body["thread_id"] != parent.body["thread_id"]
        assert (await reader.get(AgentConversationRow, child.body["thread_id"])).creation_id == child.id
        assert await reader.scalar(select(AgentWorkEventRow.id).where(AgentWorkEventRow.operation_id == parent.id)) is None
        event = await reader.scalar(select(AgentWorkEventRow).where(AgentWorkEventRow.operation_id == child.id))
        assert event.action == "activate" and event.work_id == parent.work_id
        assert parent.result["activation_context_child_id"] == child.id
        assert same_body(parent.result["receipt"], child.result)
    assert same_body(await e.acts.receipt_for(parent), parent.result)
    assert same_body((await e.acts.check(parent.id)).result, parent.result)
    assert len(calls) == 2 and len(e.launches) == 1
