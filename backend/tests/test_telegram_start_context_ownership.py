"""Conversation recovery keeps actual native ownership through interruption."""

from dataclasses import replace
from datetime import timedelta
from uuid import uuid4

import pytest
from _telegram_repository_test_support import db as db
from sqlalchemy import select
from test_telegram_work_actions import actions as actions
from test_telegram_work_start import start_args
from test_telegram_work_start import startable as startable

from app.telegram_bots.acts import Acts
from app.telegram_bots.work_actions import same_body, thaw
from app.telegram_bots.work_start_conversations import context_child_id
from deerflow.agent_instances.work_execution import WorkExecution
from deerflow.persistence.agent_instances.model import AgentConversationRow
from deerflow.persistence.agent_instances.work import AgentWorkAttemptRow
from deerflow.persistence.telegram_bots.model import TelegramActRow
from deerflow.persistence.thread_meta.model import ThreadMetaRow


def arm_conversation_change(e, monkeypatch):
    from app.gateway import work_execution

    native, locked, calls = work_execution.activate_work, WorkExecution._locked, []
    damaged = False

    async def call(request, **kwargs):
        calls.append(kwargs["body"].model_dump(mode="json"))
        return await native(request, **kwargs)

    async def changed(service, session, execution, work_id, **kwargs):
        nonlocal damaged
        if not damaged:
            damaged = True
            async with e.db.sessions.begin() as writer:
                await writer.delete(await writer.get(ThreadMetaRow, execution.thread_id))
        return await locked(service, session, execution, work_id, **kwargs)

    monkeypatch.setattr(work_execution, "activate_work", call)
    monkeypatch.setattr(WorkExecution, "_locked", changed)
    return calls


def reopened(e):
    old = e.acts
    return Acts(e.db.sessions, settings=old.settings, callbacks=old.callbacks, outbox=old.outbox, work=old.work, authority=old.authority, links=old.links)


@pytest.mark.asyncio
@pytest.mark.parametrize("interrupt", ["response-loss", "cancel"])
async def test_actual_conversation_creation_is_idempotent_and_cancel_fences_late_staging(startable, monkeypatch, interrupt):
    e = startable
    root = await e.acts.stage_activation(e.link, **start_args(e))
    calls = arm_conversation_change(e, monkeypatch)
    conversations = e.http_app.state.agent_conversations
    create = conversations.create
    interrupted = False

    async def after_create(**kwargs):
        nonlocal interrupted
        result = await create(**kwargs)
        if not interrupted:
            interrupted = True
            if interrupt == "cancel":
                await e.acts.cancel_activation(root.id)
            else:
                raise RuntimeError("lost response after actual conversation creation")
        return result

    monkeypatch.setattr(conversations, "create", after_create)
    root = await e.acts._attempt_activation(root, explicit=True)
    restarted = reopened(e)
    root = await restarted.check(root.id)
    async with e.db.sessions() as reader:
        chats = list(await reader.scalars(select(AgentConversationRow).where(AgentConversationRow.creation_id == context_child_id(root.id))))
        assert len(chats) == 1
        child = await reader.get(TelegramActRow, context_child_id(root.id))
        if interrupt == "cancel":
            assert root.cancel_requested_at is not None and child is None
            assert len(calls) == 1 and not e.launches
        else:
            assert root.state == "done" and child.state == "done"
            assert len(calls) == 2 and len(e.launches) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("damage", ["missing-child", "paired-time-drift"])
async def test_staged_context_cannot_reconstruct_a_missing_journal_or_renew_time(startable, monkeypatch, damage):
    from app.telegram_bots import work_start_conversations

    e = startable
    calls = arm_conversation_change(e, monkeypatch)
    receipt = work_start_conversations.context_receipt

    async def no_parent_receipt(acts, root):
        if len(calls) >= 2:
            raise RuntimeError("parent receipt observation interrupted")
        return await receipt(acts, root)

    monkeypatch.setattr(work_start_conversations, "context_receipt", no_parent_receipt)
    root = await e.acts.activate(e.link, **start_args(e))
    assert root.state == "refused" and len(e.launches) == 1
    async with e.db.sessions.begin() as writer:
        parent = await writer.get(TelegramActRow, root.id)
        child = await writer.get(TelegramActRow, context_child_id(root.id))
        assert child.state == "done"
        if damage == "missing-child":
            await writer.delete(child)
        else:
            parent.first_attempt_at += timedelta(hours=1)
            child.first_attempt_at += timedelta(hours=1)
    monkeypatch.setattr(work_start_conversations, "context_receipt", receipt)
    restarted = reopened(e)
    await restarted.check(root.id)
    async with e.db.sessions() as reader:
        child = await reader.get(TelegramActRow, context_child_id(root.id))
        if damage == "missing-child":
            assert child is None
        assert len(list(await reader.scalars(select(AgentWorkAttemptRow)))) == 1
    assert len(calls) == 2 and len(e.launches) == 1


@pytest.mark.asyncio
async def test_parent_recovers_the_saved_child_receipt_after_progress_settlement_and_a_successor(startable, monkeypatch):
    from app.telegram_bots import work_start_conversations
    from deerflow.agent_instances.work_execution_contract import ActivateWork, ReportWork
    from deerflow.runtime.runs.schemas import RunStatus

    e = startable
    calls = arm_conversation_change(e, monkeypatch)
    original = e.acts._activation_outcome
    lost = False

    async def lose_root(captured, **kwargs):
        nonlocal lost
        if captured.kind == "activate" and captured.operation_ids and kwargs.get("state") == "done" and not lost:
            lost = True
            raise RuntimeError("root completion interrupted after child completion")
        return await original(captured, **kwargs)

    monkeypatch.setattr(e.acts, "_activation_outcome", lose_root)
    root = await e.acts.activate(e.link, **start_args(e))
    assert root.state == "refused" and len(e.launches) == 1
    child = await e.acts._work_record(context_child_id(root.id))
    canonical = thaw(child.result)
    execution = await e.http_app.state.agent_conversations.execution(actor=e.actor, thread_id=child.body["thread_id"])
    service = WorkExecution(e.human.work)
    scope = await service.reserve(execution=execution, work_id=root.work_id, request=ActivateWork.model_validate(thaw(child.body)))
    execution = replace(execution, work=scope)
    record = await e.manager.get(scope.run_id, user_id=e.link.owner_user_id)
    await scope.bind(execution, record, execution.thread_incarnation)
    current = await e.human.work.get(actor=e.actor, instance_id=root.instance_id, work_id=root.work_id)
    await scope.report(execution, ReportWork(operation_id=uuid4().hex, expected_revision=current["revision"], action="progress", statement="Verified the first section."))
    await e.manager.set_status(record.run_id, RunStatus.success)
    await scope.finish(execution, await e.manager.get(record.run_id, user_id=e.link.owner_user_id), settled=True)
    next_start = await e.acts.activate(e.link, **start_args(e))
    assert next_start.state == "done" and len(e.launches) == 2
    recovered = await reopened(e).check(root.id)
    assert recovered.state == "done" and same_body(recovered.result["receipt"], canonical)
    assert same_body(await work_start_conversations.context_receipt(e.acts, recovered), recovered.result)
    assert len(calls) == 3 and len(e.launches) == 2


@pytest.mark.asyncio
async def test_conversation_refusal_cannot_replace_the_original_real_runless_promise(startable, monkeypatch):
    from app.gateway import services

    e = startable
    actual = services.start_run

    async def unavailable(*args, **kwargs):
        raise RuntimeError("launcher response unavailable after original reservation")

    monkeypatch.setattr(services, "start_run", unavailable)
    root = await e.acts.activate(e.link, **start_args(e))
    assert root.state == "pending" and not e.launches
    monkeypatch.setattr(services, "start_run", actual)
    arm_conversation_change(e, monkeypatch)
    await e.acts.check(root.id)
    async with e.db.sessions() as reader:
        saved = await reader.get(TelegramActRow, root.id)
        attempts = list(await reader.scalars(select(AgentWorkAttemptRow)))
        assert len(attempts) == 1 and attempts[0].activation_id == root.id and attempts[0].run_id is None
        assert same_body(attempts[0].request, root.body)
        assert not saved.operation_ids and "activation_context" not in (saved.continuation or {})
        assert await reader.get(TelegramActRow, context_child_id(root.id)) is None
    assert not e.launches


@pytest.mark.asyncio
async def test_actual_foreign_work_child_operation_holds_the_planned_family(startable, monkeypatch):
    from app.gateway.work_execution import activate_work
    from app.telegram_bots.authority import internal_request
    from app.telegram_bots.work_start_retention import context_owners
    from deerflow.agent_instances.work_contract import DelegateWork
    from deerflow.agent_instances.work_execution_contract import ActivateWork

    e = startable
    root = await e.acts.stage_activation(e.link, **start_args(e))
    foreign = await e.human.work.delegate(actor=e.actor, instance_id=e.instance.id, request=DelegateWork(operation_id=uuid4().hex, objective="Review another guide", success_criteria="Report its gaps"))
    other = await e.http_app.state.agent_conversations.create(actor=e.actor, instance_id=e.instance.id, creation_id=uuid4().hex)
    await activate_work(
        internal_request(e.http_app, e.link.owner_user_id),
        instance_id=e.instance.id,
        work_id=foreign["id"],
        body=ActivateWork(operation_id=context_child_id(root.id), expected_revision=foreign["revision"], expected_assignment_revision=foreign["assignment_revision"], thread_id=other["thread_id"]),
    )
    assert len(e.launches) == 1
    arm_conversation_change(e, monkeypatch)
    result = await e.acts.check(root.id)
    assert result.state == "uncertain" and result.continuation["activation_context"]["phase"] == "ended"
    async with e.db.sessions() as reader:
        assert await reader.get(TelegramActRow, context_child_id(root.id)) is None
        attempts = list(await reader.scalars(select(AgentWorkAttemptRow)))
        assert len(attempts) == 1 and attempts[0].work_id == foreign["id"]
        assert root.id not in await context_owners(reader)
    assert len(e.launches) == 1
