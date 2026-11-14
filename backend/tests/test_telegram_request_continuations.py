"""Confirmed request composites retain one primary and one activation intent."""

from hashlib import sha256

import pytest
from _telegram_repository_test_support import db as db
from sqlalchemy import delete, select
from test_telegram_work_actions import management_request
from test_telegram_work_control_admission import admissions, stage
from test_telegram_work_interaction_delivery import guard_for, open_view, selected
from test_telegram_work_preview_recovery import dispatch, install
from test_telegram_work_source_delivery import work_delivery as work_delivery
from test_telegram_work_start import prepare_startable
from test_telegram_work_typing_prompts import clear_old
from test_work_attention_pages import work_pages as work_pages

from deerflow.persistence.agent_instances.human_input import HumanInputRequestRow
from deerflow.persistence.telegram_bots.model import TelegramActRow, TelegramCallbackRow, TelegramOutboxRow


async def decision_confirmation(e, monkeypatch):
    install(e)
    await prepare_startable(e, monkeypatch)
    # The normal runtime leaves this off until the complete control set is
    # installed. Tests exercise the same actual reader and sender explicitly.
    e.reader.composites = True
    record, request, _seen = await management_request(e, "decision")
    async with e.db.sessions.begin() as writer:
        (await writer.get(TelegramOutboxRow, e.identity)).state = "skipped"
        (await writer.get(HumanInputRequestRow, request["id"])).choices = ["Use the current guide"]
    e.identity = await e.plans.plan(e.link, request_id=request["id"], dedupe_key="combined-decision")
    await dispatch(e, 801)
    e.identity = await open_view(e, await selected(e, "decide_choice"))
    await clear_old(e)
    await dispatch(e, 802)
    assert not e.dispatch_errors, e.dispatch_errors
    return record, request, await selected(e, "decide_confirm_resume")


@pytest.mark.asyncio
async def test_combined_decision_records_original_consent_before_primary_and_starts_one_child(work_delivery, monkeypatch):
    e = work_delivery
    record, request, callback = await decision_confirmation(e, monkeypatch)
    assert callback is not None, "The complete eligible confirmation needs an explicit combined control"
    controller = admissions(e)
    e.acts = controller.acts
    staged = await stage(e, controller, callback)
    assert staged.state == "pending" and staged.attempts == 0
    assert staged.continuation["activation_continuation"]["phase"] == "awaiting_primary"
    assert staged.continuation["request_activation"]["origin"]["callback_id"] == callback.id
    assert not e.launches
    primary = await e.acts.check(staged.id)
    assert primary.state == "done" and primary.kind == "decide"
    assert primary.continuation["activation_continuation"]["phase"] == "awaiting_primary"
    # The saved primary result and readiness commit with its durable delivery.
    from app.telegram_bots.work_outcome_delivery import WorkOutcomePlans

    receipt = await WorkOutcomePlans(e.plans, e.acts).plan(e.link, primary.id, guard=await guard_for(e))
    primary = await e.acts._work_record(primary.id)
    assert primary.continuation["activation_continuation"]["phase"] == "ready"
    assert primary.continuation["request_activation"]["outcome_id"] == receipt
    assert (await e.human.get(actor=e.actor, request_id=request["id"]))["state"] == "closed"
    completed = await e.acts.resume_continuation(primary.id)
    assert completed.continuation["activation_continuation"]["phase"] == "done"
    child_id = sha256((primary.id + ":activate").encode()).hexdigest()[:32]
    async with e.db.sessions() as reader:
        child = await reader.get(TelegramActRow, child_id)
        assert child.kind == "activate" and child.state == "done" and child.work_id == record["id"]
        assert child.body["expected_assignment_revision"] == staged.body["expected_assignment_revision"]
        assert child.id in completed.operation_ids
        assert await reader.get(TelegramCallbackRow, child_id) is None
        assert len(list(await reader.scalars(select(TelegramActRow)))) == 2
    assert len(e.launches) == 1
    assert (await e.acts.resume_continuation(primary.id)).id == primary.id
    assert len(e.launches) == 1


async def ready_decision(e, monkeypatch):
    from app.telegram_bots.work_outcome_delivery import WorkOutcomePlans, WorkOutcomeSources

    record, request, callback = await decision_confirmation(e, monkeypatch)
    controller = admissions(e)
    e.acts = controller.acts
    parent = await e.acts.check((await stage(e, controller, callback)).id)
    outcomes = WorkOutcomePlans(e.plans, e.acts)
    e.channel._services.work_outcome_sources = WorkOutcomeSources(outcomes)
    outcome = await outcomes.plan(e.link, parent.id, guard=await guard_for(e))
    return record, request, await e.acts._work_record(parent.id), outcome


@pytest.mark.asyncio
@pytest.mark.parametrize("damage", ["group", "decision", "primary_event", "closure_event", "outcome", "cancel", "inspect"])
async def test_decision_child_rechecks_original_consent_and_primary_at_service_commit(work_delivery, monkeypatch, damage):
    from app.gateway import work_execution
    from deerflow.persistence.agent_instances.human_input import HumanInputEventRow
    from deerflow.persistence.agent_instances.model import AgentInstanceGrantRow
    from deerflow.persistence.agent_instances.work import AgentWorkEventRow

    e = work_delivery
    _record, _request, parent, outcome_id = await ready_decision(e, monkeypatch)
    original, entered = work_execution.activate_work, []

    async def retire(*args, **kwargs):
        entered.append(True)
        async with e.db.sessions.begin() as writer:
            callback = await writer.get(TelegramCallbackRow, parent.id)
            if damage == "group":
                await writer.execute(delete(TelegramCallbackRow).where(TelegramCallbackRow.group_id == callback.group_id))
            elif damage == "decision":
                callback.payload = {**callback.payload, "interaction": {**callback.payload["interaction"], "decision": "Different choice after the wait"}}
            elif damage == "primary_event":
                event = await writer.scalar(select(AgentWorkEventRow).where(AgentWorkEventRow.operation_id == parent.id))
                event.request = {**event.request, "note": "Different choice after the wait"}
            elif damage == "closure_event":
                event = await writer.scalar(select(HumanInputEventRow).where(HumanInputEventRow.operation_id == parent.id))
                event.receipt = {**event.receipt, "actor_id": "other-account"}
            elif damage == "outcome":
                row = await writer.get(TelegramOutboxRow, outcome_id)
                basis = row.payload["work_outcome_basis"]
                row.payload = {**row.payload, "work_outcome_basis": {**basis, "context": {**basis["context"], "request_id": "f" * 32}}}
            elif damage == "cancel":
                (await writer.get(TelegramActRow, parent.id)).cancel_requested_at = e.clock()
            else:
                (await writer.get(AgentInstanceGrantRow, (e.instance.id, e.link.owner_user_id))).permissions = 1
        return await original(*args, **kwargs)

    monkeypatch.setattr(work_execution, "activate_work", retire)
    await e.acts.resume_continuation(parent.id)
    assert entered and not e.launches
    assert (await e.acts._work_record(parent.id)).state == "done"


@pytest.mark.asyncio
@pytest.mark.parametrize("ending", ["deadline", "cancel"])
async def test_ready_decision_ends_without_creating_a_late_child(work_delivery, monkeypatch, ending):
    e = work_delivery
    _record, _request, parent, _outcome = await ready_decision(e, monkeypatch)
    if ending == "deadline":
        e.clock.advance(3601)
        result = await e.acts.resume_continuation(parent.id)
    else:
        result = await e.acts.cancel_continuation(parent.id)
    assert result.state == "done"
    assert result.continuation["activation_continuation"]["phase"] == ("expired" if ending == "deadline" else "cancelled")
    async with e.db.sessions() as reader:
        assert await reader.get(TelegramActRow, sha256((parent.id + ":activate").encode()).hexdigest()[:32]) is None
    assert not e.launches


@pytest.mark.asyncio
async def test_missing_reserved_primary_never_becomes_a_new_combined_intent(work_delivery, monkeypatch):
    from app.telegram_bots.callbacks import CallbackRefused

    e = work_delivery
    _record, _request, callback = await decision_confirmation(e, monkeypatch)
    controller = admissions(e)
    saved = await stage(e, controller, callback)
    async with e.db.sessions.begin() as writer:
        await writer.delete(await writer.get(TelegramActRow, saved.id))
    e.clock.advance(30)
    with pytest.raises(CallbackRefused):
        await stage(e, controller, callback)
    async with e.db.sessions() as reader:
        assert await reader.get(TelegramActRow, saved.id) is None
    assert not e.launches


@pytest.mark.asyncio
@pytest.mark.parametrize("damage", ["target", "clock"])
async def test_damaged_ready_request_cannot_create_a_new_conversation(work_delivery, monkeypatch, damage):
    from app.telegram_bots.work_actions import thaw
    from deerflow.persistence.agent_instances.model import AgentConversationRow

    e = work_delivery
    _record, _request, parent, _outcome = await ready_decision(e, monkeypatch)
    async with e.db.sessions() as reader:
        before = {value.thread_id for value in await reader.scalars(select(AgentConversationRow))}
    if damage == "clock":
        e.clock.advance(3601)
    async with e.db.sessions.begin() as writer:
        row = await writer.get(TelegramActRow, parent.id)
        value = thaw(row.continuation)
        if damage == "clock":
            value["activation_continuation"]["authorized_at"] = e.clock().isoformat()
        else:
            value["activation_continuation"]["work_id"] = e.work["id"]
        row.continuation = value
    try:
        await e.acts.resume_continuation(parent.id)
    except ValueError:
        pass
    assert not e.launches
    async with e.db.sessions() as reader:
        assert {value.thread_id for value in await reader.scalars(select(AgentConversationRow))} == before
        assert await reader.get(TelegramActRow, sha256((parent.id + ":activate").encode()).hexdigest()[:32]) is None


@pytest.mark.asyncio
@pytest.mark.parametrize("uncertain", [False, True])
async def test_refused_or_unknown_primary_never_plans_an_activation(work_delivery, monkeypatch, uncertain):
    from unittest.mock import AsyncMock

    from deerflow.agent_instances.contract import Refusal
    from deerflow.persistence.agent_instances.model import AgentConversationRow

    e = work_delivery
    _record, _request, callback = await decision_confirmation(e, monkeypatch)
    controls = admissions(e)
    e.acts = controls.acts
    parent = await stage(e, controls, callback)
    failure = RuntimeError("Response unavailable") if uncertain else Refusal("Storage held", reason="storage-busy")
    monkeypatch.setattr(e.human.work, "command", AsyncMock(side_effect=failure))
    primary = await e.acts.check(parent.id)
    assert primary.state != "done"
    await e.acts.resume_continuation(parent.id)
    async with e.db.sessions() as reader:
        assert await reader.get(TelegramActRow, sha256((parent.id + ":activate").encode()).hexdigest()[:32]) is None
        assert not list(await reader.scalars(select(AgentConversationRow)))
    assert not e.launches
