"""Composite journals keep one authorized child independently of primary recovery."""

from hashlib import sha256
from uuid import uuid4

import pytest
from _telegram_repository_test_support import db as db
from test_telegram_assignment_draft_store import draft
from test_telegram_work_actions import actions as actions
from test_telegram_work_start import startable as startable


@pytest.mark.asyncio
@pytest.mark.parametrize("start", [False, True])
async def test_delegation_records_composite_before_first_service_attempt(actions, start):
    e = actions
    _repo, _inbox, source = await draft(e)
    identity = uuid4().hex
    staged = await e.acts.stage_delegate(e.link, source.id, operation_id=identity, start=start)
    assert staged.attempts == 0 and staged.result is None
    if start:
        continuation = staged.continuation["activation_continuation"]
        assert continuation == {
            "version": 1,
            "phase": "awaiting_primary",
            "authorized_at": staged.continuation["delegate_source"]["authorized_at"],
            "child_id": sha256((identity + ":activate").encode()).hexdigest()[:32],
            "work_id": None,
            "assignment_revision": None,
            "child_body": None,
        }
    else:
        assert "activation_continuation" not in staged.continuation


@pytest.mark.asyncio
async def test_ready_delegate_recovers_one_canonical_activation_child(startable, monkeypatch):
    from app.gateway import work_execution
    from deerflow.persistence.telegram_bots.model import TelegramActRow

    e = startable
    _repo, _inbox, source = await draft(e)
    identity = uuid4().hex
    parent = await e.acts.delegate(e.link, source.id, operation_id=identity, start=True)
    assert parent.state == "done" and parent.continuation["activation_continuation"]["phase"] == "ready"
    child_id = sha256((identity + ":activate").encode()).hexdigest()[:32]
    real, calls = work_execution.activate_work, []

    async def observe(*args, **kwargs):
        async with e.db.sessions() as reader:
            stored_parent = await reader.get(TelegramActRow, identity)
            stored_child = await reader.get(TelegramActRow, child_id)
            assert stored_parent.operation_ids == [child_id]
            assert stored_parent.continuation["activation_continuation"]["child_body"] == stored_child.body
            assert stored_parent.continuation["activation_continuation"]["phase"] == "child_planned"
        calls.append(kwargs["body"].model_dump(mode="json"))
        return await real(*args, **kwargs)

    monkeypatch.setattr(work_execution, "activate_work", observe)
    finished = await e.acts.resume_continuation(identity)
    assert finished.continuation["activation_continuation"]["phase"] == "done"
    assert len(e.launches) == len(calls) == 1 and calls[0]["operation_id"] == child_id
    child = await e.acts._work_record(child_id)
    assert child.state == "done" and child.first_attempt_at >= parent.first_attempt_at
    assert await e.acts.resume_continuation(identity) == finished
    assert len(e.launches) == len(calls) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("fence", ["cancelled", "expired"])
async def test_unadmitted_child_obeys_original_composite_fence(startable, fence):
    from _telegram_bot_test_support import ManualClock

    from deerflow.persistence.telegram_bots.model import TelegramActRow

    e = startable
    clock = ManualClock(e.acts.outbox.clock())
    e.acts.outbox.clock = clock.now
    _repo, _inbox, source = await draft(e)
    identity = uuid4().hex
    parent = await e.acts.delegate(e.link, source.id, operation_id=identity, start=True)
    assert parent.state == "done"
    if fence == "expired":
        clock.advance(3601)
    else:
        async with e.db.sessions.begin() as writer:
            (await writer.get(TelegramActRow, identity)).cancel_requested_at = clock.now()
    finished = await e.acts.resume_continuation(identity)
    assert finished.continuation["activation_continuation"]["phase"] == fence
    assert not e.launches
    async with e.db.sessions() as reader:
        assert await reader.get(TelegramActRow, sha256((identity + ":activate").encode()).hexdigest()[:32]) is None


@pytest.mark.asyncio
@pytest.mark.parametrize("damage", ["cancel", "body", "marker", "start-choice", "primary-receipt"])
async def test_final_service_admission_retains_exact_parent_consent(startable, monkeypatch, damage):
    from copy import deepcopy

    from sqlalchemy import delete

    from app.gateway import work_execution
    from deerflow.persistence.agent_instances.work import AgentWorkEventRow
    from deerflow.persistence.telegram_bots.model import TelegramActRow

    e = startable
    _repo, _inbox, source = await draft(e)
    identity = uuid4().hex
    parent = await e.acts.delegate(e.link, source.id, operation_id=identity, start=True)
    child_id = sha256((identity + ":activate").encode()).hexdigest()[:32]
    assert parent.state == "done"
    original, entered = work_execution.activate_work, []

    async def retire(*args, **kwargs):
        entered.append(True)
        async with e.db.sessions.begin() as writer:
            row = await writer.get(TelegramActRow, identity)
            if damage == "cancel":
                row.cancel_requested_at = e.acts.outbox.clock()
            elif damage == "body":
                row.body = {**row.body, "objective": "Unconfirmed replacement"}
            elif damage == "marker":
                (await writer.get(TelegramActRow, child_id)).continuation = None
            elif damage == "start-choice":
                value = deepcopy(row.continuation)
                value["delegate_source"]["start"] = False
                row.continuation = value
            else:
                await writer.execute(delete(AgentWorkEventRow).where(AgentWorkEventRow.instance_id == e.instance.id, AgentWorkEventRow.operation_id == identity))
        return await original(*args, **kwargs)

    monkeypatch.setattr(work_execution, "activate_work", retire)
    try:
        await e.acts.resume_continuation(identity)
    except ValueError:
        pass
    assert entered
    assert not e.launches, "lost parent consent/receipt cannot admit a child run"


@pytest.mark.asyncio
async def test_child_response_loss_recovers_exact_receipt_without_repreparing(startable, monkeypatch):
    from app.gateway import work_execution

    e = startable
    _repo, _inbox, source = await draft(e)
    identity = uuid4().hex
    parent = await e.acts.delegate(e.link, source.id, operation_id=identity, start=True)
    original, bodies = work_execution.activate_work, []

    async def lose(*args, **kwargs):
        bodies.append(kwargs["body"].model_dump(mode="json"))
        await original(*args, **kwargs)
        raise RuntimeError("Activation response lost after commit")

    monkeypatch.setattr(work_execution, "activate_work", lose)
    first = await e.acts.resume_continuation(identity)
    assert first.continuation["activation_continuation"]["phase"] == "child_planned"
    assert len(e.launches) == len(bodies) == 1
    child_id = parent.continuation["activation_continuation"]["child_id"]
    before = await e.acts._work_record(child_id)
    assert before.state == "pending" and before.attempts == 1

    async def unexpected(*args, **kwargs):
        pytest.fail("Receipt recovery prepared another conversation")

    monkeypatch.setattr(e.http_app.state.agent_conversations, "create", unexpected)
    recovered = await e.acts.resume_continuation(identity)
    assert recovered.continuation["activation_continuation"]["phase"] == "done"
    assert len(e.launches) == len(bodies) == 1
    child = await e.acts._work_record(child_id)
    assert child.state == "done" and child.body == before.body


@pytest.mark.asyncio
async def test_no_due_retry_and_activation_have_distinct_canonical_children(startable):
    from datetime import UTC, datetime, timedelta

    from app.telegram_bots.assign import DraftAssignment
    from deerflow.persistence.agent_instances.model import AgentInstanceGrantRow

    e = startable
    model = DraftAssignment(objective="Review the guide", success_criteria="List verified gaps", due_text="By Friday", due_at=datetime.now(UTC) + timedelta(days=2))
    _repo, _inbox, source = await draft(e, model=model)
    identity = uuid4().hex
    await e.acts.stage_delegate(e.link, source.id, operation_id=identity, start=True)
    async with e.db.sessions.begin() as writer:
        (await writer.get(AgentInstanceGrantRow, (e.instance.id, e.link.owner_user_id))).permissions = 3
    parent = await e.acts.check(identity)
    assert parent.state == "done"
    no_due = sha256((identity + ":no-due").encode()).hexdigest()[:32]
    child_id = sha256((identity + ":activate").encode()).hexdigest()[:32]
    assert parent.operation_ids == (no_due,)
    completed = await e.acts.resume_continuation(identity)
    assert completed.continuation["activation_continuation"]["phase"] == "done"
    assert completed.operation_ids == (no_due, child_id)
    assert len(e.launches) == 1
    replay = await e.acts.check(identity)
    assert replay.state == "done" and replay.operation_ids == (no_due, child_id)


@pytest.mark.asyncio
async def test_final_preparation_deadline_rolls_back_unadmitted_child(startable, monkeypatch):
    from _telegram_bot_test_support import ManualClock

    from app.telegram_bots import attention_authority
    from deerflow.persistence.telegram_bots.model import TelegramActRow

    e = startable
    clock = ManualClock(e.acts.outbox.clock())
    e.acts.outbox.clock = clock.now
    _repo, _inbox, source = await draft(e)
    identity = uuid4().hex
    await e.acts.delegate(e.link, source.id, operation_id=identity, start=True)
    original, entered = attention_authority.prepare_attention, []

    async def prepare(*args, **kwargs):
        current = await original(*args, **kwargs)

        async def held(*args, **kwargs):
            result = await current(*args, **kwargs)
            parent = await kwargs["session"].get(TelegramActRow, identity)
            if not entered and parent.continuation["activation_continuation"]["phase"] == "child_planned":
                entered.append(True)
                clock.advance(3601)
            return result

        return held

    monkeypatch.setattr(attention_authority, "prepare_attention", prepare)
    result = await e.acts.resume_continuation(identity)
    assert entered and result.continuation["activation_continuation"]["phase"] == "expired"
    assert not e.launches
    async with e.db.sessions() as reader:
        assert await reader.get(TelegramActRow, activation_child(identity)) is None


def activation_child(identity):
    return sha256((identity + ":activate").encode()).hexdigest()[:32]


@pytest.mark.asyncio
async def test_missing_planned_child_cannot_be_reconstructed(startable, monkeypatch):
    from deerflow.persistence.telegram_bots.model import TelegramActRow

    e = startable
    _repo, _inbox, source = await draft(e)
    identity = uuid4().hex
    await e.acts.delegate(e.link, source.id, operation_id=identity, start=True)

    async def hold(child, **kwargs):
        return child

    monkeypatch.setattr(e.acts, "_attempt_activation", hold)
    planned = await e.acts.resume_continuation(identity)
    assert planned.continuation["activation_continuation"]["phase"] == "child_planned"
    async with e.db.sessions.begin() as writer:
        await writer.delete(await writer.get(TelegramActRow, activation_child(identity)))
    with pytest.raises(ValueError):
        await e.acts.resume_continuation(identity)
    assert not e.launches
    async with e.db.sessions() as reader:
        assert await reader.get(TelegramActRow, activation_child(identity)) is None


@pytest.mark.asyncio
async def test_orphan_child_cannot_be_adopted_or_execute_independently(startable):
    e = startable
    _repo, _inbox, source = await draft(e)
    identity = uuid4().hex
    parent = await e.acts.delegate(e.link, source.id, operation_id=identity, start=True)
    state = parent.continuation["activation_continuation"]
    orphan = await e.acts.stage_activation(e.link, operation_id=activation_child(identity), work_id=state["work_id"], assignment_revision=state["assignment_revision"])
    assert orphan.continuation is None and not e.launches
    with pytest.raises(ValueError, match="orphan-activation-child"):
        await e.acts.resume_continuation(identity)
    await e.acts.check(orphan.id)
    assert not e.launches


@pytest.mark.asyncio
@pytest.mark.parametrize("same_binding", [True, False])
async def test_internal_continuation_checks_late_public_source_scope(startable, monkeypatch, same_binding):
    from app.gateway import work_execution
    from deerflow.persistence.telegram_bots.model import TelegramOutboxRow

    e = startable
    _repo, _inbox, root = await draft(e)
    parent = await e.acts.delegate(e.link, root.id, operation_id=uuid4().hex, start=True)
    assert parent.state == "done" and not e.launches
    real, entered = work_execution.activate_work, []

    async def late(*args, **kwargs):
        entered.append(True)
        now = e.acts.outbox.clock()
        async with e.db.sessions.begin() as writer:
            writer.add(
                TelegramOutboxRow(
                    id=uuid4().hex,
                    link_id=e.link.id,
                    owner_user_id=e.link.owner_user_id,
                    binding_generation=e.link.binding_generation if same_binding else e.link.binding_generation + 1,
                    chat_id=e.link.chat_id,
                    instance_id=e.link.agent_ref,
                    dedupe_key="notice:assignment:" + root.id,
                    class_="reply",
                    payload={"assignment_intent": {"version": 1, "draft_id": root.id}},
                    parts=[],
                    state="pending",
                    silent=True,
                    not_before=now,
                    created_at=now,
                )
            )
        return await real(*args, **kwargs)

    monkeypatch.setattr(work_execution, "activate_work", late)
    try:
        await e.acts.resume_continuation(parent.id)
    except ValueError:
        pass
    assert entered
    assert len(e.launches) == (0 if same_binding else 1)


@pytest.mark.asyncio
async def test_worker_recovers_ready_child_without_repeating_primary(startable, monkeypatch):
    from app.telegram_bots.acts import ActsWorker

    e = startable
    _repo, _inbox, source = await draft(e)
    identity = uuid4().hex
    await e.acts.delegate(e.link, source.id, operation_id=identity, start=True)

    async def unexpected(*args, **kwargs):
        pytest.fail("Worker repeated completed delegation")

    monkeypatch.setattr(e.human.work, "delegate", unexpected)
    await ActsWorker(e.acts).tick()
    assert len(e.launches) == 1
    assert (await e.acts._work_record(identity)).continuation["activation_continuation"]["phase"] == "done"


@pytest.mark.asyncio
async def test_unknown_child_mutations_keep_the_existing_retry_interval(startable, monkeypatch):
    from _telegram_bot_test_support import ManualClock

    from app.gateway import work_execution

    e = startable
    clock = ManualClock(e.acts.outbox.clock())
    e.acts.outbox.clock = clock.now
    _repo, _inbox, source = await draft(e)
    identity = uuid4().hex
    await e.acts.delegate(e.link, source.id, operation_id=identity, start=True)
    calls = []

    async def unavailable(*args, **kwargs):
        calls.append(kwargs["body"].model_dump(mode="json"))
        raise RuntimeError("Host admission unavailable")

    monkeypatch.setattr(work_execution, "activate_work", unavailable)
    await e.acts.resume_continuation(identity)
    assert len(calls) == 1
    await e.acts.resume_continuation(identity)
    assert len(calls) == 1
    clock.advance(e.acts.outbox.config.acts.retry_seconds + 1)
    await e.acts.resume_continuation(identity)
    assert len(calls) == 2 and calls[0] == calls[1]


@pytest.mark.asyncio
@pytest.mark.parametrize("planned", [False, True])
async def test_cancel_fences_parent_and_unadmitted_child_without_undoing_save(startable, monkeypatch, planned):
    from app.telegram_bots.acts import ActsWorker

    e = startable
    _repo, _inbox, source = await draft(e)
    identity = uuid4().hex
    await e.acts.delegate(e.link, source.id, operation_id=identity, start=True)
    if planned:
        original = e.acts._attempt_activation

        async def hold(child, **kwargs):
            return child

        monkeypatch.setattr(e.acts, "_attempt_activation", hold)
        await e.acts.resume_continuation(identity)
        monkeypatch.setattr(e.acts, "_attempt_activation", original)
    cancelled = await e.acts.cancel_continuation(identity)
    assert cancelled.state == "done" and cancelled.cancel_requested_at is not None
    assert cancelled.continuation["activation_continuation"]["phase"] == "cancelled"
    if planned:
        child = await e.acts._work_record(activation_child(identity))
        assert child.cancel_requested_at is not None and child.state == "refused" and child.reason == "cancelled"
    await ActsWorker(e.acts).tick()
    assert not e.launches


@pytest.mark.asyncio
async def test_cancel_during_child_admission_preserves_uncertainty_until_return(startable, monkeypatch):
    import asyncio

    from app.gateway import work_execution

    e = startable
    _repo, _inbox, source = await draft(e)
    identity = uuid4().hex
    await e.acts.delegate(e.link, source.id, operation_id=identity, start=True)
    original, entered, release = work_execution.activate_work, asyncio.Event(), asyncio.Event()

    async def hold(*args, **kwargs):
        entered.set()
        await release.wait()
        return await original(*args, **kwargs)

    monkeypatch.setattr(work_execution, "activate_work", hold)
    running = asyncio.create_task(e.acts.resume_continuation(identity))
    try:
        await asyncio.wait_for(entered.wait(), 10)
        observed = await asyncio.wait_for(e.acts.cancel_continuation(identity), 10)
        assert observed.state == "done" and observed.cancel_requested_at is not None
        child = await e.acts._work_record(activation_child(identity))
        assert child.cancel_requested_at is not None and child.state == "uncertain"
        assert not e.launches
    finally:
        release.set()
        await running
    assert not e.launches
    final = await e.acts._work_record(identity)
    assert final.continuation["activation_continuation"]["phase"] == "cancelled"


@pytest.mark.asyncio
async def test_cancel_after_native_commit_reads_success_without_claiming_rollback(startable, monkeypatch):
    import asyncio

    from app.gateway import work_execution

    e = startable
    _repo, _inbox, source = await draft(e)
    identity = uuid4().hex
    await e.acts.delegate(e.link, source.id, operation_id=identity, start=True)
    original, entered, release = work_execution.activate_work, asyncio.Event(), asyncio.Event()

    async def hold(*args, **kwargs):
        await original(*args, **kwargs)
        entered.set()
        await release.wait()
        raise RuntimeError("Committed activation response lost")

    monkeypatch.setattr(work_execution, "activate_work", hold)
    running = asyncio.create_task(e.acts.resume_continuation(identity))
    try:
        await asyncio.wait_for(entered.wait(), 10)
        observed = await e.acts.cancel_continuation(identity)
        assert len(e.launches) == 1
        assert observed.continuation["activation_continuation"]["phase"] == "done"
        assert (await e.acts._work_record(activation_child(identity))).state == "done"
    finally:
        release.set()
        await running
    assert len(e.launches) == 1
    assert (await e.acts._work_record(identity)).continuation["activation_continuation"]["phase"] == "done"


@pytest.mark.asyncio
async def test_cancelled_runless_promise_never_returns_to_launch_helper(startable, monkeypatch):
    from unittest.mock import AsyncMock

    from sqlalchemy import select

    from app.gateway import services
    from app.telegram_bots.acts import ActsWorker
    from deerflow.persistence.agent_instances.work import AgentWorkAttemptRow

    e = startable
    _repo, _inbox, source = await draft(e)
    identity = uuid4().hex
    await e.acts.delegate(e.link, source.id, operation_id=identity, start=True)
    launch = services.start_run
    monkeypatch.setattr(services, "start_run", AsyncMock(side_effect=RuntimeError("Launch response unavailable")))
    await e.acts.resume_continuation(identity)
    async with e.db.sessions() as reader:
        promise = await reader.scalar(select(AgentWorkAttemptRow).where(AgentWorkAttemptRow.activation_id == activation_child(identity)))
        assert promise is not None and promise.run_id is None
        promise_id = promise.id
    cancelled = await e.acts.cancel_continuation(identity)
    assert cancelled.continuation["activation_continuation"]["phase"] == "child_planned"
    child = await e.acts._work_record(activation_child(identity))
    assert child.state == "uncertain" and child.reason == "cancel-unconfirmed"
    monkeypatch.setattr(services, "start_run", launch)
    await e.acts.resume_continuation(identity)
    await ActsWorker(e.acts).tick()
    assert not e.launches
    async with e.db.sessions() as reader:
        promise = await reader.get(AgentWorkAttemptRow, promise_id)
        assert promise is not None and promise.run_id is None


@pytest.mark.asyncio
@pytest.mark.parametrize("refusal", ["work-changed", "ceiling"])
async def test_conclusive_preparation_refusal_ends_only_the_continuation(startable, monkeypatch, refusal):
    from test_agent_work_records import command

    from app.telegram_bots import work_start_actions
    from app.telegram_bots.authority import CeilingRefused
    from deerflow.persistence.telegram_bots.model import TelegramActRow

    e = startable
    _repo, _inbox, source = await draft(e)
    identity = uuid4().hex
    parent = await e.acts.delegate(e.link, source.id, operation_id=identity, start=True)
    work_id = parent.result["id"]
    if refusal == "work-changed":
        current = await e.human.work.get(actor=e.actor, instance_id=e.instance.id, work_id=work_id)
        await e.human.work.command(actor=e.actor, instance_id=e.instance.id, work_id=work_id, request=command(current, "edit", objective="A changed assignment needs another press"))
    else:
        original = work_start_actions.require

        async def deny(owner, *ceilings):
            if "runs:create" in ceilings:
                raise CeilingRefused()
            return await original(owner, *ceilings)

        monkeypatch.setattr(work_start_actions, "require", deny)
    result = await e.acts.resume_continuation(identity)
    state = result.continuation["activation_continuation"]
    assert result.state == "done" and result.result["id"] == work_id
    assert state["phase"] == "refused" and state["child_body"] is None
    assert state["refusal"]["reason"] == ("work-changed" if refusal == "work-changed" else "ceiling")
    assert not e.launches
    async with e.db.sessions() as reader:
        assert await reader.get(TelegramActRow, activation_child(identity)) is None


@pytest.mark.asyncio
async def test_recovery_skips_an_independently_active_child(startable, monkeypatch):
    import asyncio

    e = startable
    _repo, _inbox, root1 = await draft(e, identity=4681)
    _repo, _inbox, root2 = await draft(e, identity=4682)
    first, second = "1" * 32, "2" * 32
    await e.acts.delegate(e.link, root1.id, operation_id=first, start=True)
    await e.acts.delegate(e.link, root2.id, operation_id=second, start=True)
    original_attempt = e.acts._attempt_activation

    async def hold(child, **kwargs):
        return child

    monkeypatch.setattr(e.acts, "_attempt_activation", hold)
    planned = await e.acts.resume_continuation(first)
    child = planned.continuation["activation_continuation"]["child_id"]
    monkeypatch.setattr(e.acts, "_attempt_activation", original_attempt)
    real_resume, entered_first, entered_second = e.acts.resume_continuation, asyncio.Event(), asyncio.Event()

    async def observe(identity, **kwargs):
        if identity == first:
            entered_first.set()
        if identity == second:
            entered_second.set()
        return await real_resume(identity, **kwargs)

    monkeypatch.setattr(e.acts, "resume_continuation", observe)
    progressed, task = False, None
    try:
        async with e.acts._work_serial(child):
            task = asyncio.create_task(e.acts._recover_continuations())
            try:
                await asyncio.wait_for(entered_second.wait(), 5)
                progressed = True
            except TimeoutError:
                pass
    finally:
        # Release the independently owned child before draining recovery.
        if task is not None:
            await asyncio.wait_for(task, 20)
    assert progressed, "background recovery must not wait on an already-owned child before visiting independent work"


@pytest.mark.asyncio
async def test_cancel_settles_known_no_due_primary_and_child_together(startable, monkeypatch):
    import asyncio
    from datetime import UTC, datetime, timedelta

    from sqlalchemy import select

    from app.telegram_bots.assign import DraftAssignment
    from deerflow.persistence.agent_instances.model import AgentInstanceGrantRow
    from deerflow.persistence.agent_instances.work import AgentWorkEventRow

    e = startable
    model = DraftAssignment(objective="Review the guide", success_criteria="List verified gaps", due_text="By Friday", due_at=datetime.now(UTC) + timedelta(days=2))
    _repo, _inbox, source = await draft(e, model=model)
    identity = uuid4().hex
    child_id = sha256((identity + ":no-due").encode()).hexdigest()[:32]
    await e.acts.stage_delegate(e.link, source.id, operation_id=identity, start=True)
    async with e.db.sessions.begin() as writer:
        (await writer.get(AgentInstanceGrantRow, (e.instance.id, e.link.owner_user_id))).permissions = 3
    real, entered, release = e.human.work.delegate, asyncio.Event(), asyncio.Event()

    async def lose(*args, **kwargs):
        result = await real(*args, **kwargs)
        if kwargs["request"].operation_id == child_id:
            entered.set()
            await release.wait()
            raise RuntimeError("committed no-due response lost")
        return result

    monkeypatch.setattr(e.human.work, "delegate", lose)
    running = asyncio.create_task(e.acts.check(identity))
    try:
        await asyncio.wait_for(entered.wait(), 10)
        observed = await e.acts.cancel_continuation(identity)
        assert observed.state == "done" and observed.result["delegate_child_id"] == child_id
        assert observed.continuation["activation_continuation"]["phase"] == "cancelled"
        child = await e.acts._work_record(child_id)
        async with e.db.sessions() as reader:
            event = await reader.scalar(select(AgentWorkEventRow).where(AgentWorkEventRow.operation_id == child_id))
            assert event is not None
        assert child.state == "done", "a conclusive canonical child receipt must settle with its parent"
        assert child.cancel_requested_at is not None
        assert not e.launches
    finally:
        release.set()
        await running
