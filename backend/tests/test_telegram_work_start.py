"""Explicit activation recovery through the shared host path and real SQL.

The launcher is an in-memory host run store, not native worker qualification.
"""

from datetime import timedelta
from unittest.mock import AsyncMock

import pytest
import pytest_asyncio
from _storage_spaces_test_support import operation
from _telegram_repository_test_support import db as db
from sqlalchemy import func, select
from test_telegram_work_actions import actions as actions

from deerflow.agent_instances.contract import AgentConflict
from deerflow.agent_instances.work_contract import DelegateWork
from deerflow.persistence.agent_instances.model import AgentConversationRow
from deerflow.persistence.agent_instances.work import AgentWorkAttemptRow, AgentWorkEventRow


@pytest_asyncio.fixture
async def startable(actions, monkeypatch):
    from app.gateway.routers import agent_instances
    from deerflow.persistence.spaces.model import SpaceGrantRow
    from deerflow.runtime.runs.manager import RunManager
    from deerflow.runtime.runs.store.memory import MemoryRunStore
    from deerflow.spaces.principals import HostPrincipalResolver
    from deerflow.spaces.registry import SpaceRegistry

    e = actions
    directory = e.human.agents.directory
    e.human.agents.files.registry = SpaceRegistry(e.db.sessions, HostPrincipalResolver(human=directory.human, nonhuman=directory.lookup, nonhuman_in_session=directory.lookup_in_session))
    e.human.agents.residents = None
    async with e.db.engine.begin() as connection:
        await connection.run_sync(lambda sync: SpaceGrantRow.__table__.create(sync, checkfirst=True))
    async with e.db.sessions.begin() as writer:
        writer.add(SpaceGrantRow(space_id=e.instance.home_id, principal_kind="nonhuman", subject_id=e.instance.principal.subject_id, permissions=3))
    e.work = await e.human.work.delegate(actor=e.actor, instance_id=e.instance.id, request=DelegateWork(operation_id=operation(), objective="Review the documentation", success_criteria="Report verified gaps"))
    monkeypatch.setattr(agent_instances, "get_agents_api_config", lambda: type("Config", (), {"enabled": True})())
    e.manager, e.launches = RunManager(store=MemoryRunStore()), []

    async def launch(body, thread_id, request, *, idempotency_key, require_existing_thread, work_attempt):
        from app.gateway.work_execution import ACTIVATION_MESSAGE

        assert body.input == {"messages": [{"role": "user", "content": ACTIVATION_MESSAGE}]}
        from app.gateway.routers.spaces import _actor

        assert require_existing_thread and await _actor(request) == e.actor
        e.launches.append((thread_id, idempotency_key, work_attempt.attempt_id))
        return await e.manager.create_or_reject(thread_id, body.assistant_id, user_id=e.link.owner_user_id, idempotency_key=idempotency_key, metadata={"agent_work_id": work_attempt.work_id, "agent_work_attempt_id": work_attempt.attempt_id})

    monkeypatch.setattr("app.gateway.services.start_run", launch)
    return e


def start_args(e, identity=None):
    return dict(operation_id=identity or operation(), work_id=e.work["id"], assignment_revision=e.work["assignment_revision"])


@pytest.mark.asyncio
async def test_activate_records_original_body_and_separate_work_conversation(startable):
    e = startable
    args = start_args(e)
    first = await e.acts.activate(e.link, **args)
    assert first.state == "done" and first.attempts == 1
    assert first.result["activation_receipt"]["operation_id"] == first.id
    assert await e.acts.activate(e.link, **args) == first
    assert len(e.launches) == 1
    async with e.db.sessions() as reader:
        binding = await reader.get(AgentConversationRow, first.body["thread_id"])
        assert binding.creation_id == first.id and binding.instance_id == e.instance.id
        assert await reader.scalar(select(func.count()).select_from(AgentWorkAttemptRow)) == 1
        assert await reader.scalar(select(func.count()).select_from(AgentWorkEventRow).where(AgentWorkEventRow.action == "activate")) == 1
    assert await e.db.connections.get_thread_id(e.link.connection_id, str(e.link.chat_id), None) is None


@pytest.mark.asyncio
async def test_displayed_assignment_changes_before_conversation(startable):
    e = startable
    from test_agent_work_records import command

    await e.human.work.command(actor=e.actor, instance_id=e.instance.id, work_id=e.work["id"], request=command(e.work, "edit", objective="Changed objective"))
    with pytest.raises(AgentConflict) as exc:
        await e.acts.activate(e.link, **start_args(e))
    assert exc.value.reason == "work-changed"
    assert not e.launches
    async with e.db.sessions() as reader:
        assert await reader.scalar(select(func.count()).select_from(AgentConversationRow)) == 0


@pytest.mark.asyncio
async def test_lost_activation_response_recovers_by_read_only_exact_receipt(startable, monkeypatch):
    from app.gateway import work_execution

    e = startable
    real, calls = work_execution.activate_work, []

    async def lose(*args, **kwargs):
        calls.append(kwargs["body"].model_dump())
        await real(*args, **kwargs)
        raise RuntimeError("response lost after admission")

    monkeypatch.setattr(work_execution, "activate_work", lose)
    act = await e.acts.activate(e.link, **start_args(e))
    assert act.state == "pending"
    recovered = await e.acts.check(act.id)
    assert recovered.state == "done" and len(calls) == len(e.launches) == 1
    assert dict(recovered.body) == calls[0]


@pytest.mark.asyncio
async def test_runless_reservation_retries_same_body_and_conversation(startable, monkeypatch):
    from app.gateway import services

    e = startable
    real = services.start_run
    monkeypatch.setattr(services, "start_run", AsyncMock(side_effect=RuntimeError("launch unavailable")))
    act = await e.acts.activate(e.link, **start_args(e))
    assert act.state == "pending" and await e.acts.receipt_for(act) is None
    async with e.db.sessions() as reader:
        original = await reader.scalar(select(AgentWorkAttemptRow))
        attempt_id = original.id
        assert original.run_id is None
    monkeypatch.setattr(services, "start_run", real)
    recovered = await e.acts.check(act.id)
    assert recovered.state == "done"
    assert recovered.result["activation_receipt"]["attempt_id"] == attempt_id
    assert recovered.body == act.body and e.launches[0][0] == act.body["thread_id"]


@pytest.mark.asyncio
async def test_cancel_unknown_start_never_launches_a_runless_reservation(startable, monkeypatch):
    from app.gateway import services

    e = startable
    real = services.start_run
    monkeypatch.setattr(services, "start_run", AsyncMock(side_effect=RuntimeError("launch unavailable")))
    act = await e.acts.activate(e.link, **start_args(e))
    cancelled = await e.acts.cancel_activation(act.id)
    assert cancelled.cancel_requested_at is not None and cancelled.reason == "cancel-unconfirmed"
    monkeypatch.setattr(services, "start_run", real)
    for _ in range(2):
        result = await e.acts.check(act.id)
        assert result.cancel_requested_at == cancelled.cancel_requested_at
    assert not e.launches


@pytest.mark.asyncio
async def test_busy_wait_retries_identical_body_until_deadline(startable, monkeypatch):
    from app.gateway import work_execution
    from app.telegram_bots.acts import ActsWorker
    from deerflow.agent_instances.contract import ResidentBusy

    e = startable
    real, calls = work_execution.activate_work, []

    async def busy(*args, **kwargs):
        calls.append(kwargs["body"].model_dump())
        raise ResidentBusy(e.instance.name)

    monkeypatch.setattr(work_execution, "activate_work", busy)
    act = await e.acts.activate(e.link, **start_args(e))
    assert act.state == "waiting" and act.reason == "busy"
    clock = e.acts.outbox.clock
    e.acts.outbox.clock = lambda: clock() + timedelta(seconds=e.acts.outbox.config.queue.retry_seconds + 1)
    worker = ActsWorker(e.acts)
    try:
        await worker.tick()
        assert len(calls) == 2 and calls[0] == calls[1] == dict(act.body)
        e.acts.outbox.clock = lambda: act.first_attempt_at + timedelta(seconds=e.acts.outbox.config.queue.max_wait_seconds)
        await worker.tick()
        assert len(calls) == 2
        result = await e.acts._work_record(act.id)
        assert result.state == "refused" and result.reason == "busy"
    finally:
        await worker.stop()
    monkeypatch.setattr(work_execution, "activate_work", real)


@pytest.mark.asyncio
async def test_work_limit_never_retries_automatically(startable, monkeypatch):
    from app.telegram_bots.acts import ActsWorker
    from deerflow.agent_instances.contract import ResidentWorkLimit

    e = startable
    refused = AsyncMock(side_effect=ResidentWorkLimit(e.instance.name))
    monkeypatch.setattr("app.gateway.work_execution.activate_work", refused)
    act = await e.acts.activate(e.link, **start_args(e))
    assert act.state == "waiting" and act.reason == "work-limit"
    e.acts.outbox.clock = lambda: act.updated_at + timedelta(minutes=1)
    worker = ActsWorker(e.acts)
    try:
        await worker.tick()
    finally:
        await worker.stop()
    refused.assert_awaited_once()


@pytest.mark.asyncio
async def test_activation_ignores_acts_pause(startable):
    from deerflow.persistence.telegram_bots.model import TelegramBotLinkRow

    e = startable
    async with e.db.sessions.begin() as writer:
        (await writer.get(TelegramBotLinkRow, e.link.id)).acts_paused_since = e.acts.outbox.clock()
    e.link = await e.db.links.get(e.link.id)
    assert (await e.acts.activate(e.link, **start_args(e))).state == "done"


@pytest.mark.asyncio
@pytest.mark.parametrize("change", ["cancel", "binding"])
async def test_late_cancellation_or_rebind_blocks_new_host_reservation(startable, monkeypatch, change):
    from app.gateway import work_execution
    from deerflow.persistence.telegram_bots.model import TelegramBotLinkRow

    e = startable
    real = work_execution.activate_work

    async def changed(*args, **kwargs):
        identity = kwargs["body"].operation_id
        if change == "cancel":
            await e.acts.cancel_activation(identity)
        else:
            async with e.db.sessions.begin() as writer:
                (await writer.get(TelegramBotLinkRow, e.link.id)).binding_generation += 1
        return await real(*args, **kwargs)

    monkeypatch.setattr(work_execution, "activate_work", changed)
    result = await e.acts.activate(e.link, **start_args(e))
    assert result.state != "done" and not e.launches
    if change == "cancel":
        assert result.cancel_requested_at is not None and result.reason == "cancelled"
    async with e.db.sessions() as reader:
        assert await reader.scalar(select(func.count()).select_from(AgentWorkAttemptRow)) == 0


@pytest.mark.asyncio
async def test_cancel_busy_is_conclusive_and_worker_cannot_retry(startable, monkeypatch):
    from app.telegram_bots.acts import ActsWorker
    from deerflow.agent_instances.contract import ResidentBusy

    e = startable
    refused = AsyncMock(side_effect=ResidentBusy(e.instance.name))
    monkeypatch.setattr("app.gateway.work_execution.activate_work", refused)
    act = await e.acts.activate(e.link, **start_args(e))
    cancelled = await e.acts.cancel_activation(act.id)
    assert cancelled.state == "refused" and cancelled.reason == "cancelled"
    e.acts.outbox.clock = lambda: act.updated_at + timedelta(minutes=1)
    worker = ActsWorker(e.acts)
    try:
        await worker.tick()
    finally:
        await worker.stop()
    refused.assert_awaited_once()


@pytest.mark.asyncio
async def test_old_unknown_act_stops_automatic_calls_but_explicit_retry_keeps_body(startable, monkeypatch):
    from app.telegram_bots.acts import ActsWorker

    e = startable
    unavailable = AsyncMock(side_effect=RuntimeError("unknown response"))
    monkeypatch.setattr("app.gateway.work_execution.activate_work", unavailable)
    act = await e.acts.activate(e.link, **start_args(e))
    e.acts.outbox.clock = lambda: act.first_attempt_at + timedelta(hours=1)
    worker = ActsWorker(e.acts)
    try:
        await worker.tick()
    finally:
        await worker.stop()
    assert (await e.acts._work_record(act.id)).state == "uncertain"
    unavailable.assert_awaited_once()
    recovered = await e.acts.check(act.id)
    assert recovered.body == act.body and recovered.attempts == 2 and recovered.state == "uncertain"


@pytest.mark.asyncio
@pytest.mark.parametrize("reason", ["run-limit", "restarting", "execution-unavailable", "storage-busy"])
async def test_conclusive_start_refusals_never_repeat(startable, monkeypatch, reason):
    from app.telegram_bots.acts import ActsWorker
    from deerflow.agent_instances.contract import Refusal

    e = startable
    refused = AsyncMock(side_effect=Refusal("unavailable", reason=reason))
    monkeypatch.setattr("app.gateway.work_execution.activate_work", refused)
    act = await e.acts.activate(e.link, **start_args(e))
    assert act.state == "refused" and act.reason == reason
    e.acts.outbox.clock = lambda: act.updated_at + timedelta(minutes=1)
    worker = ActsWorker(e.acts)
    try:
        await worker.tick()
        await e.acts.check(act.id)
    finally:
        await worker.stop()
    refused.assert_awaited_once()


def test_start_refusals_only_describe_call_refusals():
    from app.telegram_bots.work_start_actions import START_REFUSALS

    assert START_REFUSALS == {"busy", "work-limit", "run-limit", "restarting", "execution-unavailable", "storage-busy"}
    assert not START_REFUSALS & {"starting-elsewhere", "start-failed", "files-full"}


@pytest.mark.asyncio
@pytest.mark.parametrize("status,reason", [("completed", "work-closed"), ("cancelled", "work-closed"), ("submitted", "awaiting-review")])
async def test_closed_or_submitted_work_refuses_before_conversation(startable, status, reason):
    from test_agent_work_records import command, submit_fixture

    from deerflow.agent_instances.contract import AgentDenied

    e = startable
    if status == "cancelled":
        await e.human.work.command(actor=e.actor, instance_id=e.instance.id, work_id=e.work["id"], request=command(e.work, "cancel"))
    else:
        # The existing fixture supplies a valid settled host outcome. Actual
        # native settlement remains separately qualified.
        outcome = await submit_fixture(e.db.sessions, e.work)
        if status == "completed":
            await e.human.work.command(
                actor=e.actor,
                instance_id=e.instance.id,
                work_id=e.work["id"],
                request=command(e.work, "accept", outcome_id=outcome, evidence_revision=1, basis="outcome_statement", acknowledge_unchecked_sources=True),
            )
    e.work = await e.human.work.get(actor=e.actor, instance_id=e.instance.id, work_id=e.work["id"])
    with pytest.raises(AgentDenied) as exc:
        await e.acts.activate(e.link, **start_args(e))
    assert exc.value.reason == reason and not e.launches
    async with e.db.sessions() as reader:
        assert await reader.scalar(select(func.count()).select_from(AgentConversationRow)) == 0


@pytest.mark.asyncio
@pytest.mark.parametrize("late", ["busy", "unknown", "reserved-busy"])
async def test_inflight_cancel_settles_only_proven_no_admission(startable, monkeypatch, late):
    from app.gateway import services
    from deerflow.agent_instances.contract import ResidentBusy

    e = startable
    cancelled = []

    async def delayed(*args, **kwargs):
        identity = kwargs["work_attempt"].attempt_id if late == "reserved-busy" else kwargs["body"].operation_id
        if late == "reserved-busy":
            async with e.db.sessions() as reader:
                identity = (await reader.get(AgentWorkAttemptRow, identity)).activation_id
        cancelled.append(await e.acts.cancel_activation(identity))
        if late in {"busy", "reserved-busy"}:
            raise ResidentBusy(e.instance.name)
        raise RuntimeError("unknown response")

    if late == "reserved-busy":
        monkeypatch.setattr(services, "start_run", delayed)
    else:
        monkeypatch.setattr("app.gateway.work_execution.activate_work", delayed)
    result = await e.acts.activate(e.link, **start_args(e))
    assert cancelled[0].state == "uncertain"
    assert (result.state, result.reason) == (("refused", "cancelled") if late == "busy" else ("uncertain", "cancel-unconfirmed"))
    assert not e.launches


@pytest.mark.asyncio
async def test_known_busy_replies_do_not_consume_unknown_budget(startable, monkeypatch):
    from app.telegram_bots.acts import ActsWorker
    from deerflow.agent_instances.contract import ResidentBusy

    e = startable
    calls = AsyncMock(side_effect=ResidentBusy(e.instance.name))
    monkeypatch.setattr("app.gateway.work_execution.activate_work", calls)
    act = await e.acts.activate(e.link, **start_args(e))
    worker = ActsWorker(e.acts)
    try:
        for ordinal in range(1, e.acts.outbox.config.acts.max_attempts + 2):
            e.acts.outbox.clock = lambda ordinal=ordinal: act.first_attempt_at + timedelta(seconds=ordinal * (e.acts.outbox.config.queue.retry_seconds + 1))
            await worker.tick()
        waiting = await e.acts._work_record(act.id)
        assert waiting.state == "waiting" and calls.await_count > e.acts.outbox.config.acts.max_attempts
        calls.side_effect = RuntimeError("unknown response")
        for _ in range(e.acts.outbox.config.acts.max_attempts + 1):
            current = await e.acts._work_record(act.id)
            e.acts.outbox.clock = lambda current=current: current.updated_at + timedelta(seconds=e.acts.outbox.config.queue.retry_seconds + 1)
            await worker.tick()
        unknown = await e.acts._work_record(act.id)
        assert unknown.state == "uncertain"
        assert unknown.attempts - waiting.attempts == e.acts.outbox.config.acts.max_attempts
        assert all(call.kwargs["body"].model_dump() == dict(act.body) for call in calls.await_args_list)
    finally:
        await worker.stop()
