"""Borrowed activation writers and exact admission receipts on real SQL."""

import pytest
from _storage_spaces_test_support import ALICE
from sqlalchemy import func, select
from test_agent_instances import instances as instances
from test_agent_work_records import work as work
from test_resident_registry import registry as registry
from test_work_execution import activation
from test_work_execution import execution as execution

from deerflow.agent_instances.contract import AgentDenied
from deerflow.persistence.agent_instances.work import AgentWorkAttemptRow, AgentWorkEventRow


@pytest.mark.asyncio
@pytest.mark.parametrize("reject_at", [1, 2])
async def test_activation_borrowed_guard_rolls_back_attempt_and_event(execution, reject_at):
    service, admitted, record, sf = execution
    guards, promises, rollbacks = [], [], []

    async def admission(writer):
        assert writer.in_transaction()
        guards.append(writer)
        if len(guards) == reject_at:
            raise AgentDenied("Admission withdrawn")

    with pytest.raises(AgentDenied):
        await service.reserve(execution=admitted, work_id=record["id"], request=activation(admitted, record), admission=admission, before_admit=promises.append, on_rollback=lambda: rollbacks.append(True))
    assert len(guards) == reject_at and len(rollbacks) == 1
    assert len(promises) == reject_at - 1
    async with sf() as reader:
        assert await reader.scalar(select(func.count()).select_from(AgentWorkAttemptRow)) == 0
        assert await reader.scalar(select(func.count()).select_from(AgentWorkEventRow).where(AgentWorkEventRow.action == "activate")) == 0


@pytest.mark.asyncio
@pytest.mark.parametrize("known", ["bound", "retired"])
async def test_known_replay_keeps_original_admission_without_new_guard(execution, known):
    service, admitted, record, sf = execution
    body = activation(admitted, record)
    first = await service.reserve(execution=admitted, work_id=record["id"], request=body)
    async with sf.begin() as writer:
        attempt = await writer.get(AgentWorkAttemptRow, first.attempt_id)
        if known == "bound":
            attempt.run_id = "already-admitted-run"
        else:
            attempt.status = "failed"

    async def refuse(writer):
        raise AssertionError("A replay must not acquire a new admission")

    second = await service.reserve(execution=admitted, work_id=record["id"], request=body, admission=refuse)
    assert first.attempt_id == second.attempt_id


@pytest.mark.asyncio
async def test_runless_replay_rechecks_borrowed_admission_and_keeps_original_promise(execution):
    service, admitted, record, sf = execution
    body = activation(admitted, record)
    original = await service.reserve(execution=admitted, work_id=record["id"], request=body)
    checked = []

    async def refuse(writer):
        assert writer.in_transaction()
        checked.append(True)
        raise AgentDenied("Original consent was withdrawn")

    with pytest.raises(AgentDenied):
        await service.reserve(execution=admitted, work_id=record["id"], request=body, admission=refuse)
    assert checked == [True]
    async with sf() as reader:
        attempt = await reader.get(AgentWorkAttemptRow, original.attempt_id)
        assert attempt.run_id is None and attempt.status == "starting" and attempt.request == body.model_dump(mode="json")
        assert await reader.scalar(select(func.count()).select_from(AgentWorkAttemptRow)) == 1


@pytest.mark.asyncio
async def test_resident_exact_replay_forwards_caller_admission_before_recovering_run(execution, registry, monkeypatch):
    from test_work_internal_caller import app_for, install_launcher

    from app.gateway.work_execution import activate_work
    from app.telegram_bots.authority import internal_request

    service, admitted, record, sf = execution
    body = activation(admitted, record)
    original = await service.reserve(execution=admitted, work_id=record["id"], request=body)
    residents, _provider, _records, _clock = registry(replicas=4)
    service.agents.residents = residents
    app = app_for(execution, monkeypatch)
    _manager, launches = install_launcher(monkeypatch)
    checked = []

    async def refuse(writer):
        assert writer.in_transaction()
        checked.append(True)
        raise AgentDenied("Caller consent no longer holds")

    try:
        with pytest.raises(AgentDenied):
            await activate_work(internal_request(app, "alice"), instance_id=admitted.instance.id, work_id=record["id"], body=body, admission=refuse)
        assert checked == [True] and not launches and not residents.reservations
        async with sf() as reader:
            attempt = await reader.get(AgentWorkAttemptRow, original.attempt_id)
            assert attempt.run_id is None and attempt.status == "starting"
            assert await reader.scalar(select(func.count()).select_from(AgentWorkAttemptRow)) == 1
    finally:
        await residents.close()


@pytest.mark.asyncio
async def test_reservation_alone_is_not_a_completed_activation_receipt(execution):
    service, admitted, record, sf = execution
    body = activation(admitted, record)
    scope = await service.reserve(execution=admitted, work_id=record["id"], request=body)
    read = dict(actor=ALICE, instance_id=admitted.instance.id, work_id=record["id"], request=body)
    assert await service.activation_receipt(**read) is None
    async with sf.begin() as writer:
        (await writer.get(AgentWorkAttemptRow, scope.attempt_id)).run_id = "bound-run"
    receipt = await service.activation_receipt(**read)
    assert receipt["activation_receipt"] == {"operation_id": body.operation_id, "attempt_id": scope.attempt_id}
    assert receipt["attempt"]["id"] == scope.attempt_id


@pytest.mark.asyncio
async def test_retired_runless_attempt_has_an_exact_receipt(execution):
    service, admitted, record, sf = execution
    body = activation(admitted, record)
    scope = await service.reserve(execution=admitted, work_id=record["id"], request=body)
    async with sf.begin() as writer:
        (await writer.get(AgentWorkAttemptRow, scope.attempt_id)).status = "failed"
    receipt = await service.activation_receipt(actor=ALICE, instance_id=admitted.instance.id, work_id=record["id"], request=body)
    assert receipt["activation_receipt"]["attempt_id"] == scope.attempt_id


@pytest.mark.asyncio
@pytest.mark.parametrize("reject_at", [1, 2, 3])
async def test_gateway_guard_rollback_releases_only_its_resident_promise(execution, registry, monkeypatch, reject_at):
    from test_work_internal_caller import app_for, install_launcher

    from app.gateway.work_execution import activate_work
    from app.telegram_bots.authority import internal_request

    service, admitted, record, sf = execution
    residents, _provider, _records, _clock = registry(replicas=4)
    service.agents.residents = residents
    app = app_for(execution, monkeypatch)
    _manager, launches = install_launcher(monkeypatch)
    calls = []

    async def admission(writer):
        calls.append(writer)
        if len(calls) == reject_at:
            raise AgentDenied("Caller admission withdrawn")

    try:
        with pytest.raises(AgentDenied):
            await activate_work(internal_request(app, "alice"), instance_id=admitted.instance.id, work_id=record["id"], body=activation(admitted, record), admission=admission)
        assert len(calls) == reject_at and not launches and not residents.reservations
        async with sf() as reader:
            assert await reader.scalar(select(func.count()).select_from(AgentWorkAttemptRow)) == 0
    finally:
        await residents.close()
