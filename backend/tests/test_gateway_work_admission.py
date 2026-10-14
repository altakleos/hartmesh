"""Real durable Work admission with fake run launch and capacity barriers."""

import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from test_agent_instances import instances as instances
from test_agent_work_records import work as work
from test_resident_registry import registry as registry
from test_work_execution import activation, run_record
from test_work_execution import execution as execution

from deerflow.agent_instances.contract import ResidentBusy


def request_for(service):
    return SimpleNamespace(app=SimpleNamespace(state=SimpleNamespace(agent_instances=service.agents)))


@pytest.mark.asyncio
async def test_new_activation_binds_capacity_before_durable_event_and_launch(execution, registry, monkeypatch):
    from app.gateway.work_execution import activate_work_run

    service, admitted, row, _sf = execution
    residents, _provider, _records, _clock = registry()
    service.agents.residents = residents
    body = activation(admitted, row)
    event = service.work._event
    seen = []

    async def guarded_event(*args, **kwargs):
        if args[-1] == "activate":
            seen.append(next(iter(residents.reservations.values())).attempt_id)
            assert seen[-1] is not None
        await event(*args, **kwargs)

    async def launch(_body, _thread, _request, **kwargs):
        scope = kwargs["work_attempt"]
        assert next(iter(residents.reservations.values())).attempt_id == scope.attempt_id
        return run_record(admitted, scope, status="running")

    monkeypatch.setattr(service.work, "_event", guarded_event)
    monkeypatch.setattr("app.gateway.services.start_run", launch)
    scope = await activate_work_run(request_for(service), service, admitted, row["id"], body)
    assert seen == [scope.attempt_id]


@pytest.mark.asyncio
async def test_retired_exact_replay_needs_no_new_capacity(execution, monkeypatch):
    from app.gateway.work_execution import activate_work_run
    from deerflow.persistence.agent_instances.work import AgentWorkAttemptRow

    service, admitted, row, sf = execution
    body = activation(admitted, row)
    first = await service.reserve(execution=admitted, work_id=row["id"], request=body)
    async with sf() as session, session.begin():
        (await session.get(AgentWorkAttemptRow, first.attempt_id)).status = "failed"
    service.agents.residents = SimpleNamespace(admit=AsyncMock(side_effect=AssertionError("Replay must not reserve")))
    launch = AsyncMock(side_effect=AssertionError("Retired replay must not launch"))
    monkeypatch.setattr("app.gateway.services.start_run", launch)
    same = await activate_work_run(request_for(service), service, admitted, row["id"], body)
    assert same.attempt_id == first.attempt_id and same.retired
    service.agents.residents.admit.assert_not_awaited()


@pytest.mark.asyncio
async def test_capacity_refusal_cannot_write_a_new_attempt(execution, monkeypatch):
    from app.gateway.work_execution import activate_work_run

    service, admitted, row, _sf = execution
    service.agents.residents = SimpleNamespace(admit=AsyncMock(side_effect=ResidentBusy(admitted.instance.name)))
    monkeypatch.setattr("app.gateway.services.start_run", AsyncMock(side_effect=AssertionError("Refused admission must not launch")))
    with pytest.raises(ResidentBusy):
        await activate_work_run(request_for(service), service, admitted, row["id"], activation(admitted, row))
    service.agents.residents = None
    current = await service.work.get(actor=admitted.requester, instance_id=admitted.instance.id, work_id=row["id"])
    assert current["attempt"] is None and current["revision"] == row["revision"]


@pytest.mark.asyncio
async def test_cancellation_after_attempt_write_drains_launch_and_observation(execution, registry, monkeypatch):
    from app.gateway.work_execution import activate_work_run

    service, admitted, row, _sf = execution
    residents, _provider, _records, clock = registry()
    service.agents.residents = residents
    entered, release = asyncio.Event(), asyncio.Event()
    launches = []
    event = service.work._event

    async def delayed_event(*args, **kwargs):
        if args[-1] == "activate":
            entered.set()
            await release.wait()
        await event(*args, **kwargs)

    async def launch(_body, _thread, _request, **kwargs):
        scope = kwargs["work_attempt"]
        launches.append(scope.attempt_id)
        return run_record(admitted, scope, status="running")

    monkeypatch.setattr(service.work, "_event", delayed_event)
    monkeypatch.setattr("app.gateway.services.start_run", launch)
    task = asyncio.create_task(activate_work_run(request_for(service), service, admitted, row["id"], activation(admitted, row)))
    try:
        await asyncio.wait_for(entered.wait(), 30)
        reservation = next(iter(residents.reservations.values()))
        task.cancel()
        clock[0] = reservation.expires_at + 1
        residents.attempts_in_flight()
        assert residents.reservations[reservation.id] is reservation and not task.done()
        release.set()
        with pytest.raises(asyncio.CancelledError):
            await task
        current = await service.work.get(actor=admitted.requester, instance_id=admitted.instance.id, work_id=row["id"])
        assert launches == [current["attempt"]["id"]]
        assert current["attempt"]["run_id"] is not None
    finally:
        release.set()
        await asyncio.gather(task, return_exceptions=True)


@pytest.mark.asyncio
async def test_two_runless_retries_share_one_retained_capacity_promise(execution, registry, monkeypatch):
    from app.gateway.work_execution import activate_work_run

    service, admitted, row, _sf = execution
    body = activation(admitted, row)
    await service.reserve(execution=admitted, work_id=row["id"], request=body)
    residents, _provider, _records, _clock = registry(replicas=4)
    service.agents.residents = residents
    entered, release = asyncio.Event(), asyncio.Event()
    original = residents.admit
    calls, record = [], None

    async def delayed(*args, **kwargs):
        calls.append(True)
        entered.set()
        await release.wait()
        return await original(*args, **kwargs)

    async def launch(_body, _thread, _request, **kwargs):
        nonlocal record
        if record is None:
            record = run_record(admitted, kwargs["work_attempt"], status="running")
        return record

    monkeypatch.setattr(residents, "admit", delayed)
    monkeypatch.setattr("app.gateway.services.start_run", launch)
    first = asyncio.create_task(activate_work_run(request_for(service), service, admitted, row["id"], body))
    await asyncio.wait_for(entered.wait(), 30)
    second = asyncio.create_task(activate_work_run(request_for(service), service, admitted, row["id"], body))
    try:
        await asyncio.sleep(0.1)
        assert len(calls) == 1, "One replay must own recovery through the capacity await"
        release.set()
        one, two = await asyncio.gather(first, second)
        assert one.attempt_id == two.attempt_id
        assert len(residents.reservations) == 1
    finally:
        release.set()
        await asyncio.gather(first, second, return_exceptions=True)
