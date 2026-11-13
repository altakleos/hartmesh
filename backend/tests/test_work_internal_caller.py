"""HTTP and owner-attributed in-process Work callers share durable effects."""

import asyncio
from types import SimpleNamespace

import httpx
import pytest
from _storage_spaces_test_support import ALICE, operation
from fastapi import HTTPException
from fastapi.encoders import jsonable_encoder
from sqlalchemy import func, select
from test_agent_instances import instances as instances
from test_agent_instances import request_app
from test_agent_work_records import command, submit_fixture
from test_agent_work_records import work as work
from test_resident_registry import registry as registry
from test_work_execution import activation
from test_work_execution import execution as execution

from app.gateway.routers import agent_instances as routes
from app.gateway.routers.agent_work import router
from app.gateway.routers.human_input import router as input_router
from app.gateway.routers.spaces import _actor
from deerflow.agent_instances.contract import AgentConflict, AgentDenied
from deerflow.agent_instances.human_input import HumanInput
from deerflow.agent_instances.human_input_contract import CreateRequest, Respond
from deerflow.agent_instances.work import AgentWork
from deerflow.agent_instances.work_contract import DelegateWork
from deerflow.persistence.agent_instances.model import AgentInstanceGrantRow
from deerflow.persistence.agent_instances.work import AgentWorkAttemptRow, AgentWorkEventRow
from deerflow.runtime.runs.manager import RunManager
from deerflow.runtime.runs.store.memory import MemoryRunStore


def app_for(execution, monkeypatch, *, permissions=("agents:read", "agents:write", "runs:create")):
    service, admitted, _row, _sf = execution
    monkeypatch.setattr(routes, "get_agents_api_config", lambda: SimpleNamespace(enabled=True))
    app = request_app(service.agents, permissions=permissions)
    app.state.agent_conversations = admitted.authority
    app.include_router(router)
    app.include_router(input_router)
    return app


def install_launcher(monkeypatch):
    manager, calls = RunManager(store=MemoryRunStore()), []

    async def launch(body, thread_id, request, *, idempotency_key, require_existing_thread, work_attempt):
        from app.gateway.work_execution import ACTIVATION_MESSAGE

        assert body.input == {"messages": [{"role": "user", "content": ACTIVATION_MESSAGE}]}
        assert require_existing_thread and await _actor(request) == ALICE
        calls.append((idempotency_key, work_attempt.attempt_id))
        return await manager.create_or_reject(thread_id, body.assistant_id, user_id="alice", idempotency_key=idempotency_key, metadata={"agent_work_id": work_attempt.work_id, "agent_work_attempt_id": work_attempt.attempt_id})

    monkeypatch.setattr("app.gateway.services.start_run", launch)
    return manager, calls


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ["delegate", "respond", "decide", "accept", "activate"])
@pytest.mark.parametrize("first", ["http", "inprocess"])
async def test_inprocess_actor_same_results(execution, monkeypatch, kind, first):
    from app.gateway.work_execution import activate_work
    from app.telegram_bots.authority import internal_request

    service, admitted, row, sf = execution
    app = app_for(execution, monkeypatch)
    request = internal_request(app, "alice")
    actor = await _actor(request)
    assert actor == ALICE and request.cookies == {}
    work = service.work
    inputs = HumanInput(work)
    path = f"/api/agent-instances/{admitted.instance.id}/work"
    expected_status = 200
    if kind == "delegate":
        body = DelegateWork(operation_id=operation(), objective="Check the guide", success_criteria="List verified gaps")
        expected_status = 201

        async def perform():
            return await work.delegate(actor=actor, instance_id=admitted.instance.id, request=body)
    elif kind == "activate":
        manager, calls = install_launcher(monkeypatch)
        body = activation(admitted, row)
        path += f"/{row['id']}/activate"

        async def perform():
            return await activate_work(request, instance_id=admitted.instance.id, work_id=row["id"], body=body)
    else:
        if kind == "accept":
            await submit_fixture(sf, row)
        question = await inputs.create(
            actor=actor,
            instance_id=admitted.instance.id,
            work_id=row["id"],
            request=CreateRequest(
                operation_id=operation(),
                expected_work_revision=row["revision"],
                expected_assignment_revision=row["assignment_revision"],
                purpose={"respond": "information", "decide": "decision", "accept": "review"}[kind],
                question="Check the guide?",
                reason="An explicit answer is required",
                expected_response="The current guide",
            ),
        )
        if kind == "respond":
            body = Respond(operation_id=operation(), expected_request_revision=question["request_revision"], expected_assignment_revision=question["assignment_revision"], text="Use the current guide")
            path = f"/api/human-input/{question['id']}/responses"

            async def perform():
                return await inputs.respond(actor=actor, request_id=question["id"], request=body)
        else:
            current = await work.get(actor=actor, instance_id=admitted.instance.id, work_id=row["id"])
            fields = (
                {"blocker_id": question["basis_id"], "blocker_revision": question["basis_revision"], "note": "Proceed"}
                if kind == "decide"
                else {"outcome_id": question["basis_id"], "evidence_revision": question["basis_revision"], "basis": "outcome_statement", "acknowledge_unchecked_sources": True}
            )
            basis = {"id": question["id"], "revision": question["revision"], "request_revision": question["request_revision"], "response_ids": []}
            body = command(current, kind, request_basis=basis, **fields)
            path += f"/{row['id']}/commands"

            async def perform():
                return await work.command(actor=actor, instance_id=admitted.instance.id, work_id=row["id"], request=body)

    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as client:

        async def over_http():
            response = await client.post(path, json=body.model_dump(mode="json", exclude_unset=True))
            assert response.status_code == expected_status, response.text
            return response.json()

        one = await (over_http() if first == "http" else perform())
        async with sf() as session:
            count = await session.scalar(select(func.count()).select_from(AgentWorkEventRow))
        two = await (perform() if first == "http" else over_http())
        assert jsonable_encoder(one) == jsonable_encoder(two)
        async with sf() as session:
            assert await session.scalar(select(func.count()).select_from(AgentWorkEventRow)) == count
        if kind == "activate":
            assert len(calls) == 2 and calls[0] == calls[1]
            assert len(await manager.list_by_thread(admitted.thread_id)) == 1
            assert one["activation_receipt"]["attempt_id"] == one["attempt"]["id"]


@pytest.mark.asyncio
async def test_identical_activation_returns_same_attempt_and_run(execution, monkeypatch):
    from app.gateway.work_execution import activate_work
    from app.telegram_bots.authority import internal_request

    service, admitted, row, _ = execution
    app = app_for(execution, monkeypatch)
    manager, calls = install_launcher(monkeypatch)
    request = internal_request(app, "alice")
    body = activation(admitted, row)
    one = await activate_work(request, instance_id=admitted.instance.id, work_id=row["id"], body=body)
    assert await activate_work(request, instance_id=admitted.instance.id, work_id=row["id"], body=body) == one
    with pytest.raises(AgentConflict):
        await activate_work(request, instance_id=admitted.instance.id, work_id=row["id"], body=body.model_copy(update={"operation_id": operation()}))
    assert len(calls) == 2 and len(await manager.list_by_thread(admitted.thread_id)) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ["decide", "accept", "activate"])
async def test_use_only_person_cannot_decide_or_activate(execution, monkeypatch, kind):
    from app.gateway.work_execution import activate_work
    from app.telegram_bots.authority import internal_request

    service, admitted, row, sf = execution
    app = app_for(execution, monkeypatch)
    _manager, calls = install_launcher(monkeypatch)
    async with sf() as session, session.begin():
        (await session.get(AgentInstanceGrantRow, (admitted.instance.id, "alice"))).permissions = 1
        before = await session.scalar(select(func.count()).select_from(AgentWorkEventRow))
    body = (
        activation(admitted, row)
        if kind == "activate"
        else command(
            row,
            kind,
            **({"blocker_id": operation(), "blocker_revision": 1, "note": "Proceed"} if kind == "decide" else {"outcome_id": operation(), "evidence_revision": 1, "basis": "outcome_statement", "acknowledge_unchecked_sources": True}),
        )
    )
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as client:
        response = await client.post(f"/api/agent-instances/{admitted.instance.id}/work/{row['id']}/" + ("activate" if kind == "activate" else "commands"), json=body.model_dump(mode="json", exclude_unset=True))
        assert response.status_code == 404
    with pytest.raises((AgentDenied, HTTPException)):
        if kind == "activate":
            await activate_work(internal_request(app, "alice"), instance_id=admitted.instance.id, work_id=row["id"], body=body)
        else:
            await service.work.command(actor=await _actor(internal_request(app, "alice")), instance_id=admitted.instance.id, work_id=row["id"], request=body)
    async with sf() as session:
        assert await session.scalar(select(func.count()).select_from(AgentWorkEventRow)) == before
        assert await session.scalar(select(func.count()).select_from(AgentWorkAttemptRow)) == 0
    assert not calls


@pytest.mark.asyncio
@pytest.mark.parametrize("missing", ["agents:read", "agents:write", "runs:create"])
async def test_web_activation_retains_each_route_ceiling(execution, monkeypatch, missing):
    _service, admitted, row, _ = execution
    app = app_for(execution, monkeypatch, permissions=tuple(p for p in ("agents:read", "agents:write", "runs:create") if p != missing))
    _manager, calls = install_launcher(monkeypatch)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as client:
        response = await client.post(f"/api/agent-instances/{admitted.instance.id}/work/{row['id']}/activate", json=activation(admitted, row).model_dump(mode="json"))
        assert response.status_code == 403
    assert not calls


@pytest.mark.asyncio
@pytest.mark.parametrize("boundary", ["cancellation", "runless"])
async def test_owner_activation_preserves_retained_launch_ownership(execution, registry, monkeypatch, boundary):
    from app.gateway.work_execution import activate_work
    from app.telegram_bots.authority import internal_request

    service, admitted, row, _sf = execution
    app = app_for(execution, monkeypatch)
    request = internal_request(app, "alice")
    body = activation(admitted, row)
    residents, _provider, _records, clock = registry(replicas=4)
    service.agents.residents = residents
    _manager, calls = install_launcher(monkeypatch)
    entered, release = asyncio.Event(), asyncio.Event()
    tasks = []
    if boundary == "runless":
        await service.reserve(execution=admitted, work_id=row["id"], request=body)
        original = residents.admit

        async def held_admission(*args, **kwargs):
            entered.set()
            await release.wait()
            return await original(*args, **kwargs)

        monkeypatch.setattr(residents, "admit", held_admission)
    else:
        original = AgentWork._event

        async def held_event(self, *args, **kwargs):
            if args[-1] == "activate":
                entered.set()
                await release.wait()
            return await original(self, *args, **kwargs)

        monkeypatch.setattr(AgentWork, "_event", held_event)

    async def perform():
        return await activate_work(request, instance_id=admitted.instance.id, work_id=row["id"], body=body)

    try:
        tasks.append(asyncio.create_task(perform()))
        await asyncio.wait_for(entered.wait(), 30)
        if boundary == "cancellation":
            reservation = next(iter(residents.reservations.values()))
            tasks[0].cancel()
            await asyncio.sleep(0)
            tasks[0].cancel()
            clock[0] = reservation.expires_at + 1
            residents.attempts_in_flight()
            assert residents.reservations[reservation.id] is reservation and not tasks[0].done()
            release.set()
            with pytest.raises(asyncio.CancelledError):
                await tasks[0]
        else:
            tasks.append(asyncio.create_task(perform()))
            release.set()
            one, two = await asyncio.gather(*tasks)
            assert one["activation_receipt"] == two["activation_receipt"]
            assert one["attempt"]["run_id"] == two["attempt"]["run_id"]
        current = await service.work.get(actor=ALICE, instance_id=admitted.instance.id, work_id=row["id"])
        assert calls and all(attempt == current["attempt"]["id"] for _key, attempt in calls)
        assert current["attempt"]["run_id"] is not None and len(residents.reservations) == 1
    finally:
        release.set()
        await asyncio.gather(*tasks, return_exceptions=True)
        await residents.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("wrong", ["instance", "thread"])
async def test_owner_activation_requires_the_exact_instance_conversation(execution, monkeypatch, wrong):
    from app.gateway.work_execution import activate_work
    from app.telegram_bots.authority import internal_request

    _service, admitted, row, _sf = execution
    app = app_for(execution, monkeypatch)
    _manager, calls = install_launcher(monkeypatch)
    body = activation(admitted, row)
    if wrong == "thread":
        body = body.model_copy(update={"thread_id": operation()})
    with pytest.raises(AgentDenied):
        await activate_work(internal_request(app, "alice"), instance_id=operation() if wrong == "instance" else admitted.instance.id, work_id=row["id"], body=body)
    assert not calls


@pytest.mark.parametrize("owner", [None, "", " ", " alice", "alice "])
def test_internal_request_requires_an_exact_owner(owner):
    from app.telegram_bots.authority import internal_request

    with pytest.raises(ValueError):
        internal_request(SimpleNamespace(), owner)
