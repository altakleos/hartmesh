"""Current grants and execution precede storage and durable run effects."""

from dataclasses import replace
from datetime import UTC, datetime
from types import SimpleNamespace
from unittest.mock import AsyncMock

import httpx
import pytest
from _storage_spaces_test_support import ALICE
from sqlalchemy import select
from test_agent_instances import instances as instances
from test_agent_work_records import work as work
from test_resident_registry import registry as registry
from test_work_execution import activation
from test_work_execution import execution as execution

from deerflow.agent_instances.contract import AgentDenied, ExecutionUnavailable
from deerflow.persistence.agent_instances.model import AgentInstanceGrantRow
from deerflow.persistence.agent_instances.work import AgentWorkAttemptRow


@pytest.fixture(autouse=True)
def enabled_api(monkeypatch, request):
    from deerflow.config import agents_api_config

    monkeypatch.setattr(agents_api_config, "_agents_api_config", agents_api_config.AgentsApiConfig(enabled=True, ai_assistants_per_user=10, ai_employees=10))
    if "single_connection" in request.node.name:
        import _storage_spaces_test_support as support

        factory = support.create_async_engine
        monkeypatch.setattr(support, "create_async_engine", lambda *args, **kwargs: factory(*args, **kwargs, pool_size=1, max_overflow=0, pool_timeout=0.5))


@pytest.mark.asyncio
async def test_actual_single_connection_work_admission_borrows_human_and_context_checks(execution):
    import asyncio

    from test_agent_kind_admission import sql_human

    service, admitted, row, sf = execution
    await sql_human(service.agents, service.agents.files, sf)
    async with asyncio.timeout(5):
        fresh = await admitted.authority.execution(actor=admitted.requester, thread_id=admitted.thread_id, require_ready=True)
        body = activation(fresh, row)
        scope = await service.reserve(execution=fresh, work_id=row["id"], request=body)
        assert (await service.reserve(execution=fresh, work_id=row["id"], request=body)).attempt_id == scope.attempt_id


@pytest.mark.asyncio
async def test_actual_single_connection_work_reads_release_sql_before_execution_probe(execution):
    service, admitted, row, sf = execution

    async def probe(instance_id):
        from deerflow.persistence.agent_instances.model import AgentInstanceRow

        async with sf() as session:
            assert await session.get(AgentInstanceRow, instance_id) is not None
        return SimpleNamespace(execution="unavailable", reason="operator-hold")

    service.agents.execution_probe = probe
    assert (await service.work.get(actor=admitted.requester, instance_id=admitted.instance.id, work_id=row["id"]))["execution_reason"] == "operator-hold"
    records = await service.work.list(actor=admitted.requester, instance_id=admitted.instance.id)
    assert len(records) == 1 and records[0]["execution_reason"] == "operator-hold"


@pytest.mark.asyncio
@pytest.mark.parametrize("revoked", [False, True])
async def test_late_reply_admission_rechecks_grants_then_execution_before_storage(execution, monkeypatch, revoked):
    service, admitted, _row, sf = execution
    if revoked:
        async with sf() as session, session.begin():
            await session.delete(await session.get(AgentInstanceGrantRow, (admitted.instance.id, "alice")))
    probe = AsyncMock(return_value=SimpleNamespace(execution="unavailable", reason="operator-hold"))
    service.agents.execution_probe = probe
    storage = AsyncMock(side_effect=AssertionError("Unavailable or hidden runs must not reach storage"))
    monkeypatch.setattr("deerflow.agent_instances.runtime.admit_storage", storage)
    with pytest.raises(AgentDenied if revoked else ExecutionUnavailable):
        await admitted.authority.admit(admitted)
    storage.assert_not_awaited()
    if revoked:
        probe.assert_not_awaited()
    else:
        probe.assert_awaited_once_with(admitted.instance.id)


@pytest.mark.asyncio
async def test_work_attempt_late_admission_keeps_activation_execution_decision(execution):
    service, admitted, row, _sf = execution
    scope = await service.reserve(execution=admitted, work_id=row["id"], request=activation(admitted, row))
    service.agents.execution_probe = AsyncMock(side_effect=AssertionError("Work activation owns readiness"))
    await admitted.authority.admit(replace(admitted, work=scope))
    service.agents.execution_probe.assert_not_awaited()


def activation_app(service, admitted):
    from test_agent_instances import request_app

    from app.gateway.refusals import refusal_handler
    from app.gateway.routers import agent_work
    from deerflow.agent_instances.contract import Refusal

    app = request_app(service.agents, actor=ALICE, permissions=("agents:read", "agents:write", "runs:create"))
    app.include_router(agent_work.router)
    app.add_exception_handler(Refusal, refusal_handler)
    app.state.agent_conversations = admitted.authority
    return app


@pytest.mark.asyncio
@pytest.mark.parametrize("revoked", [False, True])
async def test_http_new_work_activation_refuses_before_storage_attempt_and_launch(execution, monkeypatch, revoked):
    service, admitted, row, sf = execution
    if revoked:
        async with sf() as session, session.begin():
            await session.delete(await session.get(AgentInstanceGrantRow, (admitted.instance.id, "alice")))
    service.agents.execution_probe = AsyncMock(return_value=SimpleNamespace(execution="unavailable", reason="operator-hold"))
    storage = AsyncMock(side_effect=AssertionError("Must refuse before storage"))
    monkeypatch.setattr("deerflow.agent_instances.runtime.admit_storage", storage)
    launch = AsyncMock(side_effect=AssertionError("Must not launch"))
    monkeypatch.setattr("app.gateway.services.start_run", launch)
    body = activation(admitted, row)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=activation_app(service, admitted)), base_url="http://test") as client:
        response = await client.post(f"/api/agent-instances/{admitted.instance.id}/work/{row['id']}/activate", json=body.model_dump(mode="json"))
    assert response.status_code == (404 if revoked else 409), response.text
    if not revoked:
        assert response.json()["execution_reason"] == "operator-hold"
    storage.assert_not_awaited()
    launch.assert_not_awaited()
    async with sf() as session:
        from sqlalchemy import select

        assert (await session.scalars(select(AgentWorkAttemptRow))).all() == []


@pytest.mark.asyncio
async def test_http_retired_exact_activation_replay_is_admitted_under_hold(execution, monkeypatch):
    service, admitted, row, sf = execution
    body = activation(admitted, row)
    scope = await service.reserve(execution=admitted, work_id=row["id"], request=body)
    async with sf() as session, session.begin():
        (await session.get(AgentWorkAttemptRow, scope.attempt_id)).status = "failed"
    service.agents.execution_probe = AsyncMock(return_value=SimpleNamespace(execution="unavailable", reason="operator-hold"))
    launch = AsyncMock(side_effect=AssertionError("Retired replay must not launch"))
    monkeypatch.setattr("app.gateway.services.start_run", launch)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=activation_app(service, admitted)), base_url="http://test") as client:
        response = await client.post(f"/api/agent-instances/{admitted.instance.id}/work/{row['id']}/activate", json=body.model_dump(mode="json"))
    assert response.status_code == 200, response.text
    assert response.json()["activation_receipt"]["attempt_id"] == scope.attempt_id
    launch.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("status,ready,workspace", [("starting", False, "starting"), ("running", False, "starting"), ("running", True, "ready"), ("stopping", False, None), ("uncertain", True, "ready"), ("failed", True, None)])
async def test_work_attempt_projects_workspace_and_containment_receipts(execution, status, ready, workspace):
    service, admitted, row, sf = execution
    scope = await service.reserve(execution=admitted, work_id=row["id"], request=activation(admitted, row))
    now = datetime.now(UTC)
    async with sf() as session, session.begin():
        attempt = await session.get(AgentWorkAttemptRow, scope.attempt_id)
        attempt.status = status
        attempt.workspace_ready_at = now if ready else None
        if status == "failed":
            attempt.settled_at = now
            attempt.contained_reason = "run-orphaned"
    current = await service.work.get(actor=admitted.requester, instance_id=admitted.instance.id, work_id=row["id"])
    assert current["attempt"]["workspace"] == workspace
    contained = current["attempt"]["contained"]
    if status == "failed":
        assert contained["reason"] == "run-orphaned" and contained["at"] == current["attempt"]["settled_at"]
    else:
        assert contained is None


@pytest.mark.asyncio
async def test_host_work_history_projects_its_containment_evidence(execution):
    from deerflow.persistence.agent_instances.work import AgentWorkEventRow

    service, admitted, row, sf = execution
    async with sf() as session, session.begin():
        event = (await session.scalars(select(AgentWorkEventRow).where(AgentWorkEventRow.work_id == row["id"]))).one()
        event.actor_kind = "host"
        event.actor_id = "qualified-host"
        event.request = {"reason": "run-orphaned", "attachment_id": "a" * 32, "attempt_id": "b" * 32, "run_id": "c" * 32, "status": "failed"}
    history = await service.work.history(actor=admitted.requester, instance_id=admitted.instance.id, work_id=row["id"])
    assert history[0]["report"] == {"reason": "run-orphaned", "attachment_id": "a" * 32, "attempt_id": "b" * 32, "run_id": "c" * 32, "status": "failed"}


@pytest.mark.asyncio
async def test_work_capacity_counts_hidden_attempts_but_projects_only_inspectable_ones(execution):
    service, admitted, row, _sf = execution
    service.agents.residents = SimpleNamespace(
        work_max=lambda: 3,
        attempts_in_flight=lambda: [
            {"attempt_id": None, "instance_id": "hidden", "work_id": "hidden", "requester_id": "other", "started_at": None},
            {"attempt_id": None, "instance_id": admitted.instance.id, "work_id": row["id"], "requester_id": "alice", "started_at": None},
        ],
    )
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=activation_app(service, admitted)), base_url="http://test") as client:
        response = await client.get(f"/api/agent-instances/{admitted.instance.id}/work")
    assert response.status_code == 200, response.text
    capacity = response.json()["work_capacity"]
    assert capacity["max"] == 3 and capacity["in_use"] == 2
    assert capacity["running"] == [{"instance_id": admitted.instance.id, "instance_name": admitted.instance.name, "work_id": row["id"], "objective": row["objective"], "started_at": None, "requester_id": "alice"}]


@pytest.mark.asyncio
async def test_work_capacity_never_substitutes_historical_or_falsey_task_metadata(execution):
    service, admitted, row, sf = execution
    old = await service.reserve(execution=admitted, work_id=row["id"], request=activation(admitted, row))
    async with sf() as session, session.begin():
        attempt = await session.get(AgentWorkAttemptRow, old.attempt_id)
        attempt.status = "failed"
        attempt.requester_id = "historical-requester"
    current = await service.work.get(actor=admitted.requester, instance_id=admitted.instance.id, work_id=row["id"])
    recent = await service.reserve(execution=admitted, work_id=row["id"], request=activation(admitted, current))
    service.agents.residents = SimpleNamespace(
        work_max=lambda: 3,
        attempts_in_flight=lambda: [
            {"attempt_id": None, "run_id": "live-without-attempt", "instance_id": admitted.instance.id, "work_id": row["id"], "requester_id": None, "started_at": None},
            {"attempt_id": recent.attempt_id, "run_id": "live-with-exact-attempt", "instance_id": admitted.instance.id, "work_id": row["id"], "requester_id": "", "started_at": ""},
        ],
    )
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=activation_app(service, admitted)), base_url="http://test") as client:
        response = await client.get(f"/api/agent-instances/{admitted.instance.id}/work")
    assert response.status_code == 200, response.text
    running = response.json()["work_capacity"]["running"]
    assert len(running) == 2
    assert [(item["requester_id"], item["started_at"]) for item in running] == [(None, None), ("", "")]


@pytest.mark.asyncio
async def test_no_registry_capacity_uses_request_configuration_and_config_errors_stay_503(execution, monkeypatch):
    from fastapi import HTTPException

    from deerflow.config.app_config import AppConfig
    from deerflow.config.sandbox_config import SandboxConfig

    service, admitted, _row, _sf = execution
    config = AppConfig(models=[], sandbox=SandboxConfig(use="deerflow.sandbox.local:LocalSandboxProvider", replicas=5))
    monkeypatch.setattr("app.gateway.deps.get_config", lambda: config)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=activation_app(service, admitted)), base_url="http://test") as client:
        response = await client.get(f"/api/agent-instances/{admitted.instance.id}/work")
        assert response.status_code == 200, response.text
        assert response.json()["work_capacity"] == {"max": 4, "in_use": 0, "running": []}

        def unavailable():
            raise HTTPException(503, "Configuration not available")

        monkeypatch.setattr("app.gateway.deps.get_config", unavailable)
        response = await client.get(f"/api/agent-instances/{admitted.instance.id}/work")
        assert response.status_code == 503 and response.json()["detail"] == "Configuration not available"


@pytest.mark.asyncio
@pytest.mark.parametrize("late", [False, True])
async def test_activation_adapter_refuses_initial_or_late_hold_before_attempt_write(execution, registry, monkeypatch, late):
    from test_gateway_work_admission import request_for

    from app.gateway.work_execution import activate_work_run

    service, admitted, row, _sf = execution
    residents, _provider, _records, _clock = registry()
    service.agents.residents = residents
    held = [not late]
    service.agents.execution_probe = AsyncMock(side_effect=lambda _id: SimpleNamespace(execution="unavailable" if held[0] else "ready", reason="operator-hold" if held[0] else None))
    reserve = residents.admit

    async def changed_after_capacity(*args, **kwargs):
        reservation = await reserve(*args, **kwargs)
        held[0] = True
        return reservation

    monkeypatch.setattr(residents, "admit", changed_after_capacity)
    launch = AsyncMock(side_effect=AssertionError("Unavailable activation must not launch"))
    monkeypatch.setattr("app.gateway.services.start_run", launch)
    with pytest.raises(ExecutionUnavailable):
        await activate_work_run(request_for(service), service, admitted, row["id"], activation(admitted, row))
    current = await service.work.get(actor=admitted.requester, instance_id=admitted.instance.id, work_id=row["id"])
    assert current["attempt"] is None and current["revision"] == row["revision"]
    assert not residents.reservations
    launch.assert_not_awaited()
