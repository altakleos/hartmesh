"""Exact original-attempt reads and cancellation dispatch on both SQL stores."""

from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from _storage_spaces_test_support import ALICE, BOB, operation
from sqlalchemy import func, select
from test_agent_instances import instances as instances
from test_agent_work_records import command, delegate
from test_agent_work_records import work as work
from test_work_execution import _running_stop_attempt, activation, run_record
from test_work_execution import execution as execution

from app.gateway.authz import AuthContext
from deerflow.agent_instances.contract import AgentDenied
from deerflow.persistence.agent_instances.model import AgentInstanceGrantRow
from deerflow.persistence.agent_instances.work import AgentWorkAttemptRow, AgentWorkEventRow, AgentWorkRow
from deerflow.runtime.runs.manager import CancelOutcome


def request_for(record=None, *, actor=ALICE, allowed=True):
    manager = SimpleNamespace(get=AsyncMock(return_value=record), cancel=AsyncMock(return_value=CancelOutcome.requested))
    user = SimpleNamespace(id=actor.subject_id, system_role="user")
    request = SimpleNamespace(app=SimpleNamespace(state=SimpleNamespace(run_manager=manager)), state=SimpleNamespace(user=user, auth_source="session", auth=AuthContext(user, ["runs:cancel"] if allowed else [])))
    return request, manager


async def stopped(execution):
    service, admitted, row, sf, scope, record = await _running_stop_attempt(execution)
    receipt = await service.work.command(actor=ALICE, instance_id=admitted.instance.id, work_id=row["id"], request=command(row, "stop", attempt_id=scope.attempt_id), can_stop=True)
    return service, admitted, receipt, sf, scope, record


@pytest.mark.asyncio
@pytest.mark.parametrize("actor,permissions,allowed", [(ALICE, 3, True), (ALICE, 2, False), (ALICE, 1, False), (BOB, 3, False), (BOB, 6, True), (BOB, 7, True), (BOB, 2, False)])
async def test_original_attempt_read_requires_current_stop_authority(execution, actor, permissions, allowed):
    service, admitted, row, sf, scope, record = await stopped(execution)
    async with sf() as session, session.begin():
        (await session.get(AgentInstanceGrantRow, (admitted.instance.id, actor.subject_id))).permissions = permissions
        before = await session.scalar(select(func.count()).select_from(AgentWorkEventRow))
    options = dict(actor=actor, instance_id=admitted.instance.id, work_id=row["id"], attempt_id=scope.attempt_id)
    if allowed:
        result = await service.work.attempt_for_stop(**options)
        assert (result["id"], result["thread_id"], result["run_id"], result["status"], result["requester_id"]) == (scope.attempt_id, admitted.thread_id, record.run_id, "stopping", "alice")
        assert result["settled_at"] is None
    else:
        with pytest.raises(AgentDenied):
            await service.work.attempt_for_stop(**options)
    async with sf() as session:
        assert await session.scalar(select(func.count()).select_from(AgentWorkEventRow)) == before
        assert (await session.get(AgentWorkAttemptRow, scope.attempt_id)).status == "stopping"


@pytest.mark.asyncio
async def test_exact_read_cannot_substitute_another_work_or_missing_attempt(execution):
    service, admitted, row, _sf, scope, _record = await stopped(execution)
    other = await delegate(service.work, admitted.instance)
    assert await service.work.attempt_for_stop(actor=ALICE, instance_id=admitted.instance.id, work_id=other["id"], attempt_id=scope.attempt_id) is None
    assert await service.work.attempt_for_stop(actor=ALICE, instance_id=admitted.instance.id, work_id=row["id"], attempt_id=operation()) is None
    with pytest.raises(AgentDenied):
        await service.work.attempt_for_stop(actor=ALICE, instance_id=operation(), work_id=row["id"], attempt_id=scope.attempt_id)


@pytest.mark.asyncio
async def test_stop_waits_for_same_attempt_late_run_binding(execution):
    from app.gateway.work_execution import dispatch_work_stop

    service, admitted, row, _sf = execution
    scope = await service.reserve(execution=admitted, work_id=row["id"], request=activation(admitted, row))
    admitted = replace(admitted, work=scope)
    current = await service.work.get(actor=ALICE, instance_id=admitted.instance.id, work_id=row["id"])
    await service.work.command(actor=ALICE, instance_id=admitted.instance.id, work_id=row["id"], request=command(current, "stop", attempt_id=scope.attempt_id), can_stop=True)
    request, manager = request_for()
    options = dict(actor=ALICE, instance_id=admitted.instance.id, work_id=row["id"], expected_attempt_id=scope.attempt_id, expected_thread_id=admitted.thread_id)
    pending = await dispatch_work_stop(request, service.work, **options)
    assert pending.state == "pending" and pending.attempt_id == scope.attempt_id and pending.run_id is None
    manager.get.assert_not_awaited()
    record = run_record(admitted, scope, status="running")
    # The host records a late run even when Stop prevents worker admission.
    await scope.observe_run(admitted, record)
    manager.get.return_value = record
    requested = await dispatch_work_stop(request, service.work, **options)
    assert requested.state == "requested" and requested.run_id == record.run_id
    manager.cancel.assert_awaited_once_with(record.run_id)
    assert (await service.work.attempt_for_stop(actor=ALICE, instance_id=admitted.instance.id, work_id=row["id"], attempt_id=scope.attempt_id))["status"] == "stopping"
    # Cancellation acknowledgement is separate from actual Work settlement.
    record.status = "interrupted"
    await scope.finish(admitted, record, settled=True)
    settled_result = await dispatch_work_stop(request, service.work, **options, expected_run_id=record.run_id)
    assert settled_result.state == "settled"
    manager.cancel.assert_awaited_once_with(record.run_id)


@pytest.mark.asyncio
async def test_original_settlement_does_not_cancel_a_successor(execution):
    from app.gateway.work_execution import dispatch_work_stop

    service, admitted, row, _sf, scope, record = await stopped(execution)
    record.status = "interrupted"
    await scope.finish(admitted, record, settled=True)
    fresh = await admitted.authority.execution(actor=ALICE, thread_id=admitted.thread_id)
    current = await service.work.get(actor=ALICE, instance_id=admitted.instance.id, work_id=row["id"])
    successor = await service.reserve(execution=fresh, work_id=row["id"], request=activation(fresh, current))
    fresh = replace(fresh, work=successor)
    later_run = run_record(fresh, successor, status="running")
    await successor.bind(fresh, later_run, fresh.thread_incarnation)
    request, manager = request_for(later_run)
    result = await dispatch_work_stop(request, service.work, actor=ALICE, instance_id=admitted.instance.id, work_id=row["id"], expected_attempt_id=scope.attempt_id, expected_thread_id=admitted.thread_id, expected_run_id=record.run_id)
    assert result.state == "settled" and result.attempt_id == scope.attempt_id and result.run_id == record.run_id
    manager.get.assert_not_awaited()
    manager.cancel.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("mismatch", ["expected_thread", "expected_run", "record_thread", "record_run", "record_attempt"])
async def test_cancellation_rejects_crossed_scope_and_host_metadata(execution, mismatch):
    from app.gateway.work_execution import dispatch_work_stop

    service, admitted, row, _sf, scope, record = await stopped(execution)
    request, manager = request_for(record)
    options = dict(actor=ALICE, instance_id=admitted.instance.id, work_id=row["id"], expected_attempt_id=scope.attempt_id, expected_thread_id=admitted.thread_id, expected_run_id=record.run_id)
    if mismatch == "expected_thread":
        options["expected_thread_id"] = operation()
    elif mismatch == "expected_run":
        options["expected_run_id"] = operation()
    elif mismatch == "record_thread":
        record.thread_id = operation()
    elif mismatch == "record_run":
        record.run_id = operation()
    else:
        record.metadata = {"agent_work_attempt_id": operation()}
    with pytest.raises(AgentDenied):
        await dispatch_work_stop(request, service.work, **options)
    manager.cancel.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("outcome", list(CancelOutcome))
async def test_cancel_outcomes_do_not_invent_settlement(execution, outcome):
    from app.gateway.work_execution import dispatch_work_stop

    service, admitted, row, _sf, scope, record = await stopped(execution)
    request, manager = request_for(record)
    manager.cancel.return_value = outcome
    result = await dispatch_work_stop(request, service.work, actor=ALICE, instance_id=admitted.instance.id, work_id=row["id"], expected_attempt_id=scope.attempt_id, expected_thread_id=admitted.thread_id, expected_run_id=record.run_id)
    assert result.state == ("requested" if outcome in {CancelOutcome.cancelled, CancelOutcome.requested, CancelOutcome.taken_over} else "pending")
    assert result.run_id == record.run_id
    assert (await service.work.attempt_for_stop(actor=ALICE, instance_id=admitted.instance.id, work_id=row["id"], attempt_id=scope.attempt_id))["settled_at"] is None


@pytest.mark.asyncio
@pytest.mark.parametrize("missing", ["attempt", "run"])
async def test_missing_evidence_stays_pending(execution, missing):
    from app.gateway.work_execution import dispatch_work_stop

    service, admitted, row, _sf, scope, _record = await stopped(execution)
    request, manager = request_for()
    expected = operation() if missing == "attempt" else scope.attempt_id
    result = await dispatch_work_stop(request, service.work, actor=ALICE, instance_id=admitted.instance.id, work_id=row["id"], expected_attempt_id=expected, expected_thread_id=admitted.thread_id)
    assert result.state == "pending" and result.attempt_id == expected
    manager.cancel.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("changed", ["grant", "ceiling", "run_binding"])
async def test_cancellation_rechecks_authority_after_host_read(execution, changed):
    from app.gateway.work_execution import dispatch_work_stop

    service, admitted, row, sf, scope, record = await stopped(execution)
    request, manager = request_for(record)

    async def read(*_args, **_kwargs):
        if changed == "ceiling":
            request.state.auth.permissions = []
        else:
            async with sf() as session, session.begin():
                if changed == "grant":
                    (await session.get(AgentInstanceGrantRow, (admitted.instance.id, "alice"))).permissions = 2
                else:
                    (await session.get(AgentWorkAttemptRow, scope.attempt_id)).run_id = operation()
        return record

    manager.get.side_effect = read
    with pytest.raises(AgentDenied):
        await dispatch_work_stop(request, service.work, actor=ALICE, instance_id=admitted.instance.id, work_id=row["id"], expected_attempt_id=scope.attempt_id, expected_thread_id=admitted.thread_id, expected_run_id=record.run_id)
    manager.cancel.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("changed", ["host_identity", "ceiling"])
async def test_local_fences_are_rechecked_after_final_authority_read(execution, monkeypatch, changed):
    from app.gateway.work_execution import dispatch_work_stop

    service, admitted, row, _sf, scope, record = await stopped(execution)
    request, manager = request_for(record)
    original, reads = service.work.attempt_for_stop, []

    async def read(**kwargs):
        value = await original(**kwargs)
        reads.append(True)
        if len(reads) == 2:
            if changed == "ceiling":
                request.state.auth.permissions = []
            else:
                record.run_id = operation()
        return value

    monkeypatch.setattr(service.work, "attempt_for_stop", read)
    with pytest.raises(AgentDenied):
        await dispatch_work_stop(request, service.work, actor=ALICE, instance_id=admitted.instance.id, work_id=row["id"], expected_attempt_id=scope.attempt_id, expected_thread_id=admitted.thread_id)
    manager.cancel.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("surface", ["http", "plugin"])
@pytest.mark.parametrize("replay", [False, True])
async def test_factual_input_does_not_dispatch_another_persons_stop(execution, monkeypatch, surface, replay):
    import httpx
    from test_agent_instances import request_app

    from app.gateway.routers import agent_instances as routes
    from app.gateway.routers.agent_work import router

    service, admitted, row, sf, scope, record = await _running_stop_attempt(execution)
    record.status = "success"
    await scope.finish(admitted, record, settled=True)
    blocker_id = operation()
    async with sf() as session, session.begin():
        work_row = await session.get(AgentWorkRow, row["id"])
        work_row.status = "blocked"
        work_row.blocker = {"id": blocker_id, "revision": 1, "assignment_revision": work_row.assignment_revision, "kind": "decision", "question": "Which guide?", "sources": []}
        (await session.get(AgentInstanceGrantRow, (admitted.instance.id, "bob"))).permissions = 3
    current = await service.work.get(actor=BOB, instance_id=admitted.instance.id, work_id=row["id"])
    body = command(current, "input", blocker_id=blocker_id, blocker_revision=1, note="The guide was updated.")
    if replay:
        # Known input receipts replay even after a newer attempt becomes live.
        current = await service.work.command(actor=BOB, instance_id=admitted.instance.id, work_id=row["id"], request=body)
        current = await service.work.command(actor=ALICE, instance_id=admitted.instance.id, work_id=row["id"], request=command(current, "decide", blocker_id=blocker_id, blocker_revision=1, note="Use that guide."))
        fresh = await admitted.authority.execution(actor=ALICE, thread_id=admitted.thread_id)
        successor = await service.reserve(execution=fresh, work_id=row["id"], request=activation(fresh, current))
        fresh = replace(fresh, work=successor)
        later_run = run_record(fresh, successor, status="running")
        await successor.bind(fresh, later_run, fresh.thread_incarnation)
        current = await service.work.get(actor=ALICE, instance_id=admitted.instance.id, work_id=row["id"])
        await service.work.command(actor=ALICE, instance_id=admitted.instance.id, work_id=row["id"], request=command(current, "stop", attempt_id=successor.attempt_id), can_stop=True)
    app = request_app(service.agents, actor=BOB, permissions=("agents:read", "agents:write", "runs:cancel"))
    _request, manager = request_for(record, actor=BOB)
    app.state.run_manager = manager
    app.include_router(router)
    payload = body.model_dump(mode="json", exclude_unset=True)
    path = f"/api/agent-instances/{admitted.instance.id}/work/{row['id']}/commands"
    if surface == "plugin":
        from deerflow_extension_api.auth import EXTENSION_PRINCIPAL_RESOLVER_KEY, ExtensionPrincipal
        from deerflow_extension_api.plugins import BackendAction, PluginContribution

        from app.gateway.routers.plugins import router as plugin_router
        from deerflow.extensions.registry import ExtensionRegistry

        async def input_command(payload, context):
            return await context.human_input.call("work_command", {"instance_id": admitted.instance.id, "work_id": row["id"], "body": payload["command"]})

        plugin = PluginContribution(namespace="example.input", title="Input", enabled=True, api_version=5, human_input_api_version=1, backend=(BackendAction("input", input_command),))
        registry = ExtensionRegistry()
        with registry.attributed_to("test:input"):
            registry.plugin(plugin)
        app.state.extensions = registry.build()
        setattr(app.state, EXTENSION_PRINCIPAL_RESOLVER_KEY, lambda _request: ExtensionPrincipal("bob"))
        app.include_router(plugin_router)
        payload = {"command": payload}
        path = "/api/plugins/example.input/actions/input"
    monkeypatch.setattr(routes, "get_agents_api_config", lambda: SimpleNamespace(enabled=True))
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as client:
        response = await client.post(path, json=payload)
    assert response.status_code == 200, response.text
    manager.get.assert_not_awaited()
    manager.cancel.assert_not_awaited()
    async with sf() as session:
        assert await session.scalar(select(func.count()).select_from(AgentWorkEventRow).where(AgentWorkEventRow.operation_id == body.operation_id)) == 1


@pytest.mark.asyncio
async def test_final_guard_receives_immutable_original_attempt_and_can_refuse(execution):
    from app.gateway.work_execution import dispatch_work_stop

    service, admitted, row, _sf, scope, record = await stopped(execution)
    request, manager = request_for(record)
    seen = []

    async def guard(original):
        seen.append((original["id"], original["thread_id"], original["run_id"]))
        with pytest.raises(TypeError):
            original["run_id"] = operation()
        return False

    with pytest.raises(AgentDenied):
        await dispatch_work_stop(
            request, service.work, actor=ALICE, instance_id=admitted.instance.id, work_id=row["id"], expected_attempt_id=scope.attempt_id, expected_thread_id=admitted.thread_id, expected_run_id=record.run_id, before_dispatch=guard
        )
    assert seen == [(scope.attempt_id, admitted.thread_id, record.run_id)]
    manager.cancel.assert_not_awaited()
