"""Existing bootstrap and maintenance owners resume after an expired peer lease."""

import asyncio
import time
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from test_gateway_resident_reconciliation import build as build_runtime
from test_gateway_resident_reconciliation import execution as execution
from test_gateway_resident_reconciliation import instances as instances
from test_gateway_resident_reconciliation import work as work
from test_resident_bootstrap import build as build
from test_resident_bootstrap import fixture as fixture

from app.gateway import storage_spaces as ss
from app.gateway.work_execution import contain_orphaned_work
from deerflow.config.run_ownership_config import RunOwnershipConfig
from deerflow.persistence.agent_instances.work import AgentWorkAttemptRow
from deerflow.persistence.run.model import RunChangeClockRow, RunRow
from deerflow.persistence.run.sql import RunRepository
from deerflow.runtime.runs.manager import RunManager


def app_for(coordinator, provider, *, retry):
    return SimpleNamespace(state=SimpleNamespace(agent_execution=ss.ExecutionStatus(), resident_runtime=SimpleNamespace(coordinator=coordinator, provider=provider, accounting_retry=retry, workspace_qualified=False)))


@pytest.mark.asyncio
@pytest.mark.parametrize("retry", [False, True])
async def test_consumed_failed_initializer_starts_one_fresh_owned_pass(build, retry):
    coordinator, _registry, provider, _admission, _clock, _validate = build()
    coordinator.reconcile.side_effect = RuntimeError("offline earlier peer lease refusal")
    assert await coordinator.poll(timeout=0) is None
    old = coordinator.task
    await asyncio.gather(old, return_exceptions=True)
    coordinator.reconcile.side_effect = None
    app = app_for(coordinator, provider, retry=retry)
    try:
        assert await ss._resident_step(app, timeout=0) is False
        assert coordinator.task is not None and coordinator.task is not old
        assert coordinator._attempts == 2
        assert await coordinator.poll(timeout=2) is not None
        assert coordinator.reconcile.await_count == 2
    finally:
        await coordinator.close()


@pytest.mark.asyncio
async def test_retry_never_replaces_an_unfinished_initializer(build):
    coordinator, _registry, provider, _admission, _clock, _validate = build()
    entered, release = asyncio.Event(), asyncio.Event()

    async def held():
        entered.set()
        await release.wait()
        return True

    coordinator.reconcile.side_effect = held
    assert await coordinator.poll(timeout=0) is None
    await asyncio.wait_for(entered.wait(), 2)
    owned = coordinator.task
    try:
        assert await ss._resident_step(app_for(coordinator, provider, retry=True), timeout=0) is False
        assert coordinator.task is owned and not owned.done() and coordinator._attempts == 1
    finally:
        release.set()
        await coordinator.close()


@pytest.mark.asyncio
async def test_recovery_wake_runs_the_existing_maintenance_owner(monkeypatch):
    stop, wake, first, second = (asyncio.Event() for _ in range(4))
    calls = []

    async def inspect(_app):
        calls.append(asyncio.current_task())
        (first if len(calls) == 1 else second).set()

    monkeypatch.setattr(ss, "storage_recheck", inspect)
    app = SimpleNamespace(state=SimpleNamespace(storage_recheck_stop=stop, storage_recheck_wake=wake))
    loop = asyncio.create_task(ss._recheck_loop(app))
    try:
        await asyncio.wait_for(first.wait(), 2)
        wake.set()
        await asyncio.wait_for(second.wait(), 1)
        assert calls == [loop, loop]
        stop.set()
        await asyncio.wait_for(loop, 1)
    finally:
        stop.set()
        loop.cancel()
        await asyncio.gather(loop, return_exceptions=True)


@pytest.mark.asyncio
async def test_delayed_ready_reports_containment_once_after_actual_readiness(monkeypatch, caplog):
    runtime = SimpleNamespace(
        coordinator=SimpleNamespace(result=object()), reconcile=AsyncMock(return_value=True), foreign_run_blocks=frozenset(), accounting_retry=False, registry=SimpleNamespace(reap=AsyncMock()), settle_pending=AsyncMock()
    )
    app = SimpleNamespace(state=SimpleNamespace(storage_spaces=SimpleNamespace(settle_interrupted=AsyncMock(return_value=0)), agent_execution=ss.ExecutionStatus(attachments_fenced=1, attempts_settled=1), resident_runtime=runtime))
    monkeypatch.setattr(ss, "_resident_step", AsyncMock(return_value=True))
    monkeypatch.setattr(ss, "storage_state", AsyncMock(return_value=ss.StorageState("ready")))
    execution = AsyncMock(return_value=SimpleNamespace(execution="unavailable"))
    monkeypatch.setattr(ss, "execution_state", execution)
    monkeypatch.setattr(ss, "finish_releases", AsyncMock(return_value=0))
    monkeypatch.setattr(ss, "assign_spaces", AsyncMock())
    caplog.set_level("INFO", logger=ss.__name__)
    await ss._maintenance_step(app, timeout=1)
    assert "agents: ready (" not in caplog.text
    execution.return_value = SimpleNamespace(execution="ready")
    await ss._maintenance_step(app, timeout=1)
    await ss._maintenance_step(app, timeout=1)
    assert caplog.text.count("agents: ready (1 attachment fenced at start; 1 work attempt settled)") == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("held_maintenance", [False, True])
async def test_successful_owned_retry_wakes_maintenance_to_consume_and_report_ready(build, monkeypatch, caplog, held_maintenance):
    coordinator, registry, provider, _admission, _clock, _validate = build()
    coordinator.reconcile.side_effect = RuntimeError("earlier census invalidated during containment")
    assert await coordinator.poll(timeout=0) is None
    await asyncio.gather(coordinator.task, return_exceptions=True)
    coordinator.reconcile.side_effect = None
    entered, release, ready, maintenance_entered, maintenance_release, second_tick = (asyncio.Event() for _ in range(6))

    async def fresh_census():
        entered.set()
        await release.wait()
        return True

    coordinator.reconcile.side_effect = fresh_census
    app = app_for(coordinator, provider, retry=False)
    app.state.storage_recheck_stop = asyncio.Event()
    app.state.storage_recheck_wake = asyncio.Event()
    app.state.storage_spaces = SimpleNamespace(settle_interrupted=AsyncMock(return_value=0))
    app.state.resident_runtime.registry = registry
    app.state.resident_runtime.reconcile = AsyncMock(return_value=True)
    app.state.resident_runtime.foreign_run_blocks = frozenset()
    app.state.resident_runtime.settle_pending = AsyncMock()
    registry.reap = AsyncMock()
    monkeypatch.setattr(ss, "storage_state", AsyncMock(return_value=ss.StorageState("ready")))
    monkeypatch.setattr(ss, "finish_releases", AsyncMock(return_value=0))
    if held_maintenance:

        async def hold_releases(_app):
            maintenance_entered.set()
            await maintenance_release.wait()
            return 0

        monkeypatch.setattr(ss, "finish_releases", hold_releases)
    monkeypatch.setattr(ss, "assign_spaces", AsyncMock())
    recheck = ss.storage_recheck
    ticks = []

    async def inspect(_app):
        await recheck(_app)
        ticks.append(asyncio.current_task())
        if len(ticks) == 2:
            second_tick.set()

    monkeypatch.setattr(ss, "storage_recheck", inspect)

    async def state(_app):
        return SimpleNamespace(execution="ready" if coordinator.result is not None else "unavailable")

    monkeypatch.setattr(ss, "execution_state", state)
    actual_ready = ss._agents_ready_line

    def report(_app):
        actual_ready(_app)
        ready.set()

    monkeypatch.setattr(ss, "_agents_ready_line", report)
    caplog.set_level("INFO", logger=ss.__name__)
    loop = asyncio.create_task(ss._recheck_loop(app))
    try:
        await asyncio.wait_for(entered.wait(), 2)
        owned = coordinator.task
        assert owned is not None and not owned.done() and coordinator._attempts == 2
        if held_maintenance:
            await asyncio.wait_for(maintenance_entered.wait(), 2)
        release.set()
        if held_maintenance:
            await asyncio.wait_for(asyncio.shield(owned), 2)
            app.state.storage_recheck_wake.set()
            await asyncio.wait_for(second_tick.wait(), 2)
            assert coordinator.result is None and not app.state.agent_execution.maintenance_task.done()
            maintenance_release.set()
        await asyncio.wait_for(ready.wait(), 2)
        assert coordinator.result is not None and coordinator.task is None and coordinator._attempts == 2
        assert caplog.text.count("agents: ready (") == 1
        assert all(task is loop for task in ticks)
    finally:
        release.set()
        maintenance_release.set()
        app.state.storage_recheck_stop.set()
        loop.cancel()
        await asyncio.gather(loop, return_exceptions=True)
        task = app.state.agent_execution.maintenance_task
        if task is not None:
            await asyncio.gather(task, return_exceptions=True)
        await coordinator.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("expired", [False, True])
async def test_real_sql_lease_takeover_wakes_maintenance_and_settles_only_after_containment(execution, fixture, monkeypatch, expired):
    from test_work_execution import activation, run_record

    service, admitted, work, sf = execution
    module, app, host, _attachments, adapter, claim = await build_runtime(execution, fixture, monkeypatch)
    async with sf.kw["bind"].begin() as connection:
        for table in (RunRow.__table__, RunChangeClockRow.__table__):
            await connection.run_sync(lambda connection, table=table: table.create(connection, checkfirst=True))
    repository = RunRepository(sf)
    scope = await service.reserve(execution=admitted, work_id=work["id"], request=activation(admitted, work))
    record = run_record(admitted, scope, status="running")
    record.metadata["agent_instance_id"] = admitted.instance.id
    await scope.bind(admitted, record, admitted.thread_incarnation)
    await scope.workspace_ready(admitted, claim.id)
    await repository.put(
        record.run_id,
        thread_id=record.thread_id,
        user_id=record.user_id,
        status="running",
        metadata=record.metadata,
        owner_worker_id="old-gateway",
        lease_expires_at=(datetime.now(UTC) + timedelta(minutes=5)).isoformat(),
        created_at=(datetime.now(UTC) - timedelta(minutes=5)).isoformat(),
    )
    app.state.storage_recheck_wake = asyncio.Event()
    app.state.run_store = repository
    manager = RunManager(
        store=repository,
        worker_id="restarted-gateway",
        run_ownership_config=RunOwnershipConfig(heartbeat_enabled=True, lease_seconds=30, grace_seconds=10),
        on_orphans_recovered=lambda recovered: contain_orphaned_work(app, recovered),
    )
    host.registry.run_manager = app.state.run_manager = manager
    assert host.registry.homes == {}
    await host.inventory_runs()
    assert host.foreign_run_blocks == frozenset((record.run_id,))
    await manager._reconcile_orphans_periodic()
    assert (await repository.get(record.run_id, user_id=record.user_id))["status"] == "running" and not app.state.storage_recheck_wake.is_set()
    if expired:
        async with sf() as session, session.begin():
            (await session.get(RunRow, record.run_id)).lease_expires_at = datetime.now(UTC) - timedelta(minutes=1)
    await manager._reconcile_orphans_periodic()
    async with sf() as session:
        assert (await session.get(AgentWorkAttemptRow, scope.attempt_id)).status == "running"
    assert claim.container_id in adapter.containers and host.registry.external_slots() == 1
    assert app.state.storage_recheck_wake.is_set() is expired
    if not expired:
        assert (await repository.get(record.run_id, user_id=record.user_id))["status"] == "running"
        return
    assert (await repository.get(record.run_id, user_id=record.user_id))["status"] == "error" and not host.foreign_run_blocks
    assert await module.reconcile_residents(app, deadline=time.monotonic() + 10) == []
    assert claim.container_id not in adapter.containers and host.registry.external_slots() == 0
    async with sf() as session:
        assert (await session.get(AgentWorkAttemptRow, scope.attempt_id)).status == "failed"
    history = await service.work.history(actor=admitted.requester, instance_id=admitted.instance.id, work_id=work["id"])
    events = [event for event in history if event["action"] == "attempt_contained"]
    assert len(events) == 1 and events[0]["actor_kind"] == "host"
    assert events[0]["report"]["reason"] == "run-orphaned"
