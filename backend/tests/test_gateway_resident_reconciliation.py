"""Complete durable run inventory precedes qualified containment, offline SQL."""

import asyncio
import time
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from _storage_spaces_test_support import operation
from test_agent_instances import instances as instances
from test_agent_work_records import work as work
from test_native_create_provider import fixture as fixture
from test_storage_spaces_attachments import ContainedProvider
from test_work_execution import execution as execution

from deerflow.agent_instances.residents import ResidentRegistry
from deerflow.persistence.agent_instances.work import AgentWorkAttemptRow
from deerflow.persistence.spaces.lifecycle import SpaceAttachmentRow
from deerflow.sandbox.admission import ProviderAdmission
from deerflow.spaces.attachments import ResourceMount, SpaceAttachments
from deerflow.spaces.runtime_owner import StorageRuntimeUnavailable


async def build(execution, fixture, monkeypatch, *, host_id=None):
    from app.gateway import storage_spaces as module
    from app.gateway.resident_runtime import ResidentRuntime

    service, admitted, _row, _sf = execution
    provider, _owner = fixture
    admission = ProviderAdmission()
    provider._bind_runtime_admission(admission)
    adapter = ContainedProvider()
    if host_id is not None:
        adapter.host_id = host_id
    adapter.container_state = lambda container, _attachment: "running" if container in adapter.containers else "gone"
    attachments = SpaceAttachments(service.agents.files, adapter)
    claim = await attachments.attach(actor=admitted.instance.principal, incarnation=operation(), resources=(ResourceMount(admitted.instance.home_id, admitted.home_generation, "home", True),))
    from deerflow.runtime.runs.manager import RunManager

    manager = RunManager()
    store = SimpleNamespace(list_inflight=AsyncMock(return_value=[]))
    app = SimpleNamespace(
        state=SimpleNamespace(
            storage_spaces=service.agents.files,
            agent_instances=service.agents,
            run_manager=manager,
            run_store=store,
            startup_recovered_runs=[],
            agent_execution=module.ExecutionStatus(),
        )
    )
    residents = ResidentRegistry(attachments=attachments, provider=provider, run_manager=manager, settle=AsyncMock(return_value=[]), owner_of=AsyncMock(return_value=admitted.instance), replicas=3, idle_timeout=60)
    token = residents.begin_restore()
    residents.finish_restore(token, await attachments.retained_claims())
    host = ResidentRuntime(app, registry=residents, admission=admission, entered=time.monotonic())
    app.state.resident_runtime = host
    service.agents.residents = residents
    attachments.on_contained = residents.contained
    residents.settle = host.settle
    monkeypatch.setattr(module, "get_agents_api_config", lambda: SimpleNamespace(enabled=True))
    return module, app, host, attachments, adapter, claim


@pytest.mark.asyncio
@pytest.mark.parametrize("changed", [False, True])
async def test_native_resident_census_matches_real_sql_backing_and_full_mounts(execution, fixture, monkeypatch, changed):
    from test_bootstrap_primary_inventory import setup

    from deerflow.spaces.docker import ResidentPrimary

    _module, app, host, _attachments, adapter, claim = await build(execution, fixture, monkeypatch, host_id="daemon-A")
    local, _bridge, _calls = setup(fixture, listed=[])
    original = local._runtime_runner

    def census(command, **kwargs):
        if command[1:3] == ["network", "ls"] or any(selector in command for selector in ("label=deerflow.sandbox_id", "label=deerflow.role=network-proxy", "name=^/deer-flow-netproxy-")):
            return SimpleNamespace(returncode=0, stdout="", stderr="")
        return original(command, **kwargs)

    local._runtime_runner = census
    admitted = execution[1]
    files = app.state.storage_spaces
    for slot, spec in files.catalog.volumes.items():
        spec.mount_path = files.catalog.verified[slot].data_path.parent
    async with files.registry._sf() as session:
        from sqlalchemy import select

        from deerflow.persistence.spaces.files import SpaceBackingRow

        binding = await session.scalar(select(SpaceBackingRow).where(SpaceBackingRow.space_id == admitted.instance.home_id))
        source = str(files.catalog.volumes[binding.slot_id].mount_path / "data")
        slot = binding.slot_id
    actual = ResidentPrimary(claim.container_id, "hartmesh-space-" + claim.id, "2026-10-01T12:00:00Z", claim.id, claim.host_id, (slot,), ((source + "-changed" if changed else source, "/mnt/spaces/home", True),))
    adapter.readonly_roots = AsyncMock(return_value=())
    adapter._configured_mounts = ()
    adapter.bootstrap_residents = lambda **_kwargs: (actual,)
    permit = host.provider._begin_bootstrap_accounting(host.admission)
    try:
        if changed:
            with pytest.raises(StorageRuntimeUnavailable):
                await host.account_native(permit)
        else:
            proof = await host.account_native(permit)
            host.provider.external_changed(lambda: proof.require_current_locked(host.provider, permit))
            assert host.registry.external_slots() == 1
            assert not host.provider._sandboxes
    finally:
        host.provider._end_bootstrap_accounting(permit)


@pytest.mark.asyncio
async def test_unique_active_workspace_is_qualified_before_association(execution, fixture, monkeypatch):
    module, app, host, _attachments, adapter, claim = await build(execution, fixture, monkeypatch)
    pending = await module.reconcile_residents(app, deadline=time.monotonic() + 10)
    assert pending == []
    assert host.registry.homes[execution[1].instance.home_id].attachment_id == claim.id
    assert host.registry.external_slots() == 1 and len(adapter.started) == 1


@pytest.mark.asyncio
async def test_foreign_inflight_run_preserves_attachment_and_unresolved_attempt(execution, fixture, monkeypatch, caplog):
    from test_work_execution import activation

    service, admitted, row, sf = execution
    scope = await service.reserve(execution=admitted, work_id=row["id"], request=activation(admitted, row))
    module, app, host, _attachments, adapter, claim = await build(execution, fixture, monkeypatch)
    host.workspace_qualified = True
    app.state.run_store.list_inflight.return_value = [{"run_id": "foreign-live", "metadata": {"agent_instance_id": admitted.instance.id}, "lease_expires_at": "2099-01-01T00:00:00Z", "owner_instance_id": "foreign-worker"}]
    assert await module.reconcile_residents(app, deadline=time.monotonic() + 10) == [claim.id]
    async with sf() as session:
        assert (await session.get(SpaceAttachmentRow, claim.id)).phase == "active"
        assert (await session.get(AgentWorkAttemptRow, scope.attempt_id)).status == "starting"
    assert claim.container_id in adapter.containers and host.registry.external_slots() == 1
    assert host.foreign_run_blocks
    assert host.workspace_qualified is False
    assert "workspace unqualified (reconcile-foreign;" in caplog.text


@pytest.mark.asyncio
async def test_inflight_scan_failure_never_becomes_empty_or_authorizes_containment(execution, fixture, monkeypatch):
    module, app, host, _attachments, adapter, claim = await build(execution, fixture, monkeypatch)
    app.state.run_store.list_inflight.side_effect = RuntimeError("Unknown durable inventory")
    with pytest.raises(StorageRuntimeUnavailable):
        await module.reconcile_residents(app, deadline=time.monotonic() + 10)
    assert claim.container_id in adapter.containers and host.registry.external_slots() == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("foreign", [False, True])
async def test_operator_hold_contains_only_current_locally_owned_home(execution, fixture, monkeypatch, tmp_path, foreign):
    module, app, host, _attachments, adapter, claim = await build(execution, fixture, monkeypatch)
    held = tmp_path / "agents-hold"
    held.write_text("maintenance\n", encoding="utf-8")
    monkeypatch.setattr(module, "AGENTS_HOLD_PATH", held)
    if foreign:
        app.state.run_store.list_inflight.return_value = [{"run_id": "foreign", "metadata": {"agent_instance_id": execution[1].instance.id}, "lease_expires_at": "2099-01-01T00:00:00Z"}]
    await host.apply_hold()
    assert (claim.container_id in adapter.containers) is foreign
    assert app.state.agent_execution.hold_since is not None
    held.unlink()
    await host.apply_hold()
    assert app.state.agent_execution.hold_since is None


@pytest.mark.asyncio
async def test_operator_hold_rechecks_foreign_lease_after_home_lock_wait(execution, fixture, monkeypatch, tmp_path):
    module, app, host, _attachments, adapter, claim = await build(execution, fixture, monkeypatch)
    held = tmp_path / "agents-hold"
    held.write_text("maintenance", encoding="utf-8")
    monkeypatch.setattr(module, "AGENTS_HOLD_PATH", held)
    admitted = execution[1]
    entry = host.registry.entry(admitted.instance)
    await entry.lock.acquire()
    reached = asyncio.Event()
    original = host.registry.entry

    def entering(identity):
        result = original(identity)
        reached.set()
        return result

    monkeypatch.setattr(host.registry, "entry", entering)
    task = asyncio.create_task(host.apply_hold())
    try:
        await asyncio.wait_for(reached.wait(), 2)
        app.state.run_store.list_inflight.return_value = [{"run_id": "late-foreign", "metadata": {"agent_instance_id": admitted.instance.id}, "lease_expires_at": "2099-01-01T00:00:00Z"}]
        entry.lock.release()
        await task
        assert claim.container_id in adapter.containers
        assert host.registry.external_slots() == 1
    finally:
        if entry.lock.locked():
            entry.lock.release()
        await asyncio.gather(task, return_exceptions=True)


@pytest.mark.asyncio
async def test_periodic_recovery_requests_exact_recycle_without_physical_cleanup(execution, fixture, monkeypatch):
    from test_work_execution import activation, run_record

    from app.gateway.work_execution import contain_orphaned_work

    service, admitted, row, _sf = execution
    module, app, host, _attachments, adapter, claim = await build(execution, fixture, monkeypatch)
    await module.reconcile_residents(app, deadline=time.monotonic() + 10)
    scope = await service.reserve(execution=admitted, work_id=row["id"], request=activation(admitted, row))
    record = run_record(admitted, scope, status="running")
    record.metadata["agent_instance_id"] = admitted.instance.id
    await scope.bind(admitted, record, admitted.thread_incarnation)
    await scope.workspace_ready(admitted, claim.id)
    await contain_orphaned_work(app, [record])
    entry = host.registry.entry(admitted.instance)
    assert entry.recycle_reason == "run-orphaned" and entry.attachment_id == claim.id
    assert claim.container_id in adapter.containers


@pytest.mark.asyncio
async def test_historical_startup_recovery_cannot_close_a_newer_workspace(execution, fixture, monkeypatch):
    module, app, host, attachments, adapter, old = await build(execution, fixture, monkeypatch)
    admitted = execution[1]
    app.state.startup_recovered_runs = [SimpleNamespace(metadata={"agent_instance_id": admitted.instance.id})]
    assert await module.reconcile_residents(app, deadline=time.monotonic() + 10) == []
    assert old.container_id not in adapter.containers
    fresh = await attachments.attach(actor=admitted.instance.principal, incarnation=operation(), resources=(ResourceMount(admitted.instance.home_id, admitted.home_generation, "home", True),))
    assert await module.reconcile_residents(app, deadline=time.monotonic() + 10) == []
    assert fresh.container_id in adapter.containers
    assert host.registry.entry(admitted.instance).attachment_id == fresh.id


@pytest.mark.asyncio
async def test_ambiguous_home_claims_stay_counted_without_usable_association(execution, fixture, monkeypatch, caplog):
    module, app, host, attachments, adapter, first = await build(execution, fixture, monkeypatch)
    admitted = execution[1]
    second = await attachments.attach(actor=admitted.instance.principal, incarnation=operation(), resources=(ResourceMount(admitted.instance.home_id, admitted.home_generation, "home", True),))
    token = host.registry.begin_restore()
    host.registry.finish_restore(token, await attachments.retained_claims())
    assert set(await module.reconcile_residents(app, deadline=time.monotonic() + 10)) == {first.id, second.id}
    assert host.registry.external_slots() == 2 and len(adapter.containers) == 2
    assert host.registry.homes[admitted.instance.home_id].attachment_id is None
    assert caplog.text.count("workspace unqualified (reconcile-ambiguous-association;") == 2


@pytest.mark.asyncio
async def test_healthy_local_work_run_is_associated_without_orphan_containment(execution, fixture, monkeypatch):
    from test_work_execution import activation, run_record

    service, admitted, row, sf = execution
    scope = await service.reserve(execution=admitted, work_id=row["id"], request=activation(admitted, row))
    record = run_record(admitted, scope, status="running")
    record.metadata["agent_instance_id"] = admitted.instance.id
    await scope.bind(admitted, record, admitted.thread_incarnation)
    record.owner_worker_id = "local-worker"
    record.created_at = "2026-10-14T00:00:00Z"
    record.task = asyncio.create_task(asyncio.Event().wait())
    try:
        module, app, host, _attachments, adapter, claim = await build(execution, fixture, monkeypatch)
        app.state.run_manager._runs[record.run_id] = record
        app.state.run_store.list_inflight.return_value = [{"run_id": record.run_id, "metadata": dict(record.metadata), "owner_worker_id": record.owner_worker_id, "thread_id": record.thread_id, "user_id": record.user_id}]
        assert await module.reconcile_residents(app, deadline=time.monotonic() + 10) == []
        async with sf() as session:
            assert (await session.get(SpaceAttachmentRow, claim.id)).phase == "active"
            assert (await session.get(AgentWorkAttemptRow, scope.attempt_id)).status == "running"
        assert claim.container_id in adapter.containers and not host.foreign_run_blocks
        assert host.registry.homes[admitted.instance.home_id].attachment_id == claim.id
    finally:
        record.task.cancel()
        await asyncio.gather(record.task, return_exceptions=True)


@pytest.mark.asyncio
@pytest.mark.parametrize("arriving", ["run", "reservation"])
async def test_waiting_reconcile_rechecks_new_home_activity_before_orphan_containment(execution, fixture, monkeypatch, arriving, caplog):
    from test_work_execution import activation, run_record

    service, admitted, row, sf = execution
    scope = await service.reserve(execution=admitted, work_id=row["id"], request=activation(admitted, row))
    module, app, host, _attachments, adapter, claim = await build(execution, fixture, monkeypatch)
    entry = host.registry.entry(admitted.instance)
    async with entry.lock:
        host.registry.associate(admitted.instance, host.registry.retained[claim.id], generation=admitted.home_generation)
    host.provider.external_changed(lambda: host.admission.open(host.provider))
    await entry.lock.acquire()
    entered = asyncio.Event()
    original = host.registry.entry

    def entering(instance):
        result = original(instance)
        entered.set()
        return result

    monkeypatch.setattr(host.registry, "entry", entering)
    task = asyncio.create_task(module.reconcile_residents(app, deadline=time.monotonic() + 10))
    running = None
    try:
        await asyncio.wait_for(entered.wait(), 2)
        if arriving == "run":
            running = run_record(admitted, scope, status="running")
            running.metadata["agent_instance_id"] = admitted.instance.id
            running.created_at = "2026-10-14T00:00:00Z"
            running.task = asyncio.create_task(asyncio.Event().wait())
            app.state.run_manager._runs[running.run_id] = running
        else:
            await host.registry.admit(admitted.instance, work=True)
        entry.lock.release()
        assert await task == [claim.id]
        async with sf() as session:
            assert (await session.get(SpaceAttachmentRow, claim.id)).phase == "active"
            assert (await session.get(AgentWorkAttemptRow, scope.attempt_id)).status == "starting"
        assert claim.container_id in adapter.containers
        assert "workspace unqualified (reconcile-busy-ineligible;" in caplog.text
    finally:
        if entry.lock.locked():
            entry.lock.release()
        if running is not None:
            running.task.cancel()
            await asyncio.gather(running.task, return_exceptions=True)
        await asyncio.gather(task, return_exceptions=True)
