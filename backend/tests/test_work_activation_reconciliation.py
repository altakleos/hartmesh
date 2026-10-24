"""Work admission waits for real offline resident maintenance, with real SQL."""

import asyncio
import time
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from test_agent_instances import instances as instances
from test_agent_work_records import work as work
from test_resident_registry import registry as registry
from test_work_execution import activation, run_record
from test_work_execution import execution as execution

from app.gateway.resident_runtime import ResidentRuntime
from app.gateway.work_execution import activate_work_run
from deerflow.agent_instances.contract import AgentDenied, AgentPermission, ResidentRestarting
from deerflow.persistence.agent_instances.model import AgentInstanceGrantRow
from deerflow.sandbox.admission import ProviderAdmission
from deerflow.spaces.attachments import AttachmentState, ResourceMount, RetainedAttachment


async def environment(execution, registry, monkeypatch):
    service, admitted, row, sf = execution
    residents, provider, records, clock = registry()
    provider._backend = SimpleNamespace()
    provider.host_id = "offline-host"
    admission = ProviderAdmission()
    admission.bind(provider)
    provider._runtime_admission = admission
    claim = RetainedAttachment(
        "a" * 32,
        "b" * 32,
        admitted.instance.principal,
        provider.host_id,
        (ResourceMount(admitted.instance.home_id, admitted.home_generation, "home", True),),
        "active",
        "c" * 64,
    )
    entered, release, ready = asyncio.Event(), asyncio.Event(), asyncio.Event()

    async def inspect(**_kwargs):
        entered.set()
        await release.wait()
        return AttachmentState(claim.id, claim.phase, claim.host_id, claim.container_id)

    attachments = SimpleNamespace(
        provider=SimpleNamespace(host_id=provider.host_id),
        retained_claims=AsyncMock(return_value=(claim,)),
        inspect_preparation=inspect,
        container_state=AsyncMock(return_value="running"),
        resume=AsyncMock(return_value=claim),
        retained_attachment=AsyncMock(return_value=claim),
        contain_host=AsyncMock(return_value=True),
        uncontained={},
    )
    residents.attachments = attachments
    residents.finish_restore(residents.begin_restore(), (claim,))
    service.agents.residents = residents
    app = SimpleNamespace(state=SimpleNamespace(storage_spaces=service.agents.files, agent_instances=service.agents, startup_recovered_runs=[], agent_execution=SimpleNamespace(attachments_fenced=0)))
    runtime = ResidentRuntime(app, registry=residents, admission=admission, entered=time.monotonic())
    app.state.resident_runtime = runtime
    runtime.workspace_qualified = True
    runtime.inventory_runs = AsyncMock(return_value=[])
    monkeypatch.setattr("app.gateway.storage_spaces.get_agents_api_config", lambda: SimpleNamespace(enabled=True))
    entry = residents.entry(admitted.instance)
    lock_waiting = asyncio.Event()

    class ObservedHomeLock(asyncio.Lock):
        async def acquire(self):
            if self.locked():
                lock_waiting.set()
            return await super().acquire()

    entry.lock = ObservedHomeLock()
    capacity, launches, events = [], [], []
    original_capacity = provider.admit_external_async

    async def capacity_admission(*args, **kwargs):
        assert entry.lock.locked(), "Capacity admission must own this Home after maintenance"
        capacity.append(True)
        return await original_capacity(*args, **kwargs)

    monkeypatch.setattr(provider, "admit_external_async", capacity_admission)
    original_ready = admitted.authority.require_ready
    checks = []

    async def require_ready(value):
        await original_ready(value)
        checks.append(True)
        if len(checks) == 2:
            ready.set()

    monkeypatch.setattr(admitted.authority, "require_ready", require_ready)
    original_event = service.work._event

    async def event(*args, **kwargs):
        if args[-1] == "activate":
            assert not entry.lock.locked(), "Durable Work admission must not retain the Home lock"
            events.append(True)
        return await original_event(*args, **kwargs)

    monkeypatch.setattr(service.work, "_event", event)

    async def launch(_body, _thread, _request, **kwargs):
        assert not entry.lock.locked(), "Run admission takes its own Home lock"
        scope = kwargs["work_attempt"]
        launches.append(scope.attempt_id)
        record = run_record(admitted, scope, status="running")
        record.metadata["agent_instance_id"] = admitted.instance.id
        records[record.run_id] = {"metadata": record.metadata, "user_id": record.user_id, "created_at": "2026-10-24T00:00:00Z", "thread_id": record.thread_id}
        async with entry.lock:
            residents.claimed(admitted.instance, claim.id, attempt_id=scope.attempt_id)
        return record

    monkeypatch.setattr("app.gateway.services.start_run", launch)
    maintenance = asyncio.create_task(runtime._reconcile_rows(deadline=time.monotonic() + 30))
    try:
        await asyncio.wait_for(entered.wait(), 10)
        assert entry.reconciling and entry.lock.locked() and runtime.workspace_qualified
    except BaseException:
        release.set()
        await asyncio.gather(maintenance, return_exceptions=True)
        raise
    return SimpleNamespace(**locals())


async def assert_unpublished(e):
    saved = await e.service.work.get(actor=e.admitted.requester, instance_id=e.admitted.instance.id, work_id=e.row["id"])
    assert saved["attempt"] is None and saved["revision"] == e.row["revision"]
    assert not e.residents.reservations and not e.capacity and not e.launches and not e.events


async def assert_waiting(e, task):
    waiter = asyncio.create_task(e.lock_waiting.wait())
    try:
        done, _ = await asyncio.wait((waiter, task), timeout=10, return_when=asyncio.FIRST_COMPLETED)
        if task in done:
            await task  # Preserve the actual premature-refusal exception in red evidence.
        assert waiter in done and waiter.result(), "Activation must reach the exact held Home lock"
        assert not task.done()
        await assert_unpublished(e)
    finally:
        waiter.cancel()
        await asyncio.gather(waiter, return_exceptions=True)


async def cleanup(e, task):
    e.release.set()
    await asyncio.gather(*(item for item in (e.maintenance, task) if item is not None), return_exceptions=True)
    if e.runtime._operations:
        await asyncio.gather(*tuple(e.runtime._operations), return_exceptions=True)
    assert not e.runtime._operations and not e.residents.reservations
    e.records.clear()
    await e.residents.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("cancelled", [False, True])
async def test_healthy_reconciliation_finishes_before_one_owned_activation(execution, registry, monkeypatch, cancelled):
    e = await environment(execution, registry, monkeypatch)
    task = None
    try:
        task = asyncio.create_task(activate_work_run(SimpleNamespace(app=e.app), e.service, e.admitted, e.row["id"], activation(e.admitted, e.row)))
        await asyncio.wait_for(e.ready.wait(), 10)
        await assert_waiting(e, task)
        if cancelled:
            task.cancel()
            await asyncio.sleep(0)
            task.cancel()
            assert not task.done() and e.runtime._operations
        e.release.set()
        assert await e.maintenance == []
        if cancelled:
            with pytest.raises(asyncio.CancelledError):
                await task
        else:
            await task
        saved = await e.service.work.get(actor=e.admitted.requester, instance_id=e.admitted.instance.id, work_id=e.row["id"])
        assert e.launches == [saved["attempt"]["id"]]
        # Binding a run does not certify native workspace readiness.
        assert saved["attempt"]["status"] == "starting" and saved["attempt"]["run_id"] in e.records
        assert len(e.capacity) == len(e.events) == 1 and not e.entry.lock.locked()
        assert not e.residents.reservations
    finally:
        await cleanup(e, task)


@pytest.mark.asyncio
@pytest.mark.parametrize("changed", ["closing", "restore", "recycle", "authority"])
async def test_waiting_activation_rechecks_changed_facts_without_an_attempt(execution, registry, monkeypatch, changed):
    e = await environment(execution, registry, monkeypatch)
    task = None
    try:
        task = asyncio.create_task(activate_work_run(SimpleNamespace(app=e.app), e.service, e.admitted, e.row["id"], activation(e.admitted, e.row)))
        await asyncio.wait_for(e.ready.wait(), 10)
        await assert_waiting(e, task)
        if changed == "closing":
            await e.residents.close()
        elif changed == "restore":
            e.residents.restoration_complete = False
        elif changed == "recycle":
            e.entry.recycle_due_since, e.entry.recycle_reason = e.clock[0], "start-failed"
        else:
            async with e.sf() as session, session.begin():
                grant = await session.get(AgentInstanceGrantRow, (e.admitted.instance.id, e.admitted.requester.subject_id))
                grant.permissions = int(AgentPermission.INSPECT)
        e.release.set()
        await e.maintenance
        with pytest.raises(AgentDenied if changed == "authority" else ResidentRestarting):
            await task
        saved = await e.service.work.get(actor=e.admitted.requester, instance_id=e.admitted.instance.id, work_id=e.row["id"])
        assert saved["attempt"] is None and saved["revision"] == e.row["revision"]
        assert not e.residents.reservations and not e.launches and not e.events
    finally:
        await cleanup(e, task)
