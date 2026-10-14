"""Host lifecycle fences precede producers and outlive mandatory completion."""

import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from deerflow.sandbox.admission import ProviderAdmission
from deerflow.sandbox.exceptions import SandboxRuntimeAdmissionError


@pytest.mark.anyio
async def test_managed_chat_host_captures_closed_admission_before_provider_lookup(monkeypatch):
    from app.gateway.chat_storage import chat_storage_lifecycle
    from deerflow.spaces import chat_bridge

    monkeypatch.setattr(chat_bridge, "install_chat_storage_host", lambda _host: None)
    monkeypatch.setattr("deerflow.sandbox.sandbox_provider.get_initialized_sandbox_provider", lambda: None)
    app = SimpleNamespace(state=SimpleNamespace(pool_runtime_owner=object()))
    async with chat_storage_lifecycle(app, enabled=True):
        host = app.state.chat_storage_host
        assert isinstance(host.admission, ProviderAdmission)
        with pytest.raises(SandboxRuntimeAdmissionError):
            host.admission.require(object())


@pytest.mark.anyio
async def test_runtime_storage_drains_maintenance_and_residents_before_provider(monkeypatch):
    from app.gateway.deps import _close_runtime_storage

    order = []

    async def storage(_app):
        order.append("maintenance")

    async def residents(_app):
        order.append("residents")

    async def chat(_app):
        order.append("provider")

    async def closed(_app):
        order.append("storage")

    monkeypatch.setattr("app.gateway.storage_spaces.drain_storage_spaces", storage)
    monkeypatch.setattr("app.gateway.storage_spaces.close_storage_spaces", closed)
    monkeypatch.setattr("app.gateway.resident_runtime.close_resident_runtime", residents)
    monkeypatch.setattr("app.gateway.chat_storage.close_chat_storage_host", chat)
    app = SimpleNamespace(state=SimpleNamespace())
    await _close_runtime_storage(app)
    await _close_runtime_storage(app)
    assert order == ["maintenance", "residents", "provider", "storage"]


@pytest.mark.anyio
async def test_shared_maintenance_drain_keeps_callbacks_until_failed_provider_is_joined(monkeypatch):
    from app.gateway.deps import _close_runtime_storage
    from app.gateway.storage_spaces import ExecutionStatus

    entered, finish = asyncio.Event(), asyncio.Event()
    calls = []
    callback = object()
    repository = SimpleNamespace(seats_allowed=callback)

    async def maintenance():
        entered.set()
        await finish.wait()
        assert repository.seats_allowed is callback
        calls.append("maintenance")

    status = ExecutionStatus(maintenance_task=asyncio.create_task(maintenance()))
    app = SimpleNamespace(state=SimpleNamespace(agent_execution=status, storage_seat_callback=(repository, callback)))

    async def provider(_app):
        assert repository.seats_allowed is callback
        calls.append("provider")
        raise RuntimeError("provider drain failed")

    monkeypatch.setattr("app.gateway.chat_storage.close_chat_storage_host", provider)
    await entered.wait()
    closing = asyncio.create_task(_close_runtime_storage(app))
    try:
        await asyncio.sleep(0)
        assert not closing.done() and repository.seats_allowed is callback
        finish.set()
        with pytest.raises(RuntimeError, match="provider drain failed"):
            await closing
        assert calls == ["maintenance", "provider"]
        assert repository.seats_allowed is None and app.state.storage_seat_callback is None
        with pytest.raises(RuntimeError, match="provider drain failed"):
            await _close_runtime_storage(app)
        assert calls == ["maintenance", "provider"]
    finally:
        finish.set()
        await asyncio.gather(closing, status.maintenance_task, return_exceptions=True)


@pytest.mark.anyio
async def test_host_final_drain_includes_terminal_status_worker_still_completing():
    from app.gateway.resident_runtime import ResidentRuntime
    from deerflow.runtime.runs.manager import RunManager

    manager = RunManager()
    record = await manager.create("thread")
    record.status = "success"
    entered, release = asyncio.Event(), asyncio.Event()

    async def completion():
        entered.set()
        await release.wait()

    record.task = asyncio.create_task(completion())
    await entered.wait()
    host = ResidentRuntime.__new__(ResidentRuntime)
    host.provider = SimpleNamespace(external_changed=lambda change: change())
    host.admission = ProviderAdmission()
    host.admission.bind(host.provider)
    host.admission.open(host.provider)
    host.registry = SimpleNamespace(run_manager=manager)
    host._closing_tasks = None
    host.drain_producers = AsyncMock()
    closing = asyncio.create_task(host.drain_workers())
    try:
        await asyncio.sleep(0)
        assert not closing.done()
        with pytest.raises(SandboxRuntimeAdmissionError):
            host.admission.require(host.provider)
        release.set()
        await closing
        host.drain_producers.assert_awaited_once()
    finally:
        release.set()
        await asyncio.gather(record.task, closing, return_exceptions=True)


@pytest.mark.anyio
async def test_final_drain_waits_for_owned_launch_then_its_new_worker():
    from app.gateway.resident_runtime import ResidentRuntime
    from deerflow.runtime.runs.manager import RunManager

    manager = RunManager()
    host = ResidentRuntime.__new__(ResidentRuntime)
    host.provider = SimpleNamespace(external_changed=lambda change: change())
    host.admission = ProviderAdmission()
    host.admission.bind(host.provider)
    host.admission.open(host.provider)
    host.registry = SimpleNamespace(run_manager=manager)
    host._closing_tasks, host._closing, host._operations = None, False, set()
    host.drain_producers = AsyncMock()
    entered, launch, worker_release = asyncio.Event(), asyncio.Event(), asyncio.Event()

    async def late_launch():
        entered.set()
        await launch.wait()
        record = await manager.create("late-thread")
        record.task = asyncio.create_task(worker_release.wait())
        return record

    owning = asyncio.create_task(host.own_operation(late_launch()))
    await entered.wait()
    closing = asyncio.create_task(host.drain_workers())
    try:
        await asyncio.sleep(0)
        assert not closing.done()
        launch.set()
        record = await owning
        await asyncio.sleep(0)
        assert not closing.done() and not record.task.done()
        worker_release.set()
        await closing
        with pytest.raises(Exception, match="closing"):
            await host.own_operation(late_launch())
    finally:
        launch.set()
        worker_release.set()
        await asyncio.gather(owning, closing, return_exceptions=True)


@pytest.mark.anyio
async def test_existing_maintenance_retries_bootstrap_while_workspace_storage_unavailable(monkeypatch):
    from app.gateway import storage_spaces as module

    retry = AsyncMock(return_value=False)
    monkeypatch.setattr(module, "_resident_step", retry)
    monkeypatch.setattr(module, "storage_state", AsyncMock(return_value=module.StorageState("unavailable", "admission-failed")))
    app = SimpleNamespace(state=SimpleNamespace(storage_spaces=object(), agent_execution=module.ExecutionStatus()))
    await module._maintenance_step(app, timeout=1)
    retry.assert_awaited_once_with(app, timeout=0)


@pytest.mark.anyio
async def test_capacity_result_cannot_substitute_for_current_workspace_readiness(monkeypatch):
    from app.gateway import storage_spaces as module
    from deerflow.agent_instances.bootstrap import BootstrapResult

    monkeypatch.setattr(module, "storage_state", AsyncMock(return_value=module.StorageState("ready")))
    monkeypatch.setattr(module, "get_agents_api_config", lambda: SimpleNamespace(enabled=True, ai_assistants_per_user=1, ai_employees=1))
    runtime = SimpleNamespace(coordinator=SimpleNamespace(result=BootstrapResult(False)), workspace_qualified=False)
    app = SimpleNamespace(state=SimpleNamespace(chat_storage_host=SimpleNamespace(admission=object()), resident_runtime=runtime))
    assert (await module.execution_state(app)).execution == "unavailable"
    # A later qualified reconciliation updates execution independently of the
    # cached initial capacity result and retains the same installed runtime.
    runtime.workspace_qualified = True
    assert (await module.execution_state(app)).execution == "ready"


@pytest.mark.anyio
async def test_operator_hold_refuses_new_agent_execution_and_lifts_without_restart(monkeypatch, tmp_path):
    from app.gateway import storage_spaces as module

    held = tmp_path / "agents-hold"
    monkeypatch.setattr(module, "AGENTS_HOLD_PATH", held)
    monkeypatch.setattr(module, "storage_state", AsyncMock(return_value=module.StorageState("ready")))
    monkeypatch.setattr(module, "get_agents_api_config", lambda: SimpleNamespace(enabled=True, ai_assistants_per_user=1, ai_employees=1))
    app = SimpleNamespace(state=SimpleNamespace())
    held.write_text("Operator maintenance\n", encoding="utf-8")
    assert (await module.execution_state(app)).reason == "operator-hold"
    held.unlink()
    assert (await module.execution_state(app)).execution == "ready"


@pytest.mark.anyio
async def test_maintenance_retries_workspace_after_cached_capacity_result(monkeypatch):
    from app.gateway import storage_spaces as module
    from deerflow.agent_instances.bootstrap import BootstrapResult

    runtime = SimpleNamespace(
        coordinator=SimpleNamespace(result=BootstrapResult(False)),
        workspace_qualified=False,
        reconcile=AsyncMock(return_value=True),
        foreign_run_blocks=frozenset(),
        accounting_retry=False,
        registry=SimpleNamespace(reap=AsyncMock()),
        settle_pending=AsyncMock(),
    )
    monkeypatch.setattr(module, "_resident_step", AsyncMock(return_value=True))
    monkeypatch.setattr(module, "storage_state", AsyncMock(return_value=module.StorageState("ready")))
    monkeypatch.setattr(module, "finish_releases", AsyncMock(return_value=0))
    monkeypatch.setattr(module, "assign_spaces", AsyncMock())
    app = SimpleNamespace(state=SimpleNamespace(storage_spaces=SimpleNamespace(settle_interrupted=AsyncMock(return_value=0)), agent_execution=module.ExecutionStatus(), resident_runtime=runtime))
    await module._maintenance_step(app, timeout=1)
    runtime.reconcile.assert_awaited_once()
