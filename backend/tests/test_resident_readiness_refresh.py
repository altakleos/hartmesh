"""Real readiness publication with bounded offline refresh barriers."""

import asyncio
import threading
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from app.gateway import storage_spaces as ss
from app.gateway.resident_runtime import ResidentRuntime
from deerflow.agent_instances.bootstrap import BootstrapResult
from deerflow.agent_instances.residents import ResidentRegistry
from deerflow.sandbox.admission import ProviderAdmission
from deerflow.sandbox.exceptions import SandboxRuntimeAdmissionError


def runtime(monkeypatch, *, qualified=True):
    provider = SimpleNamespace(_backend=SimpleNamespace(), _lock=threading.Lock(), external_changed=lambda action: action())
    admission = ProviderAdmission()
    admission.bind(provider)
    provider._runtime_admission = admission
    files = SimpleNamespace(catalog=SimpleNamespace(volumes={}))
    registry = ResidentRegistry(attachments=SimpleNamespace(), provider=provider, run_manager=SimpleNamespace(_runs={}), settle=AsyncMock(), owner_of=AsyncMock(), replicas=3, idle_timeout=60)
    app = SimpleNamespace(state=SimpleNamespace(storage_spaces=files, agent_execution=ss.ExecutionStatus(), chat_storage_host=SimpleNamespace(admission=admission)))
    host = ResidentRuntime(app, registry=registry, admission=admission, entered=0)
    app.state.resident_runtime = host
    host.coordinator.result = BootstrapResult(True)
    host.workspace_qualified = qualified
    host._startup_settled = True
    host.apply_hold = AsyncMock()
    host.reconcile_rows = AsyncMock(return_value=[])
    monkeypatch.setattr(ss, "storage_state", AsyncMock(return_value=ss.StorageState("ready")))
    monkeypatch.setattr(ss, "get_agents_api_config", lambda: SimpleNamespace(enabled=True, ai_assistants_per_user=1, ai_employees=1))
    monkeypatch.setattr(ss, "hold_present", lambda: False)
    return host, app


@pytest.mark.asyncio
@pytest.mark.parametrize("qualified", [False, True])
async def test_refresh_preserves_previous_readiness_until_its_result(monkeypatch, qualified):
    host, app = runtime(monkeypatch, qualified=qualified)
    entered, release = asyncio.Event(), asyncio.Event()

    async def inspect(**_kwargs):
        entered.set()
        await release.wait()
        return []

    host.reconcile_rows.side_effect = inspect
    task = asyncio.create_task(host.reconcile())
    try:
        await asyncio.wait_for(entered.wait(), 2)
        assert (await ss.execution_state(app)).execution == ("ready" if qualified else "unavailable")
        release.set()
        assert await task is True
        assert (await ss.execution_state(app)).execution == "ready"
        with pytest.raises(SandboxRuntimeAdmissionError):
            host.admission.require(host.provider)
    finally:
        release.set()
        await asyncio.gather(task, return_exceptions=True)


@pytest.mark.asyncio
@pytest.mark.parametrize("action", ["invalidate_accounting", "seal"])
async def test_successful_old_refresh_cannot_republish_after_invalidation(monkeypatch, action):
    host, app = runtime(monkeypatch)
    entered, release = asyncio.Event(), asyncio.Event()

    async def inspect(**_kwargs):
        entered.set()
        await release.wait()
        return []

    host.reconcile_rows.side_effect = inspect
    task = asyncio.create_task(host.reconcile())
    try:
        await asyncio.wait_for(entered.wait(), 2)
        getattr(host, action)()
        assert (await ss.execution_state(app)).execution == "unavailable"
        release.set()
        assert await task is False
        assert (await ss.execution_state(app)).execution == "unavailable"
    finally:
        release.set()
        await asyncio.gather(task, return_exceptions=True)


@pytest.mark.asyncio
@pytest.mark.parametrize("outcome", ["pending", "exception", "cancelled"])
async def test_pending_or_failed_refresh_closes_previous_readiness(monkeypatch, outcome):
    host, app = runtime(monkeypatch)
    if outcome == "pending":
        host.reconcile_rows.return_value = ["unqualified-attachment"]
        assert await host.reconcile() is False
    else:
        error = RuntimeError("unknown inventory") if outcome == "exception" else asyncio.CancelledError()
        host.reconcile_rows.side_effect = error
        with pytest.raises(type(error)):
            await host.reconcile()
    assert (await ss.execution_state(app)).execution == "unavailable"


@pytest.mark.asyncio
async def test_provider_substitution_during_refresh_cannot_publish_ready(monkeypatch):
    host, app = runtime(monkeypatch)

    async def replace(**_kwargs):
        host.provider = SimpleNamespace()
        return []

    host.reconcile_rows.side_effect = replace
    assert await host.reconcile() is False
    assert (await ss.execution_state(app)).execution == "unavailable"


@pytest.mark.asyncio
async def test_fresh_qualified_pass_recovers_after_failure(monkeypatch):
    host, app = runtime(monkeypatch)
    host.reconcile_rows.return_value = ["unqualified-attachment"]
    assert await host.reconcile() is False
    host.reconcile_rows.return_value = []
    assert await host.reconcile() is True
    assert (await ss.execution_state(app)).execution == "ready"


@pytest.mark.asyncio
@pytest.mark.parametrize("reason", ["operator-hold", "storage-unavailable"])
async def test_current_hold_and_storage_refusals_precede_cached_readiness(monkeypatch, reason):
    _host, app = runtime(monkeypatch)
    if reason == "operator-hold":
        monkeypatch.setattr(ss, "hold_present", lambda: True)
    else:
        monkeypatch.setattr(ss, "storage_state", AsyncMock(return_value=ss.StorageState("unavailable", "admission-failed")))
    assert (await ss.execution_state(app)).reason == reason
