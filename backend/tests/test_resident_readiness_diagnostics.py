"""Readiness diagnostics expose fixed causes without tenant payloads or I/O."""

from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from test_resident_readiness_refresh import runtime

from app.gateway import storage_spaces as ss


@pytest.mark.asyncio
@pytest.mark.parametrize("source", ["storage-state", "runtime-missing", "bootstrap-pending", "workspace-unqualified"])
async def test_execution_refusal_names_its_internal_branch(monkeypatch, caplog, source):
    host, app = runtime(monkeypatch)
    if source == "storage-state":
        monkeypatch.setattr(ss, "storage_state", AsyncMock(return_value=ss.StorageState("unavailable", "private-storage-value")))
    elif source == "runtime-missing":
        app.state.resident_runtime = None
    elif source == "bootstrap-pending":
        host.coordinator.result = None
    else:
        host.workspace_qualified = False
    caplog.set_level("WARNING")
    state = await ss.execution_state(app)
    assert (state.execution, state.reason) == ("unavailable", "storage-unavailable")
    assert f"execution unavailable ({source};" in caplog.text
    assert "private-storage-value" not in caplog.text


@pytest.mark.asyncio
@pytest.mark.parametrize("field", ["local-missing", "task-missing", "task-done", "ownership-lost", "metadata", "worker", "thread", "user"])
async def test_foreign_inventory_reports_only_fixed_mismatch_fields(monkeypatch, caplog, field):
    host, app = runtime(monkeypatch)
    secret = "PRIVATE-TENANT-PAYLOAD"
    row = {"run_id": secret, "metadata": {"private-key": secret}, "owner_worker_id": secret, "thread_id": secret, "user_id": secret}
    local = SimpleNamespace(task=SimpleNamespace(done=lambda: False), ownership_lost=False, metadata=row["metadata"], owner_worker_id=secret, thread_id=secret, user_id=secret)
    if field == "local-missing":
        pass
    else:
        host.registry.run_manager._runs[secret] = local
        if field == "task-missing":
            local.task = None
        elif field == "task-done":
            local.task = SimpleNamespace(done=lambda: True)
        elif field == "ownership-lost":
            local.ownership_lost = True
        else:
            attribute = {"worker": "owner_worker_id", "thread": "thread_id", "user": "user_id"}.get(field, field)
            setattr(local, attribute, {} if field == "metadata" else "different-private-value")
    app.state.run_store = SimpleNamespace(list_inflight=AsyncMock(return_value=[row]))
    caplog.set_level("WARNING")
    assert await host.inventory_runs() == [row]
    assert host.foreign_run_blocks == frozenset((secret,))
    assert host.workspace_qualified is False and host.accounting_retry is True
    assert f"run inventory unqualified (foreign=1; fields={field})" in caplog.text
    assert secret not in caplog.text and "private-key" not in caplog.text and "different-private-value" not in caplog.text
    app.state.run_store.list_inflight.assert_awaited_once()


@pytest.mark.asyncio
async def test_repeated_pending_refresh_logs_even_when_already_unqualified(monkeypatch, caplog):
    host, _app = runtime(monkeypatch, qualified=False)
    host.reconcile_rows.return_value = ["PRIVATE-ATTACHMENT-ID"]
    caplog.set_level("WARNING")
    assert await host.reconcile() is False
    assert await host.reconcile() is False
    assert caplog.text.count("workspace unqualified (reconcile-pending;") == 2
    assert "PRIVATE-ATTACHMENT-ID" not in caplog.text


@pytest.mark.asyncio
async def test_ready_refresh_has_no_refusal_diagnostics(monkeypatch, caplog):
    host, app = runtime(monkeypatch)
    caplog.set_level("WARNING")
    assert await host.reconcile() is True
    assert (await ss.execution_state(app)).execution == "ready"
    assert "unqualified" not in caplog.text and "execution unavailable" not in caplog.text
