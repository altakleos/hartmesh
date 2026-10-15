"""Concurrent ordinary teardown must not strand a healthy resident workspace."""

import asyncio
import json
import time
from contextlib import asynccontextmanager
from threading import Event
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest
from test_gateway_resident_reconciliation import build
from test_gateway_resident_reconciliation import execution as execution
from test_gateway_resident_reconciliation import fixture as fixture
from test_gateway_resident_reconciliation import instances as instances
from test_gateway_resident_reconciliation import work as work
from test_native_create_provider import install
from test_native_teardown_evidence import STAMP, record

from deerflow.agent_instances.contract import ResidentWorkLimit
from deerflow.community.aio_sandbox.native_resource_set import NativeResourceSet
from deerflow.spaces.docker import StorageHostUnqualified
from deerflow.spaces.runtime_owner import StorageRuntimeUnavailable


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "uncertainty",
    [None, "filtered", "repeated", "unknown", "closing", "bridge", "backend", "daemon", "malformed", "deadline", "foreign", "graph", "restore-owned", "closing-after", "bridge-after", "deadline-after", "cancelled", "snapshot-budget"],
)
async def test_healthy_live_resident_refreshes_only_stale_accounting(execution, fixture, monkeypatch, caplog, uncertainty):
    module, app, host, attachments, adapter, claim = await build(execution, fixture, monkeypatch)
    provider, owner = fixture
    bridge = install(provider, owner)
    admitted = execution[1]
    assert await module.reconcile_residents(app, deadline=time.monotonic() + 10) == []
    host.workspace_qualified = True
    entry = host.registry.entry(admitted.instance)
    running = asyncio.create_task(asyncio.Event().wait())
    local = SimpleNamespace(
        run_id="live-resident-run",
        thread_id=admitted.thread_id,
        user_id="alice",
        owner_worker_id="local-worker",
        metadata={"agent_instance_id": admitted.instance.id, "agent_work_attempt_id": "live-attempt"},
        task=running,
        ownership_lost=False,
        created_at="2026-10-15T00:00:00Z",
    )
    app.state.run_manager._runs[local.run_id] = local
    app.state.run_store.list_inflight.return_value = [{key: getattr(local, key) for key in ("run_id", "thread_id", "user_id", "owner_worker_id", "metadata")}]
    calls, qualifications, failures, sql_admissions = [], [], [], []
    ordinary = record()
    real_qualify, real_verify = bridge.qualify, adapter.verify_active
    real_admitted = attachments.registry.admitted
    real_store = app.state.run_store.list_inflight
    original_runs = real_store.return_value
    entered, release, drained = Event(), Event(), Event()

    if uncertainty == "snapshot-budget":
        from app.gateway.resident_runtime import _ResidentSnapshotChanged

        real_reconcile = host._reconcile_rows
        scans = 0

        async def changed_snapshot(**kwargs):
            nonlocal scans
            scans += 1
            if scans == 1:
                raise _ResidentSnapshotChanged(claim.id)
            return await real_reconcile(**kwargs)

        monkeypatch.setattr(host, "_reconcile_rows", changed_snapshot)

    @asynccontextmanager
    async def admitted_sql(**kwargs):
        async with real_admitted(**kwargs) as result:
            sql_admissions.append(result)
            try:
                yield result
            finally:
                sql_admissions.pop()

    monkeypatch.setattr(attachments.registry, "admitted", admitted_sql)

    async def inventory():
        if qualifications and uncertainty == "foreign":
            return [{**original_runs[0], "owner_worker_id": "foreign-worker"}]
        return original_runs

    real_store.side_effect = inventory

    def qualify(**kwargs):
        # Failed resume must release its SQL admission and Home fence before IO.
        assert not entry.lock.locked() and not entry.reconciling and not sql_admissions
        assert provider._lock.acquire(blocking=False)
        provider._lock.release()
        qualifications.append(kwargs)
        if uncertainty == "cancelled":
            entered.set()
            assert release.wait(10), "Test must release its blocked qualification worker"
        if uncertainty == "malformed":
            (owner.pool / ".native-creates" / ("d" * 32 + ".json")).write_text("{}", encoding="utf-8")
        result = real_qualify(**kwargs)
        if uncertainty == "closing-after":
            owner.mark_closing()
        elif uncertainty == "bridge-after":
            provider._native_creation = object()
        elif uncertainty == "deadline-after":
            from app.gateway import resident_runtime

            monkeypatch.setattr(resident_runtime, "time", SimpleNamespace(monotonic=lambda: time.monotonic() + 120))
        drained.set()
        return result

    monkeypatch.setattr(bridge, "qualify", qualify)

    def inspect(command, **kwargs):
        calls.append(command)
        assert command[:2] == ["docker", "inspect"]
        if len(calls) == 1 or uncertainty == "repeated":
            # A complete unrelated ordinary-chat teardown changes the durable
            # revision without changing either resident identity or live task.
            bridge.evidence.begin_teardown(ordinary)
            bridge.teardowns.add(ordinary)
            bridge.evidence.finish_teardown(ordinary)
            bridge.teardowns.remove(ordinary)
            if uncertainty == "closing":
                owner.mark_closing()
            elif uncertainty == "bridge":
                provider._native_creation = object()
            elif uncertainty == "backend":
                provider._backend._native_creation = object()
            elif uncertainty == "daemon":

                def changed_daemon():
                    raise StorageHostUnqualified("Daemon changed")

                monkeypatch.setattr(bridge, "_check_daemon", changed_daemon)
            elif uncertainty == "deadline":
                from app.gateway import resident_runtime

                monkeypatch.setattr(resident_runtime, "time", SimpleNamespace(monotonic=lambda: time.monotonic() + 120))
        identity = "f" * 64 if uncertainty == "graph" and len(calls) > 1 else claim.container_id
        return SimpleNamespace(returncode=0, stdout=json.dumps([{"Id": identity, "Name": "/resident-current", "Created": STAMP}]), stderr="")

    def verify(container, plan):
        real_verify(container, plan)
        if uncertainty == "unknown":
            raise StorageRuntimeUnavailable("Native teardown evidence has no current restored accounting")
        reader = NativeResourceSet(SimpleNamespace(_run=inspect), guard=bridge.assert_current)
        try:
            assert reader._member("container", container, "resident-current", STAMP) is not None
        except Exception as error:
            failures.append(error)
            raise

    monkeypatch.setattr(adapter, "verify_active", verify)
    token = bridge.handoffs.begin_restore() if uncertainty == "restore-owned" else None
    refreshing = None
    try:
        if uncertainty == "cancelled":
            from deerflow.utils.file_io import run_file_io

            refreshing = asyncio.create_task(host.reconcile_rows(deadline=time.monotonic() + 10))
            assert await run_file_io(entered.wait, 5)
            refreshing.cancel()
            await asyncio.sleep(0)
            assert not refreshing.done() and not drained.is_set()
            release.set()
            with pytest.raises(asyncio.CancelledError):
                await refreshing
            assert drained.is_set() and host.accounting_retry
            pending = [claim.id]
        else:
            pending = await module.reconcile_residents(app, attachment_ids=(claim.id,) if uncertainty == "filtered" else None, deadline=time.monotonic() + 10)
        if uncertainty in (None, "filtered"):
            assert pending == [], caplog.text
            assert host.workspace_qualified and bridge.ready
            assert len(qualifications) == 1 and len(failures) == 1
            assert len(calls) == 3  # Failed post-guard, then fresh ID AND name.
            host.registry.replicas = 2
            with pytest.raises(ResidentWorkLimit):
                await host.registry.admit(admitted.instance, work=True)
        else:
            assert pending == [claim.id] and not host.workspace_qualified
            assert len(qualifications) == (0 if uncertainty in {"unknown", "closing", "bridge", "backend", "daemon", "deadline", "snapshot-budget"} else 1)
            if uncertainty == "snapshot-budget":
                assert scans == 2 and len(calls) == 1
        assert not running.done() and not local.ownership_lost
        assert entry.attachment_id == claim.id
        assert host.registry.retained[claim.id].container_id == claim.container_id
        assert host.registry.external_slots() == 1
        assert claim.container_id in adapter.containers
        assert (await attachments.retained_attachment(claim.id)).phase == "active"
    finally:
        release.set()
        if refreshing is not None:
            await asyncio.gather(refreshing, return_exceptions=True)
        provider._native_creation = provider._backend._native_creation = bridge
        if token is not None:
            bridge.handoffs.abort_restore(token)
        running.cancel()
        await asyncio.gather(running, return_exceptions=True)


@pytest.mark.asyncio
async def test_containment_never_replays_through_accounting_refresh(execution, fixture, monkeypatch):
    from deerflow.community.aio_sandbox.native_creation import _NativeAccountingStale

    module, app, host, attachments, adapter, claim = await build(execution, fixture, monkeypatch)
    provider, owner = fixture
    bridge = install(provider, owner)
    assert await module.reconcile_residents(app, deadline=time.monotonic() + 10) == []
    host.workspace_qualified = True
    adapter.containers.pop(claim.container_id)
    containment = AsyncMock(side_effect=_NativeAccountingStale(bridge))
    qualification = Mock(wraps=bridge.qualify)
    monkeypatch.setattr(attachments, "contain_host", containment)
    monkeypatch.setattr(bridge, "qualify", qualification)
    assert await module.reconcile_residents(app, deadline=time.monotonic() + 10) == [claim.id]
    containment.assert_awaited_once_with(claim.id, "container-absent")
    qualification.assert_not_called()
    assert not host.workspace_qualified
    assert host.registry.external_slots() == 1
    assert (await attachments.retained_attachment(claim.id)).phase == "active"
