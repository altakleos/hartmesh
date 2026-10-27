"""Reconcile real SQL preparation transitions without a Docker daemon."""

import asyncio
import time
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from sqlalchemy import select
from test_gateway_resident_reconciliation import build
from test_gateway_resident_reconciliation import execution as execution
from test_gateway_resident_reconciliation import fixture as fixture
from test_gateway_resident_reconciliation import instances as instances
from test_gateway_resident_reconciliation import work as work

from deerflow.agent_instances.bootstrap import BootstrapResult
from deerflow.persistence.spaces.lifecycle import SpaceAttachmentRow, SpaceMountRow
from deerflow.spaces.runtime_owner import StorageRuntimeUnavailable

NORMAL_PROGRESS_TIMEOUT_SECONDS = 10


@pytest.mark.asyncio
@pytest.mark.parametrize("outcome", ["during", "before-lock", "busy", "unreturned", "marker-retained", "actor", "incarnation", "host", "mount", "cid", "foreign", "deadline", "cancelled", "inspection"])
async def test_preparation_snapshot_waits_for_exact_completion_before_readiness(execution, fixture, monkeypatch, outcome):
    module, app, host, attachments, adapter, original = await build(execution, fixture, monkeypatch)
    admitted = execution[1]
    entry = host.registry.entry(admitted.instance)
    host.registry.retained.clear()
    async with execution[3]() as session, session.begin():
        row = await session.get(SpaceAttachmentRow, original.id)
        row.phase, row.container_id = "pending", None
    await entry.lock.acquire()
    preparation = host.registry.preparing(admitted.instance, incarnation=original.incarnation, generation=admitted.home_generation, host_id=original.host_id)
    host.registry.slots.add(entry.home_id)
    host.workspace_qualified = True
    host.coordinator.result = BootstrapResult(True)
    host._startup_settled = True
    host.apply_hold = AsyncMock()
    app.state.chat_storage_host = SimpleNamespace(admission=host.admission)
    monkeypatch.setattr(module, "storage_state", AsyncMock(return_value=module.StorageState("ready")))
    monkeypatch.setattr(module, "get_agents_api_config", lambda: SimpleNamespace(enabled=True, ai_assistants_per_user=1, ai_employees=1))
    monkeypatch.setattr(module, "hold_present", lambda: False)
    running = None
    if outcome == "busy":
        running = asyncio.create_task(asyncio.Event().wait())
        record = SimpleNamespace(
            run_id="locally-owned-preparing-run",
            thread_id=admitted.thread_id,
            user_id="alice",
            owner_worker_id="local-worker",
            metadata={"agent_instance_id": admitted.instance.id},
            task=running,
            ownership_lost=False,
            created_at="2026-10-16T00:00:00Z",
        )
        app.state.run_manager._runs[record.run_id] = record
        app.state.run_store.list_inflight.return_value = [{"run_id": record.run_id, "thread_id": record.thread_id, "user_id": record.user_id, "owner_worker_id": record.owner_worker_id, "metadata": record.metadata}]
    captured, release_snapshot, entering = asyncio.Event(), asyncio.Event(), asyncio.Event()
    snapshots = attachments.retained_claims
    registry_entry = host.registry.entry

    async def snapshot():
        claims = await snapshots()
        captured.set()
        await release_snapshot.wait()
        return claims

    def entering_home(identity):
        result = registry_entry(identity)
        entering.set()
        return result

    monkeypatch.setattr(attachments, "retained_claims", snapshot)
    monkeypatch.setattr(host.registry, "entry", entering_home)
    deadline = time.monotonic() + (0.3 if outcome == "deadline" else 10)
    progress_timeout = 2 if outcome == "deadline" else NORMAL_PROGRESS_TIMEOUT_SECONDS
    # Use the real publication path, not an isolated helper that never changes
    # readiness. Every physical provider operation remains an offline double.
    refreshing = asyncio.create_task(host.reconcile()) if outcome != "deadline" else asyncio.create_task(module.reconcile_residents(app, deadline=deadline))
    waiting = None
    try:
        await asyncio.wait_for(captured.wait(), progress_timeout)
        if outcome != "before-lock":
            release_snapshot.set()
            waiting = asyncio.create_task(entering.wait())
            done, _ = await asyncio.wait((refreshing, waiting), timeout=progress_timeout, return_when=asyncio.FIRST_COMPLETED)
            assert done, "reconciliation did not reach the preparation"
        if outcome == "cancelled":
            refreshing.cancel()
            with pytest.raises(asyncio.CancelledError):
                await refreshing
        elif outcome == "deadline":
            assert await asyncio.wait_for(refreshing, 2) == [original.id]
            assert time.monotonic() < deadline + 1
        else:
            if outcome != "unreturned":
                async with execution[3]() as session, session.begin():
                    row = await session.get(SpaceAttachmentRow, original.id)
                    row.phase, row.container_id = "active", original.container_id
                if outcome != "marker-retained":
                    assert host.registry.preparation_retained(admitted.instance, preparation, await attachments.retained_attachment(original.id))
            if outcome in {"actor", "incarnation", "host", "mount", "cid"}:
                async with execution[3]() as session, session.begin():
                    row = await session.get(SpaceAttachmentRow, original.id)
                    if outcome == "mount":
                        (await session.scalar(select(SpaceMountRow).where(SpaceMountRow.attachment_id == original.id))).alias = "changed"
                    else:
                        field = {"actor": "actor_id", "incarnation": "incarnation", "host": "host_id", "cid": "container_id"}[outcome]
                        setattr(row, field, "f" * (64 if outcome == "cid" else 32))
            elif outcome == "foreign":
                app.state.run_store.list_inflight.return_value = [{"run_id": "foreign-live", "metadata": {"agent_instance_id": admitted.instance.id}, "owner_worker_id": "foreign-worker"}]
            elif outcome == "inspection":
                monkeypatch.setattr(attachments, "inspect_preparation", AsyncMock(side_effect=OSError("unknown preparation")))
            entry.lock.release()
            release_snapshot.set()
            if outcome == "cid":
                with pytest.raises(ValueError, match="Retained attachment changed immutable identity"):
                    await asyncio.wait_for(refreshing, 3)
                result = False
                assert host.registry.retained[original.id].container_id == original.container_id
            else:
                result = await asyncio.wait_for(refreshing, 3)
            if outcome in {"during", "before-lock", "busy"}:
                assert result is True
                assert (await module.execution_state(app, admitted.instance.id)).execution == "ready"
                await module.require_execution(app, admitted.instance.id)
                assert entry.attachment_id == original.id and entry.preparation is None
            else:
                assert result is False
        if outcome not in {"during", "before-lock", "busy"}:
            assert host.workspace_qualified is False
        if outcome == "cid":
            assert host.registry.restoration_complete is False
            assert len(host.registry.retained) == 1 and entry.home_id in host.registry.slots
            with pytest.raises(StorageRuntimeUnavailable):
                host.registry.external_slots()
        else:
            # Unreturned preparations cannot deduplicate a retained SQL claim.
            assert host.registry.external_slots() >= 1
            if outcome not in {"unreturned", "marker-retained", "deadline", "cancelled"}:
                assert host.registry.external_slots() == 1
        assert len(adapter.started) == len(adapter.containers) == 1
        assert original.container_id in adapter.containers
        assert entry.reconciling is False
    finally:
        release_snapshot.set()
        if entry.lock.locked():
            entry.lock.release()
        for task in (refreshing, waiting, running):
            if task is not None and not task.done():
                task.cancel()
        await asyncio.gather(*(task for task in (refreshing, waiting, running) if task is not None), return_exceptions=True)
