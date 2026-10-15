"""A captured attachment must not invalidate its qualified live replacement."""

import asyncio
import time
from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from _storage_spaces_test_support import operation
from sqlalchemy import delete
from test_gateway_resident_reconciliation import build
from test_gateway_resident_reconciliation import execution as execution
from test_gateway_resident_reconciliation import fixture as fixture
from test_gateway_resident_reconciliation import instances as instances
from test_gateway_resident_reconciliation import work as work

from deerflow.persistence.spaces.lifecycle import SpaceAttachmentRow, SpaceMountRow
from deerflow.sandbox.exceptions import SandboxRuntimeAdmissionError
from deerflow.spaces.attachments import ResourceMount


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "uncertainty",
    [
        None,
        "delayed-snapshot",
        "retained",
        "associated",
        "preparation",
        "uncontained",
        "missing",
        "cid",
        "actor",
        "host",
        "mounts",
        "closure",
        "replacement-gone",
        "filtered",
        "deadline",
        "duplicate",
        "foreign",
        "second-change",
        "cancelled",
    ],
)
async def test_waiting_refresh_does_not_treat_the_fenced_snapshot_as_its_running_replacement(execution, fixture, monkeypatch, caplog, uncertainty):
    module, app, host, attachments, adapter, old = await build(execution, fixture, monkeypatch)
    admitted = execution[1]
    assert await module.reconcile_residents(app, deadline=time.monotonic() + 10) == []
    host.workspace_qualified = True
    old_claim = host.registry.retained[old.id]
    entry = host.registry.entry(admitted.instance)
    await entry.lock.acquire()
    entered = asyncio.Event()
    original = host.registry.entry
    release_snapshot = asyncio.Event()
    second_snapshot = asyncio.Event()

    if uncertainty in ("delayed-snapshot", "second-change"):
        original_snapshot = attachments.retained_claims
        snapshots = 0

        async def delayed_snapshot():
            nonlocal snapshots
            result = await original_snapshot()
            snapshots += 1
            if snapshots == 1 and uncertainty == "delayed-snapshot":
                entered.set()
                await release_snapshot.wait()
            elif snapshots == 2 and uncertainty == "second-change":
                second_snapshot.set()
                await release_snapshot.wait()
            return result

        monkeypatch.setattr(attachments, "retained_claims", delayed_snapshot)

    def entering(identity):
        result = original(identity)
        entered.set()
        return result

    monkeypatch.setattr(host.registry, "entry", entering)
    refreshing = asyncio.create_task(module.reconcile_residents(app, attachment_ids=(old.id,) if uncertainty == "filtered" else None, deadline=time.monotonic() + 10))
    running = None
    try:
        await asyncio.wait_for(entered.wait(), 2)
        assert await attachments.contain_host(old.id, "container-absent") is True
        replacement = await attachments.attach(actor=admitted.instance.principal, incarnation=operation(), resources=(ResourceMount(admitted.instance.home_id, admitted.home_generation, "home", True),))
        fresh = await attachments.retained_attachment(replacement.id)
        host.registry.retain(fresh)
        host.registry.associate(admitted.instance, fresh, generation=admitted.home_generation)
        running = asyncio.create_task(asyncio.Event().wait())
        record = SimpleNamespace(
            run_id="locally-owned-replacement-run",
            thread_id=admitted.thread_id,
            user_id="alice",
            owner_worker_id="local-worker",
            metadata={"agent_instance_id": admitted.instance.id},
            task=running,
            ownership_lost=False,
            created_at="2026-10-15T00:00:00Z",
        )
        app.state.run_manager._runs[record.run_id] = record
        app.state.run_store.list_inflight.return_value = [{"run_id": record.run_id, "thread_id": record.thread_id, "user_id": record.user_id, "owner_worker_id": record.owner_worker_id, "metadata": record.metadata}]
        if uncertainty == "retained":
            host.registry.retain(old_claim)
        elif uncertainty == "associated":
            host.registry._associated[admitted.instance.home_id] = old.id
        elif uncertainty == "preparation":
            entry.preparation = SimpleNamespace()
        elif uncertainty == "uncontained":
            attachments.uncontained[old.id] = "outcome-uncertain"
        elif uncertainty in {"missing", "cid", "actor", "host", "mounts", "closure"}:
            async with execution[3]() as session, session.begin():
                row = await session.get(SpaceAttachmentRow, old.id)
                if uncertainty == "missing":
                    await session.execute(delete(SpaceMountRow).where(SpaceMountRow.attachment_id == old.id))
                    await session.delete(row)
                elif uncertainty == "mounts":
                    from sqlalchemy import select

                    (await session.scalar(select(SpaceMountRow).where(SpaceMountRow.attachment_id == old.id))).alias = "changed"
                else:
                    attribute = {"cid": "container_id", "actor": "actor_id", "host": "host_id", "closure": "fence_reason"}[uncertainty]
                    setattr(row, attribute, "f" * 64 if uncertainty == "cid" else None if uncertainty == "closure" else "changed")
        elif uncertainty == "replacement-gone":
            adapter.containers.pop(replacement.container_id)
        elif uncertainty == "deadline":
            from app.gateway import resident_runtime

            monkeypatch.setattr(resident_runtime, "time", SimpleNamespace(monotonic=lambda: time.monotonic() + 120))
        elif uncertainty == "duplicate":
            duplicate = await attachments.attach(actor=admitted.instance.principal, incarnation=operation(), resources=fresh.resources)
            host.registry.retain(await attachments.retained_attachment(duplicate.id))
        elif uncertainty == "foreign":
            app.state.run_store.list_inflight.return_value[0]["owner_worker_id"] = "foreign-worker"
        elif uncertainty == "cancelled":
            refreshing.cancel()
        if uncertainty != "second-change":
            release_snapshot.set()
        entry.lock.release()
        if uncertainty == "second-change":
            await asyncio.wait_for(second_snapshot.wait(), 2)
            async with entry.lock:
                running.cancel()
                await asyncio.gather(running, return_exceptions=True)
                app.state.run_store.list_inflight.return_value = []
                assert await attachments.contain_host(replacement.id, "container-absent") is True
                replacement = await attachments.attach(actor=admitted.instance.principal, incarnation=operation(), resources=fresh.resources)
                fresh = await attachments.retained_attachment(replacement.id)
                host.registry.retain(fresh)
                host.registry.associate(admitted.instance, fresh, generation=admitted.home_generation)
                app.state.run_manager._runs.pop(record.run_id)
                record.run_id = operation()
                running = record.task = asyncio.create_task(asyncio.Event().wait())
                app.state.run_manager._runs[record.run_id] = record
                app.state.run_store.list_inflight.return_value = [{"run_id": record.run_id, "thread_id": record.thread_id, "user_id": record.user_id, "owner_worker_id": record.owner_worker_id, "metadata": record.metadata}]
            release_snapshot.set()
        if uncertainty == "cancelled":
            with pytest.raises(asyncio.CancelledError):
                await refreshing
            pending = []
        else:
            pending = await refreshing
        if uncertainty in (None, "delayed-snapshot", "cancelled"):
            assert pending == [], caplog.text
            assert host.workspace_qualified is True
            assert old.id not in host.registry.retained
        else:
            assert pending and host.workspace_qualified is False
        assert bool(host.foreign_run_blocks) is (uncertainty == "foreign")
        assert entry.attachment_id == replacement.id
        assert (replacement.container_id in adapter.containers) is (uncertainty != "replacement-gone")
        assert old.container_id not in adapter.containers
        async with execution[3]() as session:
            if uncertainty != "missing":
                assert (await session.get(SpaceAttachmentRow, old.id)).phase == "fenced"
            assert (await session.get(SpaceAttachmentRow, replacement.id)).phase == "active"
    finally:
        release_snapshot.set()
        entry.preparation = None
        if entry.lock.locked():
            entry.lock.release()
        if running is not None:
            running.cancel()
            await asyncio.gather(running, return_exceptions=True)
        await asyncio.gather(refreshing, return_exceptions=True)


@pytest.mark.asyncio
async def test_failed_snapshot_inspection_preserves_a_previously_uncounted_claim(execution, fixture, monkeypatch):
    module, _app, host, attachments, adapter, claim = await build(execution, fixture, monkeypatch)
    host.registry.retained.clear()
    monkeypatch.setattr(attachments, "inspect_preparation", AsyncMock(side_effect=OSError("offline uncertain inspection")))
    assert await module.reconcile_residents(host.app, deadline=time.monotonic() + 10) == [claim.id]
    assert claim.id in host.registry.retained and host.registry.external_slots() == 1
    assert claim.container_id in adapter.containers


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", ["query", "config"])
async def test_failure_before_claim_processing_closes_accounting_for_restoration(execution, fixture, monkeypatch, failure):
    module, app, host, _attachments, adapter, claim = await build(execution, fixture, monkeypatch)
    host.registry.retained.clear()
    host.provider.external_changed(lambda: host.admission.open(host.provider))
    error = RuntimeError("offline post-snapshot lookup failed")
    if failure == "query":
        factory = app.state.agent_instances._sf

        @asynccontextmanager
        async def failed_query():
            async with factory() as session:
                monkeypatch.setattr(session, "execute", AsyncMock(side_effect=error))
                yield session

        monkeypatch.setattr(app.state.agent_instances, "_sf", failed_query)
    else:

        def failed_config():
            raise error

        monkeypatch.setattr(module, "get_agents_api_config", failed_config)
    with pytest.raises(RuntimeError, match="offline post-snapshot lookup failed"):
        await module.reconcile_residents(app, deadline=time.monotonic() + 10)
    assert host.accounting_retry is True
    with pytest.raises(SandboxRuntimeAdmissionError):
        host.admission.require(host.provider)
    assert claim.container_id in adapter.containers
    async with execution[3]() as session:
        assert (await session.get(SpaceAttachmentRow, claim.id)).phase == "active"
