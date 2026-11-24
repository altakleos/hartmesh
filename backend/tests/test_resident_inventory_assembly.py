"""A resident census observes a coherent published run assembly on both stores."""

import asyncio
from unittest.mock import AsyncMock

import pytest
from _run_output_store_test_support import capture_run
from _run_output_store_test_support import output_store as output_store
from sqlalchemy import event
from sqlalchemy.util import await_only
from test_resident_readiness_refresh import runtime

from deerflow.runtime.assembly_evidence import EVIDENCE_KEY
from deerflow.spaces.runtime_owner import StorageRuntimeUnavailable


@pytest.mark.asyncio
@pytest.mark.parametrize("publication", ["during-census", "after-census-read"])
async def test_census_rereads_assembly_after_its_owner_commits(output_store, monkeypatch, publication):
    repository = output_store.repository
    manager, record = await capture_run(output_store, heartbeat=True, capture=False)
    host, app = runtime(monkeypatch)
    app.state.run_store = repository
    host.registry.run_manager = manager
    record.task = asyncio.current_task()
    written, sampled, commit, deliver = (asyncio.Event() for _ in range(4))
    evidence = {"run_id": record.run_id, "thread_id": record.thread_id, "version": 1}
    list_inflight = repository.list_inflight

    async def capture():
        rows = await list_inflight()
        assert EVIDENCE_KEY not in rows[0]["metadata"]
        sampled.set()
        if publication == "after-census-read":
            await deliver.wait()
        return rows

    def hold_commit(conn, cursor, statement, parameters, context, executemany):
        if statement.lstrip().upper().startswith("UPDATE RUNS SET") and "metadata" in statement.lower():
            written.set()
            await_only(commit.wait())

    event.listen(output_store.engine.sync_engine, "after_cursor_execute", hold_commit)
    monkeypatch.setattr(repository, "list_inflight", capture)
    original_get = repository.get

    async def read_committed(run_id, *, user_id):
        assert publishing.done() and publishing.exception() is None
        return await original_get(run_id, user_id=user_id)

    get = AsyncMock(wraps=read_committed)
    monkeypatch.setattr(repository, "get", get)
    publishing = inventory = None
    try:
        if publication == "after-census-read":
            inventory = asyncio.create_task(host.inventory_runs())
            await asyncio.wait_for(sampled.wait(), 5)
        publishing = asyncio.create_task(manager.record_assembly_evidence(record.run_id, evidence))
        await asyncio.wait_for(written.wait(), 5)
        if publication == "during-census":
            inventory = asyncio.create_task(host.inventory_runs())
            await asyncio.wait_for(sampled.wait(), 5)
        commit.set()
        await asyncio.wait_for(publishing, 5)
        deliver.set()
        rows = await asyncio.wait_for(inventory, 5)
        assert host.foreign_run_blocks == frozenset() and host.workspace_qualified
        assert rows[0]["metadata"] == record.metadata and rows[0]["metadata"][EVIDENCE_KEY] == evidence
        get.assert_awaited_once_with(record.run_id, user_id=None)
        monkeypatch.setattr(repository, "list_inflight", list_inflight)
        assert await host.inventory_runs() == rows
        assert get.await_count == 1
    finally:
        commit.set()
        deliver.set()
        await asyncio.gather(*(task for task in (publishing, inventory) if task is not None), return_exceptions=True)
        event.remove(output_store.engine.sync_engine, "after_cursor_execute", hold_commit)


@pytest.mark.asyncio
@pytest.mark.parametrize("outcome", ["tampered", "owner-lost", "worker-changed", "terminal", "missing", "unreadable"])
async def test_assembly_reread_never_qualifies_unproven_owners(output_store, monkeypatch, outcome):
    repository = output_store.repository
    manager, record = await capture_run(output_store, heartbeat=True, capture=False)
    host, app = runtime(monkeypatch)
    host.registry.run_manager = manager
    app.state.run_store = repository
    record.task = asyncio.current_task()
    stale = await repository.list_inflight()
    await manager.record_assembly_evidence(record.run_id, {"run_id": record.run_id, "thread_id": record.thread_id})
    monkeypatch.setattr(repository, "list_inflight", AsyncMock(return_value=stale))
    get = repository.get

    async def reread(run_id, *, user_id):
        fresh = await get(run_id, user_id=user_id)
        if outcome == "tampered":
            fresh["metadata"]["unrelated"] = "different"
        elif outcome == "owner-lost":
            record.ownership_lost = True
        elif outcome == "worker-changed":
            fresh["owner_worker_id"] = "another-worker"
        elif outcome == "terminal":
            fresh["status"] = "success"
        elif outcome == "missing":
            return None
        else:
            raise OSError("read failed")
        return fresh

    monkeypatch.setattr(repository, "get", reread)
    if outcome == "unreadable":
        with pytest.raises(StorageRuntimeUnavailable):
            await host.inventory_runs()
    else:
        assert await host.inventory_runs() == stale
        assert host.foreign_run_blocks == {record.run_id}
    assert not host.workspace_qualified and host.accounting_retry


@pytest.mark.asyncio
@pytest.mark.parametrize("replaced", ["manager", "store"])
async def test_census_refuses_a_new_binding_even_with_no_inflight_rows(monkeypatch, replaced):
    host, app = runtime(monkeypatch)

    async def list_inflight():
        if replaced == "manager":
            host.registry.run_manager = object()
        else:
            app.state.run_store = object()
        return []

    store = AsyncMock()
    store.list_inflight.side_effect = list_inflight
    app.state.run_store = store
    with pytest.raises(StorageRuntimeUnavailable):
        await host.inventory_runs()
    assert not host.workspace_qualified and host.accounting_retry
