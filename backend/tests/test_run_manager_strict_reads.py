"""Proof reads require authority without changing ordinary lifecycle caching."""

import asyncio
from copy import deepcopy
from uuid import UUID

import pytest

from deerflow.runtime.runs import manager as module
from deerflow.runtime.runs.manager import RunManager
from deerflow.runtime.runs.store.memory import MemoryRunStore

OWNER = "real-owner"
THREAD = "original-thread"


@pytest.mark.asyncio
@pytest.mark.parametrize("method", ["get", "aget", "list_by_thread"])
@pytest.mark.parametrize("positive", [False, True])
async def test_missing_store_is_refused_before_cache_or_absence(method, positive):
    manager = RunManager()
    record = await manager.create(THREAD, user_id=OWNER) if positive else None
    key = THREAD if method == "list_by_thread" else record.run_id if record else "missing"
    with pytest.raises(module.RunStoreUnavailable):
        await getattr(manager, method)(key, user_id=OWNER, require_store=True, raise_on_store_error=True)


@pytest.mark.asyncio
@pytest.mark.parametrize("method", ["get", "aget", "list_by_thread"])
@pytest.mark.parametrize("positive", [False, True])
async def test_ordinary_no_store_and_raise_only_lifecycle_behavior_remains(method, positive):
    manager = RunManager()
    record = await manager.create(THREAD, user_id=OWNER) if positive else None
    key = THREAD if method == "list_by_thread" else record.run_id if record else "missing"
    kwargs = {"raise_on_store_error": True} if method != "list_by_thread" else {}
    result = await getattr(manager, method)(key, user_id=OWNER, **kwargs)
    assert result == ([record] if positive else []) if method == "list_by_thread" else result is record


@pytest.mark.asyncio
@pytest.mark.parametrize("method", ["get", "aget"])
async def test_configured_cached_positive_is_not_a_store_health_probe(method, monkeypatch):
    store = MemoryRunStore()
    manager = RunManager(store=store)
    record = await manager.create(THREAD, user_id=OWNER)

    async def failed(*args, **kwargs):
        pytest.fail("A configured current-process positive does not query store health")

    monkeypatch.setattr(store, "get", failed)
    assert await getattr(manager, method)(record.run_id, user_id=OWNER, require_store=True, raise_on_store_error=True) is record


@pytest.mark.asyncio
async def test_strict_page_never_falls_back_to_only_cached_records(monkeypatch):
    store = MemoryRunStore()
    manager = RunManager(store=store)
    record = await manager.create(THREAD, user_id=OWNER)

    async def failed(*args, **kwargs):
        raise RuntimeError("controlled store failure")

    monkeypatch.setattr(store, "list_by_thread", failed)
    with pytest.raises(RuntimeError, match="controlled store failure"):
        await manager.list_by_thread(THREAD, user_id=OWNER, require_store=True, raise_on_store_error=True)
    assert await manager.list_by_thread(THREAD, user_id=OWNER) == [record]


@pytest.mark.asyncio
async def test_strict_page_maps_duplicate_rows_before_local_precedence():
    store = MemoryRunStore()
    manager = RunManager(store=store)
    record = await manager.create(THREAD, user_id=OWNER)
    store._runs[record.run_id]["status"] = "invalid-store-status"
    with pytest.raises(ValueError):
        await manager.list_by_thread(THREAD, user_id=OWNER, require_store=True, raise_on_store_error=True)
    assert await manager.list_by_thread(THREAD, user_id=OWNER) == [record]


@pytest.mark.asyncio
async def test_strict_page_does_not_skip_a_missing_id_row(monkeypatch):
    store = MemoryRunStore()
    manager = RunManager(store=store)

    async def malformed(*args, **kwargs):
        return [{"thread_id": THREAD, "status": "success", "kwargs": {}}]

    monkeypatch.setattr(store, "list_by_thread", malformed)
    with pytest.raises(KeyError):
        await manager.list_by_thread(THREAD, user_id=OWNER, require_store=True, raise_on_store_error=True)
    assert await manager.list_by_thread(THREAD, user_id=OWNER) == []


@pytest.mark.asyncio
async def test_aget_keeps_legacy_get_override_signature():
    class LegacyManager(RunManager):
        async def get(self, run_id, *, user_id=None, raise_on_store_error=False):
            return (run_id, user_id, raise_on_store_error)

    assert await LegacyManager().aget("legacy", user_id=OWNER, raise_on_store_error=True) == ("legacy", OWNER, True)


@pytest.mark.asyncio
@pytest.mark.parametrize("strict", [False, True])
@pytest.mark.parametrize("result_kind", ["valid", "malformed", "absent"])
async def test_hydration_race_preserves_local_ownership_but_cannot_hide_observed_mapping_failure(monkeypatch, strict, result_kind):
    identity = "00000000-0000-0000-0000-000000000123"
    entered, release = asyncio.Event(), asyncio.Event()
    store = MemoryRunStore()
    await store.put(identity, thread_id=THREAD, user_id=OWNER, status="success")
    snapshot = deepcopy(await store.get(identity))
    if result_kind == "malformed":
        snapshot["status"] = "invalid-store-status"
    elif result_kind == "absent":
        snapshot = None

    async def delayed(*args, **kwargs):
        entered.set()
        await release.wait()
        return snapshot

    monkeypatch.setattr(store, "get", delayed)
    manager = RunManager(store=store)
    flags = {"require_store": True} if strict else {}
    reading = asyncio.create_task(manager.get(identity, user_id=OWNER, raise_on_store_error=True, **flags))
    try:
        await asyncio.wait_for(entered.wait(), 2)
        with monkeypatch.context() as scoped:
            scoped.setattr(module.uuid, "uuid4", lambda: UUID(identity))
            local = await manager.create(THREAD, user_id=OWNER)
        release.set()
        if strict and result_kind == "malformed":
            with pytest.raises(ValueError):
                await reading
        else:
            assert await reading is local
        assert manager._runs[identity] is local
    finally:
        release.set()
        if not reading.done():
            reading.cancel()
        await asyncio.gather(reading, return_exceptions=True)
