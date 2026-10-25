"""The actual SDK batch Future resolves only after its real delegate returns."""

from __future__ import annotations

import asyncio

import pytest
from test_capture_resource_facades import native_resources as native_resources
from test_capture_resource_facades import provided_resources as provided_resources
from test_capture_resource_facades import resource_backend as resource_backend


@pytest.mark.asyncio
async def test_raw_store_accepted_batch_future_waits_for_native_delegate(native_resources, monkeypatch):
    """Compatibility control; it makes no capture ownership claim."""
    env = native_resources
    entered, release, completed = asyncio.Event(), asyncio.Event(), asyncio.Event()
    original = type(env.store).abatch
    batches = []

    async def held(store, ops):
        if store is not env.store:
            return await original(store, ops)
        ops = list(ops)
        batches.append(ops)
        entered.set()
        await release.wait()
        result = await original(store, ops)
        completed.set()
        return result

    monkeypatch.setattr(type(env.store), "abatch", held)
    tasks = [asyncio.create_task(env.store.aput(("batch",), str(i), {"value": i}, index=False)) for i in range(3)]
    try:
        await asyncio.wait_for(entered.wait(), 3)
        done, _ = await asyncio.wait(tasks, timeout=0.02)
        assert not done and not completed.is_set()
        # Real AsyncBatchedBaseStore convenience methods share one native batch;
        # Memory's BaseStore implementation admits one batch per convenience.
        if env.kind != "memory":
            assert len(batches) == 1 and len(batches[0]) == 3
        release.set()
        await asyncio.wait_for(asyncio.gather(*tasks), 3)
        assert completed.is_set()
        items = [await env.store.aget(("batch",), str(i)) for i in range(3)]
        assert [item.value for item in items] == [{"value": i} for i in range(3)]
    finally:
        release.set()
        await asyncio.gather(*tasks, return_exceptions=True)


@pytest.mark.asyncio
@pytest.mark.parametrize("resource_backend", ["sqlite", "postgres"], indirect=True)
async def test_accepted_shared_batch_retains_cancelled_capture_and_ordinary_neighbor(provided_resources, monkeypatch):
    from deerflow.runtime.runs.capture_io import CaptureIOOwner, capture_io_scope

    env = provided_resources
    left, right = CaptureIOOwner(), CaptureIOOwner()
    entered, release, completed = asyncio.Event(), asyncio.Event(), asyncio.Event()
    original = type(env.native_store).abatch
    batches = []

    async def batch(store, ops):
        if store is not env.native_store:
            return await original(store, ops)
        values = list(ops)
        batches.append(values)
        entered.set()
        await release.wait()
        result = await original(store, values)
        completed.set()
        return result

    async def put(owner, number):
        with capture_io_scope(owner):
            await env.store.aput(("shared-capture",), str(number), {"number": number}, index=False)

    monkeypatch.setattr(type(env.native_store), "abatch", batch)
    tasks = [asyncio.create_task(put(left, 1)), asyncio.create_task(put(right, 2)), asyncio.create_task(put(None, 3))]
    draining = None
    try:
        await asyncio.wait_for(entered.wait(), 3)
        assert len(batches) == 1 and len(batches[0]) == 3
        left.retire()
        tasks[0].cancel()
        done_first, _ = await asyncio.wait({tasks[0]}, timeout=0.05)
        tasks[0].cancel()
        draining = asyncio.create_task(left.drain())
        done, _ = await asyncio.wait({tasks[0], draining}, timeout=0.05, return_when=asyncio.FIRST_COMPLETED)
        retained = not done_first and not done and not completed.is_set()
        release.set()
        outcomes = await asyncio.wait_for(asyncio.gather(*tasks, return_exceptions=True), 3)
        await asyncio.wait_for(draining, 3)
        assert completed.is_set()
        assert isinstance(outcomes[0], asyncio.CancelledError) and outcomes[1:] == [None, None]
        assert [item.value for item in [await env.store.aget(("shared-capture",), str(i)) for i in (1, 2, 3)]] == [{"number": 1}, {"number": 2}, {"number": 3}]
        assert retained, "accepted SDK Future/native batch must outlive cancellation and retirement"
    finally:
        release.set()
        await asyncio.gather(*tasks, *(task for task in [draining] if task is not None), return_exceptions=True)
        left.retire()
        right.retire()
        await left.drain()
        await right.drain()
