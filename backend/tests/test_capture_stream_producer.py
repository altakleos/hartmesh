"""A per-run producer retains effects without closing the shared bridge."""

import asyncio
import importlib
import importlib.util

import pytest

from deerflow.runtime import END_SENTINEL, MemoryStreamBridge
from deerflow.runtime.runs.capture_io import CaptureIOClosed, CaptureIOOwner, CaptureIOUnsupported, current_capture_io_owner

MODULE = "deerflow.runtime.stream_bridge.capture"


def test_producer_api_availability_is_new_feature_scaffolding():
    assert importlib.util.find_spec(MODULE) is not None, "New producer-view API is absent; this is feature scaffolding"


@pytest.fixture
def producer():
    return importlib.import_module(MODULE).capture_stream_producer


class ObservedMemoryBridge(MemoryStreamBridge):
    def __init__(self):
        super().__init__(heartbeat_interval=0.1)
        self.close_calls = 0
        self.cleanup_calls = []

    async def close(self):
        self.close_calls += 1
        await super().close()

    async def cleanup(self, run_id, *, delay=0):
        self.cleanup_calls.append((run_id, delay))
        await super().cleanup(run_id, delay=delay)


@pytest.mark.asyncio
async def test_ordinary_producer_keeps_exact_bridge_identity(producer):
    bridge = ObservedMemoryBridge()
    assert producer(bridge, None) is bridge
    await bridge.publish("run", "values", {"value": 37})
    await bridge.publish_end("run")
    assert len([entry async for entry in bridge.subscribe("run")]) == 2


@pytest.mark.asyncio
async def test_retired_producer_refuses_effects_but_subscribers_keep_shared_history(producer):
    owner, bridge = CaptureIOOwner(), ObservedMemoryBridge()
    view = producer(bridge, owner)
    try:
        assert view.heartbeat_interval == bridge.heartbeat_interval
        assert view.supports_cross_process == bridge.supports_cross_process
        await view.publish("run", "values", {"value": 37})
        await view.publish_end("run")
        owner.retire()
        await owner.drain()
        with pytest.raises(CaptureIOClosed):
            await view.publish("run", "values", {"value": "late"})
        with pytest.raises(CaptureIOClosed):
            await view.publish_end("run")
        events = [entry async for entry in bridge.subscribe("run")]
        assert events[0].data == {"value": 37} and events[-1] is END_SENTINEL
        assert len(events) == 2 and bridge.close_calls == 0
    finally:
        owner.retire()
        await owner.drain()


@pytest.mark.asyncio
async def test_producer_reuse_keeps_original_owner_and_cannot_rebind(producer):
    first, second, bridge = CaptureIOOwner(), CaptureIOOwner(), ObservedMemoryBridge()
    try:
        view = producer(bridge, first)
        assert producer(view, first) is view
        with pytest.raises(CaptureIOUnsupported):
            producer(view, second)
        first.retire()
        with pytest.raises(CaptureIOClosed):
            await view.publish_end("run")
    finally:
        first.retire()
        second.retire()
        await asyncio.gather(first.drain(), second.drain())


@pytest.mark.asyncio
async def test_started_publish_drains_after_waiter_cancel_and_owner_retirement(producer):
    owner = CaptureIOOwner()
    entered, release, finished, delegate_cancelled = (asyncio.Event() for _ in range(4))

    class HeldBridge(ObservedMemoryBridge):
        async def publish(self, *args, **kwargs):
            assert current_capture_io_owner() is owner
            entered.set()
            try:
                await release.wait()
                await super().publish(*args, **kwargs)
                finished.set()
            except asyncio.CancelledError:
                delegate_cancelled.set()
                raise

    bridge = HeldBridge()
    view = producer(bridge, owner)
    pending, draining = asyncio.create_task(view.publish("run", "values", {"value": 37})), None
    try:
        await asyncio.wait_for(entered.wait(), 3)
        pending.cancel()
        await asyncio.wait({pending}, timeout=0.01)
        pending.cancel()
        owner.retire()
        draining = asyncio.create_task(owner.drain())
        done, _ = await asyncio.wait({pending, draining}, timeout=0.01)
        assert not done and not delegate_cancelled.is_set() and not finished.is_set()
        release.set()
        result, drained = await asyncio.gather(pending, draining, return_exceptions=True)
        assert isinstance(result, asyncio.CancelledError) and drained is None
        assert finished.is_set() and not delegate_cancelled.is_set() and bridge.close_calls == 0
        await bridge.publish_end("run")
        assert len([entry async for entry in bridge.subscribe("run")]) == 2
    finally:
        release.set()
        owner.retire()
        await asyncio.gather(*(task for task in (pending, draining) if task is not None), return_exceptions=True)
        await owner.drain()


@pytest.mark.asyncio
async def test_view_close_retires_only_producer_without_closing_shared_client(producer):
    owner, bridge = CaptureIOOwner(), ObservedMemoryBridge()
    view = producer(bridge, owner)
    try:
        await view.close()
        assert not owner.retired and bridge.close_calls == 0
        with pytest.raises(CaptureIOClosed):
            await view.publish_end("run")
        await bridge.publish_end("run")
        assert [entry async for entry in bridge.subscribe("run")] == [END_SENTINEL]
    finally:
        owner.retire()
        await owner.drain()


@pytest.mark.asyncio
async def test_capture_cleanup_requires_the_separate_service_owner(producer):
    owner, bridge = CaptureIOOwner(), ObservedMemoryBridge()
    view = producer(bridge, owner)
    try:
        with pytest.raises(CaptureIOUnsupported):
            await view.cleanup("run", delay=60)
        assert bridge.cleanup_calls == []
    finally:
        owner.retire()
        await owner.drain()


@pytest.mark.asyncio
@pytest.mark.parametrize("end", [False, True])
async def test_explicit_view_close_fences_an_owner_child_that_has_not_invoked_bridge(producer, end):
    owner, bridge = CaptureIOOwner(), ObservedMemoryBridge()
    calls = []
    original = bridge.publish_end if end else bridge.publish

    async def observed(*args, **kwargs):
        calls.append(args)
        await original(*args, **kwargs)

    setattr(bridge, "publish_end" if end else "publish", observed)
    view = producer(bridge, owner)
    pending = asyncio.create_task(view.publish_end("run") if end else view.publish("run", "values", {}))
    try:
        await asyncio.sleep(0)
        await view.close()
        with pytest.raises(CaptureIOClosed):
            await pending
        assert calls == [] and not owner.retired and bridge.close_calls == 0
    finally:
        owner.retire()
        await asyncio.gather(pending, return_exceptions=True)
        await owner.drain()
