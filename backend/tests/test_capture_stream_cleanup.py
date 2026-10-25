"""Application stream cleanup owns retention separately from started deletes."""

import asyncio
import importlib
import importlib.util
from contextvars import ContextVar

import pytest

from deerflow.runtime.runs.capture_io import CaptureIOClosed, CaptureIOOwner, capture_io_scope, current_capture_io_owner

MODULE = "app.gateway.stream_cleanup"


def test_registry_api_is_new_feature_scaffolding():
    assert importlib.util.find_spec(MODULE) is not None, "New application cleanup owner is absent; API scaffolding"


@pytest.fixture
def registry_type():
    return importlib.import_module(MODULE).StreamCleanupRegistry


class Bridge:
    def __init__(self):
        self.calls = []
        self.entered, self.release = asyncio.Event(), asyncio.Event()
        self.finished = False
        self.cancelled = False

    async def cleanup(self, run_id, *, delay=0):
        self.calls.append((run_id, delay))
        self.entered.set()
        try:
            await self.release.wait()
            self.finished = True
        except asyncio.CancelledError:
            self.cancelled = True
            raise


@pytest.mark.asyncio
async def test_shutdown_cancels_only_unstarted_retention_and_refuses_late_task(registry_type, monkeypatch):
    bridge = Bridge()
    registry = registry_type(bridge)
    assert registry.schedule("delayed", 60) is None
    await asyncio.sleep(0)
    await registry.close()
    assert bridge.calls == []
    created = []
    monkeypatch.setattr(asyncio, "create_task", lambda *args, **kwargs: created.append(args))
    with pytest.raises(CaptureIOClosed):
        registry.schedule("late", 0)
    assert created == [] and bridge.calls == []


@pytest.mark.asyncio
async def test_started_delete_drains_once_despite_repeated_shutdown_cancellation(registry_type):
    bridge = Bridge()
    registry = registry_type(bridge)
    registry.schedule("started", 0)
    await asyncio.wait_for(bridge.entered.wait(), 3)
    closing = asyncio.create_task(registry.close())
    try:
        await asyncio.sleep(0)
        for _ in range(2):
            closing.cancel()
            await asyncio.wait({closing}, timeout=0.01)
        assert not closing.done() and not bridge.finished and not bridge.cancelled
        assert bridge.calls == [("started", 0)]
        with pytest.raises(CaptureIOClosed):
            registry.schedule("late", 0)
        bridge.release.set()
        result = (await asyncio.gather(closing, return_exceptions=True))[0]
        assert isinstance(result, asyncio.CancelledError)
        await registry.close()
        assert bridge.finished and not bridge.cancelled and bridge.calls == [("started", 0)]
    finally:
        bridge.release.set()
        await asyncio.gather(closing, return_exceptions=True)
        await registry.close()


@pytest.mark.asyncio
async def test_retention_expiry_admits_one_complete_delete(registry_type):
    bridge = Bridge()
    registry = registry_type(bridge)
    registry.schedule("normal", 0.01)
    try:
        assert bridge.calls == []
        await asyncio.wait_for(bridge.entered.wait(), 3)
        assert bridge.calls == [("normal", 0)]
    finally:
        bridge.release.set()
        await registry.close()
    assert bridge.finished


@pytest.mark.asyncio
async def test_service_failure_observer_does_not_retain_scheduling_run_context(registry_type, monkeypatch):
    owner, marker = CaptureIOOwner(), ContextVar("cleanup_observer_request", default=None)
    observed, reported = [], asyncio.Event()

    class FailedBridge:
        async def cleanup(self, *args, **kwargs):
            assert current_capture_io_owner() is None and marker.get() is None
            raise ValueError("Controlled delete failure")

    def warning(*args, **kwargs):
        observed.append((current_capture_io_owner(), marker.get()))
        reported.set()

    module = importlib.import_module(MODULE)
    monkeypatch.setattr(module.logger, "warning", warning)
    registry = registry_type(FailedBridge())
    token = marker.set("request-context")
    try:
        with capture_io_scope(owner):
            registry.schedule("failed", 0)
        await asyncio.wait_for(reported.wait(), 3)
        await registry.close()
        assert observed == [(None, None)]
    finally:
        marker.reset(token)
        owner.retire()
        await owner.drain()
        await registry.close()
