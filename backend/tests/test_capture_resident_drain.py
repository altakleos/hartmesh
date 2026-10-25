"""Cancellation retains the existing launch, worker and producer drain sequence."""

import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from app.gateway.resident_runtime import ResidentRuntime
from deerflow.runtime.runs.manager import RunManager
from deerflow.sandbox.admission import ProviderAdmission


@pytest.mark.asyncio
@pytest.mark.parametrize("cancel", [True, False])
async def test_resident_drain_retains_worker_attached_after_cancelled_admission_wait(cancel):
    manager = RunManager()
    host = ResidentRuntime.__new__(ResidentRuntime)
    host._qualification_revision = 0
    host.provider = SimpleNamespace(external_changed=lambda change: change())
    host.admission = ProviderAdmission()
    host.admission.bind(host.provider)
    host.admission.open(host.provider)
    host.registry = SimpleNamespace(run_manager=manager)
    host._closing_tasks, host._closing, host._operations = None, False, set()
    host.drain_producers = AsyncMock()
    entered, launch, worker_release = asyncio.Event(), asyncio.Event(), asyncio.Event()

    async def late_launch():
        entered.set()
        await launch.wait()
        record = await manager.create("late-thread")
        record.task = asyncio.create_task(worker_release.wait())
        return record

    owning = asyncio.create_task(host.own_operation(late_launch()))
    await entered.wait()
    closing = asyncio.create_task(host.drain_workers())
    try:
        await asyncio.sleep(0)
        if cancel:
            closing.cancel()
            await asyncio.sleep(0)
            closing.cancel()
        launch.set()
        record = await owning
        done, _ = await asyncio.wait({closing}, timeout=0.02)
        assert not done and not record.task.done(), "Cancellation abandoned the newly attached resident worker"
        host.drain_producers.assert_not_awaited()
        worker_release.set()
        result = (await asyncio.gather(closing, return_exceptions=True))[0]
        assert isinstance(result, asyncio.CancelledError) if cancel else result is None
        host.drain_producers.assert_awaited_once()
    finally:
        launch.set()
        worker_release.set()
        await asyncio.gather(owning, closing, return_exceptions=True)
