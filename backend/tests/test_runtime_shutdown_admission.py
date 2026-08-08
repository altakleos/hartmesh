"""Admission and readiness become atomically draining during shutdown."""

from __future__ import annotations

import asyncio

import pytest


@pytest.mark.asyncio
async def test_begin_draining_waits_for_admission_permit_and_rejects_new_work() -> None:
    from app.runtime.readiness import RuntimeReadinessCoordinator

    class Health:
        async def admission_readiness(self, *, expected_generation: int):
            from deerflow.extensions.capabilities import CapabilityReadinessSnapshot

            return CapabilityReadinessSnapshot(status="ready", health=())

    class Store:
        async def lifecycle_readiness(self):
            from deerflow.runtime.runs.store.base import LifecycleReadiness

            return LifecycleReadiness(ready=True, reason_code="ready")

    readiness = RuntimeReadinessCoordinator(
        health_monitor=Health(),
        lifecycle_store=lambda: Store(),
        persistence_ready=lambda: True,
        extension_generation=lambda: 1,
        overall_timeout_seconds=1,
    )
    admitted = asyncio.Event()
    release = asyncio.Event()

    async def existing_admission() -> None:
        async with readiness.admission_permit() as permitted:
            assert permitted is True
            admitted.set()
            await release.wait()

    active = asyncio.create_task(existing_admission())
    await admitted.wait()
    draining = asyncio.create_task(readiness.begin_draining())
    await asyncio.sleep(0)

    assert (await readiness.readiness()).reason_codes == ("shutdown_draining",)
    async with readiness.admission_permit() as permitted:
        assert permitted is False
    assert draining.done() is False

    release.set()
    await active
    assert await draining is True
