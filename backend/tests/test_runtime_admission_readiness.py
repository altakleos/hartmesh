"""Deployment readiness and durable-admission fencing."""

from __future__ import annotations

import asyncio
import logging
from datetime import UTC, datetime, timedelta

import pytest

from app.runtime.readiness import RuntimeReadinessCoordinator
from deerflow.extensions.capabilities import (
    CapabilityHealthSnapshot,
    CapabilityReadinessSnapshot,
)
from deerflow.runtime.runs.store.base import LifecycleReadiness


class _Health:
    def __init__(self, snapshots: list[CapabilityReadinessSnapshot]) -> None:
        self._snapshots = snapshots
        self.calls: list[int] = []

    async def admission_readiness(
        self,
        *,
        expected_generation: int,
    ) -> CapabilityReadinessSnapshot:
        self.calls.append(expected_generation)
        return self._snapshots.pop(0)


class _Lifecycle:
    def __init__(self, results: list[LifecycleReadiness]) -> None:
        self._results = results

    async def lifecycle_readiness(self) -> LifecycleReadiness:
        return self._results.pop(0)


def _capability(
    *,
    status: str,
    code: str,
    generation: int = 7,
) -> CapabilityHealthSnapshot:
    checked_at = datetime(2026, 8, 8, tzinfo=UTC)
    return CapabilityHealthSnapshot(
        contribution_id="policy",
        capability_id="authorization_provider:policy",
        status=status,
        diagnostic_code=code,
        checked_at=checked_at,
        expires_at=checked_at + timedelta(seconds=10),
        extension_generation=generation,
        last_healthy_at=checked_at if status == "healthy" else None,
    )


@pytest.mark.asyncio
async def test_required_authority_outage_blocks_admission_then_recovers() -> None:
    now = datetime(2026, 8, 8, tzinfo=UTC)
    health = _Health(
        [
            CapabilityReadinessSnapshot(
                status="not_ready",
                health=(
                    _capability(
                        status="unhealthy",
                        code="authority_unavailable",
                    ),
                ),
            ),
            CapabilityReadinessSnapshot(
                status="ready",
                health=(_capability(status="healthy", code="healthy"),),
            ),
        ]
    )
    lifecycle = _Lifecycle([LifecycleReadiness(True), LifecycleReadiness(True)])
    coordinator = RuntimeReadinessCoordinator(
        health_monitor=health,
        lifecycle_store=lambda: lifecycle,
        persistence_ready=lambda: True,
        extension_generation=lambda: 7,
        clock=lambda: now,
        overall_timeout_seconds=1,
    )

    assert await coordinator.ready_for_admission() is False
    first = coordinator.last_snapshot
    assert first is not None
    assert first.status == "not_ready"
    assert first.reason_codes == ("required_capability_unhealthy",)

    assert await coordinator.ready_for_admission() is True
    recovered = coordinator.last_snapshot
    assert recovered is not None
    assert recovered.status == "ready"
    assert recovered.reason_codes == ()
    assert health.calls == [7, 7]


@pytest.mark.asyncio
async def test_lifecycle_dependency_exception_is_correlated_and_redacted(
    caplog: pytest.LogCaptureFixture,
) -> None:
    class ExplodingLifecycle:
        async def lifecycle_readiness(self) -> LifecycleReadiness:
            raise RuntimeError("postgres password=never-publish")

    health = _Health([CapabilityReadinessSnapshot(status="ready", health=())])
    coordinator = RuntimeReadinessCoordinator(
        health_monitor=health,
        lifecycle_store=lambda: ExplodingLifecycle(),
        persistence_ready=lambda: True,
        extension_generation=lambda: 7,
        clock=lambda: datetime(2026, 8, 8, tzinfo=UTC),
        overall_timeout_seconds=1,
    )

    with caplog.at_level(logging.ERROR, logger="app.runtime.readiness"):
        assert await coordinator.ready_for_admission() is False

    snapshot = coordinator.last_snapshot
    assert snapshot is not None
    assert snapshot.reason_codes == ("lifecycle_store_unavailable",)
    assert snapshot.correlation_id is not None
    assert "never-publish" not in str(snapshot.to_dict())
    matching = [record for record in caplog.records if getattr(record, "correlation_id", None) == snapshot.correlation_id]
    assert len(matching) == 1


@pytest.mark.asyncio
async def test_untrusted_lifecycle_reason_is_replaced_with_safe_correlated_code(
    caplog: pytest.LogCaptureFixture,
) -> None:
    class UnsafeLifecycle:
        async def lifecycle_readiness(self) -> LifecycleReadiness:
            return LifecycleReadiness(
                ready=False,
                reason_code="postgres password=never-publish",
            )

    health = _Health([CapabilityReadinessSnapshot(status="ready", health=())])
    coordinator = RuntimeReadinessCoordinator(
        health_monitor=health,
        lifecycle_store=lambda: UnsafeLifecycle(),
        persistence_ready=lambda: True,
        extension_generation=lambda: 7,
        clock=lambda: datetime(2026, 8, 8, tzinfo=UTC),
        overall_timeout_seconds=1,
    )

    with caplog.at_level(logging.ERROR, logger="app.runtime.readiness"):
        assert await coordinator.ready_for_admission() is False

    snapshot = coordinator.last_snapshot
    assert snapshot is not None
    assert snapshot.reason_codes == ("lifecycle_store_unavailable",)
    assert snapshot.correlation_id is not None
    assert "never-publish" not in str(snapshot.to_dict())
    assert "never-publish" not in caplog.text


@pytest.mark.asyncio
async def test_process_local_persistence_blocks_durable_admission() -> None:
    coordinator = RuntimeReadinessCoordinator(
        health_monitor=_Health(
            [CapabilityReadinessSnapshot(status="ready", health=())],
        ),
        lifecycle_store=lambda: _Lifecycle([LifecycleReadiness(True)]),
        persistence_ready=lambda: False,
        extension_generation=lambda: 7,
        clock=lambda: datetime(2026, 8, 8, tzinfo=UTC),
        overall_timeout_seconds=1,
    )

    assert await coordinator.ready_for_admission() is False
    snapshot = coordinator.last_snapshot
    assert snapshot is not None
    assert snapshot.reason_codes == ("persistence_profile_unsatisfied",)


@pytest.mark.asyncio
async def test_overall_timeout_is_bounded_and_cancels_dependency_work() -> None:
    cancelled = asyncio.Event()

    class HangingHealth:
        async def admission_readiness(
            self,
            *,
            expected_generation: int,
        ) -> CapabilityReadinessSnapshot:
            assert expected_generation == 7
            try:
                await asyncio.Event().wait()
            finally:
                cancelled.set()

    coordinator = RuntimeReadinessCoordinator(
        health_monitor=HangingHealth(),
        lifecycle_store=lambda: _Lifecycle([LifecycleReadiness(True)]),
        persistence_ready=lambda: True,
        extension_generation=lambda: 7,
        clock=lambda: datetime(2026, 8, 8, tzinfo=UTC),
        overall_timeout_seconds=0.01,
    )

    assert await coordinator.ready_for_admission() is False
    snapshot = coordinator.last_snapshot
    assert snapshot is not None
    assert snapshot.reason_codes == ("readiness_timeout",)
    assert cancelled.is_set()
