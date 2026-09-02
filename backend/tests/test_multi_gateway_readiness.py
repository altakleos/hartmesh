from __future__ import annotations

from datetime import UTC, datetime
from types import SimpleNamespace

import pytest

from app.runtime.readiness import RuntimeReadinessCoordinator
from deerflow.deployment.topology import (
    TopologyStatusV1,
    multi_gateway_run_store_ready,
)
from deerflow.extensions.capabilities import CapabilityReadinessSnapshot
from deerflow.runtime.runs.store.base import (
    LeaseClockAuthority,
    LifecycleReadiness,
)


class _Health:
    async def admission_readiness(self, *, expected_generation: int):
        assert expected_generation == 1
        return CapabilityReadinessSnapshot(status="ready", health=())


class _Lifecycle:
    async def lifecycle_readiness(self):
        return LifecycleReadiness(ready=True, reason_code="ready")


def _status(*, ready: bool, live: int, reason: str | None = None):
    return TopologyStatusV1(
        replica_id="gateway-0",
        topology_digest="a" * 64,
        ready=ready,
        live_compatible_replicas=live,
        degraded_replicas=2 - live,
        qualification_ready=ready and live == 2,
        reason_code=reason,
    )


def _coordinator(status, *, dependency_ready=True, run_store=None):
    if run_store is None:
        run_store = SimpleNamespace(
            lease_clock_authority=LeaseClockAuthority.database_v1,
        )

    async def topology_status():
        return status

    async def topology_dependencies():
        return dependency_ready and multi_gateway_run_store_ready(run_store)

    return RuntimeReadinessCoordinator(
        health_monitor=_Health(),
        lifecycle_store=lambda: _Lifecycle(),
        persistence_ready=lambda: True,
        extension_generation=lambda: 1,
        overall_timeout_seconds=1,
        topology_status=topology_status,
        topology_dependencies_ready=topology_dependencies,
        clock=lambda: datetime(2026, 9, 1, tzinfo=UTC),
    )


@pytest.mark.asyncio
async def test_own_registration_is_required_but_peer_loss_is_degraded_ready() -> None:
    coordinator = _coordinator(_status(ready=True, live=1))
    snapshot = await coordinator.readiness()
    assert snapshot.status == "ready"
    assert snapshot.reason_codes == ()


@pytest.mark.asyncio
async def test_missing_or_skewed_own_registration_blocks_readiness() -> None:
    coordinator = _coordinator(
        _status(
            ready=False,
            live=0,
            reason="topology_fingerprint_mismatch",
        )
    )
    snapshot = await coordinator.readiness()
    assert snapshot.status == "not_ready"
    assert snapshot.reason_codes == ("topology_fingerprint_mismatch",)


@pytest.mark.asyncio
async def test_shared_topology_dependency_probe_is_fail_closed() -> None:
    coordinator = _coordinator(
        _status(ready=True, live=2),
        dependency_ready=False,
    )

    snapshot = await coordinator.readiness()

    assert snapshot.status == "not_ready"
    assert snapshot.reason_codes == ("topology_dependency_not_shared",)


@pytest.mark.asyncio
async def test_process_clock_run_store_blocks_exact_two_readiness() -> None:
    coordinator = _coordinator(
        _status(ready=True, live=2),
        run_store=SimpleNamespace(
            lease_clock_authority=LeaseClockAuthority.process_v1,
        ),
    )

    snapshot = await coordinator.readiness()

    assert snapshot.status == "not_ready"
    assert snapshot.reason_codes == ("topology_dependency_not_shared",)
