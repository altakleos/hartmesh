from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta

import pytest

from deerflow.deployment.topology import (
    InMemoryTopologyRegistry,
    ReplicaRegistrationV1,
    TopologyError,
    TopologyFingerprintV1,
)


def _fingerprint(seed: str = "1") -> TopologyFingerprintV1:
    return TopologyFingerprintV1.create(
        profile="durable_two_gateway_v1",
        tenant_digest="a" * 64,
        image_digests={
            "gateway": "sha256:" + (seed * 64),
            "frontend": "sha256:" + ("a" * 64),
            "nginx": "sha256:" + ("b" * 64),
            "provisioner": "sha256:" + ("2" * 64),
            "sandbox": "sha256:" + ("3" * 64),
            "postgres": "sha256:" + ("c" * 64),
            "redis": "sha256:" + ("d" * 64),
        },
        config_digest="sha256:" + ("4" * 64),
        database_schema_ref="schema:sha256:" + ("5" * 64),
        redis_namespace_digest="sha256:" + ("6" * 64),
        extension_artifact_digest="sha256:" + ("7" * 64),
        extension_configuration_digest="sha256:" + ("8" * 64),
        capability_manifest_digest="9" * 64,
        mcp_task_replay_keyring_confirmation_version=1,
        mcp_task_replay_keyring_confirmation_digest="sha256:" + ("e" * 64),
        migration_head="0030_run_delivery_owner_backfill",
        accepted_materialization_profile="rwx_verified_copy_v2",
    )


def _registration(
    replica_id: str,
    now: datetime,
    *,
    fingerprint: TopologyFingerprintV1 | None = None,
) -> ReplicaRegistrationV1:
    return ReplicaRegistrationV1(
        replica_id=replica_id,
        topology_fingerprint=fingerprint or _fingerprint(),
        started_at=now,
        heartbeat_at=now,
    )


@pytest.mark.asyncio
async def test_two_compatible_replicas_register_concurrently_and_report_full_strength() -> None:
    now = datetime(2026, 9, 1, 12, 0, tzinfo=UTC)
    shared = InMemoryTopologyRegistry.shared_state()
    first = InMemoryTopologyRegistry(
        live_ttl_seconds=30,
        clock=lambda: now,
        shared_state=shared,
    )
    second = InMemoryTopologyRegistry(
        live_ttl_seconds=30,
        clock=lambda: now,
        shared_state=shared,
    )

    await asyncio.gather(
        first.register(_registration("gateway-a", now)),
        second.register(_registration("gateway-b", now)),
    )

    assert [item.replica_id for item in await first.compatible_live_replicas()] == [
        "gateway-a",
        "gateway-b",
    ]
    status = await first.status()
    assert status.ready is True
    assert status.live_compatible_replicas == 2
    assert status.degraded_replicas == 0
    assert status.qualification_ready is True
    assert status.to_dict()["execution_recovery"] == {
        "version": 1,
        "post_dispatch_takeover_available": False,
        "reason_code": "linearizable_execution_authority_unavailable",
    }


@pytest.mark.asyncio
async def test_concurrent_incompatible_replica_is_rejected_with_stable_code() -> None:
    now = datetime(2026, 9, 1, 12, 0, tzinfo=UTC)
    shared = InMemoryTopologyRegistry.shared_state()
    first = InMemoryTopologyRegistry(
        live_ttl_seconds=30,
        clock=lambda: now,
        shared_state=shared,
    )
    skewed = InMemoryTopologyRegistry(
        live_ttl_seconds=30,
        clock=lambda: now,
        shared_state=shared,
    )
    await first.register(_registration("gateway-a", now))

    with pytest.raises(TopologyError) as raised:
        await skewed.register(
            _registration(
                "gateway-b",
                now,
                fingerprint=_fingerprint("f"),
            )
        )

    assert raised.value.code == "topology_fingerprint_mismatch"
    assert "f" * 64 not in str(raised.value)
    assert [item.replica_id for item in await first.compatible_live_replicas()] == ["gateway-a"]


@pytest.mark.asyncio
async def test_third_live_replica_is_rejected_even_when_compatible() -> None:
    now = datetime(2026, 9, 1, 12, 0, tzinfo=UTC)
    shared = InMemoryTopologyRegistry.shared_state()
    registries = [
        InMemoryTopologyRegistry(
            live_ttl_seconds=30,
            clock=lambda: now,
            shared_state=shared,
        )
        for _ in range(3)
    ]
    await registries[0].register(_registration("gateway-a", now))
    await registries[1].register(_registration("gateway-b", now))

    with pytest.raises(TopologyError) as raised:
        await registries[2].register(_registration("gateway-c", now))

    assert raised.value.code == "topology_replica_count_invalid"


@pytest.mark.asyncio
async def test_expired_peer_leaves_survivor_ready_and_degraded() -> None:
    current = datetime(2026, 9, 1, 12, 0, tzinfo=UTC)

    def clock() -> datetime:
        return current

    shared = InMemoryTopologyRegistry.shared_state()
    first = InMemoryTopologyRegistry(
        live_ttl_seconds=30,
        clock=clock,
        shared_state=shared,
    )
    second = InMemoryTopologyRegistry(
        live_ttl_seconds=30,
        clock=clock,
        shared_state=shared,
    )
    await first.register(_registration("gateway-a", current))
    await second.register(_registration("gateway-b", current))

    current += timedelta(seconds=20)
    await first.heartbeat()
    current += timedelta(seconds=15)

    status = await first.status()
    assert status.ready is True
    assert status.live_compatible_replicas == 1
    assert status.degraded_replicas == 1
    assert status.qualification_ready is False

    expired = await second.status()
    assert expired.ready is False
    assert expired.reason_code == "topology_registration_expired"


@pytest.mark.asyncio
async def test_registration_retry_is_idempotent_but_cannot_change_identity() -> None:
    now = datetime(2026, 9, 1, 12, 0, tzinfo=UTC)
    registry = InMemoryTopologyRegistry(live_ttl_seconds=30, clock=lambda: now)
    registration = _registration("gateway-a", now)

    await registry.register(registration)
    await registry.register(registration)
    assert await registry.compatible_live_replicas() == (registration,)

    with pytest.raises(TopologyError) as raised:
        await registry.register(_registration("gateway-a", now, fingerprint=_fingerprint("e")))
    assert raised.value.code == "topology_fingerprint_mismatch"
