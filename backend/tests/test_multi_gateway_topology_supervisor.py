from __future__ import annotations

import asyncio
from datetime import UTC, datetime

import pytest

from deerflow.deployment.topology import (
    MULTI_GATEWAY_PROFILE,
    ReplicaRegistrationV1,
    TopologyError,
    TopologyFingerprintV1,
    TopologyHeartbeatSupervisor,
    TopologyStatusV1,
)


def _registration() -> ReplicaRegistrationV1:
    fingerprint = TopologyFingerprintV1.create(
        profile=MULTI_GATEWAY_PROFILE,
        tenant_digest="1" * 64,
        image_digests={
            "gateway": f"sha256:{'2' * 64}",
            "frontend": f"sha256:{'a' * 64}",
            "nginx": f"sha256:{'b' * 64}",
            "provisioner": f"sha256:{'3' * 64}",
            "sandbox": f"sha256:{'4' * 64}",
            "postgres": f"sha256:{'c' * 64}",
            "redis": f"sha256:{'d' * 64}",
        },
        config_digest=f"sha256:{'5' * 64}",
        database_schema_ref=f"schema:sha256:{'6' * 64}",
        redis_namespace_digest=f"sha256:{'7' * 64}",
        extension_artifact_digest=f"sha256:{'8' * 64}",
        extension_configuration_digest=f"sha256:{'9' * 64}",
        capability_manifest_digest="a" * 64,
        mcp_task_replay_keyring_confirmation_version=1,
        mcp_task_replay_keyring_confirmation_digest=f"sha256:{'e' * 64}",
        migration_head="0030_run_delivery_owner_backfill",
        accepted_materialization_profile="rwx_verified_copy_v2",
    )
    now = datetime.now(UTC)
    return ReplicaRegistrationV1(
        replica_id="gateway-0",
        topology_fingerprint=fingerprint,
        started_at=now,
        heartbeat_at=now,
    )


class _Registry:
    def __init__(self) -> None:
        self.register_calls = 0
        self.heartbeat_calls = 0
        self.heartbeat_seen = asyncio.Event()

    async def register(self, registration) -> None:
        assert registration.replica_id == "gateway-0"
        self.register_calls += 1

    async def heartbeat(self):
        self.heartbeat_calls += 1
        self.heartbeat_seen.set()
        if self.heartbeat_calls == 1:
            raise TopologyError("topology_registration_expired")
        return _registration()

    async def compatible_live_replicas(self):
        return (_registration(),)

    async def status(self):
        return TopologyStatusV1(
            replica_id="gateway-0",
            topology_digest=_registration().topology_fingerprint.digest,
            ready=True,
            live_compatible_replicas=1,
            degraded_replicas=1,
            qualification_ready=False,
        )


@pytest.mark.asyncio
async def test_supervisor_registers_heartbeats_and_reregisters_after_expiry() -> None:
    registry = _Registry()
    supervisor = TopologyHeartbeatSupervisor(
        registry=registry,
        registration=_registration(),
        heartbeat_interval_seconds=0.01,
    )
    await supervisor.start()
    await asyncio.wait_for(registry.heartbeat_seen.wait(), timeout=1)
    for _ in range(100):
        if registry.register_calls == 2:
            break
        await asyncio.sleep(0.001)
    assert registry.register_calls == 2
    assert (await supervisor.status()).ready is True
    await supervisor.close()
    calls = registry.heartbeat_calls
    await asyncio.sleep(0.02)
    assert registry.heartbeat_calls == calls
