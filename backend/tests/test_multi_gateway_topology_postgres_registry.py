"""PostgreSQL-backed exact-two topology registry contract."""

from __future__ import annotations

import asyncio
import os
import uuid
from datetime import UTC, datetime

import pytest
import sqlalchemy as sa
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from support.postgres import postgres_async_url

from deerflow.deployment.topology import (
    MULTI_GATEWAY_PROFILE,
    ReplicaRegistrationV1,
    TopologyError,
    TopologyFingerprintV1,
)
from deerflow.persistence.topology import PostgresTopologyRegistry

_POSTGRES_URL = os.environ.get("DEERFLOW_TEST_POSTGRES_URL")


def _fingerprint(*, tenant_digest: str, config_digit: str = "1") -> TopologyFingerprintV1:
    return TopologyFingerprintV1.create(
        profile=MULTI_GATEWAY_PROFILE,
        tenant_digest=tenant_digest,
        image_digests={
            "gateway": f"sha256:{'a' * 64}",
            "frontend": f"sha256:{'3' * 64}",
            "nginx": f"sha256:{'4' * 64}",
            "provisioner": f"sha256:{'b' * 64}",
            "sandbox": f"sha256:{'c' * 64}",
            "postgres": f"sha256:{'5' * 64}",
            "redis": f"sha256:{'6' * 64}",
        },
        config_digest=f"sha256:{config_digit * 64}",
        database_schema_ref=f"schema:sha256:{'d' * 64}",
        redis_namespace_digest=f"sha256:{'e' * 64}",
        extension_artifact_digest=f"sha256:{'f' * 64}",
        extension_configuration_digest=f"sha256:{'0' * 64}",
        capability_manifest_digest="2" * 64,
        mcp_task_replay_keyring_confirmation_version=1,
        mcp_task_replay_keyring_confirmation_digest=f"sha256:{'7' * 64}",
        migration_head="0030_run_delivery_owner_backfill",
        accepted_materialization_profile="rwx_verified_copy_v2",
    )


def _registration(replica_id: str, fingerprint: TopologyFingerprintV1) -> ReplicaRegistrationV1:
    now = datetime.now(UTC)
    return ReplicaRegistrationV1(
        replica_id=replica_id,
        topology_fingerprint=fingerprint,
        started_at=now,
        heartbeat_at=now,
    )


@pytest.mark.asyncio
async def test_postgres_registry_rejects_non_postgres_authority(tmp_path) -> None:
    engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path / 'registry.db'}")
    try:
        registry = PostgresTopologyRegistry(
            async_sessionmaker(engine, expire_on_commit=False),
            live_ttl_seconds=30,
        )
        with pytest.raises(TopologyError) as exc_info:
            await registry.register(_registration("gateway-0", _fingerprint(tenant_digest="1" * 64)))
        assert exc_info.value.code == "topology_dependency_not_shared"
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_postgres_registry_samples_statement_clock_after_lock() -> None:
    statements: list[str] = []

    class _Dialect:
        name = "postgresql"

    class _Bind:
        dialect = _Dialect()

    class _Session:
        def get_bind(self):
            return _Bind()

        async def scalar(self, statement):
            statements.append(str(statement))
            return datetime.now(UTC)

    observed = await PostgresTopologyRegistry._database_now(_Session())

    assert observed.tzinfo is not None
    assert len(statements) == 1
    assert "clock_timestamp" in statements[0]


@pytest.mark.asyncio
@pytest.mark.postgres_contract
@pytest.mark.skipif(not _POSTGRES_URL, reason="requires DEERFLOW_TEST_POSTGRES_URL for topology registry qualification")
async def test_postgres_registry_serializes_exact_two_and_reports_degraded() -> None:
    schema = f"topology_{uuid.uuid4().hex}"
    engine = create_async_engine(postgres_async_url(_POSTGRES_URL), connect_args={"server_settings": {"search_path": schema}})
    admin = create_async_engine(postgres_async_url(_POSTGRES_URL))
    try:
        async with admin.begin() as connection:
            await connection.execute(sa.text(f'CREATE SCHEMA "{schema}"'))
        async with engine.begin() as connection:
            await connection.execute(
                sa.text(
                    """
                    CREATE TABLE hartmesh_topology_replicas (
                      tenant_digest varchar(64) NOT NULL,
                      profile varchar(64) NOT NULL,
                      replica_id varchar(128) NOT NULL,
                      topology_digest varchar(64) NOT NULL,
                      fingerprint_json json NOT NULL,
                      started_at timestamptz NOT NULL,
                      heartbeat_at timestamptz NOT NULL,
                      PRIMARY KEY (tenant_digest, profile, replica_id)
                    )
                    """
                )
            )
        sf = async_sessionmaker(engine, expire_on_commit=False)
        first = PostgresTopologyRegistry(sf, live_ttl_seconds=30)
        second = PostgresTopologyRegistry(sf, live_ttl_seconds=30)
        third = PostgresTopologyRegistry(sf, live_ttl_seconds=30)
        fingerprint = _fingerprint(tenant_digest="3" * 64)

        await asyncio.gather(
            first.register(_registration("gateway-0", fingerprint)),
            second.register(_registration("gateway-1", fingerprint)),
        )
        assert len(await first.compatible_live_replicas()) == 2
        with pytest.raises(TopologyError) as exc_info:
            await third.register(
                _registration(
                    "gateway-2",
                    _fingerprint(
                        tenant_digest="3" * 64,
                        config_digit="4",
                    ),
                )
            )
        assert exc_info.value.code == "topology_fingerprint_mismatch"

        with pytest.raises(TopologyError) as exc_info:
            await third.register(_registration("gateway-2", fingerprint))
        assert exc_info.value.code == "topology_replica_count_invalid"

        async with engine.begin() as connection:
            await connection.execute(
                sa.text(
                    """
                    UPDATE hartmesh_topology_replicas
                    SET
                      started_at = CURRENT_TIMESTAMP - INTERVAL '32 seconds',
                      heartbeat_at = CURRENT_TIMESTAMP - INTERVAL '31 seconds'
                    WHERE replica_id = 'gateway-1'
                    """
                )
            )
        status = await first.status()
        assert status.ready is True
        assert status.qualification_ready is False
        assert status.live_compatible_replicas == 1
        assert status.degraded_replicas == 1
    finally:
        await engine.dispose()
        async with admin.begin() as connection:
            await connection.execute(sa.text(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE'))
        await admin.dispose()
