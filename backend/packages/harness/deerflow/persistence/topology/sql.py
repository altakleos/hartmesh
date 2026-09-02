"""PostgreSQL authority for exact-two Gateway topology registration."""

from __future__ import annotations

import hashlib
from datetime import UTC, datetime, timedelta

from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from deerflow.deployment.topology import (
    MULTI_GATEWAY_REPLICA_COUNT,
    ReplicaRegistrationV1,
    TopologyError,
    TopologyFingerprintV1,
    TopologyStatusV1,
)
from deerflow.persistence.sql_clock import (
    coerce_database_wall_clock,
    database_wall_clock_expression,
)
from deerflow.persistence.topology.model import TopologyReplicaRow


def _advisory_lock_key(*, tenant_digest: str, profile: str) -> int:
    material = f"hartmesh:topology:v1:{tenant_digest}:{profile}".encode()
    return int.from_bytes(hashlib.sha256(material).digest()[:8], "big", signed=True)


def _aware(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


class PostgresTopologyRegistry:
    """Serialize registration decisions in the tenant's PostgreSQL schema.

    A transaction-scoped advisory lock is the single decision authority for
    registration, heartbeat, and enumeration. Liveness uses PostgreSQL time,
    so pod clock skew cannot extend a lease or evict a healthy peer.
    """

    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        *,
        live_ttl_seconds: float,
    ) -> None:
        if not isinstance(live_ttl_seconds, (int, float)) or isinstance(live_ttl_seconds, bool) or not 1 <= live_ttl_seconds <= 3600:
            raise ValueError("live_ttl_seconds must be in [1, 3600]")
        self._session_factory = session_factory
        self._live_ttl = timedelta(seconds=float(live_ttl_seconds))
        self._registration: ReplicaRegistrationV1 | None = None

    @staticmethod
    def _require_postgres(session: AsyncSession) -> None:
        if session.get_bind().dialect.name != "postgresql":
            raise TopologyError("topology_dependency_not_shared")

    @staticmethod
    async def _database_now(session: AsyncSession) -> datetime:
        try:
            value = await session.scalar(
                select(
                    database_wall_clock_expression(
                        session.get_bind().dialect.name,
                    )
                )
            )
            return coerce_database_wall_clock(value)
        except (TypeError, ValueError):
            raise TopologyError("topology_dependency_not_shared")

    @staticmethod
    async def _lock(
        session: AsyncSession,
        *,
        fingerprint: TopologyFingerprintV1,
    ) -> None:
        await session.execute(
            text("SELECT pg_advisory_xact_lock(:lock_key)"),
            {
                "lock_key": _advisory_lock_key(
                    tenant_digest=fingerprint.tenant_digest,
                    profile=fingerprint.profile,
                )
            },
        )

    @staticmethod
    def _row_registration(row: TopologyReplicaRow) -> ReplicaRegistrationV1:
        fingerprint = TopologyFingerprintV1.from_dict(dict(row.fingerprint_json))
        if fingerprint.digest != row.topology_digest:
            raise TopologyError("topology_fingerprint_mismatch")
        return ReplicaRegistrationV1(
            replica_id=row.replica_id,
            topology_fingerprint=fingerprint,
            started_at=_aware(row.started_at),
            heartbeat_at=_aware(row.heartbeat_at),
        )

    @staticmethod
    async def _subject_rows(
        session: AsyncSession,
        *,
        fingerprint: TopologyFingerprintV1,
    ) -> tuple[TopologyReplicaRow, ...]:
        rows = (
            await session.scalars(
                select(TopologyReplicaRow)
                .where(
                    TopologyReplicaRow.tenant_digest == fingerprint.tenant_digest,
                    TopologyReplicaRow.profile == fingerprint.profile,
                )
                .order_by(TopologyReplicaRow.replica_id)
                .with_for_update()
            )
        ).all()
        return tuple(rows)

    def _live(
        self,
        rows: tuple[TopologyReplicaRow, ...],
        *,
        now: datetime,
    ) -> tuple[ReplicaRegistrationV1, ...]:
        cutoff = now - self._live_ttl
        return tuple(registration for registration in (self._row_registration(row) for row in rows) if registration.heartbeat_at >= cutoff)

    async def register(self, registration: ReplicaRegistrationV1) -> None:
        if not isinstance(registration, ReplicaRegistrationV1):
            raise TypeError("registration must be ReplicaRegistrationV1")
        fingerprint = registration.topology_fingerprint
        stored: ReplicaRegistrationV1
        async with self._session_factory() as session, session.begin():
            self._require_postgres(session)
            await self._lock(session, fingerprint=fingerprint)
            now = await self._database_now(session)
            rows = await self._subject_rows(session, fingerprint=fingerprint)
            live = self._live(rows, now=now)
            if any(item.topology_fingerprint.digest != fingerprint.digest for item in live):
                raise TopologyError("topology_fingerprint_mismatch")

            existing_row = next(
                (row for row in rows if row.replica_id == registration.replica_id),
                None,
            )
            existing_live = next(
                (item for item in live if item.replica_id == registration.replica_id),
                None,
            )
            if existing_live is not None:
                if existing_live.started_at != registration.started_at or existing_live.topology_fingerprint.digest != fingerprint.digest:
                    raise TopologyError("topology_fingerprint_mismatch")
                stored = existing_live
            else:
                other_live = tuple(item for item in live if item.replica_id != registration.replica_id)
                if len(other_live) >= MULTI_GATEWAY_REPLICA_COUNT:
                    raise TopologyError("topology_replica_count_invalid")
                stored = ReplicaRegistrationV1(
                    replica_id=registration.replica_id,
                    topology_fingerprint=fingerprint,
                    started_at=registration.started_at,
                    heartbeat_at=now,
                )
                if existing_row is None:
                    session.add(
                        TopologyReplicaRow(
                            tenant_digest=fingerprint.tenant_digest,
                            profile=fingerprint.profile,
                            replica_id=stored.replica_id,
                            topology_digest=fingerprint.digest,
                            fingerprint_json=fingerprint.to_dict(),
                            started_at=stored.started_at,
                            heartbeat_at=stored.heartbeat_at,
                        )
                    )
                else:
                    existing_row.topology_digest = fingerprint.digest
                    existing_row.fingerprint_json = fingerprint.to_dict()
                    existing_row.started_at = stored.started_at
                    existing_row.heartbeat_at = stored.heartbeat_at
        self._registration = stored

    async def heartbeat(self) -> ReplicaRegistrationV1:
        registration = self._registration
        if registration is None:
            raise TopologyError("topology_registration_missing")
        fingerprint = registration.topology_fingerprint
        async with self._session_factory() as session, session.begin():
            self._require_postgres(session)
            await self._lock(session, fingerprint=fingerprint)
            now = await self._database_now(session)
            row = await session.get(
                TopologyReplicaRow,
                (fingerprint.tenant_digest, fingerprint.profile, registration.replica_id),
                with_for_update=True,
            )
            if row is None:
                raise TopologyError("topology_registration_missing")
            current = self._row_registration(row)
            if current.topology_fingerprint.digest != fingerprint.digest or current.started_at != registration.started_at:
                raise TopologyError("topology_fingerprint_mismatch")
            if current.heartbeat_at < now - self._live_ttl:
                raise TopologyError("topology_registration_expired")
            row.heartbeat_at = now
            updated = ReplicaRegistrationV1(
                replica_id=current.replica_id,
                topology_fingerprint=current.topology_fingerprint,
                started_at=current.started_at,
                heartbeat_at=now,
            )
        self._registration = updated
        return updated

    async def compatible_live_replicas(
        self,
    ) -> tuple[ReplicaRegistrationV1, ...]:
        registration = self._registration
        if registration is None:
            raise TopologyError("topology_registration_missing")
        fingerprint = registration.topology_fingerprint
        async with self._session_factory() as session, session.begin():
            self._require_postgres(session)
            await self._lock(session, fingerprint=fingerprint)
            now = await self._database_now(session)
            live = self._live(
                await self._subject_rows(session, fingerprint=fingerprint),
                now=now,
            )
            if any(item.topology_fingerprint.digest != fingerprint.digest for item in live):
                raise TopologyError("topology_fingerprint_mismatch")
            return tuple(sorted(live, key=lambda item: item.replica_id))

    async def status(self) -> TopologyStatusV1:
        registration = self._registration
        if registration is None:
            return TopologyStatusV1(
                replica_id=None,
                topology_digest=None,
                ready=False,
                live_compatible_replicas=0,
                degraded_replicas=MULTI_GATEWAY_REPLICA_COUNT,
                qualification_ready=False,
                reason_code="topology_registration_missing",
            )
        try:
            live = await self.compatible_live_replicas()
        except TopologyError as exc:
            return TopologyStatusV1(
                replica_id=registration.replica_id,
                topology_digest=registration.topology_fingerprint.digest,
                ready=False,
                live_compatible_replicas=0,
                degraded_replicas=MULTI_GATEWAY_REPLICA_COUNT,
                qualification_ready=False,
                reason_code=exc.code,
            )
        own_ready = any(item.replica_id == registration.replica_id for item in live)
        count = min(len(live), MULTI_GATEWAY_REPLICA_COUNT)
        return TopologyStatusV1(
            replica_id=registration.replica_id,
            topology_digest=registration.topology_fingerprint.digest,
            ready=own_ready,
            live_compatible_replicas=count,
            degraded_replicas=MULTI_GATEWAY_REPLICA_COUNT - count,
            qualification_ready=(own_ready and count == MULTI_GATEWAY_REPLICA_COUNT),
            reason_code=(None if own_ready else "topology_registration_expired"),
        )
