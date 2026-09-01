"""Shared PostgreSQL registration rows for the exact-two Gateway profile."""

from __future__ import annotations

from datetime import datetime

from sqlalchemy import JSON, CheckConstraint, DateTime, Index, String
from sqlalchemy.orm import Mapped, mapped_column

from deerflow.persistence.base import Base


class TopologyReplicaRow(Base):
    """One bounded, redacted live-replica registration."""

    __tablename__ = "hartmesh_topology_replicas"

    tenant_digest: Mapped[str] = mapped_column(String(64), primary_key=True)
    profile: Mapped[str] = mapped_column(String(64), primary_key=True)
    replica_id: Mapped[str] = mapped_column(String(128), primary_key=True)
    topology_digest: Mapped[str] = mapped_column(String(64), nullable=False)
    fingerprint_json: Mapped[dict[str, object]] = mapped_column(JSON, nullable=False)
    started_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    heartbeat_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)

    __table_args__ = (
        CheckConstraint(
            "profile = 'durable_two_gateway_v1'",
            name="ck_hartmesh_topology_profile",
        ),
        CheckConstraint(
            "length(tenant_digest) = 64 AND length(topology_digest) = 64",
            name="ck_hartmesh_topology_digest_lengths",
        ),
        CheckConstraint(
            "length(replica_id) BETWEEN 1 AND 128",
            name="ck_hartmesh_topology_replica_id_length",
        ),
        Index(
            "ix_hartmesh_topology_live",
            "tenant_digest",
            "profile",
            "heartbeat_at",
        ),
    )
