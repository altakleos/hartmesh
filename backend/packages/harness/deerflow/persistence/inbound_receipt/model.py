"""ORM row for durable, leased native-ingress receipts."""

from __future__ import annotations

from datetime import datetime
from typing import Any

from sqlalchemy import (
    JSON,
    CheckConstraint,
    DateTime,
    Index,
    Integer,
    String,
    UniqueConstraint,
    func,
    text,
)
from sqlalchemy.orm import Mapped, mapped_column

from deerflow.persistence.base import Base


class InboundReceiptRow(Base):
    """One bounded source delivery retained until admission is settled."""

    __tablename__ = "inbound_receipts"

    receipt_id: Mapped[str] = mapped_column(String(36), primary_key=True)
    provider: Mapped[str] = mapped_column(String(64), nullable=False)
    binding_kind: Mapped[str] = mapped_column(String(32), nullable=False)
    binding_reference: Mapped[str] = mapped_column(String(320), nullable=False)
    provider_delivery_id: Mapped[str] = mapped_column(String(320), nullable=False)
    thread_id: Mapped[str] = mapped_column(String(64), nullable=False)
    payload_json: Mapped[dict[str, Any]] = mapped_column(JSON, nullable=False)
    payload_digest: Mapped[str] = mapped_column(String(64), nullable=False)
    state: Mapped[str] = mapped_column(String(16), nullable=False)
    lease_owner: Mapped[str | None] = mapped_column(String(96), nullable=True)
    lease_expires_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    fencing_token: Mapped[int] = mapped_column(Integer, nullable=False, default=0, server_default=text("0"))
    attempt_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0, server_default=text("0"))
    failure_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0, server_default=text("0"))
    next_attempt_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=func.now())
    run_id: Mapped[str | None] = mapped_column(String(64), nullable=True)
    outcome_code: Mapped[str | None] = mapped_column(String(64), nullable=True)
    received_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=func.now())
    completed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)

    __table_args__ = (
        UniqueConstraint(
            "provider",
            "binding_kind",
            "binding_reference",
            "provider_delivery_id",
            name="uq_inbound_receipts_source_delivery",
        ),
        CheckConstraint(
            "state IN ('received', 'claimed', 'admitted', 'deferred', 'completed', 'dead_letter')",
            name="ck_inbound_receipts_state",
        ),
        CheckConstraint(
            "fencing_token >= 0 AND attempt_count >= 0 AND failure_count >= 0",
            name="ck_inbound_receipts_counters_nonnegative",
        ),
        CheckConstraint(
            "(state NOT IN ('claimed', 'admitted')) OR (lease_owner IS NOT NULL AND lease_expires_at IS NOT NULL)",
            name="ck_inbound_receipts_claim_has_lease",
        ),
        CheckConstraint(
            "(state != 'admitted') OR run_id IS NOT NULL",
            name="ck_inbound_receipts_admitted_has_run",
        ),
        CheckConstraint(
            "length(provider) BETWEEN 1 AND 64 AND length(binding_reference) BETWEEN 1 AND 320 AND length(provider_delivery_id) BETWEEN 1 AND 320 AND length(thread_id) BETWEEN 1 AND 64",
            name="ck_inbound_receipts_identity_bounds",
        ),
        CheckConstraint(
            "length(payload_digest) = 64 AND lower(payload_digest) = payload_digest",
            name="ck_inbound_receipts_digest_format",
        ),
        Index(
            "ix_inbound_receipts_due",
            "state",
            "next_attempt_at",
            "received_at",
            "receipt_id",
        ),
        Index("ix_inbound_receipts_run_id", "run_id"),
        Index("ix_inbound_receipts_completed_at", "completed_at"),
    )
