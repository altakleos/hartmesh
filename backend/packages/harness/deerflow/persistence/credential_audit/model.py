"""ORM model for aggregated, secret-free credential audit observations."""

from __future__ import annotations

from datetime import datetime

from sqlalchemy import BigInteger, CheckConstraint, DateTime, Index, String
from sqlalchemy.orm import Mapped, mapped_column

from deerflow.persistence.base import Base


class CredentialAuditEventRow(Base):
    __tablename__ = "credential_audit_events"

    __table_args__ = (
        CheckConstraint(
            "length(aggregation_key) = 64 AND lower(aggregation_key) = aggregation_key AND length(tenant_ref) = 23 AND length(tenant_digest) = 64 AND lower(tenant_digest) = tenant_digest",
            name="ck_credential_audit_identity_shape",
        ),
        CheckConstraint(
            "(credential_ref IS NULL OR "
            "(length(credential_ref) BETWEEN 1 AND 128 AND "
            "substr(credential_ref, 1, 4) <> 'dfp_')) AND "
            "(actor_digest IS NULL OR "
            "(length(actor_digest) = 64 AND "
            "lower(actor_digest) = actor_digest)) AND "
            "(authority_digest IS NULL OR "
            "(length(authority_digest) = 64 AND "
            "lower(authority_digest) = authority_digest))",
            name="ck_credential_audit_safe_references",
        ),
        CheckConstraint(
            "method IN ('session', 'personal_access_token', 'internal_service', 'channel', 'development_bypass')",
            name="ck_credential_audit_method",
        ),
        CheckConstraint(
            "action IN ('created', 'authenticated', 'authentication_failed', 'admission', 'control', 'expired', 'revoked', 'scope_changed')",
            name="ck_credential_audit_action",
        ),
        CheckConstraint(
            "reason_code IS NULL OR reason_code IN "
            "('credential_invalid', 'credential_expired', "
            "'credential_revoked', 'credential_tenant_mismatch', "
            "'scope_required', 'credential_evidence_unavailable', "
            "'authority_digest_mismatch', "
            "'credential_reference_conflict', "
            "'audit_record_unavailable')",
            name="ck_credential_audit_reason",
        ),
        CheckConstraint(
            "length(route_category) BETWEEN 1 AND 64 AND event_count > 0 AND last_occurred_at >= first_occurred_at",
            name="ck_credential_audit_bounds",
        ),
        Index(
            "ix_credential_audit_tenant_credential_last",
            "tenant_digest",
            "credential_ref",
            "last_occurred_at",
        ),
        Index(
            "ix_credential_audit_tenant_last",
            "tenant_digest",
            "last_occurred_at",
        ),
        Index(
            "ix_credential_audit_aggregation_key",
            "aggregation_key",
            unique=True,
        ),
    )

    id: Mapped[str] = mapped_column(String(36), primary_key=True)
    aggregation_key: Mapped[str] = mapped_column(String(64), nullable=False)
    tenant_ref: Mapped[str] = mapped_column(String(23), nullable=False)
    tenant_digest: Mapped[str] = mapped_column(String(64), nullable=False)
    credential_ref: Mapped[str | None] = mapped_column(String(128), nullable=True)
    actor_digest: Mapped[str | None] = mapped_column(String(64), nullable=True)
    method: Mapped[str] = mapped_column(String(32), nullable=False)
    authority_digest: Mapped[str | None] = mapped_column(String(64), nullable=True)
    action: Mapped[str] = mapped_column(String(32), nullable=False)
    route_category: Mapped[str] = mapped_column(String(64), nullable=False)
    reason_code: Mapped[str | None] = mapped_column(String(64), nullable=True)
    bucket_start: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    first_occurred_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    last_occurred_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    event_count: Mapped[int] = mapped_column(BigInteger, nullable=False)
