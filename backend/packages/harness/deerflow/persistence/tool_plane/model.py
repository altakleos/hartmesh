"""ORM rows for governed tool-plane revisions and transition evidence."""

from __future__ import annotations

from datetime import datetime
from typing import Any

from sqlalchemy import (
    JSON,
    BigInteger,
    Boolean,
    CheckConstraint,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    String,
    UniqueConstraint,
    false,
)
from sqlalchemy.orm import Mapped, mapped_column

from deerflow.persistence.base import Base


class ToolPlaneScopeRow(Base):
    """Mutable active-pointer and generation fence for one tenant scope."""

    __tablename__ = "tool_plane_scopes"

    id: Mapped[str] = mapped_column(String(36), primary_key=True)
    tenant_ref: Mapped[str] = mapped_column(String(23), nullable=False)
    tenant_digest: Mapped[str] = mapped_column(String(64), nullable=False)
    scope_kind: Mapped[str] = mapped_column(String(32), nullable=False)
    scope_ref: Mapped[str] = mapped_column(String(256), nullable=False, default="")
    active_revision_id: Mapped[str | None] = mapped_column(
        String(36),
        ForeignKey("tool_plane_revisions.id", ondelete="RESTRICT", use_alter=True),
        nullable=True,
    )
    generation: Mapped[int] = mapped_column(BigInteger, nullable=False, default=0, server_default="0")
    overlay_set_generation: Mapped[int] = mapped_column(BigInteger, nullable=False, default=0, server_default="0")
    bootstrap_required: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False, server_default=false())
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)

    __table_args__ = (
        UniqueConstraint(
            "tenant_digest",
            "scope_kind",
            "scope_ref",
            name="uq_tool_plane_scope_key",
        ),
        CheckConstraint(
            "(scope_kind = 'deployment_base' AND scope_ref = '') OR (scope_kind = 'user_overlay' AND length(scope_ref) BETWEEN 1 AND 256)",
            name="ck_tool_plane_scope_shape",
        ),
        CheckConstraint(
            "length(tenant_ref) = 23 AND length(tenant_digest) = 64 AND generation >= 0 AND overlay_set_generation >= 0",
            name="ck_tool_plane_scope_bounds",
        ),
        Index("ix_tool_plane_scopes_tenant_kind", "tenant_digest", "scope_kind"),
    )


class ToolPlaneRevisionRow(Base):
    """Immutable revision material plus its monotonic lifecycle state."""

    __tablename__ = "tool_plane_revisions"

    id: Mapped[str] = mapped_column(String(36), primary_key=True)
    schema_writer_version: Mapped[int] = mapped_column(Integer, nullable=False, default=1, server_default="1")
    tenant_ref: Mapped[str] = mapped_column(String(23), nullable=False)
    tenant_digest: Mapped[str] = mapped_column(String(64), nullable=False)
    scope_kind: Mapped[str] = mapped_column(String(32), nullable=False)
    scope_ref: Mapped[str] = mapped_column(String(256), nullable=False, default="")
    revision_digest: Mapped[str] = mapped_column(String(64), nullable=False)
    content_digest: Mapped[str] = mapped_column(String(64), nullable=False)
    manifest_json: Mapped[dict[str, Any]] = mapped_column(JSON, nullable=False)
    parent_revision_digest: Mapped[str | None] = mapped_column(String(64), nullable=True)
    base_revision_digest: Mapped[str | None] = mapped_column(String(64), nullable=True)
    state: Mapped[str] = mapped_column(String(32), nullable=False)
    staging_actor_digest: Mapped[str] = mapped_column(String(64), nullable=False)
    staged_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    validation_report_json: Mapped[dict[str, Any] | None] = mapped_column(JSON, nullable=True)
    validation_report_digest: Mapped[str | None] = mapped_column(String(64), nullable=True)
    promotion_actor_digest: Mapped[str | None] = mapped_column(String(64), nullable=True)
    previous_revision_id: Mapped[str | None] = mapped_column(String(36), nullable=True)
    desired_projection_digest: Mapped[str | None] = mapped_column(String(64), nullable=True)
    observed_projection_digest: Mapped[str | None] = mapped_column(String(64), nullable=True)
    promoted_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    rollback_source_revision_id: Mapped[str | None] = mapped_column(
        String(36),
        ForeignKey("tool_plane_revisions.id", ondelete="RESTRICT", use_alter=True),
        nullable=True,
    )
    bootstrap_inventory_digest: Mapped[str | None] = mapped_column(
        String(64),
        nullable=True,
    )
    bootstrap_source_digest: Mapped[str | None] = mapped_column(
        String(64),
        nullable=True,
    )
    # Protected projection routing facts. These values are never returned by
    # ToolPlaneRevisionRecord.to_safe_json() or copied into audit evidence.
    storage_subject_id: Mapped[str | None] = mapped_column(String(512), nullable=True)
    bootstrap_overlay_revision_ids_json: Mapped[list[str]] = mapped_column(JSON, nullable=False, default=list)
    bootstrap_inventory_subject_ids_json: Mapped[list[str]] = mapped_column(JSON, nullable=False, default=list)

    __table_args__ = (
        CheckConstraint(
            "state IN ('staged', 'validating', 'validated', 'rejected', 'prepared', 'promoted', 'superseded', 'recovery_required')",
            name="ck_tool_plane_revision_state",
        ),
        CheckConstraint(
            "length(tenant_ref) = 23 AND length(tenant_digest) = 64 AND length(revision_digest) = 64 AND length(content_digest) = 64 AND length(staging_actor_digest) = 64",
            name="ck_tool_plane_revision_identity",
        ),
        CheckConstraint(
            "bootstrap_inventory_digest IS NULL OR length(bootstrap_inventory_digest) = 64",
            name="ck_tool_plane_revision_bootstrap_inventory",
        ),
        CheckConstraint(
            "bootstrap_source_digest IS NULL OR length(bootstrap_source_digest) = 64",
            name="ck_tool_plane_revision_bootstrap_source",
        ),
        CheckConstraint(
            "(scope_kind = 'deployment_base' AND storage_subject_id IS NULL) OR (scope_kind = 'user_overlay' AND length(storage_subject_id) BETWEEN 1 AND 512)",
            name="ck_tool_plane_revision_storage_subject",
        ),
        CheckConstraint(
            "(scope_kind = 'deployment_base' AND scope_ref = '' AND base_revision_digest IS NULL) OR (scope_kind = 'user_overlay' AND length(scope_ref) BETWEEN 1 AND 256 AND base_revision_digest IS NOT NULL)",
            name="ck_tool_plane_revision_scope",
        ),
        CheckConstraint(
            "(validation_report_json IS NULL) = (validation_report_digest IS NULL)",
            name="ck_tool_plane_revision_validation_pair",
        ),
        Index(
            "ix_tool_plane_revisions_scope_staged",
            "tenant_digest",
            "scope_kind",
            "scope_ref",
            "staged_at",
        ),
        Index(
            "ix_tool_plane_revisions_scope_digest",
            "tenant_digest",
            "scope_kind",
            "scope_ref",
            "revision_digest",
        ),
    )


class ToolPlaneRevisionEventRow(Base):
    """Append-only attributed transition event for one revision."""

    __tablename__ = "tool_plane_revision_events"

    id: Mapped[str] = mapped_column(String(36), primary_key=True)
    tenant_ref: Mapped[str] = mapped_column(String(23), nullable=False)
    tenant_digest: Mapped[str] = mapped_column(String(64), nullable=False)
    revision_id: Mapped[str] = mapped_column(
        String(36),
        ForeignKey("tool_plane_revisions.id", ondelete="RESTRICT"),
        nullable=False,
    )
    state: Mapped[str] = mapped_column(String(32), nullable=False)
    actor_digest: Mapped[str] = mapped_column(String(64), nullable=False)
    safe_details_json: Mapped[dict[str, Any]] = mapped_column(JSON, nullable=False, default=dict)
    occurred_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)

    __table_args__ = (
        CheckConstraint(
            "length(tenant_ref) = 23 AND length(tenant_digest) = 64 AND length(actor_digest) = 64",
            name="ck_tool_plane_event_identity",
        ),
        Index(
            "ix_tool_plane_events_tenant_revision_time",
            "tenant_digest",
            "revision_id",
            "occurred_at",
        ),
    )


class ToolPlaneOverlayCompatibilityRow(Base):
    """Immutable base/overlay compatibility attestation row."""

    __tablename__ = "tool_plane_overlay_compatibility"

    id: Mapped[str] = mapped_column(String(36), primary_key=True)
    tenant_ref: Mapped[str] = mapped_column(String(23), nullable=False)
    tenant_digest: Mapped[str] = mapped_column(String(64), nullable=False)
    base_revision_digest: Mapped[str] = mapped_column(String(64), nullable=False)
    overlay_revision_digest: Mapped[str] = mapped_column(String(64), nullable=False)
    validator_policy_digest: Mapped[str] = mapped_column(String(64), nullable=False)
    report_json: Mapped[dict[str, Any]] = mapped_column(JSON, nullable=False)
    report_digest: Mapped[str] = mapped_column(String(64), nullable=False)
    attestation_digest: Mapped[str] = mapped_column(String(64), nullable=False)
    compatible: Mapped[bool] = mapped_column(Boolean, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)

    __table_args__ = (
        UniqueConstraint(
            "tenant_digest",
            "base_revision_digest",
            "overlay_revision_digest",
            "validator_policy_digest",
            name="uq_tool_plane_overlay_compatibility",
        ),
        CheckConstraint(
            "length(tenant_ref) = 23 AND length(tenant_digest) = 64 AND "
            "length(base_revision_digest) = 64 AND length(overlay_revision_digest) = 64 AND "
            "length(validator_policy_digest) = 64 AND length(report_digest) = 64 AND "
            "length(attestation_digest) = 64",
            name="ck_tool_plane_compatibility_identity",
        ),
    )


__all__ = [
    "ToolPlaneOverlayCompatibilityRow",
    "ToolPlaneRevisionEventRow",
    "ToolPlaneRevisionRow",
    "ToolPlaneScopeRow",
]
