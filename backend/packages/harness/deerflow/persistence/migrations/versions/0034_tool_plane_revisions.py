"""Add governed skill and MCP revision evidence.

Revision ID: 0034_tool_plane_revisions
Revises: 0033_automation_identities
Create Date: 2026-09-03
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0034_tool_plane_revisions"
down_revision: str | Sequence[str] | None = "0033_automation_identities"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_TABLES = frozenset(
    {
        "tool_plane_revisions",
        "tool_plane_scopes",
        "tool_plane_revision_events",
        "tool_plane_overlay_compatibility",
    }
)


def upgrade() -> None:
    """Create the governed revision, scope, event, and attestation tables."""

    # The supported legacy bootstrap tests and backfills schemas that were
    # originally created with current ORM metadata before Alembic ownership
    # was introduced. In that case all four exact tables already exist. Treat
    # that complete shape like create_all(checkfirst=True), while refusing a
    # partial set left by a failed/manual migration instead of guessing.
    existing = _TABLES.intersection(sa.inspect(op.get_bind()).get_table_names())
    if existing:
        if existing != _TABLES:
            raise RuntimeError("governed_tool_plane_schema_incomplete")
        return

    op.create_table(
        "tool_plane_revisions",
        sa.Column("id", sa.String(length=36), nullable=False),
        sa.Column("schema_writer_version", sa.Integer(), server_default="1", nullable=False),
        sa.Column("tenant_ref", sa.String(length=23), nullable=False),
        sa.Column("tenant_digest", sa.String(length=64), nullable=False),
        sa.Column("scope_kind", sa.String(length=32), nullable=False),
        sa.Column("scope_ref", sa.String(length=256), nullable=False),
        sa.Column("revision_digest", sa.String(length=64), nullable=False),
        sa.Column("content_digest", sa.String(length=64), nullable=False),
        sa.Column("manifest_json", sa.JSON(), nullable=False),
        sa.Column("parent_revision_digest", sa.String(length=64), nullable=True),
        sa.Column("base_revision_digest", sa.String(length=64), nullable=True),
        sa.Column("state", sa.String(length=32), nullable=False),
        sa.Column("staging_actor_digest", sa.String(length=64), nullable=False),
        sa.Column("staged_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("validation_report_json", sa.JSON(), nullable=True),
        sa.Column("validation_report_digest", sa.String(length=64), nullable=True),
        sa.Column("promotion_actor_digest", sa.String(length=64), nullable=True),
        sa.Column("previous_revision_id", sa.String(length=36), nullable=True),
        sa.Column("desired_projection_digest", sa.String(length=64), nullable=True),
        sa.Column("observed_projection_digest", sa.String(length=64), nullable=True),
        sa.Column("promoted_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("rollback_source_revision_id", sa.String(length=36), nullable=True),
        sa.Column("bootstrap_inventory_digest", sa.String(length=64), nullable=True),
        sa.Column("storage_subject_id", sa.String(length=512), nullable=True),
        sa.Column(
            "bootstrap_overlay_revision_ids_json",
            sa.JSON(),
            nullable=False,
        ),
        sa.Column(
            "bootstrap_inventory_subject_ids_json",
            sa.JSON(),
            nullable=False,
        ),
        sa.CheckConstraint(
            "state IN ('staged', 'validating', 'validated', 'rejected', 'prepared', 'promoted', 'superseded', 'recovery_required')",
            name="ck_tool_plane_revision_state",
        ),
        sa.CheckConstraint(
            "length(tenant_ref) = 23 AND length(tenant_digest) = 64 AND length(revision_digest) = 64 AND length(content_digest) = 64 AND length(staging_actor_digest) = 64",
            name="ck_tool_plane_revision_identity",
        ),
        sa.CheckConstraint(
            "(scope_kind = 'deployment_base' AND scope_ref = '' AND base_revision_digest IS NULL) OR (scope_kind = 'user_overlay' AND length(scope_ref) BETWEEN 1 AND 256 AND base_revision_digest IS NOT NULL)",
            name="ck_tool_plane_revision_scope",
        ),
        sa.CheckConstraint(
            "(validation_report_json IS NULL) = (validation_report_digest IS NULL)",
            name="ck_tool_plane_revision_validation_pair",
        ),
        sa.CheckConstraint(
            "bootstrap_inventory_digest IS NULL OR length(bootstrap_inventory_digest) = 64",
            name="ck_tool_plane_revision_bootstrap_inventory",
        ),
        sa.CheckConstraint(
            "(scope_kind = 'deployment_base' AND storage_subject_id IS NULL) OR (scope_kind = 'user_overlay' AND length(storage_subject_id) BETWEEN 1 AND 512)",
            name="ck_tool_plane_revision_storage_subject",
        ),
        sa.ForeignKeyConstraint(
            ["rollback_source_revision_id"],
            ["tool_plane_revisions.id"],
            name="fk_tool_plane_revision_rollback_source",
            ondelete="RESTRICT",
        ),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        "ix_tool_plane_revisions_scope_staged",
        "tool_plane_revisions",
        ["tenant_digest", "scope_kind", "scope_ref", "staged_at"],
        unique=False,
    )
    op.create_index(
        "ix_tool_plane_revisions_scope_digest",
        "tool_plane_revisions",
        ["tenant_digest", "scope_kind", "scope_ref", "revision_digest"],
        unique=False,
    )

    op.create_table(
        "tool_plane_scopes",
        sa.Column("id", sa.String(length=36), nullable=False),
        sa.Column("tenant_ref", sa.String(length=23), nullable=False),
        sa.Column("tenant_digest", sa.String(length=64), nullable=False),
        sa.Column("scope_kind", sa.String(length=32), nullable=False),
        sa.Column("scope_ref", sa.String(length=256), nullable=False),
        sa.Column("active_revision_id", sa.String(length=36), nullable=True),
        sa.Column("generation", sa.BigInteger(), server_default="0", nullable=False),
        sa.Column("overlay_set_generation", sa.BigInteger(), server_default="0", nullable=False),
        sa.Column("bootstrap_required", sa.Boolean(), server_default=sa.false(), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.CheckConstraint(
            "(scope_kind = 'deployment_base' AND scope_ref = '') OR (scope_kind = 'user_overlay' AND length(scope_ref) BETWEEN 1 AND 256)",
            name="ck_tool_plane_scope_shape",
        ),
        sa.CheckConstraint(
            "length(tenant_ref) = 23 AND length(tenant_digest) = 64 AND generation >= 0 AND overlay_set_generation >= 0",
            name="ck_tool_plane_scope_bounds",
        ),
        sa.ForeignKeyConstraint(
            ["active_revision_id"],
            ["tool_plane_revisions.id"],
            name="fk_tool_plane_scope_active_revision",
            ondelete="RESTRICT",
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "tenant_digest",
            "scope_kind",
            "scope_ref",
            name="uq_tool_plane_scope_key",
        ),
    )
    op.create_index(
        "ix_tool_plane_scopes_tenant_kind",
        "tool_plane_scopes",
        ["tenant_digest", "scope_kind"],
        unique=False,
    )

    op.create_table(
        "tool_plane_revision_events",
        sa.Column("id", sa.String(length=36), nullable=False),
        sa.Column("tenant_ref", sa.String(length=23), nullable=False),
        sa.Column("tenant_digest", sa.String(length=64), nullable=False),
        sa.Column("revision_id", sa.String(length=36), nullable=False),
        sa.Column("state", sa.String(length=32), nullable=False),
        sa.Column("actor_digest", sa.String(length=64), nullable=False),
        sa.Column("safe_details_json", sa.JSON(), nullable=False),
        sa.Column("occurred_at", sa.DateTime(timezone=True), nullable=False),
        sa.CheckConstraint(
            "length(tenant_ref) = 23 AND length(tenant_digest) = 64 AND length(actor_digest) = 64",
            name="ck_tool_plane_event_identity",
        ),
        sa.ForeignKeyConstraint(
            ["revision_id"],
            ["tool_plane_revisions.id"],
            ondelete="RESTRICT",
        ),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        "ix_tool_plane_events_tenant_revision_time",
        "tool_plane_revision_events",
        ["tenant_digest", "revision_id", "occurred_at"],
        unique=False,
    )

    op.create_table(
        "tool_plane_overlay_compatibility",
        sa.Column("id", sa.String(length=36), nullable=False),
        sa.Column("tenant_ref", sa.String(length=23), nullable=False),
        sa.Column("tenant_digest", sa.String(length=64), nullable=False),
        sa.Column("base_revision_digest", sa.String(length=64), nullable=False),
        sa.Column("overlay_revision_digest", sa.String(length=64), nullable=False),
        sa.Column("validator_policy_digest", sa.String(length=64), nullable=False),
        sa.Column("report_json", sa.JSON(), nullable=False),
        sa.Column("report_digest", sa.String(length=64), nullable=False),
        sa.Column("attestation_digest", sa.String(length=64), nullable=False),
        sa.Column("compatible", sa.Boolean(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.CheckConstraint(
            "length(tenant_ref) = 23 AND length(tenant_digest) = 64 AND "
            "length(base_revision_digest) = 64 AND length(overlay_revision_digest) = 64 AND "
            "length(validator_policy_digest) = 64 AND length(report_digest) = 64 AND "
            "length(attestation_digest) = 64",
            name="ck_tool_plane_compatibility_identity",
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "tenant_digest",
            "base_revision_digest",
            "overlay_revision_digest",
            "validator_policy_digest",
            name="uq_tool_plane_overlay_compatibility",
        ),
    )


def downgrade() -> None:
    """Remove unused governed tables, refusing any data-bearing downgrade."""

    bind = op.get_bind()
    tables = set(sa.inspect(bind).get_table_names())
    for table in (
        "tool_plane_revision_events",
        "tool_plane_overlay_compatibility",
        "tool_plane_scopes",
        "tool_plane_revisions",
    ):
        if table in tables and bind.execute(sa.text(f"SELECT 1 FROM {table} LIMIT 1")).first() is not None:
            raise RuntimeError("governed_tool_plane_downgrade_blocked")
    for table in (
        "tool_plane_revision_events",
        "tool_plane_overlay_compatibility",
        "tool_plane_scopes",
        "tool_plane_revisions",
    ):
        if table in tables:
            op.drop_table(table)
