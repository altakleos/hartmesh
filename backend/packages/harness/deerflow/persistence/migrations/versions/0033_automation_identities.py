"""Bind credentials to tenant identity and add bounded credential audit.

Revision ID: 0033_automation_identities
Revises: 0032_subagent_batch_evidence
Create Date: 2026-09-02
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0033_automation_identities"
down_revision: str | Sequence[str] | None = "0032_subagent_batch_evidence"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def _names(kind: str, table: str) -> set[str]:
    inspector = sa.inspect(op.get_bind())
    if kind == "index":
        rows = inspector.get_indexes(table)
    elif kind == "check":
        rows = inspector.get_check_constraints(table)
    else:  # pragma: no cover - internal misuse
        raise ValueError(kind)
    return {str(row["name"]) for row in rows if isinstance(row.get("name"), str)}


def _add_pat_tenant_anchors() -> None:
    from deerflow.persistence.migrations._helpers import safe_add_column

    bind = op.get_bind()
    if "personal_access_tokens" not in sa.inspect(bind).get_table_names():
        return
    safe_add_column(
        "personal_access_tokens",
        sa.Column("tenant_ref", sa.String(length=23), nullable=True),
    )
    safe_add_column(
        "personal_access_tokens",
        sa.Column("tenant_digest", sa.String(length=64), nullable=True),
    )

    checks = _names("check", "personal_access_tokens")
    if "ck_personal_access_tokens_tenant_pair" not in checks:
        with op.batch_alter_table("personal_access_tokens") as batch:
            batch.create_check_constraint(
                "ck_personal_access_tokens_tenant_pair",
                "(tenant_ref IS NULL) = (tenant_digest IS NULL)",
            )
    indexes = _names("index", "personal_access_tokens")
    for name, columns in (
        (
            "ix_personal_access_tokens_tenant_digest_token_digest",
            ["tenant_digest", "token_digest"],
        ),
        (
            "ix_personal_access_tokens_tenant_digest_user_created",
            ["tenant_digest", "user_id", "created_at"],
        ),
    ):
        if name not in indexes:
            op.create_index(
                name,
                "personal_access_tokens",
                columns,
                unique=False,
            )


def _require_binding_for_populated_legacy_pats() -> None:
    """Fail before SQLite batch DDL can leave retry artifacts behind."""

    bind = op.get_bind()
    tables = set(sa.inspect(bind).get_table_names())
    if "personal_access_tokens" not in tables:
        return
    if bind.execute(sa.text("SELECT 1 FROM personal_access_tokens LIMIT 1")).first() is None:
        return
    binding = None
    if "hartmesh_deployment_identity" in tables:
        binding = bind.execute(sa.text("SELECT 1 FROM hartmesh_deployment_identity WHERE singleton_key = 1")).first()
    if binding is None:
        raise RuntimeError("credential_tenant_binding_required")


def _backfill_from_bound_singleton() -> None:
    bind = op.get_bind()
    tables = set(sa.inspect(bind).get_table_names())
    if (
        not {
            "personal_access_tokens",
            "hartmesh_deployment_identity",
        }
        <= tables
    ):
        return
    binding = bind.execute(sa.text("SELECT tenant_ref, tenant_digest FROM hartmesh_deployment_identity WHERE singleton_key = 1")).first()
    if binding is None:
        populated = bind.execute(sa.text("SELECT 1 FROM personal_access_tokens LIMIT 1")).first()
        if populated is not None:
            raise RuntimeError("credential_tenant_binding_required")
        return
    tenant_ref, tenant_digest = binding
    conflict = bind.execute(
        sa.text("SELECT 1 FROM personal_access_tokens WHERE tenant_digest IS NOT NULL AND (tenant_digest <> :tenant_digest OR tenant_ref <> :tenant_ref) LIMIT 1"),
        {"tenant_ref": tenant_ref, "tenant_digest": tenant_digest},
    ).first()
    if conflict is not None:
        raise RuntimeError("credential_tenant_mismatch")
    bind.execute(
        sa.text("UPDATE personal_access_tokens SET tenant_ref = :tenant_ref, tenant_digest = :tenant_digest WHERE tenant_ref IS NULL AND tenant_digest IS NULL"),
        {"tenant_ref": tenant_ref, "tenant_digest": tenant_digest},
    )


def _require_pat_tenant_anchors() -> None:
    """Finish the nullable/backfill/constraint sequence for PAT anchors."""

    bind = op.get_bind()
    if "personal_access_tokens" not in sa.inspect(bind).get_table_names():
        return
    missing = bind.execute(sa.text("SELECT 1 FROM personal_access_tokens WHERE tenant_ref IS NULL OR tenant_digest IS NULL LIMIT 1")).first()
    if missing is not None:
        raise RuntimeError("credential_tenant_binding_required")
    columns = {str(column["name"]): column for column in sa.inspect(bind).get_columns("personal_access_tokens")}
    if not columns["tenant_ref"].get("nullable") and not columns["tenant_digest"].get("nullable"):
        return
    with op.batch_alter_table("personal_access_tokens") as batch:
        batch.alter_column(
            "tenant_ref",
            existing_type=sa.String(length=23),
            nullable=False,
        )
        batch.alter_column(
            "tenant_digest",
            existing_type=sa.String(length=64),
            nullable=False,
        )


def _create_audit_table() -> None:
    bind = op.get_bind()
    if "credential_audit_events" in sa.inspect(bind).get_table_names():
        return
    op.create_table(
        "credential_audit_events",
        sa.Column("id", sa.String(length=36), nullable=False),
        sa.Column("aggregation_key", sa.String(length=64), nullable=False),
        sa.Column("tenant_ref", sa.String(length=23), nullable=False),
        sa.Column("tenant_digest", sa.String(length=64), nullable=False),
        sa.Column("credential_ref", sa.String(length=128), nullable=True),
        sa.Column("actor_digest", sa.String(length=64), nullable=True),
        sa.Column("method", sa.String(length=32), nullable=False),
        sa.Column("authority_digest", sa.String(length=64), nullable=True),
        sa.Column("action", sa.String(length=32), nullable=False),
        sa.Column("route_category", sa.String(length=64), nullable=False),
        sa.Column("reason_code", sa.String(length=64), nullable=True),
        sa.Column("bucket_start", sa.DateTime(timezone=True), nullable=False),
        sa.Column("first_occurred_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("last_occurred_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("event_count", sa.BigInteger(), nullable=False),
        sa.CheckConstraint(
            "length(aggregation_key) = 64 AND lower(aggregation_key) = aggregation_key AND length(tenant_ref) = 23 AND length(tenant_digest) = 64 AND lower(tenant_digest) = tenant_digest",
            name="ck_credential_audit_identity_shape",
        ),
        sa.CheckConstraint(
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
        sa.CheckConstraint(
            "method IN ('session', 'personal_access_token', 'internal_service', 'channel', 'development_bypass')",
            name="ck_credential_audit_method",
        ),
        sa.CheckConstraint(
            "action IN ('created', 'authenticated', 'authentication_failed', 'admission', 'control', 'expired', 'revoked', 'scope_changed')",
            name="ck_credential_audit_action",
        ),
        sa.CheckConstraint(
            "reason_code IS NULL OR reason_code IN "
            "('credential_invalid', 'credential_expired', "
            "'credential_revoked', 'credential_tenant_mismatch', "
            "'scope_required', 'credential_evidence_unavailable', "
            "'authority_digest_mismatch', "
            "'credential_reference_conflict', "
            "'audit_record_unavailable')",
            name="ck_credential_audit_reason",
        ),
        sa.CheckConstraint(
            "length(route_category) BETWEEN 1 AND 64 AND event_count > 0 AND last_occurred_at >= first_occurred_at",
            name="ck_credential_audit_bounds",
        ),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        "ix_credential_audit_aggregation_key",
        "credential_audit_events",
        ["aggregation_key"],
        unique=True,
    )
    op.create_index(
        "ix_credential_audit_tenant_credential_last",
        "credential_audit_events",
        ["tenant_digest", "credential_ref", "last_occurred_at"],
        unique=False,
    )
    op.create_index(
        "ix_credential_audit_tenant_last",
        "credential_audit_events",
        ["tenant_digest", "last_occurred_at"],
        unique=False,
    )


def upgrade() -> None:
    _require_binding_for_populated_legacy_pats()
    _add_pat_tenant_anchors()
    _backfill_from_bound_singleton()
    _require_pat_tenant_anchors()
    _create_audit_table()


def downgrade() -> None:
    from deerflow.persistence.migrations._helpers import safe_drop_column

    bind = op.get_bind()
    tables = set(sa.inspect(bind).get_table_names())
    if "credential_audit_events" in tables:
        if bind.execute(sa.text("SELECT 1 FROM credential_audit_events LIMIT 1")).first() is not None:
            raise RuntimeError("auditable_automation_identities_downgrade_blocked")
    if "personal_access_tokens" in tables:
        columns = {column["name"] for column in sa.inspect(bind).get_columns("personal_access_tokens")}
        if {"tenant_ref", "tenant_digest"} <= columns and bind.execute(sa.text("SELECT 1 FROM personal_access_tokens WHERE tenant_ref IS NOT NULL OR tenant_digest IS NOT NULL LIMIT 1")).first() is not None:
            raise RuntimeError("auditable_automation_identities_downgrade_blocked")
    if "credential_audit_events" in tables:
        op.drop_table("credential_audit_events")
    if "personal_access_tokens" not in tables:
        return
    indexes = _names("index", "personal_access_tokens")
    for name in (
        "ix_personal_access_tokens_tenant_digest_token_digest",
        "ix_personal_access_tokens_tenant_digest_user_created",
    ):
        if name in indexes:
            op.drop_index(name, table_name="personal_access_tokens")
    checks = _names("check", "personal_access_tokens")
    if "ck_personal_access_tokens_tenant_pair" in checks:
        with op.batch_alter_table("personal_access_tokens") as batch:
            batch.drop_constraint(
                "ck_personal_access_tokens_tenant_pair",
                type_="check",
            )
    safe_drop_column("personal_access_tokens", "tenant_digest")
    safe_drop_column("personal_access_tokens", "tenant_ref")
