"""Bind durable subagent batches to accepted parent evidence.

Revision ID: 0032_subagent_batch_evidence
Revises: 0031_merge_upstream_0017
Create Date: 2026-09-02
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0032_subagent_batch_evidence"
down_revision: str | Sequence[str] | None = "0031_merge_upstream_0017"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def _names(kind: str, table: str) -> set[str]:
    inspector = sa.inspect(op.get_bind())
    if kind == "index":
        rows = inspector.get_indexes(table)
    elif kind == "unique":
        rows = inspector.get_unique_constraints(table)
    elif kind == "check":
        rows = inspector.get_check_constraints(table)
    else:  # pragma: no cover - internal misuse
        raise ValueError(kind)
    return {str(row["name"]) for row in rows if isinstance(row.get("name"), str)}


def upgrade() -> None:
    from deerflow.persistence.migrations._helpers import safe_add_column

    bind = op.get_bind()
    inspector = sa.inspect(bind)
    tables = set(inspector.get_table_names())
    if "subagent_batches" not in tables or "subagent_batch_items" not in tables:
        return

    for column in (
        sa.Column(
            "schema_writer_version",
            sa.Integer(),
            nullable=False,
            server_default=sa.text("1"),
        ),
        sa.Column("tenant_ref", sa.String(length=80), nullable=True),
        sa.Column("tenant_digest", sa.String(length=64), nullable=True),
        sa.Column("acceptance_json", sa.JSON(), nullable=True),
        sa.Column("acceptance_digest", sa.String(length=64), nullable=True),
        sa.Column("execution_json", sa.JSON(), nullable=True),
        sa.Column("execution_digest", sa.String(length=64), nullable=True),
        sa.Column("parent_invocation_digest", sa.String(length=64), nullable=True),
        sa.Column("parent_assembly_fingerprint", sa.String(length=64), nullable=True),
        sa.Column("parent_tool_receipt_id", sa.String(length=80), nullable=True),
        sa.Column("parent_tool_attempt", sa.Integer(), nullable=True),
        sa.Column("subagent_catalog_digest", sa.String(length=64), nullable=True),
        sa.Column("subagent_definition_digest", sa.String(length=64), nullable=True),
        sa.Column("item_root_digest", sa.String(length=64), nullable=True),
        sa.Column(
            "parent_cancellable",
            sa.Boolean(),
            nullable=False,
            server_default=sa.false(),
        ),
        sa.Column(
            "cancel_epoch",
            sa.Integer(),
            nullable=False,
            server_default=sa.text("0"),
        ),
        sa.Column("terminal_code", sa.String(length=64), nullable=True),
        sa.Column("accepted_at", sa.DateTime(timezone=True), nullable=True),
    ):
        safe_add_column("subagent_batches", column)

    for column in (
        sa.Column("request_digest", sa.String(length=64), nullable=True),
        sa.Column(
            "lease_epoch",
            sa.Integer(),
            nullable=False,
            server_default=sa.text("0"),
        ),
        sa.Column("active_attempt_id", sa.String(length=80), nullable=True),
        sa.Column("terminal_code", sa.String(length=64), nullable=True),
        sa.Column("terminal_evidence_digest", sa.String(length=64), nullable=True),
    ):
        safe_add_column("subagent_batch_items", column)

    unique_names = _names("unique", "subagent_batches")
    check_names = _names("check", "subagent_batches")
    with op.batch_alter_table("subagent_batches") as batch:
        if "uq_subagent_batches_user_submission" in unique_names:
            batch.drop_constraint(
                "uq_subagent_batches_user_submission",
                type_="unique",
            )
        if "uq_subagent_batches_tenant_receipt_submission" not in unique_names:
            batch.create_unique_constraint(
                "uq_subagent_batches_tenant_receipt_submission",
                ["tenant_digest", "parent_tool_receipt_id", "submission_key"],
            )
        if "ck_subagent_batches_writer_version" not in check_names:
            batch.create_check_constraint(
                "ck_subagent_batches_writer_version",
                "schema_writer_version IN (1, 2)",
            )
        if "ck_subagent_batches_cancel_epoch" not in check_names:
            batch.create_check_constraint(
                "ck_subagent_batches_cancel_epoch",
                "cancel_epoch >= 0",
            )
        if "ck_subagent_batches_bound_writer" not in check_names:
            batch.create_check_constraint(
                "ck_subagent_batches_bound_writer",
                "schema_writer_version = 1 OR ("
                "tenant_ref IS NOT NULL AND tenant_digest IS NOT NULL AND "
                "acceptance_json IS NOT NULL AND acceptance_digest IS NOT NULL AND "
                "execution_json IS NOT NULL AND execution_digest IS NOT NULL AND "
                "parent_invocation_digest IS NOT NULL AND "
                "parent_assembly_fingerprint IS NOT NULL AND "
                "parent_tool_receipt_id IS NOT NULL AND parent_tool_attempt IS NOT NULL AND "
                "subagent_catalog_digest IS NOT NULL AND "
                "subagent_definition_digest IS NOT NULL AND "
                "item_root_digest IS NOT NULL AND accepted_at IS NOT NULL)",
            )

    item_checks = _names("check", "subagent_batch_items")
    if "ck_subagent_batch_items_lease_epoch" not in item_checks:
        with op.batch_alter_table("subagent_batch_items") as batch:
            batch.create_check_constraint(
                "ck_subagent_batch_items_lease_epoch",
                "lease_epoch >= 0",
            )

    indexes = _names("index", "subagent_batches")
    if "uq_subagent_batches_legacy_user_submission" not in indexes:
        op.create_index(
            "uq_subagent_batches_legacy_user_submission",
            "subagent_batches",
            ["user_id", "submission_key"],
            unique=True,
            sqlite_where=sa.text("schema_writer_version = 1"),
            postgresql_where=sa.text("schema_writer_version = 1"),
        )
    if "ix_subagent_batches_tenant_thread_created" not in indexes:
        op.create_index(
            "ix_subagent_batches_tenant_thread_created",
            "subagent_batches",
            ["tenant_digest", "thread_id", "created_at"],
        )

    inspector = sa.inspect(bind)
    if "subagent_batch_attempts" not in inspector.get_table_names():
        op.create_table(
            "subagent_batch_attempts",
            sa.Column("id", sa.String(length=80), nullable=False),
            sa.Column("batch_id", sa.String(length=64), nullable=False),
            sa.Column("item_id", sa.String(length=64), nullable=False),
            sa.Column("tenant_digest", sa.String(length=64), nullable=False),
            sa.Column("attempt_number", sa.Integer(), nullable=False),
            sa.Column("lease_epoch", sa.Integer(), nullable=False),
            sa.Column("worker_ref", sa.String(length=64), nullable=False),
            sa.Column("status", sa.String(length=32), nullable=False),
            sa.Column(
                "consumed",
                sa.Boolean(),
                nullable=False,
                server_default=sa.true(),
            ),
            sa.Column("terminal_code", sa.String(length=64), nullable=True),
            sa.Column("evidence_json", sa.JSON(), nullable=True),
            sa.Column("evidence_digest", sa.String(length=64), nullable=True),
            sa.Column("claimed_at", sa.DateTime(timezone=True), nullable=False),
            sa.Column("started_at", sa.DateTime(timezone=True), nullable=True),
            sa.Column("terminal_at", sa.DateTime(timezone=True), nullable=True),
            sa.CheckConstraint(
                "attempt_number >= 1",
                name="ck_subagent_batch_attempt_number",
            ),
            sa.CheckConstraint(
                "lease_epoch >= 1",
                name="ck_subagent_batch_attempt_epoch",
            ),
            sa.ForeignKeyConstraint(
                ["batch_id"],
                ["subagent_batches.id"],
                ondelete="CASCADE",
            ),
            sa.ForeignKeyConstraint(
                ["item_id"],
                ["subagent_batch_items.id"],
                ondelete="CASCADE",
            ),
            sa.PrimaryKeyConstraint("id"),
            sa.UniqueConstraint(
                "item_id",
                "lease_epoch",
                name="uq_subagent_batch_attempt_item_epoch",
            ),
        )
        op.create_index(
            "ix_subagent_batch_attempts_batch_id",
            "subagent_batch_attempts",
            ["batch_id"],
        )
        op.create_index(
            "ix_subagent_batch_attempts_item_id",
            "subagent_batch_attempts",
            ["item_id"],
        )
        op.create_index(
            "ix_subagent_batch_attempts_tenant_digest",
            "subagent_batch_attempts",
            ["tenant_digest"],
        )
        op.create_index(
            "ix_subagent_batch_attempts_status",
            "subagent_batch_attempts",
            ["status"],
        )
        op.create_index(
            "ix_subagent_batch_attempts_tenant_batch_item",
            "subagent_batch_attempts",
            ["tenant_digest", "batch_id", "item_id", "lease_epoch"],
        )


def downgrade() -> None:
    from deerflow.persistence.migrations._helpers import safe_drop_column

    bind = op.get_bind()
    inspector = sa.inspect(bind)
    tables = set(inspector.get_table_names())
    if "subagent_batches" not in tables:
        return
    used = bind.execute(sa.text("SELECT 1 FROM subagent_batches WHERE schema_writer_version = 2 LIMIT 1")).first()
    attempts_used = bind.execute(sa.text("SELECT 1 FROM subagent_batch_attempts LIMIT 1")).first() if "subagent_batch_attempts" in tables else None
    if used is not None or attempts_used is not None:
        raise RuntimeError("subagent_batch_evidence_downgrade_blocked")

    if "subagent_batch_attempts" in tables:
        op.drop_table("subagent_batch_attempts")

    indexes = _names("index", "subagent_batches")
    for name in (
        "uq_subagent_batches_legacy_user_submission",
        "ix_subagent_batches_tenant_thread_created",
    ):
        if name in indexes:
            op.drop_index(name, table_name="subagent_batches")

    unique_names = _names("unique", "subagent_batches")
    check_names = _names("check", "subagent_batches")
    with op.batch_alter_table("subagent_batches") as batch:
        if "uq_subagent_batches_tenant_receipt_submission" in unique_names:
            batch.drop_constraint(
                "uq_subagent_batches_tenant_receipt_submission",
                type_="unique",
            )
        for name in (
            "ck_subagent_batches_writer_version",
            "ck_subagent_batches_cancel_epoch",
            "ck_subagent_batches_bound_writer",
        ):
            if name in check_names:
                batch.drop_constraint(name, type_="check")
        if "uq_subagent_batches_user_submission" not in unique_names:
            batch.create_unique_constraint(
                "uq_subagent_batches_user_submission",
                ["user_id", "submission_key"],
            )

    item_checks = _names("check", "subagent_batch_items")
    if "ck_subagent_batch_items_lease_epoch" in item_checks:
        with op.batch_alter_table("subagent_batch_items") as batch:
            batch.drop_constraint(
                "ck_subagent_batch_items_lease_epoch",
                type_="check",
            )

    for name in (
        "request_digest",
        "lease_epoch",
        "active_attempt_id",
        "terminal_code",
        "terminal_evidence_digest",
    ):
        safe_drop_column("subagent_batch_items", name)
    for name in (
        "schema_writer_version",
        "tenant_ref",
        "tenant_digest",
        "acceptance_json",
        "acceptance_digest",
        "execution_json",
        "execution_digest",
        "parent_invocation_digest",
        "parent_assembly_fingerprint",
        "parent_tool_receipt_id",
        "parent_tool_attempt",
        "subagent_catalog_digest",
        "subagent_definition_digest",
        "item_root_digest",
        "parent_cancellable",
        "cancel_epoch",
        "terminal_code",
        "accepted_at",
    ):
        safe_drop_column("subagent_batches", name)
