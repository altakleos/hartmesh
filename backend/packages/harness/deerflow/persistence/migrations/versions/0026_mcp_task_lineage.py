"""Bind durable MCP tasks to tenant-scoped immutable lineage.

Revision ID: 0026_mcp_task_lineage
Revises: 0025_tenant_identity
Create Date: 2026-08-31
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0026_mcp_task_lineage"
down_revision: str | Sequence[str] | None = "0025_tenant_identity"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_OLD_REMOTE_UNIQUE = "uq_mcp_tasks_user_server_remote"
_NEW_REMOTE_UNIQUE = "uq_mcp_tasks_tenant_user_server_remote"
_LINEAGE_UNIQUE = "uq_mcp_tasks_tenant_lineage"


def _index_names() -> set[str]:
    bind = op.get_bind()
    if "mcp_tasks" not in sa.inspect(bind).get_table_names():
        return set()
    return {index["name"] for index in sa.inspect(bind).get_indexes("mcp_tasks") if isinstance(index.get("name"), str)}


def _unique_names() -> set[str]:
    bind = op.get_bind()
    if "mcp_tasks" not in sa.inspect(bind).get_table_names():
        return set()
    return {item["name"] for item in sa.inspect(bind).get_unique_constraints("mcp_tasks") if isinstance(item.get("name"), str)}


def upgrade() -> None:
    from deerflow.persistence.migrations._helpers import safe_add_column

    for column in (
        sa.Column(
            "schema_writer_version",
            sa.Integer(),
            nullable=False,
            server_default="1",
        ),
        sa.Column("tenant_ref", sa.String(length=23), nullable=True),
        sa.Column("tenant_digest", sa.String(length=64), nullable=True),
        sa.Column("lineage_json", sa.JSON(), nullable=True),
        sa.Column("lineage_digest", sa.String(length=64), nullable=True),
        sa.Column("parent_run_id", sa.String(length=64), nullable=True),
        sa.Column("parent_tool_receipt_id", sa.String(length=67), nullable=True),
        sa.Column("cancel_actor_ref", sa.String(length=64), nullable=True),
        sa.Column("cancel_reason_code", sa.String(length=32), nullable=True),
    ):
        safe_add_column("mcp_tasks", column)

    bind = op.get_bind()
    if "mcp_tasks" not in sa.inspect(bind).get_table_names():
        return

    unique_names = _unique_names()
    with op.batch_alter_table("mcp_tasks") as batch_op:
        if _OLD_REMOTE_UNIQUE in unique_names:
            batch_op.drop_constraint(_OLD_REMOTE_UNIQUE, type_="unique")
        if _NEW_REMOTE_UNIQUE not in unique_names:
            batch_op.create_unique_constraint(
                _NEW_REMOTE_UNIQUE,
                ["tenant_digest", "user_id", "server_name", "remote_task_id"],
            )
        if _LINEAGE_UNIQUE not in unique_names:
            batch_op.create_unique_constraint(
                _LINEAGE_UNIQUE,
                ["tenant_digest", "lineage_digest"],
            )

        checks = {item["name"] for item in sa.inspect(bind).get_check_constraints("mcp_tasks") if isinstance(item.get("name"), str)}
        if "ck_mcp_tasks_tenant_pair" not in checks:
            batch_op.create_check_constraint(
                "ck_mcp_tasks_tenant_pair",
                "(tenant_ref IS NULL) = (tenant_digest IS NULL)",
            )
        if "ck_mcp_tasks_lineage_pair" not in checks:
            batch_op.create_check_constraint(
                "ck_mcp_tasks_lineage_pair",
                "(lineage_json IS NULL) = (lineage_digest IS NULL)",
            )
        if "ck_mcp_tasks_legacy_parent_null" not in checks:
            batch_op.create_check_constraint(
                "ck_mcp_tasks_legacy_parent_null",
                "lineage_digest IS NOT NULL OR (parent_run_id IS NULL AND parent_tool_receipt_id IS NULL)",
            )
        if "ck_mcp_tasks_schema_writer_version" not in checks:
            batch_op.create_check_constraint(
                "ck_mcp_tasks_schema_writer_version",
                "schema_writer_version >= 1",
            )
        if "ck_mcp_tasks_writer_lineage_required" not in checks:
            batch_op.create_check_constraint(
                "ck_mcp_tasks_writer_lineage_required",
                "schema_writer_version < 2 OR (tenant_ref IS NOT NULL AND tenant_digest IS NOT NULL AND lineage_json IS NOT NULL AND lineage_digest IS NOT NULL)",
            )
        if "ck_mcp_tasks_cancel_intent_pair" not in checks:
            batch_op.create_check_constraint(
                "ck_mcp_tasks_cancel_intent_pair",
                "(cancel_actor_ref IS NULL) = (cancel_reason_code IS NULL)",
            )
        if "ck_mcp_tasks_writer_cancel_intent" not in checks:
            batch_op.create_check_constraint(
                "ck_mcp_tasks_writer_cancel_intent",
                "schema_writer_version < 2 OR cancel_requested_at IS NULL OR cancel_actor_ref IS NOT NULL",
            )
        if "ck_mcp_tasks_cancel_intent_shape" not in checks:
            batch_op.create_check_constraint(
                "ck_mcp_tasks_cancel_intent_shape",
                "(cancel_actor_ref IS NULL OR length(cancel_actor_ref) = 64) AND (cancel_reason_code IS NULL OR cancel_reason_code IN ('user_api', 'agent_tool'))",
            )
        if "ck_mcp_tasks_lineage_lengths" not in checks:
            batch_op.create_check_constraint(
                "ck_mcp_tasks_lineage_lengths",
                "(tenant_ref IS NULL OR length(tenant_ref) = 23) AND "
                "(tenant_digest IS NULL OR length(tenant_digest) = 64) AND "
                "(lineage_digest IS NULL OR length(lineage_digest) = 64) AND "
                "(parent_run_id IS NULL OR length(parent_run_id) <= 64) AND "
                "(parent_tool_receipt_id IS NULL OR length(parent_tool_receipt_id) = 67) AND "
                "(notification_run_id IS NULL OR length(notification_run_id) <= 64)",
            )

    indexes = _index_names()
    definitions = {
        "ix_mcp_tasks_tenant_thread_created": [
            "tenant_digest",
            "thread_id",
            "created_at",
            "id",
        ],
        "ix_mcp_tasks_tenant_due": ["tenant_digest", "status", "next_poll_at"],
        "ix_mcp_tasks_tenant_notification_due": [
            "tenant_digest",
            "notification_status",
            "next_notification_at",
        ],
        "ix_mcp_tasks_tenant_cancel_due": [
            "tenant_digest",
            "cancel_requested_at",
            "next_cancel_at",
        ],
        "ix_mcp_tasks_tenant_parent_run": [
            "tenant_digest",
            "parent_run_id",
            "created_at",
            "id",
        ],
        "ix_mcp_tasks_tenant_parent_receipt": [
            "tenant_digest",
            "parent_tool_receipt_id",
        ],
        "ix_mcp_tasks_tenant_notification_run": [
            "tenant_digest",
            "notification_run_id",
        ],
    }
    for name, columns in definitions.items():
        if name not in indexes:
            op.create_index(name, "mcp_tasks", columns, unique=False)


def downgrade() -> None:
    """Leave additive nullable lineage columns for mixed-version rollback.

    Project 05 columns and tenant-scoped uniqueness constraints remain, while
    the predecessor remote-task uniqueness constraint is restored for the old
    writer. Project 05 checks and indexes are removed so older migrations can
    still drop and restore the columns they own. Once a v2 row exists,
    downgrade is blocked so an older binary sees the unknown 0026 revision and
    refuses startup instead of mutating that row.
    """

    bind = op.get_bind()
    if "mcp_tasks" not in sa.inspect(bind).get_table_names():
        return
    maximum_writer_version = bind.execute(sa.text("SELECT MAX(schema_writer_version) FROM mcp_tasks")).scalar_one_or_none()
    if maximum_writer_version is not None and int(maximum_writer_version) >= 2:
        raise RuntimeError("mcp_task_schema_writer_rollback_blocked")

    for name in (
        "ix_mcp_tasks_tenant_thread_created",
        "ix_mcp_tasks_tenant_due",
        "ix_mcp_tasks_tenant_notification_due",
        "ix_mcp_tasks_tenant_cancel_due",
        "ix_mcp_tasks_tenant_parent_run",
        "ix_mcp_tasks_tenant_parent_receipt",
        "ix_mcp_tasks_tenant_notification_run",
    ):
        if name in _index_names():
            op.drop_index(name, table_name="mcp_tasks")

    checks = {item["name"] for item in sa.inspect(bind).get_check_constraints("mcp_tasks") if isinstance(item.get("name"), str)}
    unique_names = _unique_names()
    with op.batch_alter_table("mcp_tasks") as batch_op:
        if _OLD_REMOTE_UNIQUE not in unique_names:
            batch_op.create_unique_constraint(
                _OLD_REMOTE_UNIQUE,
                ["user_id", "server_name", "remote_task_id"],
            )
        for name in (
            "ck_mcp_tasks_tenant_pair",
            "ck_mcp_tasks_lineage_pair",
            "ck_mcp_tasks_legacy_parent_null",
            "ck_mcp_tasks_schema_writer_version",
            "ck_mcp_tasks_writer_lineage_required",
            "ck_mcp_tasks_cancel_intent_pair",
            "ck_mcp_tasks_writer_cancel_intent",
            "ck_mcp_tasks_cancel_intent_shape",
            "ck_mcp_tasks_lineage_lengths",
        ):
            if name in checks:
                batch_op.drop_constraint(name, type_="check")
