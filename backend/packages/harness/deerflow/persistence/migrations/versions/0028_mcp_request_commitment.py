"""Add private exact-request MCP commitments and audit head drift.

Revision ID: 0028_mcp_request_commitment
Revises: 0027_multi_gateway_topology
Create Date: 2026-09-01
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0028_mcp_request_commitment"
down_revision: str | Sequence[str] | None = "0027_multi_gateway_topology"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_STALE_INDEX_DEFINITIONS: dict[str, list[str]] = {
    "ix_mcp_tasks_thread_created": ["thread_id", "created_at"],
    "ix_mcp_tasks_due": ["status", "next_poll_at"],
    "ix_mcp_tasks_notification_due": [
        "notification_status",
        "next_notification_at",
    ],
    "ix_mcp_tasks_cancel_due": ["cancel_requested_at", "next_cancel_at"],
}

_COMMITMENT_CONSTRAINTS = (
    "ck_mcp_tasks_request_commitment_triple",
    "ck_mcp_tasks_writer_request_commitment",
    "ck_mcp_tasks_request_commitment_shape",
)


def _index_names(table_name: str) -> set[str]:
    bind = op.get_bind()
    if table_name not in sa.inspect(bind).get_table_names():
        return set()
    return {item["name"] for item in sa.inspect(bind).get_indexes(table_name) if isinstance(item.get("name"), str)}


def upgrade() -> None:
    from deerflow.persistence.migrations._helpers import safe_add_column

    # 0027 predated the drift-aware helper. Reasserting its desired shape is
    # idempotent and emits an operator-visible warning for manual schema drift.
    safe_add_column(
        "scheduled_tasks",
        sa.Column(
            "schedule_version",
            sa.Integer(),
            nullable=False,
            server_default="1",
        ),
    )
    for column in (
        sa.Column("request_commitment_version", sa.Integer(), nullable=True),
        sa.Column(
            "request_commitment_key_id",
            sa.String(length=32),
            nullable=True,
        ),
        sa.Column(
            "request_commitment_digest",
            sa.String(length=64),
            nullable=True,
        ),
    ):
        safe_add_column("mcp_tasks", column)

    bind = op.get_bind()
    if "mcp_tasks" not in sa.inspect(bind).get_table_names():
        return
    checks = {item["name"] for item in sa.inspect(bind).get_check_constraints("mcp_tasks") if isinstance(item.get("name"), str)}
    with op.batch_alter_table("mcp_tasks") as batch_op:
        if "ck_mcp_tasks_request_commitment_triple" not in checks:
            batch_op.create_check_constraint(
                "ck_mcp_tasks_request_commitment_triple",
                "(request_commitment_version IS NULL AND request_commitment_key_id IS NULL AND request_commitment_digest IS NULL) OR "
                "(request_commitment_version IS NOT NULL AND request_commitment_key_id IS NOT NULL AND request_commitment_digest IS NOT NULL)",
            )
        if "ck_mcp_tasks_writer_request_commitment" not in checks:
            batch_op.create_check_constraint(
                "ck_mcp_tasks_writer_request_commitment",
                "schema_writer_version < 3 OR request_commitment_digest IS NOT NULL",
            )
        if "ck_mcp_tasks_request_commitment_shape" not in checks:
            batch_op.create_check_constraint(
                "ck_mcp_tasks_request_commitment_shape",
                "request_commitment_version IS NULL OR (request_commitment_version = 1 AND length(request_commitment_key_id) BETWEEN 1 AND 32 AND length(request_commitment_digest) = 64)",
            )

    for index_name in _STALE_INDEX_DEFINITIONS:
        if index_name in _index_names("mcp_tasks"):
            op.drop_index(index_name, table_name="mcp_tasks")


def downgrade() -> None:
    """Return to the 0027 shape only while no writer-v3 row exists."""

    from deerflow.persistence.migrations._helpers import safe_drop_column

    bind = op.get_bind()
    if "mcp_tasks" not in sa.inspect(bind).get_table_names():
        return
    maximum_writer_version = bind.execute(sa.text("SELECT MAX(schema_writer_version) FROM mcp_tasks")).scalar_one_or_none()
    if maximum_writer_version is not None and int(maximum_writer_version) >= 3:
        raise RuntimeError("mcp_task_request_commitment_rollback_blocked")

    checks = {item["name"] for item in sa.inspect(bind).get_check_constraints("mcp_tasks") if isinstance(item.get("name"), str)}
    with op.batch_alter_table("mcp_tasks") as batch_op:
        for constraint_name in _COMMITMENT_CONSTRAINTS:
            if constraint_name in checks:
                batch_op.drop_constraint(constraint_name, type_="check")

    safe_drop_column("mcp_tasks", "request_commitment_digest")
    safe_drop_column("mcp_tasks", "request_commitment_key_id")
    safe_drop_column("mcp_tasks", "request_commitment_version")

    indexes = _index_names("mcp_tasks")
    for index_name, columns in _STALE_INDEX_DEFINITIONS.items():
        if index_name not in indexes:
            op.create_index(
                index_name,
                "mcp_tasks",
                columns,
                unique=False,
            )
