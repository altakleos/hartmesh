"""Add durable tool-receipt event idempotency.

Revision ID: 0024_tool_receipt_idempotency
Revises: 0023_agent_assembly_evidence
Create Date: 2026-08-30
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0024_tool_receipt_idempotency"
down_revision: str | Sequence[str] | None = "0023_agent_assembly_evidence"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_INDEX = "uq_run_events_receipt_idempotency"


def upgrade() -> None:
    from deerflow.persistence.migrations._helpers import safe_add_column

    safe_add_column(
        "run_events",
        sa.Column("idempotency_key", sa.String(length=128), nullable=True),
    )
    bind = op.get_bind()
    if "run_events" not in sa.inspect(bind).get_table_names():
        return
    indexes = {index["name"] for index in sa.inspect(bind).get_indexes("run_events")}
    if _INDEX not in indexes:
        op.create_index(
            _INDEX,
            "run_events",
            ["run_id", "event_type", "idempotency_key"],
            unique=True,
            sqlite_where=sa.text("idempotency_key IS NOT NULL"),
            postgresql_where=sa.text("idempotency_key IS NOT NULL"),
        )


def downgrade() -> None:
    from deerflow.persistence.migrations._helpers import safe_drop_column

    bind = op.get_bind()
    if "run_events" in sa.inspect(bind).get_table_names():
        indexes = {index["name"] for index in sa.inspect(bind).get_indexes("run_events")}
        if _INDEX in indexes:
            op.drop_index(_INDEX, table_name="run_events")
    safe_drop_column("run_events", "idempotency_key")
