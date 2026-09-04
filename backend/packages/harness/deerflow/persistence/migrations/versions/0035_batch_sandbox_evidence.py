"""Attach accepted sandbox evidence to durable batch attempts.

Revision ID: 0035_batch_sandbox_evidence
Revises: 0034_tool_plane_revisions
Create Date: 2026-09-03
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0035_batch_sandbox_evidence"
down_revision: str | Sequence[str] | None = "0034_tool_plane_revisions"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_TABLE = "subagent_batch_attempts"
_COLUMNS = (
    "accepted_material_request_json",
    "accepted_material_request_digest",
    "accepted_execution_evidence_json",
    "accepted_execution_evidence_digest",
    "accepted_sandbox_lifecycle_json",
)


def upgrade() -> None:
    """Add nullable fields so pre-existing attempts remain legacy-readable."""

    from deerflow.persistence.migrations._helpers import safe_add_column

    safe_add_column(
        _TABLE,
        sa.Column("accepted_material_request_json", sa.JSON(), nullable=True),
    )
    safe_add_column(
        _TABLE,
        sa.Column(
            "accepted_material_request_digest",
            sa.String(length=64),
            nullable=True,
        ),
    )
    safe_add_column(
        _TABLE,
        sa.Column("accepted_execution_evidence_json", sa.JSON(), nullable=True),
    )
    safe_add_column(
        _TABLE,
        sa.Column(
            "accepted_execution_evidence_digest",
            sa.String(length=64),
            nullable=True,
        ),
    )
    safe_add_column(
        _TABLE,
        sa.Column("accepted_sandbox_lifecycle_json", sa.JSON(), nullable=True),
    )


def downgrade() -> None:
    """Drop unused fields, but never discard accepted execution evidence."""

    from deerflow.persistence.migrations._helpers import safe_drop_column

    bind = op.get_bind()
    tables = set(sa.inspect(bind).get_table_names())
    if _TABLE in tables:
        columns = {str(column["name"]) for column in sa.inspect(bind).get_columns(_TABLE)}
        present = [name for name in _COLUMNS if name in columns]
        if present:
            predicate = " OR ".join(f"{name} IS NOT NULL" for name in present)
            used = bind.execute(
                sa.text(f"SELECT 1 FROM {_TABLE} WHERE {predicate} LIMIT 1"),
            ).first()
            if used is not None:
                raise RuntimeError("batch_sandbox_evidence_downgrade_blocked")
    for name in reversed(_COLUMNS):
        safe_drop_column(_TABLE, name)
