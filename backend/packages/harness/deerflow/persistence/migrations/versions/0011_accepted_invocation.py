"""accepted invocation audit facts.

Revision ID: 0011_accepted_invocation
Revises: 0011_mcp_tasks
Create Date: 2026-08-06
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa

revision: str = "0011_accepted_invocation"
down_revision: str | Sequence[str] | None = "0011_mcp_tasks"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_COLUMNS = (
    sa.Column("origin_json", sa.JSON(), nullable=True),
    sa.Column("principal_projection_json", sa.JSON(), nullable=True),
    sa.Column("principal_projection_digest", sa.String(length=64), nullable=True),
    sa.Column("base_origin_digest", sa.String(length=64), nullable=True),
    sa.Column("accepted_context_digest", sa.String(length=64), nullable=True),
    sa.Column("agent_revision_json", sa.JSON(), nullable=True),
    sa.Column("agent_revision_digest", sa.String(length=64), nullable=True),
    sa.Column("extension_generation", sa.Integer(), nullable=True),
    sa.Column("decision_evidence_json", sa.JSON(), nullable=True),
)


def upgrade() -> None:
    from deerflow.persistence.migrations._helpers import safe_add_column

    for column in _COLUMNS:
        safe_add_column("runs", column)


def downgrade() -> None:
    from deerflow.persistence.migrations._helpers import safe_drop_column

    for column in reversed(_COLUMNS):
        safe_drop_column("runs", column.name)
