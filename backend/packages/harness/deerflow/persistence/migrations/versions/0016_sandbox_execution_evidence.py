"""persist bounded sandbox execution evidence.

Revision ID: 0016_sandbox_execution_evidence
Revises: 0015_inbound_receipts
Create Date: 2026-08-09
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0016_sandbox_execution_evidence"
down_revision: str | Sequence[str] | None = "0015_inbound_receipts"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    from deerflow.persistence.migrations._helpers import safe_add_column

    safe_add_column(
        "runs",
        sa.Column("execution_evidence_json", sa.JSON(), nullable=True),
    )
    safe_add_column(
        "runs",
        sa.Column(
            "execution_evidence_digest",
            sa.String(length=64),
            nullable=True,
        ),
    )
    bind = op.get_bind()
    existing = {constraint["name"] for constraint in sa.inspect(bind).get_check_constraints("runs")}
    with op.batch_alter_table("runs") as batch_op:
        if "ck_runs_execution_evidence_pair" not in existing:
            batch_op.create_check_constraint(
                "ck_runs_execution_evidence_pair",
                "(execution_evidence_json IS NULL) = (execution_evidence_digest IS NULL)",
            )
        if "ck_runs_execution_evidence_run_only" not in existing:
            batch_op.create_check_constraint(
                "ck_runs_execution_evidence_run_only",
                "execution_evidence_digest IS NULL OR (operation_kind = 'run' AND length(execution_evidence_digest) = 64)",
            )


def downgrade() -> None:
    from deerflow.persistence.migrations._helpers import safe_drop_column

    with op.batch_alter_table("runs") as batch_op:
        batch_op.drop_constraint(
            "ck_runs_execution_evidence_run_only",
            type_="check",
        )
        batch_op.drop_constraint(
            "ck_runs_execution_evidence_pair",
            type_="check",
        )
    safe_drop_column("runs", "execution_evidence_digest")
    safe_drop_column("runs", "execution_evidence_json")
