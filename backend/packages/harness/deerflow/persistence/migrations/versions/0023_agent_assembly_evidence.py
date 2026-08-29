"""Persist bounded lead-agent assembly evidence.

Revision ID: 0023_agent_assembly_evidence
Revises: 0022_merge_scheduled_enqueue
Create Date: 2026-08-29
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0023_agent_assembly_evidence"
down_revision: str | Sequence[str] | None = "0022_merge_scheduled_enqueue"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    from deerflow.persistence.migrations._helpers import safe_add_column

    safe_add_column(
        "runs",
        sa.Column("assembly_evidence_json", sa.JSON(), nullable=True),
    )
    safe_add_column(
        "runs",
        sa.Column("assembly_evidence_digest", sa.String(length=64), nullable=True),
    )
    bind = op.get_bind()
    existing = {constraint["name"] for constraint in sa.inspect(bind).get_check_constraints("runs")}
    with op.batch_alter_table("runs") as batch_op:
        if "ck_runs_assembly_evidence_pair" not in existing:
            batch_op.create_check_constraint(
                "ck_runs_assembly_evidence_pair",
                "(assembly_evidence_json IS NULL) = (assembly_evidence_digest IS NULL)",
            )
        if "ck_runs_assembly_evidence_run_only" not in existing:
            batch_op.create_check_constraint(
                "ck_runs_assembly_evidence_run_only",
                "assembly_evidence_digest IS NULL OR operation_kind = 'run'",
            )
        if "ck_runs_assembly_evidence_digest_format" not in existing:
            batch_op.create_check_constraint(
                "ck_runs_assembly_evidence_digest_format",
                "assembly_evidence_digest IS NULL OR (length(assembly_evidence_digest) = 64 "
                "AND lower(assembly_evidence_digest) = assembly_evidence_digest "
                "AND length(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace("
                "replace(replace(replace(replace(replace(replace(assembly_evidence_digest, '0', ''), '1', ''), '2', ''), '3', ''), "
                "'4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), "
                "'e', ''), 'f', '')) = 0)",
            )


def downgrade() -> None:
    from deerflow.persistence.migrations._helpers import safe_drop_column

    with op.batch_alter_table("runs") as batch_op:
        batch_op.drop_constraint(
            "ck_runs_assembly_evidence_digest_format",
            type_="check",
        )
        batch_op.drop_constraint(
            "ck_runs_assembly_evidence_run_only",
            type_="check",
        )
        batch_op.drop_constraint(
            "ck_runs_assembly_evidence_pair",
            type_="check",
        )
    safe_drop_column("runs", "assembly_evidence_digest")
    safe_drop_column("runs", "assembly_evidence_json")
