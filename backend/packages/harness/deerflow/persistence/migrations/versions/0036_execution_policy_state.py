"""Add protected compact execution-policy state to runs.

Revision ID: 0036_execution_policy_state
Revises: 0035_batch_sandbox_evidence
Create Date: 2026-09-04
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0036_execution_policy_state"
down_revision: str | Sequence[str] | None = "0035_batch_sandbox_evidence"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_TABLE = "runs"
_COLUMNS = ("execution_policy_state_json", "execution_policy_state_digest")


def upgrade() -> None:
    """Add nullable fields so historical runs remain explicitly legacy."""

    from deerflow.persistence.migrations._helpers import safe_add_column

    safe_add_column(
        _TABLE,
        sa.Column("execution_policy_state_json", sa.JSON(), nullable=True),
    )
    safe_add_column(
        _TABLE,
        sa.Column("execution_policy_state_digest", sa.String(length=64), nullable=True),
    )
    bind = op.get_bind()
    existing = {constraint["name"] for constraint in sa.inspect(bind).get_check_constraints(_TABLE)}
    with op.batch_alter_table(_TABLE) as batch_op:
        if "ck_runs_execution_policy_state_pair" not in existing:
            batch_op.create_check_constraint(
                "ck_runs_execution_policy_state_pair",
                "(execution_policy_state_json IS NULL) = (execution_policy_state_digest IS NULL)",
            )
        if "ck_runs_execution_policy_state_run_only" not in existing:
            batch_op.create_check_constraint(
                "ck_runs_execution_policy_state_run_only",
                "execution_policy_state_digest IS NULL OR operation_kind = 'run'",
            )
        if "ck_runs_execution_policy_state_digest_format" not in existing:
            batch_op.create_check_constraint(
                "ck_runs_execution_policy_state_digest_format",
                "execution_policy_state_digest IS NULL OR (length(execution_policy_state_digest) = 64 "
                "AND lower(execution_policy_state_digest) = execution_policy_state_digest "
                "AND length(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace("
                "replace(replace(replace(replace(replace(replace(execution_policy_state_digest, '0', ''), '1', ''), '2', ''), '3', ''), "
                "'4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), "
                "'e', ''), 'f', '')) = 0)",
            )


def downgrade() -> None:
    """Allow rollback only while no protected policy state would be lost."""

    from deerflow.persistence.migrations._helpers import safe_drop_column

    bind = op.get_bind()
    tables = set(sa.inspect(bind).get_table_names())
    if _TABLE in tables:
        columns = {str(column["name"]) for column in sa.inspect(bind).get_columns(_TABLE)}
        present = [name for name in _COLUMNS if name in columns]
        if present:
            predicate = " OR ".join(f"{name} IS NOT NULL" for name in present)
            used = bind.execute(sa.text(f"SELECT 1 FROM {_TABLE} WHERE {predicate} LIMIT 1")).first()
            if used is not None:
                raise RuntimeError("execution_policy_state_downgrade_blocked")
    with op.batch_alter_table(_TABLE) as batch_op:
        batch_op.drop_constraint(
            "ck_runs_execution_policy_state_digest_format",
            type_="check",
        )
        batch_op.drop_constraint(
            "ck_runs_execution_policy_state_run_only",
            type_="check",
        )
        batch_op.drop_constraint(
            "ck_runs_execution_policy_state_pair",
            type_="check",
        )
    for name in reversed(_COLUMNS):
        safe_drop_column(_TABLE, name)
