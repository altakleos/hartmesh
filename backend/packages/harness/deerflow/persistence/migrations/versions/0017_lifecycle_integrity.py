"""bounded lifecycle retained-cardinality integrity.

Revision ID: 0017_lifecycle_integrity
Revises: 0016_sandbox_execution_evidence
Create Date: 2026-08-09
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0017_lifecycle_integrity"
down_revision: str | Sequence[str] | None = "0016_sandbox_execution_evidence"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def _create_triggers(dialect: str) -> None:
    if dialect == "sqlite":
        op.execute("CREATE TRIGGER IF NOT EXISTS trg_run_lifecycle_retained_insert AFTER INSERT ON run_lifecycle_events BEGIN UPDATE run_lifecycle_cursor_state SET retained_count = retained_count + 1 WHERE singleton_id = 1; END")
        op.execute("CREATE TRIGGER IF NOT EXISTS trg_run_lifecycle_retained_delete AFTER DELETE ON run_lifecycle_events BEGIN UPDATE run_lifecycle_cursor_state SET retained_count = retained_count - 1 WHERE singleton_id = 1; END")
    elif dialect == "postgresql":
        op.execute(
            "CREATE OR REPLACE FUNCTION deerflow_update_lifecycle_retained_count() "
            "RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN "
            "IF TG_OP = 'INSERT' THEN "
            "UPDATE run_lifecycle_cursor_state SET retained_count = retained_count + 1 WHERE singleton_id = 1; "
            "RETURN NEW; "
            "ELSE "
            "UPDATE run_lifecycle_cursor_state SET retained_count = retained_count - 1 WHERE singleton_id = 1; "
            "RETURN OLD; "
            "END IF; END; $$"
        )
        op.execute("CREATE TRIGGER trg_run_lifecycle_retained_insert AFTER INSERT ON run_lifecycle_events FOR EACH ROW EXECUTE FUNCTION deerflow_update_lifecycle_retained_count()")
        op.execute("CREATE TRIGGER trg_run_lifecycle_retained_delete AFTER DELETE ON run_lifecycle_events FOR EACH ROW EXECUTE FUNCTION deerflow_update_lifecycle_retained_count()")


def _drop_triggers(dialect: str) -> None:
    if dialect == "sqlite":
        op.execute("DROP TRIGGER IF EXISTS trg_run_lifecycle_retained_insert")
        op.execute("DROP TRIGGER IF EXISTS trg_run_lifecycle_retained_delete")
    elif dialect == "postgresql":
        op.execute("DROP TRIGGER IF EXISTS trg_run_lifecycle_retained_insert ON run_lifecycle_events")
        op.execute("DROP TRIGGER IF EXISTS trg_run_lifecycle_retained_delete ON run_lifecycle_events")
        op.execute("DROP FUNCTION IF EXISTS deerflow_update_lifecycle_retained_count()")


def upgrade() -> None:
    bind = op.get_bind()
    inspector = sa.inspect(bind)
    columns = {column["name"] for column in inspector.get_columns("run_lifecycle_cursor_state")}
    if "retained_count" not in columns:
        op.add_column(
            "run_lifecycle_cursor_state",
            sa.Column(
                "retained_count",
                sa.BigInteger(),
                nullable=False,
                server_default=sa.text("0"),
            ),
        )
    bind.execute(sa.text("UPDATE run_lifecycle_cursor_state SET retained_count = (SELECT count(*) FROM run_lifecycle_events) WHERE singleton_id = 1"))
    inspector = sa.inspect(bind)
    checks = {constraint["name"] for constraint in inspector.get_check_constraints("run_lifecycle_cursor_state")}
    if "ck_run_lifecycle_retained_count_nonnegative" not in checks:
        with op.batch_alter_table("run_lifecycle_cursor_state") as batch_op:
            batch_op.create_check_constraint(
                "ck_run_lifecycle_retained_count_nonnegative",
                "retained_count >= 0",
            )
    _create_triggers(bind.dialect.name)


def downgrade() -> None:
    bind = op.get_bind()
    _drop_triggers(bind.dialect.name)
    checks = {constraint["name"] for constraint in sa.inspect(bind).get_check_constraints("run_lifecycle_cursor_state")}
    if "ck_run_lifecycle_retained_count_nonnegative" in checks:
        with op.batch_alter_table("run_lifecycle_cursor_state") as batch_op:
            batch_op.drop_constraint(
                "ck_run_lifecycle_retained_count_nonnegative",
                type_="check",
            )
    if "retained_count" in {column["name"] for column in sa.inspect(bind).get_columns("run_lifecycle_cursor_state")}:
        with op.batch_alter_table("run_lifecycle_cursor_state") as batch_op:
            batch_op.drop_column("retained_count")
