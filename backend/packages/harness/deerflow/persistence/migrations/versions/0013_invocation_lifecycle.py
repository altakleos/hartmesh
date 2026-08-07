"""authoritative invocation lifecycle evidence.

Revision ID: 0013_invocation_lifecycle
Revises: 0012_invocation_idempotency
Create Date: 2026-08-07
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0013_invocation_lifecycle"
down_revision: str | Sequence[str] | None = "0012_invocation_idempotency"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    from deerflow.persistence.migrations._helpers import safe_add_column

    bind = op.get_bind()
    inspector = sa.inspect(bind)
    existing_columns = {column["name"] for column in inspector.get_columns("runs")}
    if "state_version" not in existing_columns:
        safe_add_column(
            "runs",
            sa.Column("state_version", sa.Integer(), nullable=False, server_default=sa.text("0")),
        )

    inspector = sa.inspect(bind)
    existing_checks = {constraint["name"] for constraint in inspector.get_check_constraints("runs")}
    if "ck_runs_state_version_nonnegative" not in existing_checks:
        with op.batch_alter_table("runs") as batch_op:
            batch_op.create_check_constraint(
                "ck_runs_state_version_nonnegative",
                "state_version >= 0",
            )

    inspector = sa.inspect(bind)
    if not inspector.has_table("run_lifecycle_cursor_state"):
        op.create_table(
            "run_lifecycle_cursor_state",
            sa.Column("singleton_id", sa.Integer(), nullable=False),
            sa.Column("last_cursor", sa.BigInteger(), nullable=False, server_default=sa.text("0")),
            sa.Column("pruned_through", sa.BigInteger(), nullable=False, server_default=sa.text("0")),
            sa.CheckConstraint("singleton_id = 1", name="ck_run_lifecycle_cursor_singleton"),
            sa.CheckConstraint("last_cursor >= 0", name="ck_run_lifecycle_cursor_nonnegative"),
            sa.CheckConstraint(
                "pruned_through >= 0 AND pruned_through <= last_cursor",
                name="ck_run_lifecycle_pruned_range",
            ),
            sa.PrimaryKeyConstraint("singleton_id"),
        )

    inspector = sa.inspect(bind)
    if not inspector.has_table("run_lifecycle_events"):
        op.create_table(
            "run_lifecycle_events",
            sa.Column("event_id", sa.String(length=36), nullable=False),
            sa.Column("cursor", sa.BigInteger(), nullable=False),
            sa.Column("run_id", sa.String(length=64), nullable=False),
            sa.Column("thread_id", sa.String(length=64), nullable=False),
            sa.Column("owner_scope", sa.String(length=96), nullable=False),
            sa.Column("lifecycle_type", sa.String(length=32), nullable=False),
            sa.Column("state_version", sa.Integer(), nullable=False),
            sa.Column("status", sa.String(length=20), nullable=False),
            sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
            sa.Column("payload_json", sa.JSON(), nullable=False, server_default=sa.text("'{}'")),
            sa.CheckConstraint("cursor > 0", name="ck_run_lifecycle_event_cursor_positive"),
            sa.CheckConstraint("state_version > 0", name="ck_run_lifecycle_event_version_positive"),
            sa.CheckConstraint(
                "lifecycle_type IN ('accepted', 'started', 'cancellation_requested', 'cancelled', 'succeeded', 'failed', 'timed_out', 'interrupted')",
                name="ck_run_lifecycle_event_type",
            ),
            sa.ForeignKeyConstraint(["run_id"], ["runs.run_id"], ondelete="CASCADE"),
            sa.PrimaryKeyConstraint("event_id"),
            sa.UniqueConstraint("cursor"),
        )
        op.create_index(
            "ix_run_lifecycle_events_run_cursor",
            "run_lifecycle_events",
            ["run_id", "cursor"],
        )
        op.create_index(
            "ix_run_lifecycle_events_thread_cursor",
            "run_lifecycle_events",
            ["thread_id", "cursor"],
        )
        op.create_index(
            "ix_run_lifecycle_events_owner_cursor",
            "run_lifecycle_events",
            ["owner_scope", "cursor"],
        )

    state_count = bind.scalar(sa.text("SELECT count(*) FROM run_lifecycle_cursor_state WHERE singleton_id = 1"))
    if not state_count:
        event_count = bind.scalar(sa.text("SELECT count(*) FROM run_lifecycle_events"))
        if event_count:
            raise RuntimeError("lifecycle events exist without cursor singleton; ordering state is corrupt")
        bind.execute(sa.text("INSERT INTO run_lifecycle_cursor_state (singleton_id, last_cursor, pruned_through) VALUES (1, 0, 0)"))


def downgrade() -> None:
    from deerflow.persistence.migrations._helpers import safe_drop_column

    bind = op.get_bind()
    inspector = sa.inspect(bind)
    if inspector.has_table("run_lifecycle_events"):
        op.drop_table("run_lifecycle_events")
    inspector = sa.inspect(bind)
    if inspector.has_table("run_lifecycle_cursor_state"):
        op.drop_table("run_lifecycle_cursor_state")

    existing_checks = {constraint["name"] for constraint in sa.inspect(bind).get_check_constraints("runs")}
    if "ck_runs_state_version_nonnegative" in existing_checks:
        with op.batch_alter_table("runs") as batch_op:
            batch_op.drop_constraint("ck_runs_state_version_nonnegative", type_="check")
    safe_drop_column("runs", "state_version")
