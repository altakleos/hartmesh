"""Persist the server-owned run recovery policy.

Revision ID: 0029_run_recovery_policy
Revises: 0028_mcp_request_commitment
Create Date: 2026-09-01
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0029_run_recovery_policy"
down_revision: str | Sequence[str] | None = "0028_mcp_request_commitment"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_RECOVERY_CONSTRAINT = "ck_runs_recovery_policy"
_RECOVERY_PAYLOAD_CONSTRAINT = "ck_runs_recovery_payload_policy"
_CURSOR_CONSTRAINT = "ck_runs_admission_cursor_positive"
_TERMINAL_PROJECTION_PAIR_CONSTRAINT = "ck_runs_terminal_projection_authority_pair"
_TERMINAL_PROJECTION_VERSION_CONSTRAINT = "ck_runs_terminal_projection_authority_version"
_CURSOR_INDEXES = {
    "ix_runs_thread_kind_admission": (
        "thread_id",
        "operation_kind",
        "admission_cursor",
    ),
    "uq_runs_admission_cursor": ("admission_cursor",),
}


def _runs_exists() -> bool:
    return "runs" in sa.inspect(op.get_bind()).get_table_names()


def upgrade() -> None:
    from deerflow.persistence.migrations._helpers import safe_add_column

    safe_add_column(
        "runs",
        sa.Column(
            "recovery_policy",
            sa.String(length=32),
            nullable=False,
            server_default="terminalize_v1",
        ),
    )
    if not _runs_exists():
        return
    safe_add_column(
        "runs",
        sa.Column(
            "admission_cursor",
            sa.BigInteger(),
            nullable=True,
        ),
    )
    safe_add_column(
        "runs",
        sa.Column(
            "recovery_payload_json",
            sa.JSON(none_as_null=True),
            nullable=True,
        ),
    )
    safe_add_column(
        "runs",
        sa.Column(
            "terminal_projection_owner_worker_id",
            sa.String(length=128),
            nullable=True,
        ),
    )
    safe_add_column(
        "runs",
        sa.Column(
            "terminal_projection_active_state_version",
            sa.Integer(),
            nullable=True,
        ),
    )
    bind = op.get_bind()
    inspector = sa.inspect(bind)
    if "run_admission_cursor_state" not in inspector.get_table_names():
        op.create_table(
            "run_admission_cursor_state",
            sa.Column("singleton_id", sa.Integer(), nullable=False),
            sa.Column(
                "last_cursor",
                sa.BigInteger(),
                nullable=False,
                server_default=sa.text("0"),
            ),
            sa.CheckConstraint(
                "singleton_id = 1",
                name="ck_run_admission_cursor_singleton",
            ),
            sa.CheckConstraint(
                "last_cursor >= 0",
                name="ck_run_admission_cursor_nonnegative",
            ),
            sa.PrimaryKeyConstraint("singleton_id"),
        )

    maximum = bind.execute(sa.text("SELECT COALESCE(MAX(admission_cursor), 0) FROM runs")).scalar_one()
    # Historical timestamps and random run IDs are not a trustworthy
    # admission order across replicas. Leave legacy rows NULL so any derived
    # latest-run projection fails closed instead of fabricating authority.
    cursor = int(maximum or 0)
    existing_state = bind.execute(sa.text("SELECT last_cursor FROM run_admission_cursor_state WHERE singleton_id = 1")).scalar_one_or_none()
    if existing_state is None:
        bind.execute(
            sa.text("INSERT INTO run_admission_cursor_state (singleton_id, last_cursor) VALUES (1, :cursor)"),
            {"cursor": cursor},
        )
    elif int(existing_state) < cursor:
        bind.execute(
            sa.text("UPDATE run_admission_cursor_state SET last_cursor = :cursor WHERE singleton_id = 1"),
            {"cursor": cursor},
        )

    checks = {item["name"] for item in sa.inspect(bind).get_check_constraints("runs") if isinstance(item.get("name"), str)}
    missing_checks = {
        _RECOVERY_CONSTRAINT,
        _RECOVERY_PAYLOAD_CONSTRAINT,
        _CURSOR_CONSTRAINT,
        _TERMINAL_PROJECTION_PAIR_CONSTRAINT,
        _TERMINAL_PROJECTION_VERSION_CONSTRAINT,
    } - checks
    if missing_checks:
        with op.batch_alter_table("runs") as batch_op:
            if _RECOVERY_CONSTRAINT in missing_checks:
                batch_op.create_check_constraint(
                    _RECOVERY_CONSTRAINT,
                    "recovery_policy IN ('terminalize_v1', 'exact_two_takeover_v1')",
                )
            if _RECOVERY_PAYLOAD_CONSTRAINT in missing_checks:
                batch_op.create_check_constraint(
                    _RECOVERY_PAYLOAD_CONSTRAINT,
                    "(recovery_policy = 'exact_two_takeover_v1') = (recovery_payload_json IS NOT NULL) AND (operation_kind = 'run' OR recovery_policy = 'terminalize_v1')",
                )
            if _CURSOR_CONSTRAINT in missing_checks:
                batch_op.create_check_constraint(
                    _CURSOR_CONSTRAINT,
                    "admission_cursor IS NULL OR admission_cursor > 0",
                )
            if _TERMINAL_PROJECTION_PAIR_CONSTRAINT in missing_checks:
                batch_op.create_check_constraint(
                    _TERMINAL_PROJECTION_PAIR_CONSTRAINT,
                    "(terminal_projection_owner_worker_id IS NULL) = (terminal_projection_active_state_version IS NULL)",
                )
            if _TERMINAL_PROJECTION_VERSION_CONSTRAINT in missing_checks:
                batch_op.create_check_constraint(
                    _TERMINAL_PROJECTION_VERSION_CONSTRAINT,
                    "terminal_projection_active_state_version IS NULL OR (operation_kind = 'run' AND terminal_projection_active_state_version >= 0 AND terminal_projection_active_state_version < state_version)",
                )
    indexes = {item["name"] for item in sa.inspect(bind).get_indexes("runs") if isinstance(item.get("name"), str)}
    if "ix_runs_thread_kind_admission" not in indexes:
        op.create_index(
            "ix_runs_thread_kind_admission",
            "runs",
            list(_CURSOR_INDEXES["ix_runs_thread_kind_admission"]),
        )
    if "uq_runs_admission_cursor" not in indexes:
        op.create_index(
            "uq_runs_admission_cursor",
            "runs",
            list(_CURSOR_INDEXES["uq_runs_admission_cursor"]),
            unique=True,
            sqlite_where=sa.text("admission_cursor IS NOT NULL"),
            postgresql_where=sa.text("admission_cursor IS NOT NULL"),
        )


def downgrade() -> None:
    """Remove the revision only before recovery/cursor semantics were used."""

    from deerflow.persistence.migrations._helpers import safe_drop_column

    if not _runs_exists():
        return
    bind = op.get_bind()
    used = bind.execute(
        sa.text("SELECT 1 FROM runs WHERE recovery_policy = 'exact_two_takeover_v1' OR admission_cursor IS NOT NULL OR terminal_projection_owner_worker_id IS NOT NULL OR terminal_projection_active_state_version IS NOT NULL LIMIT 1")
    ).first()
    cursor_state_used = None
    if "run_admission_cursor_state" in sa.inspect(bind).get_table_names():
        cursor_state_used = bind.execute(sa.text("SELECT 1 FROM run_admission_cursor_state WHERE last_cursor > 0 LIMIT 1")).first()
    if used is not None or cursor_state_used is not None:
        raise RuntimeError("run_recovery_policy_rollback_blocked")
    indexes = {item["name"] for item in sa.inspect(op.get_bind()).get_indexes("runs") if isinstance(item.get("name"), str)}
    for index_name in _CURSOR_INDEXES:
        if index_name in indexes:
            op.drop_index(index_name, table_name="runs")
    checks = {item["name"] for item in sa.inspect(op.get_bind()).get_check_constraints("runs") if isinstance(item.get("name"), str)}
    present_checks = {
        _RECOVERY_CONSTRAINT,
        _RECOVERY_PAYLOAD_CONSTRAINT,
        _CURSOR_CONSTRAINT,
        _TERMINAL_PROJECTION_PAIR_CONSTRAINT,
        _TERMINAL_PROJECTION_VERSION_CONSTRAINT,
    } & checks
    if present_checks:
        with op.batch_alter_table("runs") as batch_op:
            for constraint in present_checks:
                batch_op.drop_constraint(constraint, type_="check")
    safe_drop_column("runs", "admission_cursor")
    safe_drop_column("runs", "terminal_projection_active_state_version")
    safe_drop_column("runs", "terminal_projection_owner_worker_id")
    safe_drop_column("runs", "recovery_payload_json")
    safe_drop_column("runs", "recovery_policy")
    if "run_admission_cursor_state" in sa.inspect(op.get_bind()).get_table_names():
        op.drop_table("run_admission_cursor_state")
