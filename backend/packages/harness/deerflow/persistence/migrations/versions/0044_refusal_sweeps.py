"""How Gateway processes confirm that a refusal reached what they hold for an account.

Revision ID: 0044_refusal_sweeps
Revises: 0043_role_limits
Create Date: 2026-09-26

alembic_version.version_num is VARCHAR(32); revision ids in this chain must
stay at or under that length or stamping/upgrading a database fails outright.

A connection that authenticated once, and state a process keeps for a person,
are ended by the process that holds them; the account command, in its own
process, confirms it through these rows. ``refusal_checks`` orders a request
to look again after the refusal its requester committed; ``gateway_processes``
is each live process's heartbeat on the database clock and the latest check
it acted on; ``surface_endings`` is what it ended, for whom. Nothing here
outlives the processes that wrote it, so the downgrade drops it all.
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0044_refusal_sweeps"
down_revision: str | Sequence[str] | None = "0043_role_limits"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    # A fresh database got these tables from ``create_all`` before being
    # stamped at head; only an existing one reaches here without them.
    existing = set(sa.inspect(op.get_bind()).get_table_names())
    if "refusal_checks" not in existing:
        op.create_table(
            "refusal_checks",
            sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
            sa.Column("requested_at", sa.DateTime(timezone=True), nullable=False),
        )
    if "gateway_processes" not in existing:
        op.create_table(
            "gateway_processes",
            sa.Column("process_id", sa.String(length=128), primary_key=True),
            sa.Column("started_at", sa.DateTime(timezone=True), nullable=False),
            sa.Column("heartbeat_at", sa.DateTime(timezone=True), nullable=False),
            sa.Column("checked_through", sa.Integer(), nullable=False),
        )
    if "surface_endings" not in existing:
        op.create_table(
            "surface_endings",
            sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
            sa.Column("process_id", sa.String(length=128), nullable=False),
            sa.Column("user_id", sa.String(length=64), nullable=False),
            sa.Column("surface", sa.String(length=64), nullable=False),
            sa.Column("count", sa.Integer(), nullable=False),
            sa.Column("check_id", sa.Integer(), nullable=False),
            sa.Column("ended_at", sa.DateTime(timezone=True), nullable=False),
        )
        op.create_index("ix_surface_endings_user_check", "surface_endings", ["user_id", "check_id"])


def downgrade() -> None:
    existing = set(sa.inspect(op.get_bind()).get_table_names())
    if "surface_endings" in existing:
        op.drop_index("ix_surface_endings_user_check", table_name="surface_endings")
        op.drop_table("surface_endings")
    for table in ("gateway_processes", "refusal_checks"):
        if table in existing:
            op.drop_table(table)
