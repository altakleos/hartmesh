"""What a Gateway process could not confirm ended, and the surfaces it cannot reach.

Revision ID: 0045_refusal_sweep_reach
Revises: 0044_refusal_sweeps
Create Date: 2026-09-26

alembic_version.version_num is VARCHAR(32); revision ids in this chain must
stay at or under that length or stamping/upgrading a database fails outright.

State a process keeps for a person -- a parked sandbox above all -- can fail
to end: a container that will not stop is still running, and the account
command must not report it ended. ``surface_endings.failed`` counts what a
look tried to end and could not confirm. ``gateway_processes.unreached``
names, as a JSON list, the surfaces a process has no way to end at all (a
sandbox provider that cannot end an owner's sandboxes), so the command reads
that as unconfirmed rather than as nothing held. Both are about processes
that are running now, so the downgrade drops them.
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa

from deerflow.persistence.migrations._helpers import safe_add_column, safe_drop_column

revision: str = "0045_refusal_sweep_reach"
down_revision: str | Sequence[str] | None = "0044_refusal_sweeps"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    safe_add_column("surface_endings", sa.Column("failed", sa.Integer(), nullable=False, server_default="0"))
    safe_add_column("gateway_processes", sa.Column("unreached", sa.Text(), nullable=True))


def downgrade() -> None:
    safe_drop_column("gateway_processes", "unreached")
    safe_drop_column("surface_endings", "failed")
