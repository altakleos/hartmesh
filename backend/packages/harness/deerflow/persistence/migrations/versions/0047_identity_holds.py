"""What turning an identity off held: the schedules it paused and the channel bindings it stopped.

Revision ID: 0047_identity_holds
Revises: 0046_mcp_task_disable_reason
Create Date: 2026-09-26

alembic_version.version_num is VARCHAR(32); revision ids in this chain must
stay at or under that length or stamping/upgrading a database fails outright.

``identity_holds`` is keyed by the identity provider's ``(issuer, subject)``,
like ``disabled_identities``, and by what was held. ``enable --restore-held``
turns back on exactly what it names, and every ``enable`` discards it. What it
names stays off without it, so dropping the table on a downgrade loses only
the ability to restore, never turns anything back on.
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0047_identity_holds"
down_revision: str | Sequence[str] | None = "0046_mcp_task_disable_reason"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_TABLE = "identity_holds"


def upgrade() -> None:
    # A fresh database got this table from ``create_all`` before being
    # stamped at head; only an existing one reaches here without it.
    if _TABLE in set(sa.inspect(op.get_bind()).get_table_names()):
        return
    op.create_table(
        _TABLE,
        sa.Column("issuer", sa.String(length=512), primary_key=True),
        sa.Column("subject", sa.String(length=255), primary_key=True),
        sa.Column("kind", sa.String(length=32), primary_key=True),
        sa.Column("target_id", sa.String(length=64), primary_key=True),
        sa.Column("user_id", sa.String(length=64), nullable=False),
        sa.Column("held_at", sa.DateTime(timezone=True), nullable=False),
    )


def downgrade() -> None:
    if _TABLE not in set(sa.inspect(op.get_bind()).get_table_names()):
        return
    op.drop_table(_TABLE)
