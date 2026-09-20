"""The Shared area's publication records.

Revision ID: 0038_shared_publications
Revises: 0037_merge_upstream_0018
Create Date: 2026-09-19

alembic_version.version_num is VARCHAR(32); revision ids in this chain must
stay at or under that length or stamping/upgrading a database fails outright.
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0038_shared_publications"
down_revision: str | Sequence[str] | None = "0037_merge_upstream_0018"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_TABLE = "shared_publications"


def upgrade() -> None:
    # A fresh database got this table from ``create_all`` before being
    # stamped at head; only an existing one reaches here without it.
    if _TABLE in set(sa.inspect(op.get_bind()).get_table_names()):
        return
    op.create_table(
        _TABLE,
        sa.Column("publication_id", sa.String(length=64), primary_key=True),
        sa.Column("path", sa.String(length=1024), nullable=False),
        sa.Column("size", sa.Integer(), nullable=False),
        sa.Column("sha256", sa.String(length=64), nullable=False),
        sa.Column("published_by", sa.String(length=64), nullable=False),
        sa.Column("published_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("from_thread_id", sa.String(length=64), nullable=True),
        sa.Column("from_path", sa.String(length=4096), nullable=True),
        sa.Column("removed_by", sa.String(length=64), nullable=True),
        sa.Column("removed_at", sa.DateTime(timezone=True), nullable=True),
    )
    op.create_index("ix_shared_publications_path", _TABLE, ["path"])
    op.create_index("ix_shared_publications_published_by", _TABLE, ["published_by"])


def downgrade() -> None:
    bind = op.get_bind()
    if _TABLE not in set(sa.inspect(bind).get_table_names()):
        return
    # This table is the only record of who put a file in front of the whole
    # company and who took it away; a removed file's row is the only place
    # that survives it. Dropping it once anything has been published would
    # erase that, so the downgrade is reversible only before first use.
    if bind.execute(sa.text(f"SELECT 1 FROM {_TABLE} LIMIT 1")).first() is not None:
        raise RuntimeError(f"Refusing to drop {_TABLE}: it holds publication records, which are the only account of what was shared and by whom")
    op.drop_table(_TABLE)
