"""The highest role the deployer lets an identity hold, whatever its sign-in's claim says.

Revision ID: 0043_role_limits
Revises: 0042_provider_keys
Create Date: 2026-09-26

alembic_version.version_num is VARCHAR(32); revision ids in this chain must
stay at or under that length or stamping/upgrading a database fails outright.

``role_limits`` is keyed by the identity provider's ``(issuer, subject)``,
like ``disabled_identities``: a deployer can demote a person before their
first sign-in, when no account exists, and the limit must hold for every
account that identity has here. It is durable because the provider can be
restored from a backup taken before the demotion, and a limit the next
sign-in overrode would hand the role back.
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0043_role_limits"
down_revision: str | Sequence[str] | None = "0042_provider_keys"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_TABLE = "role_limits"


def upgrade() -> None:
    # A fresh database got this table from ``create_all`` before being
    # stamped at head; only an existing one reaches here without it.
    if _TABLE in set(sa.inspect(op.get_bind()).get_table_names()):
        return
    op.create_table(
        _TABLE,
        sa.Column("issuer", sa.String(length=512), primary_key=True),
        sa.Column("subject", sa.String(length=255), primary_key=True),
        sa.Column("role", sa.String(length=16), nullable=False),
        sa.Column("limited_at", sa.DateTime(timezone=True), nullable=False),
    )


def downgrade() -> None:
    bind = op.get_bind()
    if _TABLE not in set(sa.inspect(bind).get_table_names()):
        return
    # A row here is a demoted person whose provider may still say otherwise.
    # Dropping the table would hand them the claim's role at their next
    # sign-in, so the downgrade is reversible only while no limit holds.
    if bind.execute(sa.text(f"SELECT 1 FROM {_TABLE} LIMIT 1")).first() is not None:
        raise RuntimeError(f"Refusing to drop {_TABLE}: it holds role limits the deployer set, and dropping it would let the next sign-in restore each role")
    op.drop_table(_TABLE)
