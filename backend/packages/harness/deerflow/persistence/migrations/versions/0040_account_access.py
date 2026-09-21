"""Accounts the deployer turned off, and when each account last signed in.

Revision ID: 0040_account_access
Revises: 0039_users_oauth_issuer
Create Date: 2026-09-21

alembic_version.version_num is VARCHAR(32); revision ids in this chain must
stay at or under that length or stamping/upgrading a database fails outright.

``disabled_identities`` is keyed by the identity provider's ``(issuer,
subject)``, not by a user row: a deployer can turn a person off before their
first sign-in, when no account exists, and the refusal must hold when one
would be created. An account is "disabled" exactly when its identity has a
row here; the user row carries no second copy of that fact.

``users.last_sign_in_at`` is stamped at every provider sign-in so the
deployer's account list can be compared with the provider's.
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

from deerflow.persistence.migrations._helpers import safe_add_column, safe_drop_column

revision: str = "0040_account_access"
down_revision: str | Sequence[str] | None = "0039_users_oauth_issuer"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_TABLE = "disabled_identities"


def upgrade() -> None:
    safe_add_column("users", sa.Column("last_sign_in_at", sa.DateTime(timezone=True), nullable=True))
    # A fresh database got this table from ``create_all`` before being
    # stamped at head; only an existing one reaches here without it.
    if _TABLE in set(sa.inspect(op.get_bind()).get_table_names()):
        return
    op.create_table(
        _TABLE,
        sa.Column("issuer", sa.String(length=512), primary_key=True),
        sa.Column("subject", sa.String(length=255), primary_key=True),
        sa.Column("disabled_at", sa.DateTime(timezone=True), nullable=False),
    )


def downgrade() -> None:
    bind = op.get_bind()
    if _TABLE in set(sa.inspect(bind).get_table_names()):
        # A row here is a person the deployer turned off. Dropping the table
        # would let every one of them back in on the next sign-in, so the
        # downgrade is reversible only while nobody is turned off.
        if bind.execute(sa.text(f"SELECT 1 FROM {_TABLE} LIMIT 1")).first() is not None:
            raise RuntimeError(f"Refusing to drop {_TABLE}: it holds accounts the deployer turned off, and dropping it would turn them back on")
        op.drop_table(_TABLE)
    safe_drop_column("users", "last_sign_in_at")
