"""Accounts the deployer turned off, limited in role or released, and what turning one off held.

Revision ID: 0027_account_access
Revises: 0026_mcp_task_lease_tokens
Create Date: 2026-10-01

alembic_version.version_num is VARCHAR(32); revision ids in this chain must
stay at or under that length or stamping/upgrading a database fails outright.

One revision for the account-access schema: ``users.oauth_issuer`` pins an
account to the issuer whose assertion created it; ``users.last_sign_in_at``
is stamped at every provider sign-in; ``users.email_released_from`` records
the address a turned-off account gave up. ``disabled_identities``,
``role_limits`` and ``identity_holds`` are keyed by the identity provider's
``(issuer, subject)``, not by a user row: a deployer can turn a person off or
limit their role before their first sign-in, when no account exists.
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

from deerflow.persistence.migrations._helpers import safe_add_column, safe_drop_column

revision: str = "0027_account_access"
down_revision: str | Sequence[str] | None = "0026_mcp_task_lease_tokens"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_HELD_FACTS = ("disabled_identities", "role_limits")


def upgrade() -> None:
    safe_add_column("users", sa.Column("oauth_issuer", sa.String(length=512), nullable=True))
    safe_add_column("users", sa.Column("last_sign_in_at", sa.DateTime(timezone=True), nullable=True))
    safe_add_column("users", sa.Column("email_released_from", sa.String(length=320), nullable=True))
    # A fresh database got these tables from ``create_all`` before being
    # stamped at head; only an existing one reaches here without them.
    existing = set(sa.inspect(op.get_bind()).get_table_names())
    if "disabled_identities" not in existing:
        op.create_table(
            "disabled_identities",
            sa.Column("issuer", sa.String(length=512), primary_key=True),
            sa.Column("subject", sa.String(length=255), primary_key=True),
            sa.Column("disabled_at", sa.DateTime(timezone=True), nullable=False),
        )
    if "role_limits" not in existing:
        op.create_table(
            "role_limits",
            sa.Column("issuer", sa.String(length=512), primary_key=True),
            sa.Column("subject", sa.String(length=255), primary_key=True),
            sa.Column("role", sa.String(length=16), nullable=False),
            sa.Column("limited_at", sa.DateTime(timezone=True), nullable=False),
        )
    if "identity_holds" not in existing:
        op.create_table(
            "identity_holds",
            sa.Column("issuer", sa.String(length=512), primary_key=True),
            sa.Column("subject", sa.String(length=255), primary_key=True),
            sa.Column("kind", sa.String(length=32), primary_key=True),
            sa.Column("target_id", sa.String(length=64), primary_key=True),
            sa.Column("user_id", sa.String(length=64), nullable=False),
            sa.Column("held_at", sa.DateTime(timezone=True), nullable=False),
        )


def downgrade() -> None:
    bind = op.get_bind()
    existing = set(sa.inspect(bind).get_table_names())
    # A row in either table is a person the deployer turned off or demoted.
    # Dropping it would let them back in, or hand the role back, at the next
    # sign-in, so the downgrade is reversible only while neither holds a row.
    for table in _HELD_FACTS:
        if table in existing and bind.execute(sa.text(f"SELECT 1 FROM {table} LIMIT 1")).first() is not None:
            raise RuntimeError(f"Refusing to drop {table}: it holds what the deployer set against an identity, and dropping it would undo that")
    for table in ("identity_holds", *_HELD_FACTS):
        if table in existing:
            op.drop_table(table)
    safe_drop_column("users", "email_released_from")
    safe_drop_column("users", "last_sign_in_at")
    safe_drop_column("users", "oauth_issuer")
