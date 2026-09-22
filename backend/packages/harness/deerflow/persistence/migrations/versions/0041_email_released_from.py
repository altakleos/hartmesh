"""The address an account held before the deployer released it.

Revision ID: 0041_email_released_from
Revises: 0040_account_access
Create Date: 2026-09-22

alembic_version.version_num is VARCHAR(32); revision ids in this chain must
stay at or under that length or stamping/upgrading a database fails outright.

``users.email`` is unique, so one address belongs to one account for good.
That is right while the account is someone's, and wrong once it is nobody's:
a company that deletes a person and invites them again, or gives a departed
person's address to someone new, would otherwise lock the new subject out
for good. ``release-email`` replaces a turned-off account's address with one
that can never be a person's, and records here what it held -- so the fact
that an account is released is a column, not the shape of its address, and
the deployer's list can still say which address it used to hold.
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa

from deerflow.persistence.migrations._helpers import safe_add_column, safe_drop_column

revision: str = "0041_email_released_from"
down_revision: str | Sequence[str] | None = "0040_account_access"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    safe_add_column("users", sa.Column("email_released_from", sa.String(length=320), nullable=True))


def downgrade() -> None:
    # Dropping this loses which address a released account used to hold. The
    # release itself survives: the account keeps the address the release gave
    # it, which is still nobody's, so nothing is handed back by accident.
    safe_drop_column("users", "email_released_from")
