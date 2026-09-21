"""Record the issuer an OIDC-linked account was created under.

Revision ID: 0039_users_oauth_issuer
Revises: 0038_shared_publications
Create Date: 2026-09-20

alembic_version.version_num is VARCHAR(32); revision ids in this chain must
stay at or under that length or stamping/upgrading a database fails outright.

An account's identity-provider key was ``(provider name in config, subject)``,
so renaming a provider, or pointing the same name at another issuer, moved
accounts between people. ``users.oauth_issuer`` pins the account to the
issuer whose assertion created it. Nullable: rows linked before this revision
carry NULL and adopt the configured issuer on their next sign-in
(``app.gateway.auth.user_provisioning``); nothing here can know which issuer
a legacy row belonged to, so the column is not backfilled.
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa

from deerflow.persistence.migrations._helpers import safe_add_column, safe_drop_column

revision: str = "0039_users_oauth_issuer"
down_revision: str | Sequence[str] | None = "0038_shared_publications"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    safe_add_column("users", sa.Column("oauth_issuer", sa.String(length=512), nullable=True))


def downgrade() -> None:
    safe_drop_column("users", "oauth_issuer")
