"""Provider keys an administrator set in the product, and the record of each change.

Revision ID: 0042_provider_keys
Revises: 0041_email_released_from
Create Date: 2026-09-23

alembic_version.version_num is VARCHAR(32); revision ids in this chain must
stay at or under that length or stamping/upgrading a database fails outright.

A key an administrator sets in the product outranks the one the deployment's
environment carries for the same provider, and it has to keep outranking it
across a restart, a redeploy and a restore -- so it lives here, in the
database every backup holds, wrapped under a key that lives in the
environment and never on this disk.
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0042_provider_keys"
down_revision: str | Sequence[str] | None = "0041_email_released_from"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_KEYS = "provider_keys"
_EVENTS = "provider_key_events"


def upgrade() -> None:
    # A fresh database got these tables from ``create_all`` before being
    # stamped at head; only an existing one reaches here without them.
    existing = set(sa.inspect(op.get_bind()).get_table_names())
    if _KEYS not in existing:
        op.create_table(
            _KEYS,
            sa.Column("variable", sa.String(length=64), primary_key=True),
            sa.Column("ciphertext", sa.Text(), nullable=False),
            sa.Column("changed_at", sa.DateTime(timezone=True), nullable=False),
            sa.Column("changed_by", sa.String(length=320), nullable=False),
        )
    if _EVENTS not in existing:
        op.create_table(
            _EVENTS,
            sa.Column("event_id", sa.String(length=36), primary_key=True),
            sa.Column("variable", sa.String(length=64), nullable=False),
            sa.Column("action", sa.String(length=16), nullable=False),
            sa.Column("actor_id", sa.String(length=64), nullable=False),
            sa.Column("actor_email", sa.String(length=320), nullable=True),
            sa.Column("occurred_at", sa.DateTime(timezone=True), nullable=False),
        )
        op.create_index("ix_provider_key_events_variable", _EVENTS, ["variable"])
        op.create_index("ix_provider_key_events_occurred_at", _EVENTS, ["occurred_at"])


def downgrade() -> None:
    bind = op.get_bind()
    existing = set(sa.inspect(bind).get_table_names())
    # Dropping a stored key puts its provider back on whatever key the
    # environment carries -- the key the company believes it replaced, and is
    # billed on. So the downgrade is reversible only while none is stored.
    if _KEYS in existing and bind.execute(sa.text(f"SELECT 1 FROM {_KEYS} LIMIT 1")).first() is not None:
        raise RuntimeError(f"Refusing to drop {_KEYS}: it holds provider keys set in the product; remove them in the product first")
    if _EVENTS in existing:
        op.drop_table(_EVENTS)
    if _KEYS in existing:
        op.drop_table(_KEYS)
