"""Bind inbound receipts to authenticated provider-event evidence.

Revision ID: 0019_inbound_event_identity
Revises: 0018_inbound_receipt_failures
Create Date: 2026-08-10
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0019_inbound_event_identity"
down_revision: str | Sequence[str] | None = "0018_inbound_receipt_failures"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    inspector = sa.inspect(op.get_bind())
    columns = {column["name"] for column in inspector.get_columns("inbound_receipts")}
    checks = {constraint["name"] for constraint in inspector.get_check_constraints("inbound_receipts")}
    with op.batch_alter_table("inbound_receipts") as batch_op:
        if "provider_event_digest" not in columns:
            batch_op.add_column(
                sa.Column(
                    "provider_event_digest",
                    sa.String(length=64),
                    nullable=True,
                )
            )
        if "ck_inbound_receipts_provider_event_digest_format" not in checks:
            batch_op.create_check_constraint(
                "ck_inbound_receipts_provider_event_digest_format",
                "provider_event_digest IS NULL OR (length(provider_event_digest) = 64 AND lower(provider_event_digest) = provider_event_digest)",
            )


def downgrade() -> None:
    inspector = sa.inspect(op.get_bind())
    columns = {column["name"] for column in inspector.get_columns("inbound_receipts")}
    if "provider_event_digest" not in columns:
        return
    checks = {constraint["name"] for constraint in inspector.get_check_constraints("inbound_receipts")}
    with op.batch_alter_table("inbound_receipts") as batch_op:
        if "ck_inbound_receipts_provider_event_digest_format" in checks:
            batch_op.drop_constraint(
                "ck_inbound_receipts_provider_event_digest_format",
                type_="check",
            )
        batch_op.drop_column("provider_event_digest")
