"""persist leased native-ingress receipts.

Revision ID: 0015_inbound_receipts
Revises: 0014_canonical_caller_intent
Create Date: 2026-08-08
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0015_inbound_receipts"
down_revision: str | Sequence[str] | None = "0014_canonical_caller_intent"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    bind = op.get_bind()
    if sa.inspect(bind).has_table("inbound_receipts"):
        return
    op.create_table(
        "inbound_receipts",
        sa.Column("receipt_id", sa.String(length=36), nullable=False),
        sa.Column("provider", sa.String(length=64), nullable=False),
        sa.Column("binding_kind", sa.String(length=32), nullable=False),
        sa.Column("binding_reference", sa.String(length=320), nullable=False),
        sa.Column("provider_delivery_id", sa.String(length=320), nullable=False),
        sa.Column("thread_id", sa.String(length=64), nullable=False),
        sa.Column("payload_json", sa.JSON(), nullable=False),
        sa.Column("payload_digest", sa.String(length=64), nullable=False),
        sa.Column("state", sa.String(length=16), nullable=False),
        sa.Column("lease_owner", sa.String(length=96), nullable=True),
        sa.Column("lease_expires_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("fencing_token", sa.Integer(), nullable=False, server_default=sa.text("0")),
        sa.Column("attempt_count", sa.Integer(), nullable=False, server_default=sa.text("0")),
        sa.Column("next_attempt_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("run_id", sa.String(length=64), nullable=True),
        sa.Column("outcome_code", sa.String(length=64), nullable=True),
        sa.Column("received_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("completed_at", sa.DateTime(timezone=True), nullable=True),
        sa.CheckConstraint(
            "state IN ('received', 'claimed', 'admitted', 'deferred', 'completed', 'dead_letter')",
            name="ck_inbound_receipts_state",
        ),
        sa.CheckConstraint(
            "fencing_token >= 0 AND attempt_count >= 0",
            name="ck_inbound_receipts_counters_nonnegative",
        ),
        sa.CheckConstraint(
            "(state NOT IN ('claimed', 'admitted')) OR (lease_owner IS NOT NULL AND lease_expires_at IS NOT NULL)",
            name="ck_inbound_receipts_claim_has_lease",
        ),
        sa.CheckConstraint(
            "(state != 'admitted') OR run_id IS NOT NULL",
            name="ck_inbound_receipts_admitted_has_run",
        ),
        sa.CheckConstraint(
            "length(provider) BETWEEN 1 AND 64 AND length(binding_reference) BETWEEN 1 AND 320 AND length(provider_delivery_id) BETWEEN 1 AND 320 AND length(thread_id) BETWEEN 1 AND 64",
            name="ck_inbound_receipts_identity_bounds",
        ),
        sa.CheckConstraint(
            "length(payload_digest) = 64 AND lower(payload_digest) = payload_digest",
            name="ck_inbound_receipts_digest_format",
        ),
        sa.PrimaryKeyConstraint("receipt_id"),
        sa.UniqueConstraint(
            "provider",
            "binding_kind",
            "binding_reference",
            "provider_delivery_id",
            name="uq_inbound_receipts_source_delivery",
        ),
    )
    op.create_index(
        "ix_inbound_receipts_due",
        "inbound_receipts",
        ["state", "next_attempt_at", "received_at", "receipt_id"],
    )
    op.create_index(
        "ix_inbound_receipts_run_id",
        "inbound_receipts",
        ["run_id"],
    )
    op.create_index(
        "ix_inbound_receipts_completed_at",
        "inbound_receipts",
        ["completed_at"],
    )


def downgrade() -> None:
    if sa.inspect(op.get_bind()).has_table("inbound_receipts"):
        op.drop_table("inbound_receipts")
