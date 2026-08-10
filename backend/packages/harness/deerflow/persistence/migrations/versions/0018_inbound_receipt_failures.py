"""separate inbound receipt contention from poison failures.

Revision ID: 0018_inbound_receipt_failures
Revises: 0017_lifecycle_integrity
Create Date: 2026-08-09
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0018_inbound_receipt_failures"
down_revision: str | Sequence[str] | None = "0017_lifecycle_integrity"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    columns = {column["name"] for column in sa.inspect(op.get_bind()).get_columns("inbound_receipts")}
    if "failure_count" in columns:
        return
    with op.batch_alter_table("inbound_receipts") as batch_op:
        batch_op.drop_constraint(
            "ck_inbound_receipts_counters_nonnegative",
            type_="check",
        )
        batch_op.add_column(
            sa.Column(
                "failure_count",
                sa.Integer(),
                nullable=False,
                server_default=sa.text("0"),
            )
        )
        batch_op.create_check_constraint(
            "ck_inbound_receipts_counters_nonnegative",
            "fencing_token >= 0 AND attempt_count >= 0 AND failure_count >= 0",
        )


def downgrade() -> None:
    columns = {column["name"] for column in sa.inspect(op.get_bind()).get_columns("inbound_receipts")}
    if "failure_count" not in columns:
        return
    with op.batch_alter_table("inbound_receipts") as batch_op:
        batch_op.drop_constraint(
            "ck_inbound_receipts_counters_nonnegative",
            type_="check",
        )
        batch_op.create_check_constraint(
            "ck_inbound_receipts_counters_nonnegative",
            "fencing_token >= 0 AND attempt_count >= 0",
        )
        batch_op.drop_column("failure_count")
