"""Repair legacy delivery receipts with their authoritative run owner.

Revision ID: 0030_run_delivery_owner_backfill
Revises: 0029_run_recovery_policy
Create Date: 2026-09-02
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0030_run_delivery_owner_backfill"
down_revision: str | Sequence[str] | None = "0029_run_recovery_policy"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    run_events = sa.table(
        "run_events",
        sa.column("run_id", sa.String()),
        sa.column("thread_id", sa.String()),
        sa.column("tenant_digest", sa.String()),
        sa.column("event_type", sa.String()),
        sa.column("user_id", sa.String()),
    )
    runs = sa.table(
        "runs",
        sa.column("run_id", sa.String()),
        sa.column("thread_id", sa.String()),
        sa.column("tenant_digest", sa.String()),
        sa.column("user_id", sa.String()),
    )
    owner_match = sa.and_(
        runs.c.run_id == run_events.c.run_id,
        runs.c.thread_id == run_events.c.thread_id,
        sa.or_(
            runs.c.tenant_digest == run_events.c.tenant_digest,
            sa.and_(
                runs.c.tenant_digest.is_(None),
                run_events.c.tenant_digest.is_(None),
            ),
        ),
        runs.c.user_id.is_not(None),
    )
    matching_owner_count = sa.select(sa.func.count()).select_from(runs).where(owner_match).correlate(run_events).scalar_subquery()
    authoritative_owner = sa.select(runs.c.user_id).where(owner_match).correlate(run_events).scalar_subquery()
    op.get_bind().execute(
        sa.update(run_events)
        .where(
            run_events.c.event_type == "run.delivery",
            run_events.c.user_id.is_(None),
            matching_owner_count == 1,
        )
        .values(user_id=authoritative_owner)
    )


def downgrade() -> None:
    """Preserve repaired owner evidence; its previous NULL state is unknowable."""

    pass
