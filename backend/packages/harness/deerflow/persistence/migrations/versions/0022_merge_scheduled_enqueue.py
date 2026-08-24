"""Merge the HartMesh and scheduled-enqueue migration branches.

Revision ID: 0022_merge_scheduled_enqueue
Revises: 0021_merge_managed_subagents, 0015_scheduled_task_enqueue
"""

from __future__ import annotations

from collections.abc import Sequence

revision: str = "0022_merge_scheduled_enqueue"
down_revision: str | Sequence[str] | None = (
    "0021_merge_managed_subagents",
    "0015_scheduled_task_enqueue",
)
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Join two already-applied schema branches without additional DDL."""


def downgrade() -> None:
    """Split the graph back into its two existing branch heads."""
