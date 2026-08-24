"""Merge the HartMesh and managed-subagent migration branches.

Revision ID: 0021_merge_managed_subagents
Revises: 0020_merge_mcp_task_results, 0014_managed_subagents
"""

from __future__ import annotations

from collections.abc import Sequence

revision: str = "0021_merge_managed_subagents"
down_revision: str | Sequence[str] | None = (
    "0020_merge_mcp_task_results",
    "0014_managed_subagents",
)
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Join two already-applied schema branches without additional DDL."""


def downgrade() -> None:
    """Split the graph back into its two existing branch heads."""
