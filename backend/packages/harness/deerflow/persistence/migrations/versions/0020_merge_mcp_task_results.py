"""Merge the durable-invocation and MCP task-result migration branches.

Revision ID: 0020_merge_mcp_task_results
Revises: 0019_inbound_event_identity, 0012_mcp_task_results
"""

from __future__ import annotations

from collections.abc import Sequence

revision: str = "0020_merge_mcp_task_results"
down_revision: str | Sequence[str] | None = (
    "0019_inbound_event_identity",
    "0012_mcp_task_results",
)
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Join two already-applied schema branches without additional DDL."""


def downgrade() -> None:
    """Split the graph back into its two existing branch heads."""
