"""Merge HartMesh and upstream migration heads.

Revision ID: 0037_merge_upstream_0018
Revises: 0036_execution_policy_state, 0018_oauth_identity_pg_partial
Create Date: 2026-09-05
"""

from __future__ import annotations

from collections.abc import Sequence

revision: str = "0037_merge_upstream_0018"
down_revision: str | Sequence[str] | None = (
    "0036_execution_policy_state",
    "0018_oauth_identity_pg_partial",
)
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Join the two already-applied schema branches."""


def downgrade() -> None:
    """Split the schema history back into its two parent branches."""
