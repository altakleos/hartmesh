"""Merge HartMesh and upstream migration heads.

Revision ID: 0031_merge_upstream_0017
Revises: 0030_run_delivery_owner_backfill, 0017_personal_access_tokens
Create Date: 2026-09-02
"""

from __future__ import annotations

from collections.abc import Sequence

revision: str = "0031_merge_upstream_0017"
down_revision: str | Sequence[str] | None = (
    "0030_run_delivery_owner_backfill",
    "0017_personal_access_tokens",
)
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Join the two already-applied schema branches."""


def downgrade() -> None:
    """Split the schema history back into its two parent branches."""
