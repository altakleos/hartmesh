"""Add the shared exact-two Gateway topology registry.

Revision ID: 0027_multi_gateway_topology
Revises: 0026_mcp_task_lineage
Create Date: 2026-09-01
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0027_multi_gateway_topology"
down_revision: str | Sequence[str] | None = "0026_mcp_task_lineage"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    bind = op.get_bind()
    inspector = sa.inspect(bind)
    if inspector.has_table("scheduled_tasks"):
        columns = {column["name"] for column in inspector.get_columns("scheduled_tasks")}
        if "schedule_version" not in columns:
            with op.batch_alter_table("scheduled_tasks", schema=None) as batch_op:
                batch_op.add_column(
                    sa.Column(
                        "schedule_version",
                        sa.Integer(),
                        nullable=False,
                        server_default="1",
                    )
                )
    if not inspector.has_table("hartmesh_topology_replicas"):
        op.create_table(
            "hartmesh_topology_replicas",
            sa.Column("tenant_digest", sa.String(length=64), nullable=False),
            sa.Column("profile", sa.String(length=64), nullable=False),
            sa.Column("replica_id", sa.String(length=128), nullable=False),
            sa.Column("topology_digest", sa.String(length=64), nullable=False),
            sa.Column("fingerprint_json", sa.JSON(), nullable=False),
            sa.Column("started_at", sa.DateTime(timezone=True), nullable=False),
            sa.Column("heartbeat_at", sa.DateTime(timezone=True), nullable=False),
            sa.CheckConstraint(
                "profile = 'durable_two_gateway_v1'",
                name="ck_hartmesh_topology_profile",
            ),
            sa.CheckConstraint(
                "length(tenant_digest) = 64 AND length(topology_digest) = 64",
                name="ck_hartmesh_topology_digest_lengths",
            ),
            sa.CheckConstraint(
                "length(replica_id) BETWEEN 1 AND 128",
                name="ck_hartmesh_topology_replica_id_length",
            ),
            sa.PrimaryKeyConstraint(
                "tenant_digest",
                "profile",
                "replica_id",
                name="pk_hartmesh_topology_replicas",
            ),
        )
        op.create_index(
            "ix_hartmesh_topology_live",
            "hartmesh_topology_replicas",
            ["tenant_digest", "profile", "heartbeat_at"],
            unique=False,
        )


def downgrade() -> None:
    bind = op.get_bind()
    inspector = sa.inspect(bind)
    if inspector.has_table("hartmesh_topology_replicas"):
        op.drop_index(
            "ix_hartmesh_topology_live",
            table_name="hartmesh_topology_replicas",
        )
        op.drop_table("hartmesh_topology_replicas")
    if inspector.has_table("scheduled_tasks"):
        columns = {column["name"] for column in inspector.get_columns("scheduled_tasks")}
        if "schedule_version" in columns:
            with op.batch_alter_table("scheduled_tasks", schema=None) as batch_op:
                batch_op.drop_column("schedule_version")
