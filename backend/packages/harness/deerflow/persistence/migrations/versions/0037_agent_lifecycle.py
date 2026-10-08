"""Durable agent intent and exact native containment scope.

Revision ID: 0037_agent_lifecycle
Revises: 0036_agent_instance_memory
"""

import importlib

import sqlalchemy as sa
from alembic import op

revision = "0037_agent_lifecycle"
down_revision = "0036_agent_instance_memory"
branch_labels = None
depends_on = None
_metadata = sa.MetaData()
sa.Table("agent_instances", _metadata, sa.Column("id", sa.String(32), primary_key=True))
sa.Table("storage_spaces", _metadata, sa.Column("id", sa.String(32), primary_key=True))
_table = sa.Table(
    "agent_lifecycle_operations",
    _metadata,
    sa.Column("instance_id", sa.String(32), sa.ForeignKey("agent_instances.id"), primary_key=True, nullable=False),
    sa.Column("operation_id", sa.String(32), primary_key=True, nullable=False),
    sa.Column("actor_id", sa.String(128), nullable=False),
    sa.Column("generation", sa.Integer(), nullable=False),
    sa.Column("home_generation", sa.Integer(), nullable=False),
    sa.Column("home_id", sa.String(32), sa.ForeignKey("storage_spaces.id"), nullable=False),
    sa.Column("request", sa.JSON(), nullable=False),
    sa.Column("attachment_ids", sa.JSON(), nullable=False),
    sa.Column("phase", sa.String(16), nullable=False),
    sa.Column("prior_status", sa.String(16), nullable=False),
    sa.Column("resolved_by", sa.String(128), nullable=True),
    sa.CheckConstraint("generation >= 1 AND generation < 2147483647", name="ck_agent_lifecycle_generation"),
    sa.CheckConstraint("phase IN ('pending', 'complete', 'abandoned')", name="ck_agent_lifecycle_phase"),
    sa.CheckConstraint("prior_status IN ('active', 'suspended', 'archived', 'deleted')", name="ck_agent_lifecycle_prior_status"),
)


def upgrade():
    bind = op.get_bind()
    inspector = sa.inspect(bind)
    if _table.name in inspector.get_table_names():
        frozen = importlib.import_module("deerflow.persistence.migrations.versions.0035_agent_conversations")
        frozen._validate_existing(inspector, _table)
        if inspector.get_indexes(_table.name):
            raise RuntimeError("Incompatible agent lifecycle indexes")
    else:
        _table.create(bind)


def downgrade():
    bind = op.get_bind()
    if _table.name not in sa.inspect(bind).get_table_names():
        return
    if bind.execute(sa.select(sa.literal(1)).select_from(_table).limit(1)).first() is not None:
        raise RuntimeError("Refusing to erase used agent lifecycle intents")
    _table.drop(bind)
