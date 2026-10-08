"""Instance platform facts, summaries and delayed-write epoch.

Revision ID: 0036_agent_instance_memory
Revises: 0035_agent_conversations
"""

import importlib

import sqlalchemy as sa
from alembic import op

revision = "0036_agent_instance_memory"
down_revision = "0035_agent_conversations"
branch_labels = None
depends_on = None
_metadata = sa.MetaData()
sa.Table("agent_instances", _metadata, sa.Column("id", sa.String(32), primary_key=True))
_table = sa.Table(
    "agent_instance_memory",
    _metadata,
    sa.Column("instance_id", sa.String(32), sa.ForeignKey("agent_instances.id"), primary_key=True, nullable=False),
    sa.Column("epoch", sa.Integer(), nullable=False),
    sa.Column("document", sa.JSON(), nullable=False),
    sa.CheckConstraint("epoch >= 1 AND epoch <= 2147483647", name="ck_agent_memory_epoch"),
)


def upgrade():
    bind = op.get_bind()
    inspector = sa.inspect(bind)
    if _table.name in inspector.get_table_names():
        frozen = importlib.import_module("deerflow.persistence.migrations.versions.0035_agent_conversations")
        frozen._validate_existing(inspector, _table)
        if inspector.get_indexes(_table.name):
            raise RuntimeError("Incompatible instance memory indexes")
    else:
        _table.create(bind)


def downgrade():
    bind = op.get_bind()
    if _table.name not in sa.inspect(bind).get_table_names():
        return
    if bind.execute(sa.select(sa.literal(1)).select_from(_table).limit(1)).first() is not None:
        raise RuntimeError("Refusing to erase used instance memory")
    _table.drop(bind)
