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
sa.Table("agent_conversations", _metadata, sa.Column("thread_id", sa.String(64), primary_key=True))
_table = sa.Table(
    "agent_instance_memory",
    _metadata,
    sa.Column("instance_id", sa.String(32), sa.ForeignKey("agent_instances.id"), primary_key=True, nullable=False),
    sa.Column("epoch", sa.Integer(), nullable=False),
    sa.Column("document", sa.JSON(), nullable=False),
    sa.CheckConstraint("epoch >= 1 AND epoch <= 2147483647", name="ck_agent_memory_epoch"),
)
_context = sa.Table(
    "agent_protected_contexts",
    _metadata,
    sa.Column("thread_id", sa.String(64), sa.ForeignKey("agent_conversations.thread_id"), primary_key=True, nullable=False),
)


def upgrade():
    bind = op.get_bind()
    inspector = sa.inspect(bind)
    frozen = importlib.import_module("deerflow.persistence.migrations.versions.0035_agent_conversations")
    for table in (_table, _context):
        if table.name in inspector.get_table_names():
            frozen._validate_existing(inspector, table)
            if inspector.get_indexes(table.name):
                raise RuntimeError("Incompatible instance memory indexes")
        else:
            table.create(bind)


def downgrade():
    bind = op.get_bind()
    existing = sa.inspect(bind).get_table_names()
    for table in (_context, _table):
        if table.name in existing and bind.execute(sa.select(sa.literal(1)).select_from(table).limit(1)).first() is not None:
            raise RuntimeError("Refusing to erase used instance memory or context provenance")
    for table in (_context, _table):
        if table.name in existing:
            table.drop(bind)
