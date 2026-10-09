"""Durable Work-backed human input. Frozen additive DDL."""

import importlib

import sqlalchemy as sa
from alembic import op

revision = "0039_human_input"
down_revision = "0038_agent_work"
branch_labels = None
depends_on = None
_metadata = sa.MetaData()
sa.Table("agent_instances", _metadata, sa.Column("id", sa.String(32), primary_key=True))
sa.Table("agent_work", _metadata, sa.Column("id", sa.String(32), primary_key=True))
_tables = []

_table = sa.Table(
    "human_input_requests",
    _metadata,
    sa.Column("id", sa.String(length=32), nullable=False, primary_key=True),
    sa.Column("source_kind", sa.String(length=16), nullable=False, primary_key=False),
    sa.Column("instance_id", sa.String(length=32), sa.ForeignKey("agent_instances.id"), nullable=False, primary_key=False),
    sa.Column("work_id", sa.String(length=32), sa.ForeignKey("agent_work.id"), nullable=False, primary_key=False),
    sa.Column("assignment_revision", sa.Integer(), nullable=False, primary_key=False),
    sa.Column("basis_id", sa.String(length=32), nullable=False, primary_key=False),
    sa.Column("basis_revision", sa.Integer(), nullable=False, primary_key=False),
    sa.Column("creator_id", sa.String(length=128), nullable=False, primary_key=False),
    sa.Column("recipient_id", sa.String(length=128), nullable=True, primary_key=False),
    sa.Column("revision", sa.Integer(), nullable=False, primary_key=False),
    sa.Column("request_revision", sa.Integer(), nullable=False, primary_key=False),
    sa.Column("purpose", sa.String(length=16), nullable=False, primary_key=False),
    sa.Column("question", sa.Text(), nullable=False, primary_key=False),
    sa.Column("reason", sa.Text(), nullable=False, primary_key=False),
    sa.Column("expected_response", sa.Text(), nullable=False, primary_key=False),
    sa.Column("choices", sa.JSON(), nullable=False, primary_key=False),
    sa.Column("sources", sa.JSON(), nullable=False, primary_key=False),
    sa.Column("state", sa.String(length=16), nullable=False, primary_key=False),
    sa.Column("closed_reason", sa.String(length=16), nullable=True, primary_key=False),
    sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, primary_key=False),
    sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, primary_key=False),
    sa.CheckConstraint("(state = 'closed' AND closed_reason IS NOT NULL AND closed_reason IN ('resolved', 'superseded', 'withdrawn')) OR (state != 'closed' AND closed_reason IS NULL)", name="ck_human_input_closed"),
    sa.CheckConstraint("purpose IN ('information', 'decision', 'review')", name="ck_human_input_purpose"),
    sa.CheckConstraint("revision >= request_revision AND request_revision >= 1 AND revision <= 2147483647 AND assignment_revision >= 1 AND basis_revision >= 1", name="ck_human_input_revision"),
    sa.CheckConstraint("source_kind = 'work'", name="ck_human_input_source"),
    sa.CheckConstraint("state IN ('pending', 'answered', 'closed') AND (state != 'answered' OR purpose = 'information')", name="ck_human_input_state"),
)
sa.Index("ix_human_input_recipient", _table.c.recipient_id, _table.c.state, _table.c.updated_at, _table.c.id, unique=False)
sa.Index("uq_human_input_live_work", _table.c.work_id, unique=True, sqlite_where=sa.text("state != 'closed'"), postgresql_where=sa.text("state != 'closed'"))
_tables.append(_table)

_table = sa.Table(
    "human_input_responses",
    _metadata,
    sa.Column("id", sa.String(length=32), nullable=False, primary_key=True),
    sa.Column("request_id", sa.String(length=32), sa.ForeignKey("human_input_requests.id"), nullable=False, primary_key=False),
    sa.Column("actor_id", sa.String(length=128), nullable=False, primary_key=False),
    sa.Column("request_revision", sa.Integer(), nullable=False, primary_key=False),
    sa.Column("assignment_revision", sa.Integer(), nullable=False, primary_key=False),
    sa.Column("disposition", sa.String(length=32), nullable=False, primary_key=False),
    sa.Column("text", sa.Text(), nullable=True, primary_key=False),
    sa.Column("choice", sa.Text(), nullable=True, primary_key=False),
    sa.Column("sources", sa.JSON(), nullable=False, primary_key=False),
    sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, primary_key=False),
    sa.CheckConstraint("disposition IN ('supplied', 'cannot_provide', 'wrong_recipient')", name="ck_human_response_disposition"),
    sa.CheckConstraint("request_revision >= 1 AND assignment_revision >= 1", name="ck_human_response_revision"),
)
sa.Index("ix_human_response_request", _table.c.request_id, _table.c.created_at, _table.c.id, unique=False)
_tables.append(_table)

_table = sa.Table(
    "human_input_events",
    _metadata,
    sa.Column("id", sa.String(length=32), nullable=False, primary_key=True),
    sa.Column("instance_id", sa.String(length=32), sa.ForeignKey("agent_instances.id"), nullable=False, primary_key=False),
    sa.Column("request_id", sa.String(length=32), sa.ForeignKey("human_input_requests.id"), nullable=False, primary_key=False),
    sa.Column("actor_id", sa.String(length=128), nullable=False, primary_key=False),
    sa.Column("operation_id", sa.String(length=32), nullable=False, primary_key=False),
    sa.Column("action", sa.String(length=32), nullable=False, primary_key=False),
    sa.Column("revision", sa.Integer(), nullable=False, primary_key=False),
    sa.Column("request", sa.JSON(), nullable=False, primary_key=False),
    sa.Column("receipt", sa.JSON(), nullable=False, primary_key=False),
    sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, primary_key=False),
    sa.UniqueConstraint("request_id", "revision", name="uq_human_input_event_revision"),
    sa.UniqueConstraint("instance_id", "operation_id", name="uq_human_input_operation"),
)
_tables.append(_table)

_table = sa.Table(
    "human_input_reads",
    _metadata,
    sa.Column("request_id", sa.String(length=32), sa.ForeignKey("human_input_requests.id"), nullable=False, primary_key=True),
    sa.Column("actor_id", sa.String(length=128), nullable=False, primary_key=True),
    sa.Column("revision", sa.Integer(), nullable=False, primary_key=False),
    sa.CheckConstraint("revision >= 1", name="ck_human_input_read_revision"),
)
_tables.append(_table)


def upgrade():
    bind = op.get_bind()
    frozen = importlib.import_module("deerflow.persistence.migrations.versions.0035_agent_conversations")
    indexes = importlib.import_module("deerflow.persistence.migrations.versions.0038_agent_work")
    for table in _tables:
        inspector = sa.inspect(bind)
        if table.name in inspector.get_table_names():
            frozen._validate_existing(inspector, table)
            indexes._validate_indexes(bind, table)
        else:
            table.create(bind)


def downgrade():
    bind = op.get_bind()
    existing = set(sa.inspect(bind).get_table_names())
    for table in _tables:
        if table.name in existing and bind.execute(sa.select(sa.literal(1)).select_from(table).limit(1)).first() is not None:
            raise RuntimeError("Refusing to erase used human input or history")
    for table in reversed(_tables):
        if table.name in existing:
            table.drop(bind)
