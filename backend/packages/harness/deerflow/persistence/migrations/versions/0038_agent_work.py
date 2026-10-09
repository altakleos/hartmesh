"""Durable Work, reserved attempts and attributed human commands. Frozen DDL."""

import importlib
import uuid

import sqlalchemy as sa
from alembic import op

revision = "0038_agent_work"
down_revision = "0037_agent_lifecycle"
branch_labels = None
depends_on = None
_metadata = sa.MetaData()
sa.Table("agent_instances", _metadata, sa.Column("id", sa.String(32), primary_key=True))
sa.Table("agent_definition_revisions", _metadata, sa.Column("revision", sa.String(64), primary_key=True))
_tables = []

_table = sa.Table(
    "agent_work",
    _metadata,
    sa.Column("id", sa.String(length=32), nullable=False, primary_key=True),
    sa.Column("instance_id", sa.String(length=32), sa.ForeignKey("agent_instances.id"), nullable=False),
    sa.Column("creator_id", sa.String(length=128), nullable=False),
    sa.Column("objective", sa.Text(), nullable=False),
    sa.Column("success_criteria", sa.Text(), nullable=False),
    sa.Column("responsibility", sa.String(length=64), nullable=True),
    sa.Column("priority", sa.String(length=16), nullable=False),
    sa.Column("due_at", sa.DateTime(timezone=True), nullable=True),
    sa.Column("definition_revision", sa.String(length=64), sa.ForeignKey("agent_definition_revisions.revision"), nullable=False),
    sa.Column("assignment_revision", sa.Integer(), nullable=False),
    sa.Column("revision", sa.Integer(), nullable=False),
    sa.Column("status", sa.String(length=16), nullable=False),
    sa.Column("progress", sa.Text(), nullable=False),
    sa.Column("next_action", sa.Text(), nullable=False),
    sa.Column("sources", sa.JSON(), nullable=False),
    sa.Column("blocker", sa.JSON(none_as_null=True), nullable=True),
    sa.Column("review_required", sa.Boolean(), nullable=False),
    sa.Column("review_state", sa.String(length=16), nullable=False),
    sa.Column("outcome_id", sa.String(length=32), nullable=True),
    sa.Column("outcome_assignment_revision", sa.Integer(), nullable=True),
    sa.Column("outcome", sa.JSON(none_as_null=True), nullable=True),
    sa.Column("review", sa.JSON(none_as_null=True), nullable=True),
    sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
    sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
    sa.CheckConstraint("priority IN ('low', 'normal', 'high', 'urgent')", name="ck_agent_work_priority"),
    sa.CheckConstraint(
        "(status IN ('open', 'blocked', 'cancelled') AND outcome_id IS NULL AND outcome_assignment_revision IS NULL AND outcome IS NULL AND review IS NULL AND review_state = 'none') OR "
        "(status = 'submitted' AND review_required AND review_state = 'pending' AND review IS NULL AND outcome_id IS NOT NULL AND outcome_assignment_revision IS NOT NULL AND "
        "outcome_assignment_revision = assignment_revision AND outcome IS NOT NULL) OR (status = 'completed' AND outcome_id IS NOT NULL AND outcome_assignment_revision IS NOT NULL AND "
        "outcome_assignment_revision = assignment_revision AND outcome IS NOT NULL AND ((review_required AND review_state = 'accepted' AND review IS NOT NULL) OR (NOT review_required AND "
        "review_state = 'reported' AND review IS NULL)))",
        name="ck_agent_work_review",
    ),
    sa.CheckConstraint("revision >= assignment_revision AND assignment_revision >= 1 AND revision <= 2147483647", name="ck_agent_work_revision"),
    sa.CheckConstraint("status IN ('open', 'blocked', 'submitted', 'completed', 'cancelled')", name="ck_agent_work_status"),
)
sa.Index("ix_agent_work_instance_created", _table.c.instance_id, _table.c.created_at, _table.c.id, unique=False)
_tables.append(_table)

_table = sa.Table(
    "agent_work_attempts",
    _metadata,
    sa.Column("id", sa.String(length=32), nullable=False, primary_key=True),
    sa.Column("work_id", sa.String(length=32), sa.ForeignKey("agent_work.id"), nullable=False),
    sa.Column("activation_id", sa.String(length=32), nullable=False),
    sa.Column("requester_id", sa.String(length=128), nullable=False),
    sa.Column("assignment_revision", sa.Integer(), nullable=False),
    sa.Column("instance_generation", sa.Integer(), nullable=False),
    sa.Column("definition_revision", sa.String(length=64), sa.ForeignKey("agent_definition_revisions.revision"), nullable=False),
    sa.Column("status", sa.String(length=16), nullable=False),
    sa.Column("thread_id", sa.String(length=64), nullable=True),
    sa.Column("run_id", sa.String(length=64), nullable=True),
    sa.Column("incarnation", sa.String(length=64), nullable=True),
    sa.Column("request", sa.JSON(), nullable=False),
    sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
    sa.CheckConstraint("assignment_revision >= 1 AND assignment_revision <= 2147483647 AND instance_generation >= 1 AND instance_generation <= 2147483647", name="ck_agent_work_attempt_revision"),
    sa.CheckConstraint("status IN ('starting', 'running', 'stopping', 'uncertain', 'succeeded', 'failed', 'cancelled')", name="ck_agent_work_attempt_status"),
    sa.UniqueConstraint("work_id", "activation_id", name="uq_agent_work_attempt_activation"),
)
sa.Index(
    "uq_agent_work_attempt_unresolved",
    _table.c.work_id,
    unique=True,
    sqlite_where=sa.text("status IN ('starting', 'running', 'stopping', 'uncertain')"),
    postgresql_where=sa.text("status IN ('starting', 'running', 'stopping', 'uncertain')"),
)
_tables.append(_table)

_table = sa.Table(
    "agent_work_events",
    _metadata,
    sa.Column("id", sa.String(length=32), nullable=False, primary_key=True),
    sa.Column("instance_id", sa.String(length=32), sa.ForeignKey("agent_instances.id"), nullable=False),
    sa.Column("work_id", sa.String(length=32), sa.ForeignKey("agent_work.id"), nullable=False),
    sa.Column("actor_id", sa.String(length=128), nullable=False),
    sa.Column("actor_kind", sa.String(length=16), nullable=False),
    sa.Column("operation_id", sa.String(length=32), nullable=False),
    sa.Column("action", sa.String(length=32), nullable=False),
    sa.Column("revision", sa.Integer(), nullable=False),
    sa.Column("assignment_revision", sa.Integer(), nullable=False),
    sa.Column("request", sa.JSON(), nullable=False),
    sa.Column("result", sa.JSON(), nullable=False),
    sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
    sa.CheckConstraint("actor_kind IN ('human', 'nonhuman')", name="ck_agent_work_event_actor"),
    sa.CheckConstraint("revision >= assignment_revision AND assignment_revision >= 1 AND revision <= 2147483647", name="ck_agent_work_event_revision"),
    sa.UniqueConstraint("instance_id", "operation_id", name="uq_agent_work_event_operation"),
    sa.UniqueConstraint("work_id", "revision", name="uq_agent_work_event_revision"),
)
_tables.append(_table)


def _index_shapes(inspector, name, schema=None):
    return sorted((tuple(i["column_names"]), bool(i["unique"]), str(i.get("dialect_options", {}).get(inspector.bind.dialect.name + "_where", ""))) for i in inspector.get_indexes(name, schema=schema) if not i.get("duplicates_constraint"))


def _validate_indexes(bind, table):
    if bind.dialect.name == "sqlite":
        expected = sorted((tuple(c.name for c in i.columns), bool(i.unique), str(i.dialect_options["sqlite"]["where"]) if i.dialect_options["sqlite"].get("where") is not None else "") for i in table.indexes)
        if _index_shapes(sa.inspect(bind), table.name) != expected:
            raise RuntimeError("Incompatible Work indexes: " + table.name)
        return
    # Canonicalize partial predicates on the actual server, including PG casts.
    metadata = sa.MetaData()
    template = sa.Table("work_shape_" + uuid.uuid4().hex, metadata, *[sa.Column(c.name, c.type, nullable=c.nullable) for c in table.columns], prefixes=["TEMPORARY"])
    for index in table.indexes:
        sa.Index("work_index_" + uuid.uuid4().hex, *[template.c[c.name] for c in index.columns], unique=index.unique, **dict(index.dialect_kwargs))
    template.create(bind)
    try:
        inspector = sa.inspect(bind)
        if _index_shapes(inspector, table.name) != _index_shapes(inspector, template.name, "temp" if bind.dialect.name == "sqlite" else None):
            raise RuntimeError("Incompatible Work indexes: " + table.name)
    finally:
        template.drop(bind)


def upgrade():
    bind = op.get_bind()
    frozen = importlib.import_module("deerflow.persistence.migrations.versions.0035_agent_conversations")
    for table in _tables:
        inspector = sa.inspect(bind)
        if table.name in inspector.get_table_names():
            frozen._validate_existing(inspector, table)
            _validate_indexes(bind, table)
        else:
            table.create(bind)


def downgrade():
    bind = op.get_bind()
    existing = set(sa.inspect(bind).get_table_names())
    for table in _tables:
        if table.name in existing and bind.execute(sa.select(sa.literal(1)).select_from(table).limit(1)).first() is not None:
            raise RuntimeError("Refusing to erase used Work records or history")
    for table in reversed(_tables):
        if table.name in existing:
            table.drop(bind)
