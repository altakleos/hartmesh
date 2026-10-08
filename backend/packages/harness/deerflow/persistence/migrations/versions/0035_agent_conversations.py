"""Durable instance conversation bindings, retaining deleted-chat identity.

Revision ID: 0035_agent_conversations
Revises: 0034_agent_instances
"""

import uuid

import sqlalchemy as sa
from alembic import op

revision = "0035_agent_conversations"
down_revision = "0034_agent_instances"
branch_labels = None
depends_on = None
_metadata = sa.MetaData()
sa.Table("agent_instances", _metadata, sa.Column("id", sa.String(32), primary_key=True))
_table = sa.Table(
    "agent_conversations",
    _metadata,
    sa.Column("thread_id", sa.String(64), primary_key=True, nullable=False),
    sa.Column("instance_id", sa.String(32), sa.ForeignKey("agent_instances.id"), nullable=False),
    sa.Column("requester_id", sa.String(128), nullable=False),
    sa.Column("creation_id", sa.String(32), nullable=False),
    sa.UniqueConstraint("requester_id", "creation_id", name="uq_agent_conversations_creation"),
)
sa.Index("ix_agent_conversations_instance_id", _table.c.instance_id)


def _validate_existing(inspector, table: sa.Table) -> None:
    """A name alone must not stamp a partial or incompatible schema as current."""
    columns = inspector.get_columns(table.name)
    expected = [(c.name, str(c.type.compile(dialect=inspector.bind.dialect)), c.nullable, None) for c in table.columns]
    actual = [
        (
            c["name"],
            str(c["type"].compile(dialect=inspector.bind.dialect)),
            c["nullable"],
            c["default"],
        )
        for c in columns
    ]
    primary = inspector.get_pk_constraint(table.name)["constrained_columns"]
    uniques = {(u["name"], tuple(u["column_names"])) for u in inspector.get_unique_constraints(table.name)}
    expected_unique = {(c.name, tuple(column.name for column in c.columns)) for c in table.constraints if isinstance(c, sa.UniqueConstraint)}
    checks = {c["name"]: " ".join(c["sqltext"].split()) for c in inspector.get_check_constraints(table.name)}
    expected_checks = {c.name: " ".join(str(c.sqltext).split()) for c in table.constraints if isinstance(c, sa.CheckConstraint)}
    if inspector.bind.dialect.name == "postgresql":
        # PostgreSQL rewrites IN/casts/parentheses in reflected predicates.
        # Ask that server to canonicalize our frozen checks on an empty
        # session-local temporary table, rather than weaken shape validation.
        template = sa.Table(
            "agent_shape_" + uuid.uuid4().hex,
            sa.MetaData(),
            *[sa.Column(c.name, c.type, nullable=c.nullable) for c in table.columns],
            *[sa.CheckConstraint(str(c.sqltext), name=c.name) for c in table.constraints if isinstance(c, sa.CheckConstraint)],
            prefixes=["TEMPORARY"],
        )
        template.create(inspector.bind)
        try:
            expected_checks = {c["name"]: " ".join(c["sqltext"].split()) for c in sa.inspect(inspector.bind).get_check_constraints(template.name)}
        finally:
            template.drop(inspector.bind)
    foreign_keys = inspector.get_foreign_keys(table.name)
    foreign = {
        (
            tuple(f["constrained_columns"]),
            f["referred_table"],
            tuple(f["referred_columns"]),
            f["options"].get("ondelete") or "NO ACTION",
            f["options"].get("onupdate") or "NO ACTION",
            bool(f["options"].get("deferrable")),
            f["options"].get("initially") or "IMMEDIATE",
        )
        for f in foreign_keys
    }
    expected_foreign = {
        (
            tuple(c.parent.name for c in f.elements),
            f.elements[0].column.table.name,
            tuple(c.column.name for c in f.elements),
            f.ondelete or "NO ACTION",
            f.onupdate or "NO ACTION",
            bool(f.deferrable),
            f.initially or "IMMEDIATE",
        )
        for f in table.foreign_key_constraints
    }
    foreign_schema = any(f.get("referred_schema") not in (None, inspector.default_schema_name) for f in foreign_keys)
    if actual != expected or primary != [c.name for c in table.primary_key] or uniques != expected_unique or checks != expected_checks or foreign != expected_foreign or foreign_schema:
        raise RuntimeError(f"Incompatible agent instance schema for {table.name}; preserve data and reconcile its shape before upgrading")


def upgrade():
    bind = op.get_bind()
    inspector = sa.inspect(bind)
    if _table.name in inspector.get_table_names():
        _validate_existing(inspector, _table)
        indexes = {(entry["name"], tuple(entry["column_names"]), bool(entry["unique"])) for entry in inspector.get_indexes(_table.name) if not entry.get("duplicates_constraint")}
        if indexes != {("ix_agent_conversations_instance_id", ("instance_id",), False)}:
            raise RuntimeError("Incompatible agent conversation index")
    else:
        _table.create(bind)


def downgrade():
    bind = op.get_bind()
    if _table.name not in sa.inspect(bind).get_table_names():
        return
    if bind.execute(sa.select(sa.literal(1)).select_from(_table).limit(1)).first() is not None:
        raise RuntimeError("Refusing to erase used agent conversation bindings")
    _table.drop(bind)
