"""Persistent agent identities and adopted definitions after release43.

Revision ID: 0034_agent_instances
Revises: 0033_storage_features
"""

import uuid

import sqlalchemy as sa
from alembic import op

revision = "0034_agent_instances"
down_revision = "0033_storage_features"
branch_labels = None
depends_on = None

_metadata = sa.MetaData()
sa.Table("storage_spaces", _metadata, sa.Column("id", sa.String(32), primary_key=True))

_table_0 = sa.Table(
    "agent_definition_revisions",
    _metadata,
    sa.Column("revision", sa.String(length=64), primary_key=True, nullable=False),
    sa.Column("owner_id", sa.String(length=128), nullable=False),
    sa.Column("config", sa.JSON(), nullable=False),
    sa.Column("soul", sa.Text(), nullable=False),
)

_table_1 = sa.Table(
    "agent_instances",
    _metadata,
    sa.Column("id", sa.String(length=32), primary_key=True, nullable=False),
    sa.Column("principal_id", sa.String(length=128), nullable=False),
    sa.Column("name", sa.String(length=128), nullable=False),
    sa.Column("custody", sa.String(length=16), nullable=False),
    sa.Column("owner_id", sa.String(length=128), nullable=True),
    sa.Column("creator_id", sa.String(length=128), nullable=False),
    sa.Column("supervisor_id", sa.String(length=128), nullable=False),
    sa.Column("definition_revision", sa.String(length=64), sa.ForeignKey("agent_definition_revisions.revision"), nullable=False),
    sa.Column("home_id", sa.String(length=32), sa.ForeignKey("storage_spaces.id"), nullable=True),
    sa.Column("status", sa.String(length=16), nullable=False),
    sa.Column("generation", sa.Integer(), nullable=False),
    sa.Column("creation_id", sa.String(length=32), nullable=False),
    sa.Column("creation_request", sa.JSON(), nullable=False),
    sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
    sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
    sa.CheckConstraint("(custody = 'personal' AND owner_id IS NOT NULL) OR (custody = 'company' AND owner_id IS NULL)", name="ck_agent_instances_custody"),
    sa.CheckConstraint("generation >= 1 AND generation <= 2147483647", name="ck_agent_instances_generation"),
    sa.CheckConstraint("status = 'provisioning' OR home_id IS NOT NULL", name="ck_agent_instances_home"),
    sa.CheckConstraint("principal_id = 'agent:' || id", name="ck_agent_instances_principal"),
    sa.CheckConstraint("status IN ('provisioning', 'active', 'suspended', 'archived', 'deleted')", name="ck_agent_instances_status"),
    sa.UniqueConstraint("creator_id", "creation_id", name="uq_agent_instances_creation"),
    sa.UniqueConstraint("home_id", name="uq_agent_instances_home"),
    sa.UniqueConstraint("principal_id", name="uq_agent_instances_principal"),
)

_table_2 = sa.Table(
    "agent_instance_grants",
    _metadata,
    sa.Column("instance_id", sa.String(length=32), sa.ForeignKey("agent_instances.id"), primary_key=True, nullable=False),
    sa.Column("user_id", sa.String(length=128), primary_key=True, nullable=False),
    sa.Column("permissions", sa.Integer(), nullable=False),
    sa.CheckConstraint("permissions >= 1 AND permissions <= 7", name="ck_agent_instance_grants_permissions"),
)

_tables = (_table_0, _table_1, _table_2)


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


def upgrade() -> None:
    bind = op.get_bind()
    inspector = sa.inspect(bind)
    existing = set(inspector.get_table_names())
    for table in _tables:
        if table.name in existing:
            _validate_existing(inspector, table)
    for table in _tables:
        if table.name not in existing:
            table.create(bind)


def downgrade() -> None:
    bind = op.get_bind()
    existing = set(sa.inspect(bind).get_table_names())
    for table in _tables:
        if table.name in existing and bind.execute(sa.select(sa.literal(1)).select_from(table).limit(1)).first() is not None:
            raise RuntimeError("Refusing to erase used agent identities, grants or adopted definitions; preserve their history")
    for table in reversed(_tables):
        if table.name in existing:
            table.drop(bind)
