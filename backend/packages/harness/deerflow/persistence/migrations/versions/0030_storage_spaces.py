"""Generic storage resource identity, explicit grants and authority events.

Revision ID: 0030_storage_spaces
Revises: 0029_shared_publications

Frozen DDL: never import the live resource model into historical migrations.
"""

from __future__ import annotations

import uuid

import sqlalchemy as sa
from alembic import op

revision = "0030_storage_spaces"
down_revision = "0029_shared_publications"
branch_labels = None
depends_on = None

_metadata = sa.MetaData()
_spaces = sa.Table(
    "storage_spaces",
    _metadata,
    sa.Column("id", sa.String(32), primary_key=True),
    sa.Column("backing_handle", sa.String(32), nullable=False),
    sa.Column("name", sa.String(128), nullable=False),
    sa.Column("custody_kind", sa.String(16), nullable=False),
    sa.Column("custodian_kind", sa.String(16), nullable=True),
    sa.Column("custodian_id", sa.String(128), nullable=True),
    sa.Column("mode", sa.String(16), nullable=False),
    sa.Column("status", sa.String(16), nullable=False),
    sa.Column("generation", sa.Integer(), nullable=False),
    sa.Column("feature_namespace", sa.String(128), nullable=True),
    sa.Column("feature_controller", sa.String(128), nullable=True),
    sa.Column("feature_metadata_version", sa.Integer(), nullable=True),
    sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
    sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
    sa.UniqueConstraint("backing_handle", name="uq_storage_spaces_backing_handle"),
    sa.CheckConstraint("mode IN ('native', 'mediated')", name="ck_storage_spaces_mode"),
    sa.CheckConstraint("status IN ('active', 'archived', 'deleted')", name="ck_storage_spaces_status"),
    sa.CheckConstraint("generation > 0", name="ck_storage_spaces_generation"),
    sa.CheckConstraint(
        "(custody_kind = 'company' AND custodian_kind IS NULL AND custodian_id IS NULL) OR (custody_kind = 'personal' AND custodian_kind IS NOT NULL AND custodian_kind IN ('human', 'nonhuman') AND custodian_id IS NOT NULL)",
        name="ck_storage_spaces_custody",
    ),
    sa.CheckConstraint(
        "(feature_namespace IS NULL AND feature_controller IS NULL AND feature_metadata_version IS NULL AND mode = 'native') OR "
        "(feature_namespace IS NOT NULL AND feature_controller IS NOT NULL AND feature_metadata_version IS NOT NULL AND feature_metadata_version > 0)",
        name="ck_storage_spaces_feature",
    ),
)
_grants = sa.Table(
    "storage_space_grants",
    _metadata,
    sa.Column("space_id", sa.String(32), sa.ForeignKey("storage_spaces.id"), primary_key=True),
    sa.Column("principal_kind", sa.String(16), primary_key=True),
    sa.Column("subject_id", sa.String(128), primary_key=True),
    sa.Column("permissions", sa.Integer(), nullable=False),
    sa.CheckConstraint("principal_kind IN ('human', 'nonhuman')", name="ck_storage_space_grants_kind"),
    sa.CheckConstraint(
        "permissions > 0 AND permissions <= 31",
        name="ck_storage_space_grants_permissions",
    ),
)
_events = sa.Table(
    "storage_space_events",
    _metadata,
    sa.Column("space_id", sa.String(32), sa.ForeignKey("storage_spaces.id"), primary_key=True),
    sa.Column("generation", sa.Integer(), primary_key=True),
    sa.Column("action", sa.String(32), nullable=False),
    sa.Column("actor_kind", sa.String(16), nullable=False),
    sa.Column("actor_id", sa.String(128), nullable=False),
    sa.Column("details", sa.JSON(), nullable=False),
    sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
    sa.CheckConstraint("actor_kind IN ('human', 'nonhuman')", name="ck_storage_space_events_actor"),
    sa.CheckConstraint("generation > 0", name="ck_storage_space_events_generation"),
)
_tables = (_spaces, _grants, _events)


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
            "storage_shape_" + uuid.uuid4().hex,
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
        raise RuntimeError(f"Incompatible storage schema for {table.name}; preserve data and reconcile its shape before upgrading")


def upgrade() -> None:
    bind = op.get_bind()
    inspector = sa.inspect(bind)
    existing = set(inspector.get_table_names())
    # Validate every pre-existing table before changing anything. A clean
    # partial DDL attempt can finish; malformed tables never get overwritten.
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
            raise RuntimeError("Refusing to erase storage resource identity, custody, grants or authority events after first use")
    for table in reversed(_tables):
        if table.name in existing:
            table.drop(bind)
