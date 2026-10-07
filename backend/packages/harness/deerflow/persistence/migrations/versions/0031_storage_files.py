"""Durable qualified backing bindings and host file-operation intents.

Revision ID: 0031_storage_files
Revises: 0030_storage_spaces

Frozen definitions; deliberately independent of current ORM models.
"""

from __future__ import annotations

import uuid

import sqlalchemy as sa
from alembic import op

revision = "0031_storage_files"
down_revision = "0030_storage_spaces"
branch_labels = None
depends_on = None

_metadata = sa.MetaData()
# Referenced identity belongs to the immutable parent revision.
sa.Table("storage_spaces", _metadata, sa.Column("id", sa.String(32), primary_key=True))
_backings = sa.Table(
    "storage_space_backings",
    _metadata,
    sa.Column("backing_handle", sa.String(32), primary_key=True),
    sa.Column("space_id", sa.String(32), sa.ForeignKey("storage_spaces.id"), nullable=False),
    sa.Column("slot_id", sa.String(32), nullable=False),
    sa.Column("filesystem_uuid", sa.String(36), nullable=False),
    sa.Column("max_bytes", sa.BigInteger(), nullable=False),
    sa.Column("max_inodes", sa.BigInteger(), nullable=False),
    sa.Column("root_inode", sa.BigInteger(), nullable=False),
    sa.Column("control_inode", sa.BigInteger(), nullable=False),
    sa.UniqueConstraint("space_id", name="uq_storage_space_backings_space"),
    sa.UniqueConstraint("slot_id", name="uq_storage_space_backings_slot"),
    sa.UniqueConstraint("filesystem_uuid", name="uq_storage_space_backings_filesystem"),
    sa.CheckConstraint("max_bytes > 0 AND max_inodes > 0", name="ck_storage_space_backings_capacity"),
    sa.CheckConstraint("root_inode > 0 AND control_inode > 0", name="ck_storage_space_backings_incarnation"),
)
_operations = sa.Table(
    "storage_space_file_operations",
    _metadata,
    sa.Column("space_id", sa.String(32), sa.ForeignKey("storage_spaces.id"), primary_key=True),
    sa.Column("operation_id", sa.String(32), primary_key=True),
    sa.Column("actor_kind", sa.String(16), nullable=False),
    sa.Column("actor_id", sa.String(128), nullable=False),
    sa.Column("generation", sa.Integer(), nullable=False),
    sa.Column("phase", sa.String(16), nullable=False),
    sa.Column("request", sa.JSON(), nullable=False),
    sa.Column("result", sa.JSON(), nullable=True),
    sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
    sa.CheckConstraint("actor_kind IN ('human', 'nonhuman')", name="ck_storage_space_file_operations_actor"),
    sa.CheckConstraint("generation > 0", name="ck_storage_space_file_operations_generation"),
    sa.CheckConstraint("phase IN ('pending', 'complete', 'failed')", name="ck_storage_space_file_operations_phase"),
)
_tables = (_backings, _operations)


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
            raise RuntimeError("Refusing to erase a used storage binding or file operation; preserve resource recovery facts")
    for table in reversed(_tables):
        if table.name in existing:
            table.drop(bind)
