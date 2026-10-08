"""Frozen additive native containment and quiesced backup facts.

Revision ID: 0032_storage_lifecycle
Revises: 0031_storage_files
"""

from __future__ import annotations

import uuid

import sqlalchemy as sa
from alembic import op

revision = "0032_storage_lifecycle"

down_revision = "0031_storage_files"

branch_labels = None

depends_on = None

_metadata = sa.MetaData()

sa.Table("storage_spaces", _metadata, sa.Column("id", sa.String(32), primary_key=True))

_storage_space_attachments = sa.Table(
    "storage_space_attachments",
    _metadata,
    sa.Column("id", sa.String(length=32), primary_key=True, nullable=False),
    sa.Column("incarnation", sa.String(length=32), nullable=False),
    sa.Column("actor_kind", sa.String(length=16), nullable=False),
    sa.Column("actor_id", sa.String(length=128), nullable=False),
    sa.Column("host_id", sa.String(length=128), nullable=False),
    sa.Column("container_id", sa.String(length=64), nullable=True),
    sa.Column("phase", sa.String(length=16), nullable=False),
    sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
    sa.CheckConstraint("actor_kind IN ('human', 'nonhuman')", name="ck_storage_space_attachments_actor"),
    sa.CheckConstraint("phase IN ('pending', 'active', 'fence_pending', 'fenced')", name="ck_storage_space_attachments_phase"),
    sa.UniqueConstraint("incarnation", name="uq_storage_space_attachments_incarnation"),
)

_storage_space_mounts = sa.Table(
    "storage_space_mounts",
    _metadata,
    sa.Column("attachment_id", sa.String(length=32), sa.ForeignKey("storage_space_attachments.id"), primary_key=True, nullable=False),
    sa.Column("space_id", sa.String(length=32), sa.ForeignKey("storage_spaces.id"), primary_key=True, nullable=False),
    sa.Column("generation", sa.Integer(), nullable=False),
    sa.Column("alias", sa.String(length=64), nullable=False),
    sa.Column("writable", sa.Integer(), nullable=False),
    sa.CheckConstraint("generation > 0", name="ck_storage_space_mounts_generation"),
    sa.CheckConstraint("writable IN (0, 1)", name="ck_storage_space_mounts_writable"),
    sa.UniqueConstraint("attachment_id", "alias", name="uq_storage_space_mounts_alias"),
)

_storage_space_backups = sa.Table(
    "storage_space_backups",
    _metadata,
    sa.Column("space_id", sa.String(length=32), sa.ForeignKey("storage_spaces.id"), primary_key=True, nullable=False),
    sa.Column("id", sa.String(length=32), primary_key=True, nullable=False),
    sa.Column("generation", sa.Integer(), nullable=False),
    sa.Column("size_bytes", sa.BigInteger(), nullable=False),
    sa.Column("sha256", sa.String(length=64), nullable=False),
    sa.Column("consistency", sa.String(length=32), nullable=False),
    sa.Column("actor_kind", sa.String(length=16), nullable=False),
    sa.Column("actor_id", sa.String(length=128), nullable=False),
    sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
    sa.CheckConstraint("actor_kind IN ('human', 'nonhuman')", name="ck_storage_space_backups_actor"),
    sa.CheckConstraint("consistency = 'quiesced-filesystem'", name="ck_storage_space_backups_consistency"),
    sa.CheckConstraint("generation > 0 AND size_bytes > 0", name="ck_storage_space_backups_size"),
)

_tables = (_storage_space_attachments, _storage_space_mounts, _storage_space_backups)


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
            raise RuntimeError("Refusing to erase a used storage containment or backup; preserve resource recovery facts")
    for table in reversed(_tables):
        if table.name in existing:
            table.drop(bind)
