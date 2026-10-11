"""Docker host replacements, released seats, containment attribution and host-settled Work attempts."""

import importlib

import sqlalchemy as sa
from alembic import op

from deerflow.persistence.migrations._helpers import (
    _normalize_default,
    safe_add_column,
    safe_drop_column,
)

revision = "0041_storage_host_replacements"
down_revision = "0040_work_execution"
branch_labels = None
depends_on = None

ACTOR_CHECK = "ck_agent_work_event_actor"
ACTORS_BEFORE = "actor_kind IN ('human', 'nonhuman')"
ACTORS_AFTER = "actor_kind IN ('human', 'nonhuman', 'host')"
USER_COLUMNS = ["released_at"]
ATTACHMENT_COLUMNS = ["fenced_by_kind", "fenced_by_id", "fence_reason", "fenced_at"]
ATTEMPT_COLUMNS = ["contained_reason", "workspace_ready_at", "attachment_id"]

_replacements = sa.Table(
    "storage_host_replacements",
    sa.MetaData(),
    sa.Column("old_host_id", sa.String(length=128), primary_key=True, nullable=False),
    sa.Column("new_host_id", sa.String(length=128), primary_key=True, nullable=False),
    sa.Column("attachments_named", sa.Integer(), nullable=False),
    sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
    sa.CheckConstraint("old_host_id <> new_host_id", name="ck_storage_host_replacements_distinct"),
)


def columns():
    return [
        ("users", sa.Column("released_at", sa.DateTime(timezone=True), nullable=True)),
        (
            "storage_space_attachments",
            sa.Column("fenced_by_kind", sa.VARCHAR(16), nullable=True),
        ),
        (
            "storage_space_attachments",
            sa.Column("fenced_by_id", sa.VARCHAR(128), nullable=True),
        ),
        (
            "storage_space_attachments",
            sa.Column("fence_reason", sa.VARCHAR(32), nullable=True),
        ),
        (
            "storage_space_attachments",
            sa.Column("fenced_at", sa.DateTime(timezone=True), nullable=True),
        ),
        (
            "agent_work_attempts",
            sa.Column("contained_reason", sa.VARCHAR(32), nullable=True),
        ),
        (
            "agent_work_attempts",
            sa.Column("workspace_ready_at", sa.DateTime(timezone=True), nullable=True),
        ),
        (
            "agent_work_attempts",
            sa.Column("attachment_id", sa.VARCHAR(32), nullable=True),
        ),
    ]


def _events(actors: str) -> sa.Table:
    """agent_work_events exactly as 0038 froze it, with the given actor predicate."""
    metadata = sa.MetaData()
    sa.Table("agent_instances", metadata, sa.Column("id", sa.String(32), primary_key=True))
    sa.Table("agent_work", metadata, sa.Column("id", sa.String(32), primary_key=True))
    return sa.Table(
        "agent_work_events",
        metadata,
        sa.Column("id", sa.String(length=32), nullable=False, primary_key=True),
        sa.Column(
            "instance_id",
            sa.String(length=32),
            sa.ForeignKey("agent_instances.id"),
            nullable=False,
        ),
        sa.Column(
            "work_id",
            sa.String(length=32),
            sa.ForeignKey("agent_work.id"),
            nullable=False,
        ),
        sa.Column("actor_id", sa.String(length=128), nullable=False),
        sa.Column("actor_kind", sa.String(length=16), nullable=False),
        sa.Column("operation_id", sa.String(length=32), nullable=False),
        sa.Column("action", sa.String(length=32), nullable=False),
        sa.Column("revision", sa.Integer(), nullable=False),
        sa.Column("assignment_revision", sa.Integer(), nullable=False),
        sa.Column("request", sa.JSON(), nullable=False),
        sa.Column("result", sa.JSON(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.CheckConstraint(actors, name=ACTOR_CHECK),
        sa.CheckConstraint(
            "revision >= assignment_revision AND assignment_revision >= 1 AND revision <= 2147483647",
            name="ck_agent_work_event_revision",
        ),
        sa.UniqueConstraint("instance_id", "operation_id", name="uq_agent_work_event_operation"),
        sa.UniqueConstraint("work_id", "revision", name="uq_agent_work_event_revision"),
    )


def _admits_host(bind) -> bool:
    checks = {c["name"]: c["sqltext"] for c in sa.inspect(bind).get_check_constraints("agent_work_events")}
    return "'host'" in checks.get(ACTOR_CHECK, "")


def _replace_actor_check(before: str, after: str) -> None:
    # PostgreSQL: DROP CONSTRAINT and ADD CONSTRAINT. SQLite: one rebuild of a
    # table that nothing references.
    with op.batch_alter_table("agent_work_events", copy_from=_events(before)) as batch:
        batch.drop_constraint(ACTOR_CHECK, type_="check")
        batch.create_check_constraint(ACTOR_CHECK, after)


def upgrade():
    bind = op.get_bind()
    inspector = sa.inspect(bind)
    # Fail before partial DDL when a manually installed column has an incompatible shape.
    for table, column in columns():
        actual = next((c for c in inspector.get_columns(table) if c["name"] == column.name), None)
        if actual and (
            actual["nullable"] != column.nullable or actual["type"].compile(dialect=bind.dialect) != column.type.compile(dialect=bind.dialect) or _normalize_default(actual.get("default")) != _normalize_default(column.server_default)
        ):
            raise RuntimeError(f"Incompatible column: {table}.{column.name}")
    validate = importlib.import_module("deerflow.persistence.migrations.versions.0035_agent_conversations")._validate_existing
    admits_host = _admits_host(bind)
    validate(inspector, _events(ACTORS_AFTER if admits_host else ACTORS_BEFORE))
    exists = _replacements.name in inspector.get_table_names()
    if exists:
        validate(inspector, _replacements)
        if any(not index.get("duplicates_constraint") for index in inspector.get_indexes(_replacements.name)):
            raise RuntimeError("Incompatible indexes on storage_host_replacements")
    # Validate every existing shape before the first DDL, including retry state.
    if not exists:
        _replacements.create(bind)
    for table, column in columns():
        safe_add_column(table, column)
    if not _admits_host(bind):
        _replace_actor_check(ACTORS_BEFORE, ACTORS_AFTER)


def downgrade():
    bind = op.get_bind()
    inspector = sa.inspect(bind)
    tables = set(inspector.get_table_names())

    def used(sql: str) -> bool:
        return bind.execute(sa.text(sql)).first() is not None

    def present(table: str, names: list[str]) -> list[str]:
        have = {c["name"] for c in inspector.get_columns(table)}
        return [name for name in names if name in have]

    def any_set(table: str, names: list[str]) -> bool:
        return bool(names) and used(f"SELECT 1 FROM {table} WHERE " + " OR ".join(f"{name} IS NOT NULL" for name in names) + " LIMIT 1")

    attachment_columns = present("storage_space_attachments", ATTACHMENT_COLUMNS)
    if _replacements.name in tables and used("SELECT 1 FROM storage_host_replacements LIMIT 1"):
        raise RuntimeError("Refusing to erase a recorded Docker host replacement")
    if any_set("users", present("users", USER_COLUMNS)):
        raise RuntimeError("Refusing to erase a released seat")
    if any_set("storage_space_attachments", attachment_columns):
        raise RuntimeError("Refusing to erase containment attribution")
    if any_set("agent_work_attempts", present("agent_work_attempts", ATTEMPT_COLUMNS)):
        raise RuntimeError("Refusing to erase host-settled Work attempt facts")
    if used("SELECT 1 FROM agent_work_events WHERE actor_kind = 'host' LIMIT 1"):
        raise RuntimeError("Refusing to erase host-settled Work history")
    if bind.dialect.name == "sqlite" and attachment_columns and used("SELECT 1 FROM storage_space_mounts LIMIT 1"):
        raise RuntimeError("Refusing to rebuild storage_space_attachments on SQLite while mounts reference it")
    if bind.dialect.name == "sqlite" and present("users", USER_COLUMNS):
        version = tuple(int(part) for part in bind.scalar(sa.text("SELECT sqlite_version()")).split("."))
        if version < (3, 35, 0):
            raise RuntimeError("SQLite 3.35 or later is required to preserve user references during downgrade")
    if _admits_host(bind):
        _replace_actor_check(ACTORS_AFTER, ACTORS_BEFORE)
    for table, column in reversed(columns()):
        if table == "users" and bind.dialect.name == "sqlite":
            # Batch rebuilding this parent cascades into user_preferences.
            # This nullable column has no constraints: native DROP preserves rows.
            if present(table, [column.name]):
                op.drop_column(table, column.name)
        else:
            safe_drop_column(table, column.name)
    if _replacements.name in tables:
        _replacements.drop(bind)
