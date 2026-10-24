"""Retain independent chat-local inbox identities. Frozen primary-key DDL."""

import importlib

import sqlalchemy as sa
from alembic import op

revision = "0043_telegram_inbox_chat_scope"
down_revision = "0042_telegram_bots"
branch_labels = None
depends_on = None

_NAME = "telegram_inbound_marks"
_OLD_KEY = ("link_id", "message_id")
_NEW_KEY = (*_OLD_KEY, "chat_id")
_COLLAPSE_ERROR = "Refusing to collapse retained Telegram inbox identities across chats"


def _inbox(*, chat_scope):
    # These two frozen definitions differ only in the existing chat_id's PK
    # membership. Never import live ORM metadata into an applied migration.
    metadata = sa.MetaData()
    sa.Table("telegram_bot_links", metadata, sa.Column("id", sa.String(32), primary_key=True))
    table = sa.Table(
        _NAME,
        metadata,
        sa.Column("link_id", sa.String(32), sa.ForeignKey("telegram_bot_links.id", ondelete="CASCADE"), primary_key=True, nullable=False),
        sa.Column("message_id", sa.BigInteger(), primary_key=True, nullable=False),
        sa.Column("update_id", sa.BigInteger(), nullable=False),
        sa.Column("chat_id", sa.BigInteger(), primary_key=chat_scope, nullable=False),
        sa.Column("message_thread_id", sa.BigInteger(), nullable=True),
        sa.Column("binding_generation", sa.Integer(), nullable=False),
        sa.Column("accepted_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("route_kind", sa.String(24), nullable=True),
        sa.Column("route_target_id", sa.String(64), nullable=True),
        sa.Column("route_basis", sa.JSON(), nullable=False),
        sa.Column("sanitized_input", sa.JSON(), nullable=False),
        sa.Column("turn_id", sa.String(32), nullable=True),
        sa.Column("grouped_message_ids", sa.JSON(), nullable=False),
        sa.Column("thread_id", sa.String(64), nullable=True),
        sa.Column("run_id", sa.String(64), nullable=True),
        sa.Column("assistant_message_id", sa.String(64), nullable=True),
        sa.Column("operation_ids", sa.JSON(), nullable=False),
        sa.Column("outbox_id", sa.String(32), nullable=True),
        sa.Column("delivery_ids", sa.JSON(), nullable=False),
        sa.Column("route_complete", sa.Boolean(), nullable=False),
        sa.Column("state", sa.String(16), nullable=False),
        sa.Column("closed_at", sa.DateTime(timezone=True), nullable=True),
        sa.CheckConstraint("state IN ('accepted', 'dispatched', 'reply_pending', 'done', 'interrupted')", name="ck_telegram_inbound_marks_state"),
        sa.UniqueConstraint("link_id", "update_id", name="uq_telegram_inbound_marks_update"),
    )
    sa.Index("ix_telegram_inbound_marks_closed", table.c.closed_at)
    return table


_before = _inbox(chat_scope=False)
_after = _inbox(chat_scope=True)


def _incompatible():
    return RuntimeError("Incompatible Telegram inbox schema; preserve data and reconcile its shape before upgrading")


def _validate(bind, expected):
    inspector = sa.inspect(bind)
    # The frozen validator compares ordered columns, types, nullability,
    # defaults, keys, named checks and FK behavior (including PG check casts).
    frozen = importlib.import_module("deerflow.persistence.migrations.versions.0035_agent_conversations")
    frozen._validate_existing(inspector, expected)
    postgres = bind.dialect.name == "postgresql"
    primary_name = _NAME + "_pkey" if postgres else None
    foreign_name = _NAME + "_link_id_fkey" if postgres else None
    if inspector.get_pk_constraint(_NAME)["name"] != primary_name:
        raise _incompatible()
    if [f["name"] for f in inspector.get_foreign_keys(_NAME)] != [foreign_name]:
        raise _incompatible()
    indexes = [i for i in inspector.get_indexes(_NAME) if not i.get("duplicates_constraint")]
    if len(indexes) != 1:
        raise _incompatible()
    index = indexes[0]
    options = {"postgresql_include": []} if postgres else {}
    if index["name"] != "ix_telegram_inbound_marks_closed" or index["column_names"] != ["closed_at"] or index["unique"] or index.get("dialect_options", {}) != options or index.get("include_columns", []):
        raise _incompatible()


def _change(*, chat_scope):
    bind = op.get_bind()
    if bind.dialect.name not in {"sqlite", "postgresql"}:
        raise RuntimeError("Telegram inbox migration supports only SQLite and PostgreSQL")
    if _NAME not in sa.inspect(bind).get_table_names():
        raise _incompatible()
    # Savepoints make SQLite's table-copy DDL atomic even with legacy driver
    # transaction control. They also leave a caller's existing transaction owned
    # by that caller. Locks remain held until its transaction completes.
    with bind.begin_nested():
        if bind.dialect.name == "postgresql":
            bind.exec_driver_sql("LOCK TABLE telegram_inbound_marks IN ACCESS EXCLUSIVE MODE")
        else:
            # Reserve SQLite's writer before the duplicate preflight. A SELECT
            # alone would allow another connection to insert before the rebuild.
            bind.exec_driver_sql("UPDATE telegram_inbound_marks SET binding_generation = binding_generation WHERE 0")
        key = tuple(sa.inspect(bind).get_pk_constraint(_NAME)["constrained_columns"])
        if key not in {_OLD_KEY, _NEW_KEY}:
            raise _incompatible()
        if not chat_scope and key == _NEW_KEY:
            duplicate = sa.select(_after.c.link_id, _after.c.message_id).group_by(_after.c.link_id, _after.c.message_id).having(sa.func.count() > 1).limit(1)
            if bind.execute(duplicate).first() is not None:
                # Check before validation's PG temporary canonicalization DDL,
                # as well as before any alteration of the retained inbox.
                raise RuntimeError(_COLLAPSE_ERROR)
        current = _after if key == _NEW_KEY else _before
        target = _after if chat_scope else _before
        _validate(bind, current)
        if current is target:
            return
        if bind.dialect.name == "sqlite":
            # All columns are identical and validated above. Supplying frozen
            # target metadata copies every field and keeps SQLite's unnamed PK
            # and FK, without disabling foreign keys on this child table.
            with op.batch_alter_table(_NAME, copy_from=target, recreate="always"):
                pass
        else:
            name = sa.inspect(bind).get_pk_constraint(_NAME)["name"]
            op.drop_constraint(name, _NAME, type_="primary")
            op.create_primary_key(name, _NAME, list(target.primary_key.columns.keys()))
        _validate(bind, target)


def upgrade():
    _change(chat_scope=True)


def downgrade():
    _change(chat_scope=False)
