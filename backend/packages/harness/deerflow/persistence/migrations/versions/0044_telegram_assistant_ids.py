"""Preserve exact opaque assistant identities. Frozen additive type change."""

import importlib

import sqlalchemy as sa
from alembic import op

revision = "0044_telegram_assistant_ids"
down_revision = "0043_telegram_inbox_chat_scope"
branch_labels = None
depends_on = None

_NAME = "telegram_inbound_marks"
_frozen = importlib.import_module("deerflow.persistence.migrations.versions.0043_telegram_inbox_chat_scope")
# Both definitions come from the immutable historical factory, never the ORM.
# Their only difference is this nullable, nonindexed scalar's representation.
_before = _frozen._inbox(chat_scope=True)
_after = _frozen._inbox(chat_scope=True)
_after.c.assistant_message_id.type = sa.Text()
_TRUNCATION = "Refusing to truncate retained Telegram assistant identities"


def _change(*, text_ids):
    bind = op.get_bind()
    if bind.dialect.name not in {"sqlite", "postgresql"}:
        raise RuntimeError("Telegram assistant identity migration supports only SQLite and PostgreSQL")
    if _NAME not in sa.inspect(bind).get_table_names():
        raise _frozen._incompatible()
    with bind.begin_nested():
        if bind.dialect.name == "postgresql":
            bind.exec_driver_sql("LOCK TABLE telegram_inbound_marks IN ACCESS EXCLUSIVE MODE")
        else:
            bind.exec_driver_sql("UPDATE telegram_inbound_marks SET binding_generation = binding_generation WHERE 0")
        if not text_ids:
            # This must precede validation's temporary canonicalization DDL and
            # every physical alteration. No downgrade may truncate a saved ID.
            too_long = sa.select(sa.literal(1)).select_from(_after).where(sa.func.length(_after.c.assistant_message_id) > 64).limit(1)
            if bind.execute(too_long).first() is not None:
                raise RuntimeError(_TRUNCATION)
        columns = {column["name"]: column for column in sa.inspect(bind).get_columns(_NAME)}
        actual = columns.get("assistant_message_id", {}).get("type")
        if isinstance(actual, sa.Text):
            current = _after
        elif isinstance(actual, sa.String) and actual.length == 64:
            current = _before
        else:
            raise _frozen._incompatible()
        target = _after if text_ids else _before
        _frozen._validate(bind, current)
        if current is target:
            return
        if bind.dialect.name == "sqlite":
            # Rebuild only this child with frozen target metadata. Foreign keys
            # stay enabled and all retained columns/keys/indexes are preserved.
            with op.batch_alter_table(_NAME, copy_from=target, recreate="always"):
                pass
        else:
            op.alter_column(_NAME, "assistant_message_id", existing_type=current.c.assistant_message_id.type, type_=target.c.assistant_message_id.type, existing_nullable=True)
        _frozen._validate(bind, target)


def upgrade():
    _change(text_ids=True)


def downgrade():
    _change(text_ids=False)
