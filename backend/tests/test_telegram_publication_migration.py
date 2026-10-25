"""Only assistant-message identity storage widens; no proof is manufactured here."""

import importlib

import pytest
import sqlalchemy as sa
from alembic.migration import MigrationContext
from alembic.operations import Operations
from alembic.script import ScriptDirectory
from test_telegram_inbox_chat_migration import apply as apply_chat_scope
from test_telegram_inbox_chat_migration import connection as connection
from test_telegram_inbox_chat_migration import database as database
from test_telegram_inbox_chat_migration import historical, rows, seed, shape

from deerflow.persistence.bootstrap import _MIGRATIONS_DIR

REVISION = "0044_telegram_assistant_ids"
PREVIOUS = "0043_telegram_inbox_chat_scope"
TABLE = "telegram_inbound_marks"


def migration():
    return importlib.import_module("deerflow.persistence.migrations.versions." + REVISION)


def apply(connection, direction="upgrade"):
    with Operations.context(MigrationContext.configure(connection)):
        getattr(migration(), direction)()


def test_migration_is_appended_and_changes_only_assistant_representation():
    from deerflow.persistence.telegram_bots.model import TelegramInboundMarkRow

    module = migration()
    script = ScriptDirectory(str(_MIGRATIONS_DIR))
    assert script.get_heads() == [REVISION]
    assert module.down_revision == PREVIOUS
    column = TelegramInboundMarkRow.__table__.c.assistant_message_id
    assert isinstance(column.type, sa.Text) and column.nullable is True
    assert not any("assistant_message_id" in index.columns for index in TelegramInboundMarkRow.__table__.indexes)
    assert tuple(TelegramInboundMarkRow.__table__.primary_key.columns.keys()) == ("link_id", "message_id", "chat_id")


@pytest.mark.asyncio(loop_scope="module")
async def test_upgrade_preserves_rows_and_all_other_schema_facts(connection):
    await seed(connection)
    await connection.run_sync(apply_chat_scope)
    before, retained = await connection.run_sync(shape), await rows(connection)
    await connection.run_sync(apply)
    after = await connection.run_sync(shape)
    expected = before[TABLE]["columns"]
    before[TABLE]["columns"] = [(name, "TEXT" if name == "assistant_message_id" else type_, nullable, default) for name, type_, nullable, default in expected]
    assert after == before and await rows(connection) == retained
    await connection.run_sync(apply)
    assert await connection.run_sync(shape) == after and await rows(connection) == retained


@pytest.mark.asyncio(loop_scope="module")
async def test_exact_long_opaque_id_roundtrips_and_downgrade_refuses_before_ddl(connection):
    await seed(connection)
    await connection.run_sync(apply_chat_scope)
    await connection.run_sync(apply)
    identity = "opaque-model-identity:" + "汉🙂/raw " * 20
    table = historical()._metadata.tables[TABLE]
    await connection.execute(sa.update(table).values(assistant_message_id=identity))
    retained = await rows(connection)
    assert all(row["assistant_message_id"] == identity for row in retained)
    before = await connection.run_sync(shape)
    ddl = []

    def record_ddl(conn, cursor, statement, parameters, context, executemany):
        if statement.lstrip().upper().startswith(("CREATE ", "ALTER ", "DROP ")):
            ddl.append(statement)

    sa.event.listen(connection.sync_connection, "before_cursor_execute", record_ddl)
    try:
        with pytest.raises(RuntimeError, match="Refusing to truncate retained Telegram assistant identities"):
            await connection.run_sync(apply, "downgrade")
    finally:
        sa.event.remove(connection.sync_connection, "before_cursor_execute", record_ddl)
    assert ddl == []
    assert await connection.run_sync(shape) == before
    assert await rows(connection) == retained


@pytest.mark.asyncio(loop_scope="module")
async def test_short_ids_downgrade_and_upgrade_without_data_loss(connection):
    await seed(connection)
    await connection.run_sync(apply_chat_scope)
    original = await rows(connection)
    old_shape = await connection.run_sync(shape)
    await connection.run_sync(apply)
    await connection.run_sync(apply, "downgrade")
    assert await connection.run_sync(shape) == old_shape
    assert await rows(connection) == original
    await connection.run_sync(apply)
    assert await rows(connection) == original


@pytest.mark.asyncio(loop_scope="module")
async def test_latest_fresh_orm_matches_actual_0044_schema(connection):
    from deerflow.persistence.base import Base
    from deerflow.persistence.telegram_bots.model import TelegramInboundMarkRow

    await connection.run_sync(apply_chat_scope)
    await connection.run_sync(apply)
    upgraded = await connection.run_sync(shape)
    await connection.run_sync(historical()._metadata.tables[TABLE].drop)
    await connection.run_sync(TelegramInboundMarkRow.__table__.create)
    assert await connection.run_sync(shape) == upgraded
    assert Base.metadata.tables[TABLE].c.assistant_message_id.nullable is True
    await connection.run_sync(apply)
    assert await connection.run_sync(shape) == upgraded
