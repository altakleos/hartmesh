"""Preserve retained inbox identities while widening only their primary key."""

from __future__ import annotations

import asyncio
import importlib
import os
from datetime import UTC, datetime
from uuid import uuid4

import pytest
import pytest_asyncio
import sqlalchemy as sa
from alembic import command
from alembic.migration import MigrationContext
from alembic.operations import Operations
from alembic.script import ScriptDirectory
from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.schema import CreateSchema, DropSchema

from deerflow.persistence.bootstrap import _MIGRATIONS_DIR, _get_alembic_config

PREVIOUS = "0042_telegram_bots"
REVISION = "0043_telegram_inbox_chat_scope"
TABLE = "telegram_inbound_marks"
OLD_KEY = ["link_id", "message_id"]
NEW_KEY = ["link_id", "message_id", "chat_id"]
COLLAPSE_ERROR = "Refusing to collapse retained Telegram inbox identities across chats"


def historical():
    return importlib.import_module(f"deerflow.persistence.migrations.versions.{PREVIOUS}")


def migration():
    return importlib.import_module(f"deerflow.persistence.migrations.versions.{REVISION}")


def apply(connection, direction="upgrade"):
    with Operations.context(MigrationContext.configure(connection)):
        getattr(migration(), direction)()


def apply_historical(connection):
    with Operations.context(MigrationContext.configure(connection)):
        historical().upgrade()


def shape(connection, names=(TABLE,)):
    inspector = sa.inspect(connection)
    return {
        name: {
            "columns": [(c["name"], str(c["type"]), c["nullable"], c["default"]) for c in inspector.get_columns(name)],
            "primary": inspector.get_pk_constraint(name),
            "checks": sorted((c["name"], c["sqltext"]) for c in inspector.get_check_constraints(name)),
            "unique": sorted((c["name"], c["column_names"]) for c in inspector.get_unique_constraints(name)),
            "foreign": sorted((f.get("name"), f["constrained_columns"], f.get("referred_schema"), f["referred_table"], f["referred_columns"], f["options"]) for f in inspector.get_foreign_keys(name)),
            "indexes": sorted((i["name"], i["column_names"], i["unique"], sorted((key, str(value)) for key, value in i.get("dialect_options", {}).items())) for i in inspector.get_indexes(name)),
        }
        for name in names
    }


@pytest_asyncio.fixture(scope="module", loop_scope="module", params=["sqlite", "postgres"])
async def database(tmp_path_factory, request):
    admin = None
    schema = None
    if request.param == "postgres":
        uri = os.environ.get("TEST_POSTGRES_URI")
        if not uri:
            pytest.skip("TEST_POSTGRES_URI is not configured")
        url = make_url(uri).set(drivername="postgresql+asyncpg")
        ssl = {"ssl": False} if url.query.get("sslmode") == "disable" else {}
        url = url.difference_update_query(["sslmode"])
        admin = create_async_engine(url, connect_args=ssl)
        schema = "telegram_inbox_" + uuid4().hex
        async with admin.begin() as connection:
            await connection.execute(CreateSchema(schema))
        engine = create_async_engine(url, connect_args={**ssl, "server_settings": {"search_path": schema}})
    else:
        directory = tmp_path_factory.mktemp("telegram-inbox")
        engine = create_async_engine(f"sqlite+aiosqlite:///{directory / 'migration.db'}")

        @sa.event.listens_for(engine.sync_engine, "connect")
        def foreign_keys(connection, _):
            connection.execute("PRAGMA foreign_keys=ON")

    try:
        # The immutable0042 metadata is the exact historical input. No current
        # ORM is used to manufacture the schema the new migration must repair.
        async with engine.begin() as connection:
            if engine.dialect.name == "sqlite":
                await connection.exec_driver_sql("BEGIN")
            await connection.run_sync(historical()._metadata.create_all)
        config = _get_alembic_config(engine, postgres_schema=schema)
        await asyncio.to_thread(command.stamp, config, PREVIOUS)
        yield engine, config
    finally:
        await engine.dispose()
        if admin is not None:
            try:
                async with admin.begin() as connection:
                    await connection.execute(DropSchema(schema, cascade=True, if_exists=True))
            finally:
                await admin.dispose()


@pytest_asyncio.fixture(loop_scope="module")
async def connection(database):
    engine, _ = database
    async with engine.connect() as connection:
        transaction = await connection.begin()
        if engine.dialect.name == "sqlite":
            await connection.exec_driver_sql("BEGIN")
        try:
            yield connection
        finally:
            await transaction.rollback()


def link_values():
    values = {}
    table = historical()._metadata.tables["telegram_bot_links"]
    for column in table.columns:
        if column.nullable:
            continue
        if isinstance(column.type, sa.Boolean):
            values[column.name] = False
        elif isinstance(column.type, sa.Integer):
            values[column.name] = 1
        elif isinstance(column.type, sa.DateTime):
            values[column.name] = datetime.now(UTC)
        elif isinstance(column.type, sa.JSON):
            values[column.name] = {}
        else:
            values[column.name] = "synthetic"
    values.update(id="link", agent_kind="lead_agent", origin="pasted", bind_mode="code", state="waiting_connect", health="ok")
    return values


def inbound_values(message=101, update=201, chat=7001):
    now = datetime.now(UTC)
    return dict(
        link_id="link",
        message_id=message,
        update_id=update,
        chat_id=chat,
        message_thread_id=17,
        binding_generation=3,
        accepted_at=now,
        route_kind="start",
        route_target_id="target",
        route_basis={"binding_generation": 3},
        sanitized_input={"text": "Olá"},
        turn_id="turn",
        grouped_message_ids=[message],
        thread_id="thread",
        run_id="run",
        assistant_message_id="assistant",
        operation_ids=["operation"],
        outbox_id="outbox",
        delivery_ids=["delivery"],
        route_complete=True,
        state="done",
        closed_at=now,
    )


async def seed(connection):
    await connection.execute(historical()._metadata.tables["telegram_bot_links"].insert().values(**link_values()))
    table = historical()._metadata.tables[TABLE]
    first, second = inbound_values(), inbound_values(message=102, update=202)
    second.update(state="reply_pending", closed_at=None)
    await connection.execute(table.insert(), [first, second])


async def rows(connection):
    table = historical()._metadata.tables[TABLE]
    return (await connection.execute(sa.select(table).order_by(table.c.message_id, table.c.chat_id))).mappings().all()


def test_chain_and_current_orm_key():
    from deerflow.persistence.telegram_bots.model import TelegramInboundMarkRow

    script = ScriptDirectory(str(_MIGRATIONS_DIR))
    assert script.get_heads() == [REVISION]
    assert script.get_revision(REVISION).down_revision == PREVIOUS
    assert list(TelegramInboundMarkRow.__table__.primary_key.columns.keys()) == NEW_KEY
    assert list(historical()._metadata.tables[TABLE].primary_key.columns.keys()) == OLD_KEY


@pytest.mark.asyncio(loop_scope="module")
async def test_upgrade_retains_all_rows_and_changes_only_primary_columns(connection):
    await seed(connection)
    before, retained = await connection.run_sync(shape), await rows(connection)
    await connection.run_sync(apply)
    after = await connection.run_sync(shape)
    expected = before[TABLE]["primary"].copy()
    expected["constrained_columns"] = NEW_KEY
    before[TABLE]["primary"] = expected
    assert after == before
    assert await rows(connection) == retained
    await connection.run_sync(apply)
    assert await connection.run_sync(shape) == after
    assert await rows(connection) == retained
    if connection.dialect.name == "sqlite":
        assert (await connection.exec_driver_sql("PRAGMA foreign_keys")).scalar_one() == 1
        assert (await connection.exec_driver_sql("PRAGMA foreign_key_check")).all() == []


@pytest.mark.asyncio(loop_scope="module")
async def test_same_message_across_chats_is_retained_but_replays_stay_unique(connection):
    await seed(connection)
    await connection.run_sync(apply)
    table = historical()._metadata.tables[TABLE]
    await connection.execute(table.insert().values(**inbound_values(chat=7002, update=203)))
    assert len(await rows(connection)) == 3
    for duplicate in (inbound_values(update=204), inbound_values(message=103, update=201)):
        with pytest.raises(sa.exc.IntegrityError):
            async with connection.begin_nested():
                await connection.execute(table.insert().values(**duplicate))
    # FK action, named check and index remain functional after the rebuild.
    with pytest.raises(sa.exc.IntegrityError):
        async with connection.begin_nested():
            await connection.execute(table.insert().values(**{**inbound_values(message=104, update=204), "state": "invalid"}))
    await connection.execute(sa.delete(historical()._metadata.tables["telegram_bot_links"]))
    assert await rows(connection) == []


@pytest.mark.asyncio(loop_scope="module")
async def test_0042_replay_recognizes_exact_successor_shape_without_collapsing_rows(connection):
    await seed(connection)
    await connection.run_sync(apply)
    table = historical()._metadata.tables[TABLE]
    await connection.execute(table.insert().values(**inbound_values(chat=7002, update=203)))
    before, retained = await connection.run_sync(shape), await rows(connection)
    assert len(retained) == 3
    # Some legacy stamps coexist with create_all's newer tables. The historical
    # revision may recognize only its independently frozen immediate successor,
    # preserving both chats rather than temporarily narrowing their identity.
    await connection.run_sync(apply_historical)
    await connection.run_sync(apply_historical)
    assert await connection.run_sync(shape) == before
    assert await rows(connection) == retained
    await connection.run_sync(apply)
    assert await connection.run_sync(shape) == before
    assert await rows(connection) == retained


@pytest.mark.asyncio(loop_scope="module")
@pytest.mark.parametrize("drift", ["column", "missing_index", "renamed_index"])
async def test_0042_replay_rejects_malformed_successor_shape(connection, drift):
    await seed(connection)
    await connection.run_sync(apply)
    if drift == "column":
        await connection.execute(sa.text(f"ALTER TABLE {TABLE} ADD COLUMN unexpected INTEGER"))
    else:
        await connection.execute(sa.text("DROP INDEX ix_telegram_inbound_marks_closed"))
        if drift == "renamed_index":
            await connection.execute(sa.text(f"CREATE INDEX unexpected_index ON {TABLE} (closed_at)"))
    before, retained = await connection.run_sync(shape), await rows(connection)
    with pytest.raises(RuntimeError, match="Incompatible"):
        await connection.run_sync(apply_historical)
    assert await connection.run_sync(shape) == before
    assert await rows(connection) == retained


@pytest.mark.asyncio(loop_scope="module")
async def test_downgrade_preserves_representable_rows_and_is_repeatable(connection):
    await seed(connection)
    original, retained = await connection.run_sync(shape), await rows(connection)
    await connection.run_sync(apply)
    await connection.run_sync(apply, "downgrade")
    assert await connection.run_sync(shape) == original
    assert await rows(connection) == retained
    await connection.run_sync(apply, "downgrade")
    assert await connection.run_sync(shape) == original
    await connection.run_sync(apply)
    assert await rows(connection) == retained


@pytest.mark.asyncio(loop_scope="module")
async def test_downgrade_refuses_colliding_chats_before_any_ddl(connection):
    await seed(connection)
    await connection.run_sync(apply)
    table = historical()._metadata.tables[TABLE]
    await connection.execute(table.insert().values(**inbound_values(chat=7002, update=203)))
    before, retained = await connection.run_sync(shape), await rows(connection)
    ddl = []

    def record_ddl(_, __, statement, ___, ____, _____):
        if statement.lstrip().upper().startswith(("CREATE ", "ALTER ", "DROP ")):
            ddl.append(statement)

    sa.event.listen(connection.sync_connection, "before_cursor_execute", record_ddl)
    try:
        with pytest.raises(RuntimeError, match=f"^{COLLAPSE_ERROR}$"):
            await connection.run_sync(apply, "downgrade")
    finally:
        sa.event.remove(connection.sync_connection, "before_cursor_execute", record_ddl)
    assert ddl == []
    assert await connection.run_sync(shape) == before
    assert await rows(connection) == retained


@pytest.mark.asyncio(loop_scope="module")
async def test_current_orm_matches_all_fifteen_tables_at_new_head(connection):
    import deerflow.persistence.models  # noqa: F401
    from deerflow.persistence.base import Base

    names = [table.name for table in historical()._tables]
    await connection.run_sync(apply)
    frozen = await connection.run_sync(shape, names)
    for table in reversed(historical()._tables):
        await connection.run_sync(table.drop)
    await connection.run_sync(lambda c: Base.metadata.create_all(c, tables=[Base.metadata.tables[name] for name in names]))
    assert await connection.run_sync(shape, names) == frozen
    assert all(c.server_default is None for name in names for c in Base.metadata.tables[name].columns)
    await connection.run_sync(apply)
    assert await connection.run_sync(shape, names) == frozen


@pytest.mark.asyncio(loop_scope="module")
@pytest.mark.parametrize("upgraded", [False, True], ids=["old-key", "new-key"])
@pytest.mark.parametrize("drift", ["column", "missing_index", "renamed_index", "default", "unique", "foreign"])
async def test_schema_drift_is_refused_before_rebuild(connection, upgraded, drift):
    if upgraded:
        await connection.run_sync(apply)
    if drift == "column":
        await connection.execute(sa.text(f"ALTER TABLE {TABLE} ADD COLUMN unexpected INTEGER"))
    elif drift in ("missing_index", "renamed_index"):
        await connection.execute(sa.text("DROP INDEX ix_telegram_inbound_marks_closed"))
        if drift == "renamed_index":
            await connection.execute(sa.text(f"CREATE INDEX unexpected_index ON {TABLE} (closed_at)"))
    else:
        # Build a deliberately incompatible table through real DDL on both DBs.
        def corrupt(c):
            metadata = sa.MetaData()
            table = sa.Table(TABLE, metadata, autoload_with=c)
            sa.Table("telegram_bot_links", metadata, sa.Column("id", sa.String(32), primary_key=True), extend_existing=True)
            table.drop(c)
            if drift == "default":
                table.c.binding_generation.server_default = sa.DefaultClause("1")
            elif drift == "unique":
                table.constraints.remove(next(k for k in table.constraints if isinstance(k, sa.UniqueConstraint)))
            else:
                foreign = next(iter(table.foreign_key_constraints))
                table.constraints.remove(foreign)
            table.create(c)

        await connection.run_sync(corrupt)
    before = await connection.run_sync(shape)
    with pytest.raises(RuntimeError, match="Incompatible"):
        await connection.run_sync(apply)
    assert await connection.run_sync(shape) == before


@pytest.mark.asyncio(loop_scope="module")
async def test_real_alembic_upgrade_and_downgrade_stamp(database):
    engine, config = database
    try:
        await asyncio.to_thread(command.upgrade, config, REVISION)
        async with engine.connect() as connection:
            assert (await connection.execute(sa.text("SELECT version_num FROM alembic_version"))).scalar_one() == REVISION
            assert (await connection.run_sync(shape))[TABLE]["primary"]["constrained_columns"] == NEW_KEY
        await asyncio.to_thread(command.upgrade, config, REVISION)
    finally:
        await asyncio.to_thread(command.downgrade, config, PREVIOUS)
    async with engine.connect() as connection:
        assert (await connection.execute(sa.text("SELECT version_num FROM alembic_version"))).scalar_one() == PREVIOUS
        assert (await connection.run_sync(shape))[TABLE]["primary"]["constrained_columns"] == OLD_KEY


@pytest.mark.asyncio(loop_scope="module")
async def test_downgrade_waits_for_inflight_writer_before_collision_preflight(database):
    engine, _ = database
    async with engine.begin() as connection:
        await connection.run_sync(apply)
        await seed(connection)
    writer = await engine.connect()
    transaction = await writer.begin()
    table = historical()._metadata.tables[TABLE]
    await writer.execute(table.insert().values(**inbound_values(chat=7002, update=203)))
    locking = asyncio.Event()

    async def downgrade():
        async with engine.begin() as connection:

            def observe_lock(_, __, statement, ___, ____, _____):
                sql = statement.lstrip().upper()
                if sql.startswith(("LOCK TABLE TELEGRAM_INBOUND_MARKS", "UPDATE TELEGRAM_INBOUND_MARKS SET BINDING_GENERATION")):
                    locking.set()

            sa.event.listen(connection.sync_connection, "before_cursor_execute", observe_lock)
            try:
                await connection.run_sync(apply, "downgrade")
            finally:
                sa.event.remove(connection.sync_connection, "before_cursor_execute", observe_lock)

    task = asyncio.create_task(downgrade())
    try:
        await asyncio.wait_for(locking.wait(), timeout=5)
        assert not task.done()
        await transaction.commit()
        with pytest.raises(RuntimeError, match=f"^{COLLAPSE_ERROR}$"):
            await asyncio.wait_for(task, timeout=10)
        async with engine.connect() as connection:
            assert len(await rows(connection)) == 3
            assert (await connection.run_sync(shape))[TABLE]["primary"]["constrained_columns"] == NEW_KEY
    finally:
        if transaction.is_active:
            await transaction.rollback()
        await writer.close()
        if not task.done():
            task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        async with engine.begin() as connection:
            await connection.execute(sa.delete(historical()._metadata.tables["telegram_bot_links"]))
            await connection.run_sync(apply, "downgrade")


@pytest.mark.asyncio(loop_scope="module")
async def test_sqlite_rebuild_failure_restores_original_table_and_rows(connection):
    if connection.dialect.name != "sqlite":
        pytest.skip("SQLite-specific table-copy failure")
    await seed(connection)
    before, retained = await connection.run_sync(shape), await rows(connection)

    def fail_before_rename(_, __, statement, ___, ____, _____):
        if statement.lstrip().upper().startswith("ALTER TABLE _ALEMBIC_TMP_TELEGRAM_INBOUND_MARKS RENAME"):
            raise RuntimeError("synthetic rename failure")

    sa.event.listen(connection.sync_connection, "before_cursor_execute", fail_before_rename)
    try:
        with pytest.raises(RuntimeError, match="^synthetic rename failure$"):
            await connection.run_sync(apply)
    finally:
        sa.event.remove(connection.sync_connection, "before_cursor_execute", fail_before_rename)
    assert await connection.run_sync(shape) == before
    assert await rows(connection) == retained
    assert "_alembic_tmp_telegram_inbound_marks" not in await connection.run_sync(lambda c: sa.inspect(c).get_table_names())
    await connection.run_sync(apply)
    assert await rows(connection) == retained
