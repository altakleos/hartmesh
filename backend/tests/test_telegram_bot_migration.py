"""Frozen Telegram schema parity, repeatable upgrades and guarded rollback."""

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

REVISION = "0042_telegram_bots"
PREVIOUS = "0041_storage_host_replacements"
TABLES = (
    "telegram_bot_links",
    "telegram_bot_link_events",
    "telegram_accounts",
    "telegram_person_settings",
    "telegram_bot_intents",
    "telegram_instance_policy",
    "telegram_assistant_adds",
    "telegram_work_topics",
    "telegram_callbacks",
    "telegram_acts",
    "telegram_work_cursor",
    "telegram_outbox",
    "telegram_inbound_marks",
    "telegram_pending_files",
    "telegram_deployment",
)
GUARDED = ("telegram_bot_links", "telegram_bot_link_events", "telegram_acts", "telegram_outbox", "telegram_instance_policy", "telegram_assistant_adds")
UNCHANGED = ("channel_connections", "channel_conversations", "channel_credentials", "channel_oauth_states", "user_preferences")


def migration():
    return importlib.import_module(f"deerflow.persistence.migrations.versions.{REVISION}")


def apply(connection, direction="upgrade"):
    with Operations.context(MigrationContext.configure(connection)):
        getattr(migration(), direction)()


def shape(connection, names=TABLES):
    inspector = sa.inspect(connection)
    existing = set(inspector.get_table_names())
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
        if name in existing
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
        schema = "telegram_migration_" + uuid4().hex
        async with admin.begin() as connection:
            await connection.execute(CreateSchema(schema))
        engine = create_async_engine(url, connect_args={**ssl, "server_settings": {"search_path": schema}})
    else:
        directory = tmp_path_factory.mktemp("telegram-migration")
        engine = create_async_engine(f"sqlite+aiosqlite:///{directory / 'migration.db'}")

        @sa.event.listens_for(engine.sync_engine, "connect")
        def foreign_keys(connection, _):
            connection.execute("PRAGMA foreign_keys=ON")

    try:
        config = _get_alembic_config(engine, postgres_schema=schema)
        await asyncio.to_thread(command.upgrade, config, PREVIOUS)
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
        # SQLite supports transactional DDL, but its driver's legacy mode does
        # not start a real transaction for CREATE TABLE. Own that boundary so
        # every test restores the exact0041 schema without rebuilding it.
        if engine.dialect.name == "sqlite":
            await connection.exec_driver_sql("BEGIN")
        try:
            yield connection
        finally:
            await transaction.rollback()


def test_frozen_0042_parent_is_unchanged():
    script = ScriptDirectory(str(_MIGRATIONS_DIR))
    assert script.get_revision(REVISION).down_revision == PREVIOUS


@pytest.mark.asyncio(loop_scope="module")
async def test_alembic_upgrade_from_previous_head_updates_stamp(database):
    engine, config = database
    try:
        await asyncio.to_thread(command.upgrade, config, REVISION)
        async with engine.connect() as connection:
            assert (await connection.execute(sa.text("SELECT version_num FROM alembic_version"))).scalar_one() == REVISION
            first = await connection.run_sync(shape)
            assert set(first) == set(TABLES)
        await asyncio.to_thread(command.upgrade, config, REVISION)
        async with engine.connect() as connection:
            assert await connection.run_sync(shape) == first
    finally:
        await asyncio.to_thread(command.downgrade, config, PREVIOUS)
    async with engine.connect() as connection:
        assert (await connection.execute(sa.text("SELECT version_num FROM alembic_version"))).scalar_one() == PREVIOUS
        assert await connection.run_sync(shape) == {}


@pytest.mark.asyncio(loop_scope="module")
async def test_fifteen_tables_created(connection):
    before = await connection.run_sync(lambda c: set(sa.inspect(c).get_table_names()))
    assert not set(TABLES) & before
    await connection.run_sync(apply)
    after = await connection.run_sync(lambda c: set(sa.inspect(c).get_table_names()))
    assert after - before == set(TABLES)
    first = await connection.run_sync(shape)
    assert all(column[3] is None for table in first.values() for column in table["columns"])
    await connection.run_sync(apply)
    assert await connection.run_sync(shape) == first


@pytest.mark.asyncio(loop_scope="module")
async def test_upgrade_twice_validates_the_existing_tables(connection):
    await connection.run_sync(apply)
    before = await connection.run_sync(shape)
    table = migration()._metadata.tables["telegram_accounts"]
    await connection.execute(table.insert().values(telegram_user_id=2**45, owner_user_id="owner", created_at=datetime.now(UTC)))
    await connection.run_sync(apply)
    assert await connection.run_sync(shape) == before
    assert (await connection.execute(sa.select(table.c.telegram_user_id))).scalar_one() == 2**45


@pytest.mark.asyncio(loop_scope="module")
async def test_no_change_to_channel_connection_tables(connection):
    before = await connection.run_sync(shape, UNCHANGED)
    assert set(before) == set(UNCHANGED)
    await connection.run_sync(apply)
    assert await connection.run_sync(shape, UNCHANGED) == before
    await connection.run_sync(apply, "downgrade")
    assert await connection.run_sync(shape, UNCHANGED) == before


@pytest.mark.asyncio(loop_scope="module")
async def test_person_settings_columns_are_final(connection):
    await connection.run_sync(apply)
    table = (await connection.run_sync(shape))["telegram_person_settings"]
    columns = {column[0]: column for column in table["columns"]}
    assert columns["voice_agreed_for"][1:3] == ("VARCHAR(16)", True)
    assert columns["voice_agreed_at"][2] is True
    assert columns["notify"][1:3] == ("VARCHAR(12)", False)
    assert {name for name, _ in table["checks"]} == {
        "ck_telegram_person_settings_locale",
        "ck_telegram_person_settings_notify",
        "ck_telegram_person_settings_voice",
        "ck_telegram_person_settings_quiet_start",
        "ck_telegram_person_settings_quiet_end",
        "ck_telegram_person_settings_summary",
    }


@pytest.mark.asyncio(loop_scope="module")
async def test_historical_revision_matches_its_frozen_metadata(connection):
    await connection.run_sync(apply)
    frozen = await connection.run_sync(shape)
    await connection.run_sync(apply, "downgrade")
    assert await connection.run_sync(shape) == {}
    await connection.run_sync(lambda c: migration()._metadata.create_all(c, tables=list(migration()._tables)))
    assert await connection.run_sync(shape) == frozen
    for name in TABLES:
        assert all(column.server_default is None for column in migration()._metadata.tables[name].columns)
    # 0042 remains frozen; current cold-ORM parity is measured after 0043.
    await connection.run_sync(apply)
    await connection.run_sync(apply)
    assert await connection.run_sync(shape) == frozen


def required_row(table):
    """Synthetic mandatory values only; no credentials or remote requests."""
    now = datetime.now(UTC)
    values = {}
    arrays = {"operation_ids", "parts", "grouped_message_ids", "delivery_ids"}
    for column in table.columns:
        if column.nullable:
            continue
        if isinstance(column.type, sa.Boolean):
            values[column.name] = False
        elif isinstance(column.type, (sa.Integer, sa.BigInteger)):
            values[column.name] = 1
        elif isinstance(column.type, sa.DateTime):
            values[column.name] = now
        elif isinstance(column.type, sa.JSON):
            values[column.name] = [] if column.name in arrays else {}
        else:
            values[column.name] = "synthetic"
    values.update(
        {
            "telegram_bot_links": {"id": "link", "agent_kind": "lead_agent", "origin": "pasted", "bind_mode": "code", "state": "waiting_connect", "health": "ok"},
            "telegram_bot_link_events": {"action": "add"},
            "telegram_acts": {"state": "pending"},
            "telegram_outbox": {"link_id": "link", "class": "reply", "state": "pending"},
            "telegram_instance_policy": {"policy": "allowed"},
            "telegram_assistant_adds": {"state": "draft"},
        }.get(table.name, {})
    )
    return values


@pytest.mark.asyncio(loop_scope="module")
@pytest.mark.parametrize("guarded", GUARDED)
async def test_downgrade_refuses_with_a_link_row(connection, guarded):
    await connection.run_sync(apply)
    table = migration()._metadata.tables[guarded]
    if guarded == "telegram_outbox":
        parent = migration()._metadata.tables["telegram_bot_links"]
        await connection.execute(parent.insert().values(**required_row(parent)))
    await connection.execute(table.insert().values(**required_row(table)))
    before = await connection.run_sync(shape)
    with pytest.raises(RuntimeError, match="^Refusing to erase Telegram bot links, tokens or history$"):
        await connection.run_sync(apply, "downgrade")
    assert await connection.run_sync(shape) == before
    assert (await connection.execute(sa.select(sa.func.count()).select_from(table))).scalar_one() == 1


@pytest.mark.asyncio(loop_scope="module")
async def test_outbox_guard_preflights_partial_schema_before_dropping_any_table(connection):
    # A valid linked outbox also has a guarded parent. This deliberately partial
    # interrupted schema isolates the outbox guard without disabling foreign keys.
    links = migration()._metadata.tables["telegram_bot_links"]
    await connection.run_sync(links.create)
    await connection.execute(sa.text("CREATE TABLE telegram_outbox (id VARCHAR(32) PRIMARY KEY)"))
    await connection.execute(sa.text("INSERT INTO telegram_outbox (id) VALUES ('retained')"))
    before = await connection.run_sync(lambda c: set(sa.inspect(c).get_table_names()))
    with pytest.raises(RuntimeError, match="Refusing to erase Telegram bot links, tokens or history"):
        await connection.run_sync(apply, "downgrade")
    assert await connection.run_sync(lambda c: set(sa.inspect(c).get_table_names())) == before
    assert (await connection.execute(sa.text("SELECT id FROM telegram_outbox"))).scalar_one() == "retained"


@pytest.mark.asyncio(loop_scope="module")
async def test_unused_downgrade_drops_all_fifteen_and_is_repeatable(connection):
    await connection.run_sync(apply)
    await connection.run_sync(apply, "downgrade")
    assert await connection.run_sync(shape) == {}
    await connection.run_sync(apply, "downgrade")


@pytest.mark.asyncio(loop_scope="module")
@pytest.mark.parametrize("drift", ["column", "missing_index"])
async def test_upgrade_rejects_existing_schema_drift(connection, drift):
    await connection.run_sync(apply)
    if drift == "column":
        await connection.execute(sa.text("ALTER TABLE telegram_person_settings ADD COLUMN unexpected INTEGER"))
    else:
        await connection.execute(sa.text("DROP INDEX ix_telegram_callbacks_expires"))
    with pytest.raises(RuntimeError, match="Incompatible"):
        await connection.run_sync(apply)
