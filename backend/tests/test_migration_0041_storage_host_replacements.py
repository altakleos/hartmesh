"""Upgrade, retry and rollback preserve the durable facts of the preceding release."""

import asyncio
import importlib
import os
import uuid
from datetime import UTC, datetime

import pytest
import pytest_asyncio
import sqlalchemy as sa
from alembic import command
from alembic.migration import MigrationContext
from alembic.operations import Operations
from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.schema import CreateSchema, DropSchema

from deerflow.persistence.bootstrap import _get_alembic_config

REVISION = "0041_storage_host_replacements"
PREVIOUS = "0040_work_execution"
TABLES = ("users", "storage_space_attachments", "agent_work_attempts", "agent_work_events", "storage_host_replacements")


def shape(connection):
    inspector = sa.inspect(connection)
    return {
        name: {
            "columns": sorted((c["name"], str(c["type"]), c["nullable"], c["default"]) for c in inspector.get_columns(name)),
            "primary": inspector.get_pk_constraint(name)["constrained_columns"],
            "checks": sorted((c["name"], c["sqltext"]) for c in inspector.get_check_constraints(name)),
            "unique": sorted((c["name"], c["column_names"]) for c in inspector.get_unique_constraints(name)),
            "foreign": sorted((f["constrained_columns"], f["referred_table"], f["referred_columns"], f["options"]) for f in inspector.get_foreign_keys(name)),
            "indexes": sorted((i["name"], i["column_names"], i["unique"], sorted((key, str(value)) for key, value in i.get("dialect_options", {}).items())) for i in inspector.get_indexes(name)),
        }
        for name in TABLES
        if name in inspector.get_table_names()
    }


@pytest_asyncio.fixture(params=["sqlite", "postgres"])
async def database(tmp_path, request):
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
        schema = "storage_hosts_" + uuid.uuid4().hex
        async with admin.begin() as connection:
            await connection.execute(CreateSchema(schema))
        engine = create_async_engine(url, connect_args={**ssl, "server_settings": {"search_path": schema}})
    else:
        engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path / 'migration.db'}")

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


def apply(connection, direction):
    module = importlib.import_module(f"deerflow.persistence.migrations.versions.{REVISION}")
    with Operations.context(MigrationContext.configure(connection)):
        getattr(module, direction)()


@pytest.mark.asyncio
async def test_upgrade_retry_and_unused_downgrade_match_frozen_and_cold_shapes(database):
    import deerflow.persistence.models  # noqa: F401 -- register the complete cold schema
    from deerflow.persistence.base import Base

    engine, config = database
    async with engine.connect() as connection:
        before = await connection.run_sync(shape)
    # Resume an interrupted additive upgrade without re-adding its existing DDL.
    async with engine.begin() as connection:
        await connection.execute(sa.text("ALTER TABLE agent_work_attempts ADD COLUMN attachment_id VARCHAR(32)"))
        timestamp = "TIMESTAMP WITH TIME ZONE" if engine.dialect.name == "postgresql" else "DATETIME"
        await connection.execute(
            sa.text(
                "CREATE TABLE storage_host_replacements (old_host_id VARCHAR(128) NOT NULL, new_host_id VARCHAR(128) NOT NULL, "
                f"attachments_named INTEGER NOT NULL, created_at {timestamp} NOT NULL, PRIMARY KEY (old_host_id,new_host_id), "
                "CONSTRAINT ck_storage_host_replacements_distinct CHECK (old_host_id <> new_host_id))"
            )
        )
    await asyncio.to_thread(command.upgrade, config, REVISION)
    async with engine.begin() as connection:
        upgraded = await connection.run_sync(shape)
        await connection.run_sync(apply, "upgrade")
        assert await connection.run_sync(shape) == upgraded
    await asyncio.to_thread(command.downgrade, config, PREVIOUS)
    async with engine.connect() as connection:
        assert await connection.run_sync(shape) == before

    # The empty-database bootstrap creates these models directly, then stamps head.
    async with engine.begin() as connection:

        def cold(c):
            metadata = sa.MetaData()
            for table in Base.metadata.sorted_tables:
                table.to_metadata(metadata)
            metadata.drop_all(c)
            metadata.create_all(c)

        await connection.run_sync(cold)
        assert await connection.run_sync(shape) == upgraded


def seed(connection):
    metadata = sa.MetaData()
    metadata.reflect(connection)
    now = datetime.now(UTC)

    def insert(table, **values):
        connection.execute(metadata.tables[table].insert().values(**values))

    insert("users", id="person", email="person@example.com", system_role="user", created_at=now, needs_setup=False, token_version=0)
    insert("user_preferences", user_id="person", key="theme", value="dark")
    insert("storage_spaces", id="space", backing_handle="root", name="Files", custody_kind="company", mode="native", status="active", generation=1, created_at=now, updated_at=now)
    insert("storage_space_attachments", id="attachment", incarnation="incarnation", actor_kind="nonhuman", actor_id="agent:agent", host_id="host-a", phase="active", created_at=now)
    insert("agent_definition_revisions", revision="definition", owner_id="person", config={}, soul="Do assigned work.")
    insert(
        "agent_instances",
        id="agent",
        principal_id="agent:agent",
        name="Agent",
        custody="company",
        creator_id="person",
        supervisor_id="person",
        definition_revision="definition",
        home_id="space",
        status="active",
        generation=1,
        creation_id="creation",
        creation_request={},
        created_at=now,
        updated_at=now,
    )
    insert(
        "agent_work",
        id="work",
        instance_id="agent",
        creator_id="person",
        objective="Read the guide",
        success_criteria="Describe findings",
        priority="normal",
        definition_revision="definition",
        assignment_revision=1,
        revision=1,
        status="open",
        progress="",
        next_action="Read",
        sources=[],
        review_required=True,
        review_state="none",
        created_at=now,
        updated_at=now,
    )
    insert("agent_work_attempts", id="attempt", work_id="work", activation_id="activation", requester_id="person", assignment_revision=1, instance_generation=1, definition_revision="definition", status="running", request={}, created_at=now)
    insert("agent_work_events", id="event", instance_id="agent", work_id="work", actor_id="person", actor_kind="human", operation_id="operation", action="delegated", revision=1, assignment_revision=1, request={}, result={}, created_at=now)
    return metadata


@pytest.mark.asyncio
async def test_host_actor_and_all_downgrade_guards_preserve_schema_and_data(database):
    engine, config = database
    async with engine.begin() as connection:
        await connection.run_sync(seed)
        with pytest.raises(sa.exc.IntegrityError):
            async with connection.begin_nested():
                await connection.execute(sa.text("UPDATE agent_work_events SET actor_kind='host'"))
    await asyncio.to_thread(command.upgrade, config, REVISION)
    async with engine.begin() as connection:
        expected = await connection.run_sync(shape)
        facts = [
            ("INSERT INTO storage_host_replacements VALUES ('host-a','host-b',1,CURRENT_TIMESTAMP)", "host replacement"),
            ("UPDATE users SET released_at=CURRENT_TIMESTAMP", "released seat"),
            *[
                (f"UPDATE storage_space_attachments SET {column}={value}", "containment attribution")
                for column, value in (("fenced_by_kind", "'host'"), ("fenced_by_id", "'host-a'"), ("fence_reason", "'operator'"), ("fenced_at", "CURRENT_TIMESTAMP"))
            ],
            *[(f"UPDATE agent_work_attempts SET {column}={value}", "attempt facts") for column, value in (("contained_reason", "'operator'"), ("workspace_ready_at", "CURRENT_TIMESTAMP"), ("attachment_id", "'attachment'"))],
            ("UPDATE agent_work_events SET actor_kind='host'", "Work history"),
        ]
        if engine.dialect.name == "sqlite":
            facts.append(("INSERT INTO storage_space_mounts VALUES ('attachment','space',1,'home',1)", "mounts reference"))
        for statement, message in facts:
            savepoint = await connection.begin_nested()
            try:
                await connection.execute(sa.text(statement))
                with pytest.raises(RuntimeError, match=message):
                    await connection.run_sync(apply, "downgrade")
                assert await connection.run_sync(shape) == expected
                assert (await connection.execute(sa.text("SELECT next_action FROM agent_work"))).scalar_one() == "Read"
            finally:
                await savepoint.rollback()
        with pytest.raises(sa.exc.IntegrityError):
            async with connection.begin_nested():
                await connection.execute(sa.text("INSERT INTO storage_host_replacements VALUES ('host-a','host-a',1,CURRENT_TIMESTAMP)"))

        # Unused additions may be removed without rebuilding the users parent.
        await connection.run_sync(apply, "downgrade")
        assert (await connection.execute(sa.text("SELECT value FROM user_preferences WHERE user_id='person'").columns(value=sa.JSON()))).scalar_one() == "dark"
        assert (await connection.execute(sa.text("SELECT email FROM users"))).scalar_one() == "person@example.com"
        assert (await connection.execute(sa.text("SELECT next_action FROM agent_work"))).scalar_one() == "Read"


@pytest.mark.asyncio
@pytest.mark.parametrize("drift", ["column", "table", "actor"])
async def test_incompatible_partial_ddl_refuses_before_any_other_change(database, drift):
    engine, _config = database
    async with engine.begin() as connection:
        if drift == "column":
            await connection.execute(sa.text("ALTER TABLE agent_work_attempts ADD COLUMN attachment_id INTEGER"))
        elif drift == "table":
            await connection.execute(sa.text("CREATE TABLE storage_host_replacements (old_host_id VARCHAR(128) PRIMARY KEY)"))
        else:

            def wrong_actor(c):
                with Operations.context(MigrationContext.configure(c)):
                    from alembic import op

                    with op.batch_alter_table("agent_work_events") as batch:
                        batch.drop_constraint("ck_agent_work_event_actor", type_="check")
                        batch.create_check_constraint("ck_agent_work_event_actor", "actor_kind IN ('human', 'host', 'unexpected')")

            await connection.run_sync(wrong_actor)
        before = await connection.run_sync(shape)
        with pytest.raises(RuntimeError, match="Incompatible"):
            await connection.run_sync(apply, "upgrade")
        assert await connection.run_sync(shape) == before
