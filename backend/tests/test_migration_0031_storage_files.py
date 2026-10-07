"""Published resource ancestry stays fixed; new file facts survive upgrades."""

import asyncio
import os
import uuid

import pytest
from alembic import command
from alembic.migration import MigrationContext
from alembic.operations import Operations
from alembic.script import ScriptDirectory
from sqlalchemy import inspect, text
from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.schema import CreateSchema, DropSchema

from deerflow.persistence.base import Base
from deerflow.persistence.bootstrap import _get_alembic_config
from deerflow.persistence.spaces.files import SpaceBackingRow, SpaceFileOperationRow
from deerflow.persistence.spaces.model import SpaceRow

REVISION = "0031_storage_files"
PREVIOUS = "0030_storage_spaces"
TABLES = [SpaceBackingRow.__table__, SpaceFileOperationRow.__table__]


def shape(connection):
    inspector = inspect(connection)
    return {
        t.name: {
            "columns": [(c["name"], str(c["type"]), c["nullable"], c["default"]) for c in inspector.get_columns(t.name)],
            "primary_key": inspector.get_pk_constraint(t.name)["constrained_columns"],
            "unique": sorted((u["name"], u["column_names"]) for u in inspector.get_unique_constraints(t.name)),
            "checks": sorted((c["name"], c["sqltext"]) for c in inspector.get_check_constraints(t.name)),
            "foreign_keys": sorted((f["constrained_columns"], f["referred_table"], f["referred_columns"], f["options"]) for f in inspector.get_foreign_keys(t.name)),
        }
        for t in TABLES
    }


@pytest.mark.asyncio
async def test_upgrade_matches_fresh_shape_and_keeps_prior_resources(tmp_path):
    engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path / 'upgraded.db'}")
    fresh = create_async_engine(f"sqlite+aiosqlite:///{tmp_path / 'fresh.db'}")
    try:
        config = _get_alembic_config(engine)
        await asyncio.to_thread(command.upgrade, config, PREVIOUS)
        async with engine.begin() as c:
            await c.execute(
                text("INSERT INTO storage_spaces (id,backing_handle,name,custody_kind,mode,status,generation,created_at,updated_at) VALUES ('retained','root','label','company','native','active',1,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)")
            )
        await asyncio.to_thread(command.upgrade, config, REVISION)
        async with fresh.begin() as c:
            await c.run_sync(Base.metadata.create_all, tables=[SpaceRow.__table__, *TABLES])
        async with engine.connect() as c:
            upgraded = await c.run_sync(shape)
            assert (await c.execute(text("SELECT id FROM storage_spaces"))).scalar_one() == "retained"
        async with fresh.connect() as c:
            assert upgraded == await c.run_sync(shape)
        await asyncio.to_thread(command.downgrade, config, PREVIOUS)
    finally:
        await engine.dispose()
        await fresh.dispose()


@pytest.mark.asyncio
async def test_wrong_shape_and_used_journal_never_get_erased(tmp_path):
    engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path / 'preserve.db'}")
    try:
        config = _get_alembic_config(engine)
        await asyncio.to_thread(command.upgrade, config, PREVIOUS)
        async with engine.begin() as c:
            await c.execute(text("CREATE TABLE storage_space_backings (backing_handle TEXT PRIMARY KEY)"))
            await c.execute(text("INSERT INTO storage_space_backings VALUES ('operator-data')"))
        with pytest.raises(Exception, match="storage schema"):
            await asyncio.to_thread(command.upgrade, config, REVISION)
        async with engine.begin() as c:
            assert (await c.execute(text("SELECT backing_handle FROM storage_space_backings"))).scalar_one() == "operator-data"
            await c.execute(text("DROP TABLE storage_space_backings"))
        await asyncio.to_thread(command.upgrade, config, REVISION)
        async with engine.begin() as c:
            await c.execute(
                text("INSERT INTO storage_spaces (id,backing_handle,name,custody_kind,mode,status,generation,created_at,updated_at) VALUES ('retained','root','label','company','native','active',1,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)")
            )
            await c.execute(
                text("INSERT INTO storage_space_file_operations (space_id,operation_id,actor_kind,actor_id,generation,phase,request,created_at) VALUES ('retained','intent','nonhuman','worker',1,'pending','{}',CURRENT_TIMESTAMP)")
            )
        with pytest.raises(Exception, match="binding or file operation"):
            await asyncio.to_thread(command.downgrade, config, PREVIOUS)
        async with engine.connect() as c:
            assert (await c.execute(text("SELECT phase FROM storage_space_file_operations"))).scalar_one() == "pending"
    finally:
        await engine.dispose()


def test_cold_registration_contains_new_file_tables():
    import subprocess
    import sys

    result = subprocess.run(
        [sys.executable, "-c", "import deerflow.persistence.models; from deerflow.persistence.base import Base; assert {'storage_space_backings','storage_space_file_operations'} <= set(Base.metadata.tables)"], capture_output=True, text=True
    )
    assert result.returncode == 0, result.stderr


@pytest.mark.asyncio
async def test_postgres_accepts_cold_and_partial_ddl_but_refuses_operation_phase_drift():
    uri = os.environ.get("TEST_POSTGRES_URI")
    if not uri:
        pytest.skip("TEST_POSTGRES_URI is not configured")
    url = make_url(uri).set(drivername="postgresql+asyncpg")
    ssl = {"ssl": False} if url.query.get("sslmode") == "disable" else {}
    url = url.difference_update_query(["sslmode"])
    admin = create_async_engine(url, connect_args=ssl)
    schema = "storage_files_migration_" + uuid.uuid4().hex
    engine = None
    try:
        async with admin.begin() as c:
            await c.execute(CreateSchema(schema))
        engine = create_async_engine(url, connect_args={**ssl, "server_settings": {"search_path": schema}})
        config = _get_alembic_config(engine, postgres_schema=schema)
        module = await asyncio.to_thread(lambda: ScriptDirectory.from_config(config).get_revision(REVISION).module)

        def apply(connection, callback):
            with Operations.context(MigrationContext.configure(connection)):
                callback()

        async with engine.begin() as c:
            await c.run_sync(Base.metadata.create_all, tables=[SpaceRow.__table__, *TABLES])
            await c.run_sync(apply, module.upgrade)
            await c.run_sync(apply, module.downgrade)
            await c.run_sync(Base.metadata.create_all, tables=[SpaceBackingRow.__table__])
            await c.run_sync(apply, module.upgrade)
            assert {t.name for t in TABLES} <= set(await c.run_sync(lambda conn: inspect(conn).get_table_names()))
            await c.execute(text("ALTER TABLE storage_space_file_operations DROP CONSTRAINT ck_storage_space_file_operations_phase"))
            await c.execute(text("ALTER TABLE storage_space_file_operations ADD CONSTRAINT ck_storage_space_file_operations_phase CHECK (phase <> '')"))
            with pytest.raises(RuntimeError, match="storage schema"):
                await c.run_sync(apply, module.upgrade)
    finally:
        if engine is not None:
            await engine.dispose()
        try:
            async with admin.begin() as c:
                await c.execute(DropSchema(schema, cascade=True, if_exists=True))
        finally:
            await admin.dispose()
