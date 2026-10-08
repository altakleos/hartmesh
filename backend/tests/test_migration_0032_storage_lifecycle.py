"""Frozen additive containment/backup DDL; never erase used recovery facts."""

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
from deerflow.persistence.spaces.lifecycle import SpaceAttachmentRow, SpaceBackupRow, SpaceMountRow
from deerflow.persistence.spaces.model import SpaceRow

REVISION = "0032_storage_lifecycle"
PREVIOUS = "0031_storage_files"
TABLES = [SpaceAttachmentRow.__table__, SpaceMountRow.__table__, SpaceBackupRow.__table__]


def shape(connection):
    inspector = inspect(connection)
    return {
        t.name: {
            "columns": [(c["name"], str(c["type"]), c["nullable"], c["default"]) for c in inspector.get_columns(t.name)],
            "primary": inspector.get_pk_constraint(t.name)["constrained_columns"],
            "unique": sorted((u["name"], u["column_names"]) for u in inspector.get_unique_constraints(t.name)),
            "checks": sorted((c["name"], c["sqltext"]) for c in inspector.get_check_constraints(t.name)),
            "foreign": sorted((f["constrained_columns"], f["referred_table"], f["referred_columns"], f["options"]) for f in inspector.get_foreign_keys(t.name)),
        }
        for t in TABLES
    }


@pytest.mark.asyncio
async def test_lifecycle_upgrade_matches_cold_shape_and_keeps_previous_space(tmp_path):
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
async def test_lifecycle_wrong_shape_and_used_containment_never_get_erased(tmp_path):
    engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path / 'preserve.db'}")
    try:
        config = _get_alembic_config(engine)
        await asyncio.to_thread(command.upgrade, config, PREVIOUS)
        async with engine.begin() as c:
            await c.execute(text("CREATE TABLE storage_space_attachments (id TEXT PRIMARY KEY)"))
            await c.execute(text("INSERT INTO storage_space_attachments VALUES ('operator-data')"))
        with pytest.raises(Exception, match="storage schema"):
            await asyncio.to_thread(command.upgrade, config, REVISION)
        async with engine.begin() as c:
            assert (await c.execute(text("SELECT id FROM storage_space_attachments"))).scalar_one() == "operator-data"
            await c.execute(text("DROP TABLE storage_space_attachments"))
        await asyncio.to_thread(command.upgrade, config, REVISION)
        async with engine.begin() as c:
            await c.execute(text("INSERT INTO storage_space_attachments (id,incarnation,actor_kind,actor_id,host_id,phase,created_at) VALUES ('intent','execution','nonhuman','worker','host','fence_pending',CURRENT_TIMESTAMP)"))
        with pytest.raises(Exception, match="containment or backup"):
            await asyncio.to_thread(command.downgrade, config, PREVIOUS)
        async with engine.connect() as c:
            assert (await c.execute(text("SELECT phase FROM storage_space_attachments"))).scalar_one() == "fence_pending"
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_lifecycle_postgres_accepts_partial_ddl_and_refuses_backup_constraint_drift():
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
            await c.run_sync(Base.metadata.create_all, tables=[SpaceAttachmentRow.__table__])
            await c.run_sync(apply, module.upgrade)
            assert {t.name for t in TABLES} <= set(await c.run_sync(lambda conn: inspect(conn).get_table_names()))
            await c.execute(text("ALTER TABLE storage_space_backups DROP CONSTRAINT ck_storage_space_backups_size"))
            await c.execute(text("ALTER TABLE storage_space_backups ADD CONSTRAINT ck_storage_space_backups_size CHECK (size_bytes > 0)"))
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
