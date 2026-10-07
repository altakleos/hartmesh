"""Storage custody/grants survive upgrades and cannot be erased after first use."""

from __future__ import annotations

import asyncio
import os
import sqlite3
import uuid

import pytest
import sqlalchemy as sa
from alembic import command
from alembic.operations import Operations
from alembic.runtime.migration import MigrationContext
from alembic.script import ScriptDirectory
from sqlalchemy import inspect, text
from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.schema import CreateSchema, DropSchema

from deerflow.persistence.base import Base
from deerflow.persistence.bootstrap import _get_alembic_config
from deerflow.persistence.spaces.model import SpaceEventRow, SpaceGrantRow, SpaceRow

REVISION = "0030_storage_spaces"
PREVIOUS = "0029_shared_publications"
TABLES = [SpaceRow.__table__, SpaceGrantRow.__table__, SpaceEventRow.__table__]


@pytest.mark.asyncio
async def test_upgrade_from_last_release_matches_fresh_orm_tables(tmp_path):
    engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path / 'upgrade.db'}")
    fresh = create_async_engine(f"sqlite+aiosqlite:///{tmp_path / 'fresh.db'}")
    try:
        config = _get_alembic_config(engine)
        await asyncio.to_thread(command.upgrade, config, PREVIOUS)
        await asyncio.to_thread(command.upgrade, config, REVISION)
        async with fresh.begin() as connection:
            await connection.run_sync(Base.metadata.create_all, tables=TABLES)

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

        async with engine.connect() as connection:
            upgraded_shape = await connection.run_sync(shape)
        async with fresh.connect() as connection:
            assert upgraded_shape == await connection.run_sync(shape)
        await asyncio.to_thread(command.downgrade, config, PREVIOUS)
        async with engine.connect() as connection:
            remaining = await connection.run_sync(lambda c: inspect(c).get_table_names())
            assert not {t.name for t in TABLES} & set(remaining)
    finally:
        await engine.dispose()
        await fresh.dispose()


@pytest.mark.asyncio
@pytest.mark.parametrize("drift", ["server-default", "update-cascade"])
async def test_migration_refuses_authority_affecting_defaults_and_cascades(tmp_path, drift):
    engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path / 'drift.db'}")
    try:
        config = _get_alembic_config(engine)
        module = await asyncio.to_thread(lambda: ScriptDirectory.from_config(config).get_revision(REVISION).module)
        private = sa.MetaData()
        SpaceRow.__table__.to_metadata(private)
        grants = SpaceGrantRow.__table__.to_metadata(private)
        if drift == "server-default":
            grants.c.permissions.server_default = sa.DefaultClause("31")
        else:
            constraint = next(iter(grants.foreign_key_constraints))
            constraint.onupdate = "CASCADE"

        def apply(connection):
            Base.metadata.create_all(connection, tables=[SpaceRow.__table__, SpaceEventRow.__table__])
            grants.create(connection)
            with Operations.context(MigrationContext.configure(connection)):
                module.upgrade()

        with pytest.raises(RuntimeError, match="storage schema"):
            async with engine.begin() as connection:
                await connection.run_sync(apply)
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_downgrade_refuses_to_erase_resource_identity_and_membership(tmp_path):
    path = tmp_path / "used.db"
    engine = create_async_engine(f"sqlite+aiosqlite:///{path}")
    try:
        config = _get_alembic_config(engine)
        await asyncio.to_thread(command.upgrade, config, REVISION)
        with sqlite3.connect(path) as connection:
            connection.execute(
                "INSERT INTO storage_spaces (id, backing_handle, name, custody_kind, mode, status, generation, created_at, updated_at) "
                "VALUES ('stable', 'opaque', 'company', 'company', 'native', 'active', 1, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)"
            )
            connection.execute("INSERT INTO storage_space_grants VALUES ('stable', 'nonhuman', 'member', 1)")
            connection.commit()
        with pytest.raises(Exception, match="resource identity"):
            await asyncio.to_thread(command.downgrade, config, PREVIOUS)
        with sqlite3.connect(path) as connection:
            assert connection.execute("SELECT subject_id FROM storage_space_grants").fetchone() == ("member",)
            assert connection.execute("SELECT id FROM storage_spaces").fetchone() == ("stable",)
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_existing_fresh_schema_is_accepted_but_wrong_shapes_fail_closed(tmp_path):
    path = tmp_path / "existing.db"
    engine = create_async_engine(f"sqlite+aiosqlite:///{path}")
    try:
        config = _get_alembic_config(engine)
        await asyncio.to_thread(command.upgrade, config, PREVIOUS)
        async with engine.begin() as connection:
            await connection.run_sync(Base.metadata.create_all, tables=TABLES)
        await asyncio.to_thread(command.upgrade, config, REVISION)
        await asyncio.to_thread(command.downgrade, config, PREVIOUS)
        with sqlite3.connect(path) as connection:
            connection.execute("CREATE TABLE storage_spaces (id VARCHAR(32) PRIMARY KEY)")
        with pytest.raises(Exception, match="storage schema"):
            await asyncio.to_thread(command.upgrade, config, REVISION)
        with sqlite3.connect(path) as connection:
            assert connection.execute("PRAGMA table_info(storage_spaces)").fetchall()[0][1] == "id"
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_postgres_migration_accepts_fresh_model_schema_and_clean_partial_ddl():
    uri = os.environ.get("TEST_POSTGRES_URI")
    if not uri:
        pytest.skip("TEST_POSTGRES_URI is not configured")
    pytest.importorskip("asyncpg")
    url = make_url(uri).set(drivername="postgresql+asyncpg")
    ssl = {"ssl": False} if url.query.get("sslmode") == "disable" else {}
    url = url.difference_update_query(["sslmode"])
    admin = create_async_engine(url, connect_args=ssl)
    schema = "storage_migration_" + uuid.uuid4().hex
    engine = None
    try:
        async with admin.begin() as connection:
            await connection.execute(CreateSchema(schema))
        engine = create_async_engine(url, connect_args={**ssl, "server_settings": {"search_path": schema}})
        config = _get_alembic_config(engine, postgres_schema=schema)
        module = await asyncio.to_thread(lambda: ScriptDirectory.from_config(config).get_revision(REVISION).module)

        def apply(connection, operation):
            with Operations.context(MigrationContext.configure(connection)):
                operation()

        async with engine.begin() as connection:
            await connection.run_sync(Base.metadata.create_all, tables=TABLES)
            await connection.run_sync(apply, module.upgrade)
            await connection.run_sync(apply, module.downgrade)
            # A restart can finish a clean partial DDL attempt without dropping
            # pre-existing resources or stamping an incompatible table shape.
            await connection.run_sync(Base.metadata.create_all, tables=[SpaceRow.__table__])
            await connection.run_sync(apply, module.upgrade)
            assert {t.name for t in TABLES} <= set(await connection.run_sync(lambda c: inspect(c).get_table_names()))
            await connection.execute(text("ALTER TABLE storage_spaces DROP CONSTRAINT ck_storage_spaces_generation"))
            await connection.execute(text("ALTER TABLE storage_spaces ADD CONSTRAINT ck_storage_spaces_generation CHECK (generation >= 0)"))
            with pytest.raises(RuntimeError, match="storage schema"):
                await connection.run_sync(apply, module.upgrade)
    finally:
        if engine is not None:
            await engine.dispose()
        try:
            async with admin.begin() as connection:
                await connection.execute(DropSchema(schema, cascade=True, if_exists=True))
        finally:
            await admin.dispose()
