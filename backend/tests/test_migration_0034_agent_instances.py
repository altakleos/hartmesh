"""Release43 upgrades append persistent identities without changing its schema."""

import asyncio

import pytest
import sqlalchemy as sa
from alembic import command
from alembic.operations import Operations
from alembic.runtime.migration import MigrationContext
from alembic.script import ScriptDirectory
from sqlalchemy.ext.asyncio import create_async_engine

from deerflow.persistence.agent_instances.model import AgentDefinitionRevisionRow, AgentInstanceGrantRow, AgentInstanceRow
from deerflow.persistence.base import Base
from deerflow.persistence.bootstrap import _get_alembic_config
from deerflow.persistence.spaces.model import SpaceRow

REVISION = "0034_agent_instances"
PREVIOUS = "0033_storage_features"
TABLES = [row.__table__ for row in (AgentDefinitionRevisionRow, AgentInstanceRow, AgentInstanceGrantRow)]


def shape(connection):
    inspector = sa.inspect(connection)
    return {
        table.name: {
            "columns": [(c["name"], str(c["type"]), c["nullable"], c["default"]) for c in inspector.get_columns(table.name)],
            "primary": inspector.get_pk_constraint(table.name)["constrained_columns"],
            "unique": sorted((c["name"], c["column_names"]) for c in inspector.get_unique_constraints(table.name)),
            "checks": sorted((c["name"], c["sqltext"]) for c in inspector.get_check_constraints(table.name)),
            "foreign": sorted((c["constrained_columns"], c["referred_table"], c["referred_columns"], c["options"]) for c in inspector.get_foreign_keys(table.name)),
        }
        for table in TABLES
    }


@pytest.mark.asyncio
async def test_release43_upgrade_matches_fresh_orm_and_preserves_ancestry(tmp_path):
    engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path / 'upgrade.db'}")
    fresh = create_async_engine(f"sqlite+aiosqlite:///{tmp_path / 'fresh.db'}")
    try:
        config = _get_alembic_config(engine)
        script = ScriptDirectory.from_config(config)
        assert script.get_revision(REVISION).down_revision == PREVIOUS
        assert script.get_revision(PREVIOUS).down_revision == "0032_storage_lifecycle"
        await asyncio.to_thread(command.upgrade, config, PREVIOUS)
        async with engine.connect() as connection:
            before = await connection.run_sync(lambda c: {name: sa.inspect(c).get_columns(name) for name in sa.inspect(c).get_table_names() if name != "alembic_version"})
        await asyncio.to_thread(command.upgrade, config, REVISION)
        async with engine.connect() as connection:
            upgraded = await connection.run_sync(shape)
            after = await connection.run_sync(lambda c: {name: sa.inspect(c).get_columns(name) for name in before})
        assert {name: [(c["name"], str(c["type"]), c["nullable"], c["default"]) for c in columns] for name, columns in before.items()} == {
            name: [(c["name"], str(c["type"]), c["nullable"], c["default"]) for c in columns] for name, columns in after.items()
        }
        async with fresh.begin() as connection:
            await connection.run_sync(Base.metadata.create_all)
            assert upgraded == await connection.run_sync(shape)
        await asyncio.to_thread(command.downgrade, config, PREVIOUS)
    finally:
        await engine.dispose()
        await fresh.dispose()


@pytest.mark.asyncio
async def test_downgrade_keeps_adopted_definition_facts(tmp_path):
    engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path / 'used.db'}")
    try:
        config = _get_alembic_config(engine)
        await asyncio.to_thread(command.upgrade, config, REVISION)
        async with engine.begin() as connection:
            await connection.execute(AgentDefinitionRevisionRow.__table__.insert().values(revision="a" * 64, owner_id="alice", config={"name": "analyst"}, soul="retained"))
        with pytest.raises(RuntimeError, match="Refusing to erase"):
            await asyncio.to_thread(command.downgrade, config, PREVIOUS)
        async with engine.connect() as connection:
            assert await connection.scalar(sa.select(AgentDefinitionRevisionRow.soul)) == "retained"
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_existing_authority_column_default_is_refused(tmp_path):
    engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path / 'shape.db'}")
    try:
        config = _get_alembic_config(engine)
        await asyncio.to_thread(command.upgrade, config, PREVIOUS)
        module = ScriptDirectory.from_config(config).get_revision(REVISION).module
        private = sa.MetaData()
        SpaceRow.__table__.to_metadata(private)
        for table in TABLES:
            table.to_metadata(private)
        grants = private.tables["agent_instance_grants"]
        grants.c.permissions.server_default = sa.DefaultClause("7")

        def apply(connection):
            private.create_all(connection)
            with Operations.context(MigrationContext.configure(connection)):
                module.upgrade()

        with pytest.raises(RuntimeError, match="schema"):
            async with engine.begin() as connection:
                await connection.run_sync(apply)
    finally:
        await engine.dispose()
