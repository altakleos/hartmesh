"""Additive memory DDL preserves prior schema and refuses used downgrades."""

import asyncio

import pytest
import sqlalchemy as sa
from _storage_spaces_test_support import ALICE
from alembic import command
from alembic.operations import Operations
from alembic.runtime.migration import MigrationContext
from alembic.script import ScriptDirectory
from sqlalchemy.ext.asyncio import create_async_engine
from test_agent_instance_memory import setup_memory
from test_agent_instances import create
from test_agent_instances import instances as instances

from deerflow.persistence.agent_instances.model import AgentMemoryRow
from deerflow.persistence.bootstrap import _get_alembic_config


@pytest.mark.asyncio
async def test_fresh_upgrade_keeps_previous_tables_and_matches_model(tmp_path):
    engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path / 'memory.db'}")
    try:
        config = _get_alembic_config(engine)
        await asyncio.to_thread(command.upgrade, config, "0035_agent_conversations")
        async with engine.connect() as connection:
            before = await connection.run_sync(lambda c: {n: [(r["name"], str(r["type"]), r["nullable"], r["default"]) for r in sa.inspect(c).get_columns(n)] for n in sa.inspect(c).get_table_names() if n != "alembic_version"})
        await asyncio.to_thread(command.upgrade, config, "0036_agent_instance_memory")
        async with engine.connect() as connection:
            after = await connection.run_sync(lambda c: {n: [(r["name"], str(r["type"]), r["nullable"], r["default"]) for r in sa.inspect(c).get_columns(n)] for n in before})
            assert after == before
            columns = await connection.run_sync(lambda c: [(r["name"], str(r["type"]), r["nullable"]) for r in sa.inspect(c).get_columns("agent_instance_memory")])
            assert columns == [(c.name, str(c.type), c.nullable) for c in AgentMemoryRow.__table__.columns]
        await asyncio.to_thread(command.downgrade, config, "0035_agent_conversations")
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_existing_shape_and_used_memory_cannot_be_downgraded(instances):
    agents, authority, threads, sf, memory = await setup_memory(instances)
    agent = await create(agents)
    await memory.bind(actor=ALICE, instance_id=agent.id)
    revision = ScriptDirectory.from_config(_get_alembic_config(sf.kw["bind"])).get_revision("0036_agent_instance_memory").module

    def check(connection):
        with Operations.context(MigrationContext.configure(connection)):
            revision.upgrade()
            with pytest.raises(RuntimeError, match="Refusing to erase"):
                revision.downgrade()

    async with sf.kw["bind"].begin() as connection:
        await connection.run_sync(check)
