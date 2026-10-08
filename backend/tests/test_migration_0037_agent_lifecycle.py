"""Additive frozen lifecycle DDL never erases used containment intents."""

import asyncio

import pytest
import sqlalchemy as sa
from _storage_spaces_test_support import ALICE, operation
from alembic import command
from alembic.operations import Operations
from alembic.runtime.migration import MigrationContext
from alembic.script import ScriptDirectory
from sqlalchemy.ext.asyncio import create_async_engine
from test_agent_instances import create
from test_agent_instances import instances as instances
from test_agent_lifecycle import lifecycle_fixture

from deerflow.persistence.agent_instances.model import AgentLifecycleRow
from deerflow.persistence.bootstrap import _get_alembic_config


@pytest.mark.asyncio
async def test_fresh_0037_preserves_existing_shape_and_matches_model(tmp_path):
    engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path / 'lifecycle.db'}")
    try:
        config = _get_alembic_config(engine)
        await asyncio.to_thread(command.upgrade, config, "0036_agent_instance_memory")
        async with engine.connect() as connection:
            before = await connection.run_sync(lambda c: {n: [(r["name"], str(r["type"]), r["nullable"], r["default"]) for r in sa.inspect(c).get_columns(n)] for n in sa.inspect(c).get_table_names() if n != "alembic_version"})
        await asyncio.to_thread(command.upgrade, config, "0037_agent_lifecycle")
        async with engine.connect() as connection:
            after = await connection.run_sync(lambda c: {n: [(r["name"], str(r["type"]), r["nullable"], r["default"]) for r in sa.inspect(c).get_columns(n)] for n in before})
            assert after == before
            columns = await connection.run_sync(lambda c: [(r["name"], str(r["type"]), r["nullable"]) for r in sa.inspect(c).get_columns("agent_lifecycle_operations")])
            assert columns == [(c.name, str(c.type), c.nullable) for c in AgentLifecycleRow.__table__.columns]
        await asyncio.to_thread(command.downgrade, config, "0036_agent_instance_memory")
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_0037_existing_shape_and_used_downgrade_refusal_on_both_sql_backends(instances):
    agents, authority, sf, lifecycle, attachments, provider, memory = await lifecycle_fixture(instances)
    agent = await create(agents)
    await lifecycle.change(actor=ALICE, instance_id=agent.id, expected_generation=agent.generation, operation_id=operation(), action="suspend")
    revision = ScriptDirectory.from_config(_get_alembic_config(sf.kw["bind"])).get_revision("0037_agent_lifecycle").module

    def check(connection):
        with Operations.context(MigrationContext.configure(connection)):
            revision.upgrade()
            with pytest.raises(RuntimeError, match="Refusing to erase"):
                revision.downgrade()

    async with sf.kw["bind"].begin() as connection:
        await connection.run_sync(check)
