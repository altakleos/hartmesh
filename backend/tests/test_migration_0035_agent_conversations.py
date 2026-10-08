"""The new binding table is additive and used tombstones cannot be erased."""

import asyncio

import pytest
import sqlalchemy as sa
from _storage_spaces_test_support import ALICE, operation
from alembic import command
from alembic.operations import Operations
from alembic.runtime.migration import MigrationContext
from alembic.script import ScriptDirectory
from sqlalchemy.ext.asyncio import create_async_engine
from test_agent_conversations import conversations
from test_agent_instances import create
from test_agent_instances import instances as instances

from deerflow.persistence.agent_instances.model import AgentConversationRow
from deerflow.persistence.bootstrap import _get_alembic_config

REVISION = "0035_agent_conversations"
PREVIOUS = "0034_agent_instances"


@pytest.mark.asyncio
async def test_upgrade_matches_orm_and_preserves_prior_tables(tmp_path):
    engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path / 'upgrade.db'}")
    try:
        config = _get_alembic_config(engine)
        script = ScriptDirectory.from_config(config)
        assert script.get_revision(REVISION).down_revision == PREVIOUS
        await asyncio.to_thread(command.upgrade, config, PREVIOUS)
        async with engine.connect() as connection:
            before = await connection.run_sync(lambda c: {name: [(r["name"], str(r["type"]), r["nullable"], r["default"]) for r in sa.inspect(c).get_columns(name)] for name in sa.inspect(c).get_table_names() if name != "alembic_version"})
        await asyncio.to_thread(command.upgrade, config, REVISION)
        async with engine.connect() as connection:
            after = await connection.run_sync(lambda c: {name: [(r["name"], str(r["type"]), r["nullable"], r["default"]) for r in sa.inspect(c).get_columns(name)] for name in before})
            assert before == after
            actual = await connection.run_sync(lambda c: [(r["name"], str(r["type"]), r["nullable"]) for r in sa.inspect(c).get_columns("agent_conversations")])
            assert actual == [(c.name, str(c.type), c.nullable) for c in AgentConversationRow.__table__.columns]
        await asyncio.to_thread(command.downgrade, config, PREVIOUS)
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_existing_sql_shape_validates_and_used_tombstone_refuses_downgrade(instances):
    agents, authority, threads, sf = await conversations(instances)
    agent = await create(agents)
    chat = await authority.create(actor=ALICE, instance_id=agent.id, creation_id=operation())
    await threads.delete(chat["thread_id"], user_id="alice")
    module = ScriptDirectory.from_config(_get_alembic_config(sf.kw["bind"])).get_revision(REVISION).module

    def apply(connection):
        with Operations.context(MigrationContext.configure(connection)):
            module.upgrade()
            with pytest.raises(RuntimeError, match="Refusing to erase"):
                module.downgrade()

    async with sf.kw["bind"].begin() as connection:
        await connection.run_sync(apply)
    assert await authority.binding(chat["thread_id"]) == agent.id
