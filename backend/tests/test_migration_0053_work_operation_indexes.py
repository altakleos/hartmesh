"""Global operation fences stay indexed on upgraded and newly bootstrapped SQL."""

import asyncio
import os
from uuid import uuid4

import pytest
from alembic import command
from sqlalchemy import inspect, text
from sqlalchemy.ext.asyncio import create_async_engine
from support.postgres import asyncpg_test_url

from deerflow.persistence import bootstrap
from deerflow.persistence.agent_instances.work import AgentWorkAttemptRow, AgentWorkEventRow

REVISION = "0053_work_operation_indexes"
PREVIOUS = "0052_batch_continuation"
INDEXES = {
    "agent_work_attempts": ("ix_agent_work_attempt_activation_id", "activation_id"),
    "agent_work_events": ("ix_agent_work_event_operation_id", "operation_id"),
}


def test_global_operation_fences_have_leading_key_indexes_in_metadata():
    for model in (AgentWorkAttemptRow, AgentWorkEventRow):
        name, column = INDEXES[model.__tablename__]
        indexes = {index.name: index for index in model.__table__.indexes}
        assert name in indexes
        assert [value.name for value in indexes[name].columns] == [column]
        assert indexes[name].unique is False


@pytest.mark.asyncio
@pytest.mark.parametrize("backend", ["sqlite", "postgres"])
@pytest.mark.parametrize("fresh", [False, True])
async def test_upgrade_fresh_bootstrap_and_index_only_downgrade(tmp_path, backend, fresh):
    schema = None
    if backend == "postgres":
        uri = os.environ.get("TEST_POSTGRES_URI")
        if not uri:
            pytest.skip("requires TEST_POSTGRES_URI")
        schema = "work_operation_indexes_" + uuid4().hex
        engine = create_async_engine(asyncpg_test_url(uri), connect_args={"server_settings": {"search_path": schema}})
    else:
        engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path / 'migration.db'}")
    cfg = bootstrap._get_alembic_config(engine, postgres_schema=schema or "")
    try:
        if schema:
            async with engine.begin() as conn:
                await conn.execute(text(f'CREATE SCHEMA "{schema}"'))
        if fresh:
            await bootstrap.bootstrap_schema(engine, backend=backend, postgres_schema=schema or "")
        else:
            await asyncio.to_thread(command.upgrade, cfg, PREVIOUS)
            await asyncio.to_thread(command.upgrade, cfg, REVISION)
        for upgraded in (True, False, True):
            if not upgraded:
                await asyncio.to_thread(command.downgrade, cfg, PREVIOUS)
            async with engine.connect() as conn:
                for table, (name, column) in INDEXES.items():
                    indexes = await conn.run_sync(lambda sync: {index["name"]: index for index in inspect(sync).get_indexes(table)})
                    assert (name in indexes) is upgraded
                    if upgraded:
                        assert indexes[name]["column_names"] == [column] and not indexes[name]["unique"]
            if not upgraded:
                await asyncio.to_thread(command.upgrade, cfg, REVISION)
    finally:
        if schema:
            async with engine.begin() as conn:
                await conn.execute(text(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE'))
        await engine.dispose()
