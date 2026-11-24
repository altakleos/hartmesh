"""Verify nullable Stop attribution across upgrade, rollback and re-upgrade."""

import asyncio
import os
from uuid import uuid4

import pytest
from alembic import command
from sqlalchemy import inspect, text
from sqlalchemy.ext.asyncio import create_async_engine
from support.postgres import asyncpg_test_url

from deerflow.persistence import bootstrap
from deerflow.persistence.postgres_schema import build_asyncpg_connect_args

PREVIOUS = "0056_upstream_adoption"
REVISION = "0057_run_cancel_actor_kind"


@pytest.mark.asyncio
@pytest.mark.parametrize("backend", ["sqlite", "postgres"])
@pytest.mark.parametrize("preexisting", [False, True], ids=["new-column", "preexisting-column"])
async def test_upgrade_and_downgrade_keep_historical_run_and_cancel_action(tmp_path, backend, preexisting):
    schema = None
    if backend == "sqlite":
        engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path / 'upgrade.db'}")
    else:
        uri = os.environ.get("TEST_POSTGRES_URI")
        if not uri:
            pytest.skip("requires TEST_POSTGRES_URI")
        schema = "cancel_actor_" + uuid4().hex
        engine = create_async_engine(asyncpg_test_url(uri), connect_args=build_asyncpg_connect_args(schema))
    config = bootstrap._get_alembic_config(engine, postgres_schema=schema or "")

    def run_columns(connection):
        return {column["name"] for column in inspect(connection).get_columns("runs")}

    try:
        if schema:
            async with engine.begin() as conn:
                await conn.execute(text(f'CREATE SCHEMA "{schema}"'))
        await asyncio.to_thread(command.upgrade, config, PREVIOUS)
        async with engine.begin() as conn:
            assert "cancel_actor_kind" not in await conn.run_sync(run_columns)
            await conn.execute(
                text("""INSERT INTO runs (
                    run_id,thread_id,user_id,status,multitask_strategy,metadata_json,kwargs_json,cancel_action,cancel_requested_at,
                    message_count,total_input_tokens,total_output_tokens,total_tokens,llm_call_count,
                    lead_agent_tokens,subagent_tokens,middleware_tokens,created_at,updated_at
                ) VALUES ('old','thread','alice','interrupted','reject','{}','{}','interrupt',CURRENT_TIMESTAMP,0,0,0,0,0,0,0,0,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)""")
            )
            if preexisting:
                await conn.execute(text("ALTER TABLE runs ADD COLUMN cancel_actor_kind VARCHAR(16)"))
                await conn.execute(text("UPDATE runs SET cancel_actor_kind='requester' WHERE run_id='old'"))
        await asyncio.to_thread(command.upgrade, config, REVISION)
        async with engine.begin() as conn:
            assert "cancel_actor_kind" in await conn.run_sync(run_columns)
            expected_role = "requester" if preexisting else None
            assert (await conn.execute(text("SELECT cancel_action,cancel_actor_kind FROM runs WHERE run_id='old'"))).one() == ("interrupt", expected_role)
            await conn.execute(text("UPDATE runs SET cancel_actor_kind='manager' WHERE run_id='old'"))
        await asyncio.to_thread(command.downgrade, config, PREVIOUS)
        async with engine.connect() as conn:
            assert "cancel_actor_kind" not in await conn.run_sync(run_columns)
            assert (await conn.execute(text("SELECT status,cancel_action FROM runs WHERE run_id='old'"))).one() == ("interrupted", "interrupt")
        await asyncio.to_thread(command.upgrade, config, REVISION)
        async with engine.connect() as conn:
            assert (await conn.execute(text("SELECT cancel_action,cancel_actor_kind FROM runs WHERE run_id='old'"))).one() == ("interrupt", None)
    finally:
        if schema:
            async with engine.begin() as conn:
                await conn.execute(text(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE'))
        await engine.dispose()
