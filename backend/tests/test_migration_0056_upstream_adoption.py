"""Upgrade the actual published Work-index head without rewriting its ancestry."""

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


@pytest.mark.asyncio
@pytest.mark.parametrize("backend", ["sqlite", "postgres"])
async def test_published_work_head_upgrade_preserves_rows_and_both_branches(tmp_path, backend):
    schema = None
    if backend == "sqlite":
        engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path / 'upgrade.db'}")
    else:
        uri = os.environ.get("TEST_POSTGRES_URI")
        if not uri:
            pytest.skip("requires TEST_POSTGRES_URI")
        schema = "upstream_merge_" + uuid4().hex
        engine = create_async_engine(asyncpg_test_url(uri), connect_args=build_asyncpg_connect_args(schema))
    config = bootstrap._get_alembic_config(engine, postgres_schema=schema or "")
    try:
        if schema:
            async with engine.begin() as conn:
                await conn.execute(text(f'CREATE SCHEMA "{schema}"'))
        await asyncio.to_thread(command.upgrade, config, "0053_work_operation_indexes")
        async with engine.begin() as conn:
            await conn.execute(text("INSERT INTO threads_meta (thread_id,user_id,status,metadata_json,created_at,updated_at) VALUES ('preserved','owner','idle','{}',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)"))
        await asyncio.to_thread(command.upgrade, config, "head")
        async with engine.connect() as conn:
            assert (await conn.execute(text("SELECT version_num FROM alembic_version"))).scalars().all() == ["0056_upstream_adoption"]
            assert (await conn.execute(text("SELECT user_id FROM threads_meta WHERE thread_id='preserved'"))).scalar_one() == "owner"

            def shape(sync):
                inspector = inspect(sync)
                return set(inspector.get_table_names()), {c["name"] for c in inspector.get_columns("projects")}, {i["name"] for i in inspector.get_indexes("agent_work_attempts")}

            tables, project_columns, work_indexes = await conn.run_sync(shape)
            assert {"tool_approvals", "thread_shares", "share_artifacts"} <= tables
            assert "memory" in project_columns
            assert "ix_agent_work_attempt_activation_id" in work_indexes
        await asyncio.to_thread(command.upgrade, config, "head")
    finally:
        if schema:
            async with engine.begin() as conn:
                await conn.execute(text(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE'))
        await engine.dispose()
