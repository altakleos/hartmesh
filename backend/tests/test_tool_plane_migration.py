"""Migration contract for governed tool-plane persistence."""

from __future__ import annotations

import pytest
import sqlalchemy as sa
from sqlalchemy.ext.asyncio import create_async_engine

import deerflow.persistence.models  # noqa: F401
from deerflow.persistence.bootstrap import bootstrap_schema


@pytest.mark.asyncio
async def test_fresh_sqlite_bootstrap_installs_tool_plane_schema_at_0034(
    tmp_path,
) -> None:
    engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path / 'migration.db'}")
    try:
        await bootstrap_schema(engine, backend="sqlite")
        async with engine.connect() as connection:
            tables = await connection.run_sync(lambda conn: set(sa.inspect(conn).get_table_names()))
            head = (await connection.execute(sa.text("SELECT version_num FROM alembic_version"))).scalar_one()

        assert head == "0036_execution_policy_state"
        assert {
            "tool_plane_scopes",
            "tool_plane_revisions",
            "tool_plane_revision_events",
            "tool_plane_overlay_compatibility",
        } <= tables
    finally:
        await engine.dispose()
