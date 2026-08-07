"""Migration coverage for nullable accepted-invocation run facts."""

from __future__ import annotations

import asyncio
import sqlite3
from pathlib import Path

import pytest
import sqlalchemy as sa
from alembic import command
from sqlalchemy.ext.asyncio import create_async_engine

import deerflow.persistence.models  # noqa: F401
from deerflow.persistence.base import Base
from deerflow.persistence.bootstrap import _get_alembic_config, bootstrap_schema

_COLUMNS = {
    "origin_json",
    "principal_projection_json",
    "principal_projection_digest",
    "base_origin_digest",
    "accepted_context_digest",
    "agent_revision_json",
    "agent_revision_digest",
    "extension_generation",
    "decision_evidence_json",
}


def _columns(path: Path) -> set[str]:
    with sqlite3.connect(path) as connection:
        return {row[1] for row in connection.execute("PRAGMA table_info(runs)")}


@pytest.mark.asyncio
async def test_fresh_schema_contains_nullable_accepted_invocation_columns(tmp_path: Path) -> None:
    path = tmp_path / "fresh.db"
    engine = create_async_engine(f"sqlite+aiosqlite:///{path}")
    try:
        await bootstrap_schema(engine, backend="sqlite")
    finally:
        await engine.dispose()
    assert _COLUMNS <= _columns(path)


@pytest.mark.asyncio
async def test_upgrade_downgrade_and_reupgrade_are_nondestructive(tmp_path: Path) -> None:
    path = tmp_path / "upgrade.db"
    sync = sa.create_engine(f"sqlite:///{path}")
    Base.metadata.create_all(sync)
    with sync.begin() as connection:
        for column in _COLUMNS:
            connection.execute(sa.text(f"ALTER TABLE runs DROP COLUMN {column}"))
        connection.execute(sa.text("CREATE TABLE IF NOT EXISTS alembic_version (version_num VARCHAR(32) NOT NULL)"))
        connection.execute(sa.text("DELETE FROM alembic_version"))
        connection.execute(sa.text("INSERT INTO alembic_version (version_num) VALUES ('0010_run_cancel_request')"))
        connection.execute(
            sa.text(
                "INSERT INTO runs (run_id, thread_id, status, operation_kind, multitask_strategy, metadata_json, kwargs_json, "
                "message_count, total_input_tokens, total_output_tokens, total_tokens, llm_call_count, lead_agent_tokens, "
                "subagent_tokens, middleware_tokens, token_usage_by_model, created_at, updated_at) "
                "VALUES ('legacy', 'thread-1', 'success', 'run', 'reject', '{}', '{}', 0, 0, 0, 0, 0, 0, 0, 0, '{}', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)"
            )
        )
    sync.dispose()

    engine = create_async_engine(f"sqlite+aiosqlite:///{path}")
    config = _get_alembic_config(engine)
    try:
        await asyncio.to_thread(command.upgrade, config, "head")
        assert _COLUMNS <= _columns(path)
        with sqlite3.connect(path) as connection:
            values = connection.execute("SELECT origin_json, agent_revision_digest FROM runs WHERE run_id='legacy'").fetchone()
        assert values == (None, None)

        await asyncio.to_thread(command.downgrade, config, "0010_run_cancel_request")
        assert not (_COLUMNS & _columns(path))
        await asyncio.to_thread(command.upgrade, config, "head")
        assert _COLUMNS <= _columns(path)
        with sqlite3.connect(path) as connection:
            assert connection.execute("SELECT COUNT(*) FROM runs WHERE run_id='legacy'").fetchone()[0] == 1
    finally:
        await engine.dispose()
