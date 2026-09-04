from __future__ import annotations

import asyncio
import sqlite3
from pathlib import Path

import pytest
from alembic import command
from sqlalchemy.ext.asyncio import create_async_engine

from deerflow.persistence.bootstrap import _get_alembic_config

_REVISION = "0036_execution_policy_state"
_PREVIOUS = "0035_batch_sandbox_evidence"


@pytest.mark.asyncio
async def test_migration_adds_policy_state_and_downgrades_only_when_unused(
    tmp_path: Path,
) -> None:
    path = tmp_path / "policy-state.db"
    engine = create_async_engine(f"sqlite+aiosqlite:///{path}")
    config = _get_alembic_config(engine)
    try:
        await asyncio.to_thread(command.upgrade, config, _PREVIOUS)
        await asyncio.to_thread(command.upgrade, config, _REVISION)
        with sqlite3.connect(path) as connection:
            columns = {row[1] for row in connection.execute("PRAGMA table_info(runs)")}
            assert {
                "execution_policy_state_json",
                "execution_policy_state_digest",
            } <= columns
            constraints = connection.execute(
                "SELECT sql FROM sqlite_master WHERE type = 'table' AND name = 'runs'",
            ).fetchone()[0]
            assert "ck_runs_execution_policy_state_pair" in constraints
            assert "ck_runs_execution_policy_state_run_only" in constraints
            assert "ck_runs_execution_policy_state_digest_format" in constraints
        await asyncio.to_thread(command.downgrade, config, _PREVIOUS)
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_migration_refuses_to_drop_used_policy_state(tmp_path: Path) -> None:
    path = tmp_path / "policy-state-used.db"
    engine = create_async_engine(f"sqlite+aiosqlite:///{path}")
    config = _get_alembic_config(engine)
    try:
        await asyncio.to_thread(command.upgrade, config, _REVISION)
        with sqlite3.connect(path) as connection:
            connection.execute(
                "INSERT INTO runs (run_id, thread_id, status, state_version, "
                "operation_kind, recovery_policy, multitask_strategy, metadata_json, kwargs_json, "
                "message_count, total_input_tokens, total_output_tokens, "
                "total_tokens, llm_call_count, lead_agent_tokens, "
                "subagent_tokens, middleware_tokens, token_usage_by_model, "
                "created_at, updated_at, execution_policy_state_json, "
                "execution_policy_state_digest) VALUES "
                "('run-1', 'thread-1', 'success', 1, 'run', 'terminalize_v1', 'reject', "
                "'{}', '{}', 0, 0, 0, 0, 0, 0, 0, 0, '{}', CURRENT_TIMESTAMP, "
                "CURRENT_TIMESTAMP, '{}', ?)",
                ("a" * 64,),
            )
            connection.commit()
        with pytest.raises(
            RuntimeError,
            match="execution_policy_state_downgrade_blocked",
        ):
            await asyncio.to_thread(command.downgrade, config, _PREVIOUS)
    finally:
        await engine.dispose()
