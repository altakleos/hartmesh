from __future__ import annotations

import asyncio
import sqlite3
from pathlib import Path

import pytest
from alembic import command
from sqlalchemy.ext.asyncio import create_async_engine

from deerflow.persistence.bootstrap import _get_alembic_config
from deerflow.persistence.run.model import RunRow

_REVISION = "0029_run_recovery_policy"
_PREVIOUS_REVISION = "0028_mcp_request_commitment"


def test_run_model_constrains_server_owned_recovery_policy() -> None:
    column = RunRow.__table__.c.recovery_policy
    assert column.nullable is False
    assert column.server_default is not None
    assert "terminalize_v1" in str(column.server_default.arg)
    constraints = {constraint.name for constraint in RunRow.__table__.constraints}
    assert "ck_runs_recovery_policy" in constraints
    assert "ck_runs_terminal_projection_authority_pair" in constraints
    assert "ck_runs_terminal_projection_authority_version" in constraints
    assert RunRow.__table__.c.terminal_projection_owner_worker_id.nullable
    assert RunRow.__table__.c.terminal_projection_active_state_version.nullable


@pytest.mark.asyncio
async def test_additive_head_backfills_legacy_runs_and_is_reversible_before_use(
    tmp_path: Path,
) -> None:
    path = tmp_path / "run-recovery-policy.db"
    engine = create_async_engine(f"sqlite+aiosqlite:///{path}")
    config = _get_alembic_config(engine)
    try:
        await asyncio.to_thread(command.upgrade, config, _PREVIOUS_REVISION)
        with sqlite3.connect(path) as connection:
            connection.execute(
                "INSERT INTO runs "
                "(run_id, thread_id, status, state_version, operation_kind, "
                "multitask_strategy, metadata_json, kwargs_json, message_count, "
                "total_input_tokens, total_output_tokens, total_tokens, "
                "llm_call_count, lead_agent_tokens, subagent_tokens, "
                "middleware_tokens, token_usage_by_model, created_at, updated_at) "
                "VALUES ('legacy-run', 'legacy-thread', 'error', 1, 'run', "
                "'reject', '{}', '{}', 0, 0, 0, 0, 0, 0, 0, 0, '{}', "
                "CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)"
            )
            connection.commit()

        await asyncio.to_thread(command.upgrade, config, _REVISION)
        with sqlite3.connect(path) as connection:
            row = connection.execute("SELECT recovery_policy, admission_cursor FROM runs WHERE run_id='legacy-run'").fetchone()
            assert row == ("terminalize_v1", None)
            assert connection.execute("SELECT last_cursor FROM run_admission_cursor_state WHERE singleton_id=1").fetchone() == (0,)
            table_sql = connection.execute("SELECT sql FROM sqlite_master WHERE type='table' AND name='runs'").fetchone()[0]
            assert "ck_runs_recovery_policy" in table_sql
            assert "ck_runs_admission_cursor_positive" in table_sql
            assert "ck_runs_terminal_projection_authority_pair" in table_sql
            assert "ck_runs_terminal_projection_authority_version" in table_sql

        await asyncio.to_thread(command.downgrade, config, _PREVIOUS_REVISION)
        with sqlite3.connect(path) as connection:
            columns = {row[1] for row in connection.execute("PRAGMA table_info(runs)")}
            assert "recovery_policy" not in columns
            assert "admission_cursor" not in columns
            assert "terminal_projection_owner_worker_id" not in columns
            assert "terminal_projection_active_state_version" not in columns
            assert connection.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='run_admission_cursor_state'").fetchone() is None
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_additive_head_refuses_to_erase_an_accepted_takeover_policy(
    tmp_path: Path,
) -> None:
    path = tmp_path / "run-recovery-policy-used.db"
    engine = create_async_engine(f"sqlite+aiosqlite:///{path}")
    config = _get_alembic_config(engine)
    try:
        await asyncio.to_thread(command.upgrade, config, _REVISION)
        with sqlite3.connect(path) as connection:
            connection.execute(
                "INSERT INTO runs "
                "(run_id, thread_id, status, state_version, operation_kind, "
                "recovery_policy, recovery_payload_json, multitask_strategy, "
                "metadata_json, kwargs_json, message_count, total_input_tokens, "
                "total_output_tokens, total_tokens, llm_call_count, "
                "lead_agent_tokens, subagent_tokens, middleware_tokens, "
                "token_usage_by_model, created_at, updated_at) "
                "VALUES (?, ?, 'error', 1, 'run', 'exact_two_takeover_v1', ?, "
                "'reject', '{}', '{}', 0, 0, 0, 0, 0, 0, 0, 0, '{}', "
                "CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)",
                (
                    "candidate-run",
                    "candidate-thread",
                    '{"version":1,"input_kind":"graph","input_value":{"messages":[]},"config":{"configurable":{"thread_id":"candidate-thread"}},"stream_modes":["values"],"stream_subgraphs":false,"interrupt_before":null,"interrupt_after":null}',
                ),
            )
            connection.commit()

        with pytest.raises(RuntimeError, match="run_recovery_policy_rollback_blocked"):
            await asyncio.to_thread(command.downgrade, config, _PREVIOUS_REVISION)
    finally:
        await engine.dispose()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "used_surface",
    ("admission_cursor", "cursor_state", "terminal_projection"),
)
async def test_additive_head_refuses_to_erase_semantic_admission_cursor_use(
    tmp_path: Path,
    used_surface: str,
) -> None:
    path = tmp_path / f"run-recovery-policy-{used_surface}.db"
    engine = create_async_engine(f"sqlite+aiosqlite:///{path}")
    config = _get_alembic_config(engine)
    try:
        await asyncio.to_thread(command.upgrade, config, _REVISION)
        with sqlite3.connect(path) as connection:
            if used_surface == "admission_cursor":
                connection.execute(
                    "INSERT INTO runs "
                    "(run_id, thread_id, status, state_version, operation_kind, "
                    "admission_cursor, multitask_strategy, metadata_json, "
                    "kwargs_json, message_count, total_input_tokens, "
                    "total_output_tokens, total_tokens, llm_call_count, "
                    "lead_agent_tokens, subagent_tokens, middleware_tokens, "
                    "token_usage_by_model, created_at, updated_at) "
                    "VALUES ('cursor-run', 'cursor-thread', 'error', 1, 'run', "
                    "1, 'reject', '{}', '{}', 0, 0, 0, 0, 0, 0, 0, 0, '{}', "
                    "CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)"
                )
            elif used_surface == "cursor_state":
                connection.execute("UPDATE run_admission_cursor_state SET last_cursor = 1 WHERE singleton_id = 1")
            else:
                connection.execute(
                    "INSERT INTO runs "
                    "(run_id, thread_id, status, state_version, operation_kind, "
                    "terminal_projection_owner_worker_id, "
                    "terminal_projection_active_state_version, "
                    "multitask_strategy, metadata_json, kwargs_json, "
                    "message_count, total_input_tokens, total_output_tokens, "
                    "total_tokens, llm_call_count, lead_agent_tokens, "
                    "subagent_tokens, middleware_tokens, token_usage_by_model, "
                    "created_at, updated_at) "
                    "VALUES ('terminal-run', 'terminal-thread', 'success', 2, "
                    "'run', 'worker-real', 1, 'reject', '{}', '{}', 0, 0, 0, "
                    "0, 0, 0, 0, 0, '{}', CURRENT_TIMESTAMP, "
                    "CURRENT_TIMESTAMP)"
                )
            connection.commit()

        with pytest.raises(
            RuntimeError,
            match="run_recovery_policy_rollback_blocked",
        ):
            await asyncio.to_thread(
                command.downgrade,
                config,
                _PREVIOUS_REVISION,
            )
    finally:
        await engine.dispose()
