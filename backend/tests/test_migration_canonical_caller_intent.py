"""Migration coverage for canonical caller-intent replay evidence."""

from __future__ import annotations

import asyncio
import json
import sqlite3
from pathlib import Path

import pytest
import sqlalchemy as sa
from alembic import command
from sqlalchemy.ext.asyncio import create_async_engine

import deerflow.persistence.models  # noqa: F401
from app.runtime.idempotency import CanonicalCallerIntent
from deerflow.persistence.bootstrap import _get_alembic_config, bootstrap_schema
from deerflow.persistence.run.model import RunRow

_COLUMNS = {
    "caller_intent_json",
    "caller_intent_digest",
    "caller_intent_digest_version",
}
_CHECKS = {
    "ck_runs_caller_intent_set",
    "ck_runs_caller_intent_run_only",
    "ck_runs_caller_intent_digest_format",
    "ck_runs_caller_intent_digest_version_format",
}


def _columns(path: Path) -> set[str]:
    with sqlite3.connect(path) as connection:
        return {row[1] for row in connection.execute("PRAGMA table_info(runs)")}


def _schema_sql(path: Path) -> str:
    with sqlite3.connect(path) as connection:
        row = connection.execute("SELECT sql FROM sqlite_master WHERE type='table' AND name='runs'").fetchone()
    assert row is not None
    return row[0]


def test_orm_metadata_declares_caller_intent_columns_and_checks() -> None:
    table = RunRow.__table__
    assert _COLUMNS <= set(table.columns.keys())
    assert _CHECKS <= {constraint.name for constraint in table.constraints}


@pytest.mark.asyncio
async def test_fresh_schema_enforces_complete_run_only_caller_intent(tmp_path: Path) -> None:
    path = tmp_path / "fresh.db"
    engine = create_async_engine(f"sqlite+aiosqlite:///{path}")
    try:
        await bootstrap_schema(engine, backend="sqlite")
    finally:
        await engine.dispose()

    assert _COLUMNS <= _columns(path)
    schema = _schema_sql(path)
    assert all(name in schema for name in _CHECKS)

    with sqlite3.connect(path) as connection:
        with pytest.raises(sqlite3.IntegrityError):
            connection.execute(
                "INSERT INTO runs (run_id, thread_id, status, operation_kind, multitask_strategy, metadata_json, kwargs_json, "
                "message_count, total_input_tokens, total_output_tokens, total_tokens, llm_call_count, lead_agent_tokens, "
                "subagent_tokens, middleware_tokens, token_usage_by_model, created_at, updated_at, caller_intent_digest) "
                "VALUES ('partial', 'thread-1', 'success', 'run', 'reject', '{}', '{}', 0, 0, 0, 0, 0, 0, 0, 0, '{}', "
                "CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, ?)",
                ("a" * 64,),
            )
        caller = CanonicalCallerIntent({"input": {"messages": []}})
        with pytest.raises(sqlite3.IntegrityError):
            connection.execute(
                "INSERT INTO runs (run_id, thread_id, status, operation_kind, multitask_strategy, metadata_json, kwargs_json, "
                "message_count, total_input_tokens, total_output_tokens, total_tokens, llm_call_count, lead_agent_tokens, "
                "subagent_tokens, middleware_tokens, token_usage_by_model, created_at, updated_at, caller_intent_json, "
                "caller_intent_digest, caller_intent_digest_version) VALUES ('aux', 'thread-2', 'success', 'checkpoint_write', "
                "'reject', '{}', '{}', 0, 0, 0, 0, 0, 0, 0, 0, '{}', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, ?, ?, ?)",
                (json.dumps(caller.to_persisted()), caller.digest, caller.digest_version),
            )


@pytest.mark.asyncio
async def test_upgrade_keeps_old_rows_readable_and_new_rows_versioned_through_round_trip(tmp_path: Path) -> None:
    path = tmp_path / "upgrade.db"
    engine = create_async_engine(f"sqlite+aiosqlite:///{path}")
    config = _get_alembic_config(engine)
    try:
        await asyncio.to_thread(command.upgrade, config, "0013_invocation_lifecycle")
    finally:
        await engine.dispose()

    sync = sa.create_engine(f"sqlite:///{path}")
    with sync.begin() as connection:
        connection.execute(
            sa.text(
                "INSERT INTO runs (run_id, thread_id, status, state_version, operation_kind, multitask_strategy, metadata_json, kwargs_json, "
                "message_count, total_input_tokens, total_output_tokens, total_tokens, llm_call_count, lead_agent_tokens, subagent_tokens, "
                "middleware_tokens, token_usage_by_model, external_scope, external_key, request_digest, request_digest_version, created_at, updated_at) "
                "VALUES ('old-keyed', 'thread-old', 'success', 0, 'run', 'reject', '{}', '{}', 0, 0, 0, 0, 0, 0, 0, 0, '{}', "
                "'http:v1:sha256:" + "a" * 64 + "', 'raw:key', '" + "b" * 64 + "', 'sha256-canonical-json-v1', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)"
            )
        )
    sync.dispose()

    engine = create_async_engine(f"sqlite+aiosqlite:///{path}")
    config = _get_alembic_config(engine)
    try:
        await asyncio.to_thread(command.upgrade, config, "head")
        with sqlite3.connect(path) as connection:
            assert connection.execute("SELECT caller_intent_json, caller_intent_digest, caller_intent_digest_version FROM runs WHERE run_id='old-keyed'").fetchone() == (None, None, None)
            caller = CanonicalCallerIntent({"input": {"messages": []}})
            connection.execute(
                "INSERT INTO runs (run_id, thread_id, status, state_version, operation_kind, multitask_strategy, metadata_json, kwargs_json, "
                "message_count, total_input_tokens, total_output_tokens, total_tokens, llm_call_count, lead_agent_tokens, subagent_tokens, "
                "middleware_tokens, token_usage_by_model, caller_intent_json, caller_intent_digest, caller_intent_digest_version, created_at, updated_at) "
                "VALUES ('new-row', 'thread-new', 'success', 0, 'run', 'reject', '{}', '{}', 0, 0, 0, 0, 0, 0, 0, 0, '{}', ?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)",
                (json.dumps(caller.to_persisted()), caller.digest, caller.digest_version),
            )
            connection.commit()

        await asyncio.to_thread(command.downgrade, config, "0013_invocation_lifecycle")
        assert not (_COLUMNS & _columns(path))
        await asyncio.to_thread(command.upgrade, config, "head")
        with sqlite3.connect(path) as connection:
            rows = connection.execute("SELECT run_id, caller_intent_json, caller_intent_digest, caller_intent_digest_version FROM runs WHERE run_id IN ('old-keyed', 'new-row') ORDER BY run_id").fetchall()
        assert rows == [("new-row", None, None, None), ("old-keyed", None, None, None)]
    finally:
        await engine.dispose()
