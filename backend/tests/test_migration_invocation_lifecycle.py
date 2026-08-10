"""Migration coverage for authoritative invocation lifecycle evidence."""

from __future__ import annotations

import asyncio
import sqlite3
from pathlib import Path

import pytest
import sqlalchemy as sa
from alembic import command
from sqlalchemy.ext.asyncio import create_async_engine

import deerflow.persistence.models  # noqa: F401
from deerflow.persistence.bootstrap import _get_alembic_config, bootstrap_schema
from deerflow.persistence.run.model import (
    RunLifecycleCursorStateRow,
    RunLifecycleEventRow,
    RunRow,
)

_TABLES = {"run_lifecycle_events", "run_lifecycle_cursor_state"}
_INDEXES = {
    "ix_run_lifecycle_events_run_cursor",
    "ix_run_lifecycle_events_thread_cursor",
    "ix_run_lifecycle_events_owner_cursor",
}


def _tables(path: Path) -> set[str]:
    with sqlite3.connect(path) as connection:
        return {row[0] for row in connection.execute("SELECT name FROM sqlite_master WHERE type='table'")}


def _run_columns(path: Path) -> set[str]:
    with sqlite3.connect(path) as connection:
        return {row[1] for row in connection.execute("PRAGMA table_info(runs)")}


def test_orm_metadata_declares_lifecycle_tables_checks_and_indexes() -> None:
    assert "state_version" in RunRow.__table__.columns
    assert "ck_runs_state_version_nonnegative" in {constraint.name for constraint in RunRow.__table__.constraints}
    assert {constraint.name for constraint in RunLifecycleCursorStateRow.__table__.constraints} >= {
        "ck_run_lifecycle_cursor_singleton",
        "ck_run_lifecycle_cursor_nonnegative",
        "ck_run_lifecycle_pruned_range",
        "ck_run_lifecycle_retained_count_nonnegative",
    }
    assert {index.name for index in RunLifecycleEventRow.__table__.indexes} >= _INDEXES


@pytest.mark.asyncio
async def test_fresh_schema_has_lifecycle_tables_and_seed(tmp_path: Path) -> None:
    path = tmp_path / "fresh.db"
    engine = create_async_engine(f"sqlite+aiosqlite:///{path}")
    try:
        await bootstrap_schema(engine, backend="sqlite")
    finally:
        await engine.dispose()

    assert _TABLES <= _tables(path)
    assert "state_version" in _run_columns(path)
    with sqlite3.connect(path) as connection:
        assert connection.execute("SELECT singleton_id, last_cursor, pruned_through, retained_count FROM run_lifecycle_cursor_state").fetchall() == [(1, 0, 0, 0)]
        indexes = {row[1] for row in connection.execute("PRAGMA index_list(run_lifecycle_events)")}
        assert _INDEXES <= indexes
        triggers = {row[0] for row in connection.execute("SELECT name FROM sqlite_master WHERE type='trigger'")}
        assert {
            "trg_run_lifecycle_retained_insert",
            "trg_run_lifecycle_retained_delete",
        } <= triggers


@pytest.mark.asyncio
async def test_upgrade_downgrade_reupgrade_preserves_legacy_and_auxiliary_rows(
    tmp_path: Path,
) -> None:
    path = tmp_path / "upgrade.db"
    engine = create_async_engine(f"sqlite+aiosqlite:///{path}")
    config = _get_alembic_config(engine)
    try:
        await asyncio.to_thread(command.upgrade, config, "0012_invocation_idempotency")
    finally:
        await engine.dispose()

    sync = sa.create_engine(f"sqlite:///{path}")
    with sync.begin() as connection:
        for run_id, kind in (("legacy", "run"), ("aux", "checkpoint_write")):
            connection.execute(
                sa.text(
                    "INSERT INTO runs "
                    "(run_id, thread_id, status, operation_kind, multitask_strategy, "
                    "metadata_json, kwargs_json, message_count, total_input_tokens, "
                    "total_output_tokens, total_tokens, llm_call_count, lead_agent_tokens, "
                    "subagent_tokens, middleware_tokens, token_usage_by_model, created_at, updated_at) "
                    "VALUES (:run_id, :thread_id, 'success', :kind, 'reject', '{}', '{}', "
                    "0, 0, 0, 0, 0, 0, 0, 0, '{}', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)"
                ),
                {"run_id": run_id, "thread_id": f"thread-{run_id}", "kind": kind},
            )
    sync.dispose()

    engine = create_async_engine(f"sqlite+aiosqlite:///{path}")
    config = _get_alembic_config(engine)
    try:
        await asyncio.to_thread(command.upgrade, config, "head")
        with sqlite3.connect(path) as connection:
            assert connection.execute("SELECT run_id, state_version FROM runs ORDER BY run_id").fetchall() == [("aux", 0), ("legacy", 0)]
            assert connection.execute("SELECT count(*) FROM run_lifecycle_events").fetchone() == (0,)
            assert connection.execute("SELECT last_cursor, pruned_through FROM run_lifecycle_cursor_state").fetchone() == (0, 0)

        await asyncio.to_thread(command.downgrade, config, "0012_invocation_idempotency")
        assert not (_TABLES & _tables(path))
        assert "state_version" not in _run_columns(path)

        await asyncio.to_thread(command.upgrade, config, "head")
        with sqlite3.connect(path) as connection:
            assert connection.execute("SELECT run_id, state_version FROM runs ORDER BY run_id").fetchall() == [("aux", 0), ("legacy", 0)]
            assert connection.execute("SELECT singleton_id, last_cursor, pruned_through, retained_count FROM run_lifecycle_cursor_state").fetchall() == [(1, 0, 0, 0)]
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_lifecycle_integrity_upgrade_backfills_and_survives_round_trip(
    tmp_path: Path,
) -> None:
    path = tmp_path / "integrity-upgrade.db"
    engine = create_async_engine(f"sqlite+aiosqlite:///{path}")
    config = _get_alembic_config(engine)
    try:
        await asyncio.to_thread(
            command.upgrade,
            config,
            "0016_sandbox_execution_evidence",
        )
    finally:
        await engine.dispose()

    with sqlite3.connect(path) as connection:
        connection.execute(
            "INSERT INTO runs "
            "(run_id, thread_id, status, state_version, operation_kind, "
            "multitask_strategy, metadata_json, kwargs_json, message_count, "
            "total_input_tokens, total_output_tokens, total_tokens, llm_call_count, "
            "lead_agent_tokens, subagent_tokens, middleware_tokens, "
            "token_usage_by_model, created_at, updated_at) "
            "VALUES ('legacy-event-run', 'thread-legacy', 'pending', 1, 'run', "
            "'reject', '{}', '{}', 0, 0, 0, 0, 0, 0, 0, 0, '{}', "
            "CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)"
        )
        connection.execute(
            "INSERT INTO run_lifecycle_events "
            "(event_id, cursor, run_id, thread_id, owner_scope, lifecycle_type, "
            "state_version, status, payload_json) "
            "VALUES ('legacy-event', 1, 'legacy-event-run', 'thread-legacy', "
            "'anonymous', 'accepted', 1, 'pending', '{}')"
        )
        connection.execute("UPDATE run_lifecycle_cursor_state SET last_cursor = 1 WHERE singleton_id = 1")
        connection.commit()

    engine = create_async_engine(f"sqlite+aiosqlite:///{path}")
    config = _get_alembic_config(engine)
    try:
        await asyncio.to_thread(command.upgrade, config, "head")
        with sqlite3.connect(path) as connection:
            assert connection.execute("SELECT retained_count FROM run_lifecycle_cursor_state").fetchone() == (1,)

        await asyncio.to_thread(
            command.downgrade,
            config,
            "0016_sandbox_execution_evidence",
        )
        with sqlite3.connect(path) as connection:
            assert "retained_count" not in {row[1] for row in connection.execute("PRAGMA table_info(run_lifecycle_cursor_state)")}
            assert connection.execute("SELECT count(*) FROM run_lifecycle_events").fetchone() == (1,)

        await asyncio.to_thread(command.upgrade, config, "head")
        with sqlite3.connect(path) as connection:
            assert connection.execute("SELECT retained_count FROM run_lifecycle_cursor_state").fetchone() == (1,)
    finally:
        await engine.dispose()
