from __future__ import annotations

import asyncio
import sqlite3
from pathlib import Path

import pytest
from alembic import command
from sqlalchemy.dialects import postgresql
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.schema import CreateTable

from deerflow.persistence.bootstrap import _get_alembic_config
from deerflow.persistence.subagent_batches.model import (
    SubagentBatchAttemptRow,
    SubagentBatchRow,
)

_REVISION = "0032_subagent_batch_evidence"
_PREVIOUS_REVISION = "0031_merge_upstream_0017"


def test_batch_model_boolean_defaults_render_for_postgres() -> None:
    """Fresh PostgreSQL schemas must use native Boolean default literals."""

    batch_ddl = str(
        CreateTable(SubagentBatchRow.__table__).compile(
            dialect=postgresql.dialect(),
        )
    ).lower()
    attempt_ddl = str(
        CreateTable(SubagentBatchAttemptRow.__table__).compile(
            dialect=postgresql.dialect(),
        )
    ).lower()

    assert "parent_cancellable boolean default false" in batch_ddl
    assert "consumed boolean default true" in attempt_ddl


def _legacy_batch(connection: sqlite3.Connection) -> None:
    connection.execute(
        "INSERT INTO subagent_batches "
        "(id, user_id, thread_id, run_id, tool_call_id, submission_key, title, "
        "subagent_type, status, total_items, max_live_items, max_running_items, "
        "max_attempts, execution_spec, created_at, updated_at) "
        "VALUES ('legacy-batch', 'user-1', 'thread-1', 'run-1', 'call-1', "
        "'run-1:call-1', 'Legacy', 'general-purpose', 'queued', 1, 1, 1, 2, "
        "'{}', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)"
    )
    connection.execute(
        "INSERT INTO subagent_batch_items "
        "(id, batch_id, item_key, position, prompt, status, attempt, "
        "result_truncated, created_at, updated_at) VALUES "
        "('legacy-item', 'legacy-batch', 'one', 0, 'private prompt', "
        "'pending', 0, 0, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)"
    )
    connection.commit()


@pytest.mark.asyncio
async def test_migration_marks_existing_rows_legacy_and_downgrades_before_use(
    tmp_path: Path,
) -> None:
    path = tmp_path / "batch-evidence.db"
    engine = create_async_engine(f"sqlite+aiosqlite:///{path}")
    config = _get_alembic_config(engine)
    try:
        await asyncio.to_thread(command.upgrade, config, _PREVIOUS_REVISION)
        with sqlite3.connect(path) as connection:
            _legacy_batch(connection)

        await asyncio.to_thread(command.upgrade, config, _REVISION)
        with sqlite3.connect(path) as connection:
            batch_columns = {row[1] for row in connection.execute("PRAGMA table_info(subagent_batches)")}
            item_columns = {row[1] for row in connection.execute("PRAGMA table_info(subagent_batch_items)")}
            assert {
                "schema_writer_version",
                "tenant_digest",
                "acceptance_json",
                "acceptance_digest",
                "execution_json",
                "execution_digest",
                "parent_tool_receipt_id",
                "cancel_epoch",
            } <= batch_columns
            assert {"request_digest", "lease_epoch", "active_attempt_id"} <= item_columns
            assert connection.execute("SELECT schema_writer_version, tenant_digest, acceptance_digest FROM subagent_batches WHERE id='legacy-batch'").fetchone() == (1, None, None)
            assert connection.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='subagent_batch_attempts'").fetchone() == ("subagent_batch_attempts",)

        await asyncio.to_thread(command.downgrade, config, _PREVIOUS_REVISION)
        with sqlite3.connect(path) as connection:
            columns = {row[1] for row in connection.execute("PRAGMA table_info(subagent_batches)")}
            assert "schema_writer_version" not in columns
            assert connection.execute("SELECT id FROM subagent_batches WHERE id='legacy-batch'").fetchone() == ("legacy-batch",)
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_migration_refuses_downgrade_after_bound_batch_use(tmp_path: Path) -> None:
    path = tmp_path / "batch-evidence-used.db"
    engine = create_async_engine(f"sqlite+aiosqlite:///{path}")
    config = _get_alembic_config(engine)
    try:
        await asyncio.to_thread(command.upgrade, config, _REVISION)
        with sqlite3.connect(path) as connection:
            connection.execute(
                "INSERT INTO subagent_batches "
                "(id, schema_writer_version, tenant_ref, tenant_digest, user_id, "
                "thread_id, submission_key, title, subagent_type, status, total_items, "
                "max_live_items, max_running_items, max_attempts, execution_spec, "
                "acceptance_json, acceptance_digest, execution_json, execution_digest, "
                "parent_invocation_digest, parent_assembly_fingerprint, "
                "parent_tool_receipt_id, parent_tool_attempt, subagent_catalog_digest, "
                "subagent_definition_digest, item_root_digest, accepted_at, "
                "parent_cancellable, cancel_epoch, created_at, updated_at) VALUES "
                "('bound', 2, 'tn_ref', ?, 'user-1', 'thread-1', 'submission', "
                "'Bound', 'general-purpose', 'queued', 1, 1, 1, 1, '{}', '{}', ?, "
                "'{}', ?, ?, ?, "
                "'tr_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa', "
                "1, ?, ?, ?, CURRENT_TIMESTAMP, 0, 0, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)",
                (
                    "a" * 64,
                    "b" * 64,
                    "c" * 64,
                    "d" * 64,
                    "e" * 64,
                    "f" * 64,
                    "1" * 64,
                    "2" * 64,
                ),
            )
            connection.commit()

        with pytest.raises(RuntimeError, match="subagent_batch_evidence_downgrade_blocked"):
            await asyncio.to_thread(command.downgrade, config, _PREVIOUS_REVISION)
    finally:
        await engine.dispose()
