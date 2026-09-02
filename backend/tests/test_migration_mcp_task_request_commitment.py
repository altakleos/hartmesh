from __future__ import annotations

import asyncio
import sqlite3
from pathlib import Path

import pytest
from alembic import command
from sqlalchemy.ext.asyncio import create_async_engine

from deerflow.persistence.bootstrap import _get_alembic_config
from deerflow.persistence.mcp_tasks.model import McpTaskRow

_REVISION = "0028_mcp_request_commitment"
_PREVIOUS_REVISION = "0027_multi_gateway_topology"
_STALE_INDEXES = {
    "ix_mcp_tasks_cancel_due",
    "ix_mcp_tasks_due",
    "ix_mcp_tasks_notification_due",
    "ix_mcp_tasks_thread_created",
}


def test_orm_metadata_requires_private_commitment_for_writer_v3() -> None:
    table = McpTaskRow.__table__

    assert table.c.request_commitment_version.nullable is True
    assert table.c.request_commitment_key_id.nullable is True
    assert table.c.request_commitment_digest.nullable is True
    constraints = {constraint.name for constraint in table.constraints}
    assert "ck_mcp_tasks_request_commitment_triple" in constraints
    assert "ck_mcp_tasks_writer_request_commitment" in constraints
    assert "ck_mcp_tasks_request_commitment_shape" in constraints


@pytest.mark.asyncio
async def test_additive_head_adds_commitment_and_replaces_stale_indexes(
    tmp_path: Path,
) -> None:
    path = tmp_path / "mcp-request-commitment.db"
    engine = create_async_engine(f"sqlite+aiosqlite:///{path}")
    config = _get_alembic_config(engine)
    try:
        await asyncio.to_thread(command.upgrade, config, _PREVIOUS_REVISION)
        with sqlite3.connect(path) as connection:
            indexes = {row[1] for row in connection.execute("PRAGMA index_list(mcp_tasks)")}
            assert _STALE_INDEXES <= indexes

        await asyncio.to_thread(command.upgrade, config, _REVISION)
        with sqlite3.connect(path) as connection:
            columns = {row[1]: row for row in connection.execute("PRAGMA table_info(mcp_tasks)")}
            assert {
                "request_commitment_version",
                "request_commitment_key_id",
                "request_commitment_digest",
            } <= columns.keys()
            indexes = {row[1] for row in connection.execute("PRAGMA index_list(mcp_tasks)")}
            assert not (_STALE_INDEXES & indexes)
            table_sql = connection.execute("SELECT sql FROM sqlite_master WHERE type = 'table' AND name = 'mcp_tasks'").fetchone()[0]
            assert "ck_mcp_tasks_request_commitment_triple" in table_sql
            assert "ck_mcp_tasks_writer_request_commitment" in table_sql
            assert "ck_mcp_tasks_request_commitment_shape" in table_sql

        await asyncio.to_thread(command.downgrade, config, _PREVIOUS_REVISION)
        with sqlite3.connect(path) as connection:
            columns = {row[1] for row in connection.execute("PRAGMA table_info(mcp_tasks)")}
            assert "request_commitment_digest" not in columns
            indexes = {row[1] for row in connection.execute("PRAGMA index_list(mcp_tasks)")}
            assert _STALE_INDEXES <= indexes
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_additive_head_refuses_downgrade_after_writer_v3_row(
    tmp_path: Path,
) -> None:
    path = tmp_path / "mcp-request-commitment-downgrade.db"
    engine = create_async_engine(f"sqlite+aiosqlite:///{path}")
    config = _get_alembic_config(engine)
    try:
        await asyncio.to_thread(command.upgrade, config, _REVISION)
        with sqlite3.connect(path) as connection:
            connection.execute(
                """
                INSERT INTO mcp_tasks (
                    id, schema_writer_version, tenant_ref, tenant_digest,
                    lineage_json, lineage_digest, user_id, thread_id,
                    server_name, driver_name, remote_task_id, task_name, status,
                    result_truncated, driver_data, notification_status,
                    event_version, notified_version, dispatch_attempt,
                    notification_attempt_count, poll_attempt_count,
                    consecutive_poll_error_count, cancel_attempt_count,
                    request_commitment_version, request_commitment_key_id,
                    request_commitment_digest, created_at, updated_at
                ) VALUES (
                    'task-v3', 3, 'tenant-aaaaaaaaaaaaaaaa', :digest,
                    '{}', :digest, 'user-1', 'thread-1', 'reports', 'ordinary',
                    'remote-1', 'report', 'working', 0, '{}', 'none',
                    0, 0, 0, 0, 0, 0, 0, 1, 'v1', :digest, :now, :now
                )
                """,
                {
                    "digest": "a" * 64,
                    "now": "2026-09-01T00:00:00+00:00",
                },
            )
            connection.commit()

        with pytest.raises(RuntimeError, match="mcp_task_request_commitment_rollback_blocked"):
            await asyncio.to_thread(command.downgrade, config, _PREVIOUS_REVISION)
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_additive_head_audits_existing_schedule_version_shape(
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
) -> None:
    path = tmp_path / "schedule-version-drift.db"
    engine = create_async_engine(f"sqlite+aiosqlite:///{path}")
    config = _get_alembic_config(engine)
    try:
        await asyncio.to_thread(command.upgrade, config, "0026_mcp_task_lineage")
        with sqlite3.connect(path) as connection:
            connection.execute("ALTER TABLE scheduled_tasks ADD COLUMN schedule_version TEXT")
            connection.commit()
        await asyncio.to_thread(command.stamp, config, _PREVIOUS_REVISION)

        with caplog.at_level("WARNING"):
            await asyncio.to_thread(command.upgrade, config, _REVISION)

        assert any("scheduled_tasks.schedule_version" in record.getMessage() and "drifts from the model definition" in record.getMessage() for record in caplog.records)
    finally:
        await engine.dispose()
