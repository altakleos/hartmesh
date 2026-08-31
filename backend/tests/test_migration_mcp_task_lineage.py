from __future__ import annotations

import asyncio
import sqlite3
from pathlib import Path

import pytest
from alembic import command
from sqlalchemy.ext.asyncio import create_async_engine

from deerflow.persistence.bootstrap import _get_alembic_config

_REVISION = "0026_mcp_task_lineage"
_PREVIOUS_REVISION = "0025_tenant_identity"


@pytest.mark.asyncio
async def test_sqlite_upgrade_adds_mcp_task_lineage_indexes_and_additive_rollback(
    tmp_path: Path,
) -> None:
    path = tmp_path / "mcp-task-lineage-migration.db"
    engine = create_async_engine(f"sqlite+aiosqlite:///{path}")
    config = _get_alembic_config(engine)
    try:
        await asyncio.to_thread(command.upgrade, config, _PREVIOUS_REVISION)
        await asyncio.to_thread(command.upgrade, config, _REVISION)

        with sqlite3.connect(path) as connection:
            columns = {row[1]: row for row in connection.execute("PRAGMA table_info(mcp_tasks)")}
            assert {
                "schema_writer_version",
                "tenant_ref",
                "tenant_digest",
                "lineage_json",
                "lineage_digest",
                "parent_run_id",
                "parent_tool_receipt_id",
                "cancel_actor_ref",
                "cancel_reason_code",
            }.issubset(columns)
            assert columns["schema_writer_version"][3] == 1
            indexes = {row[1] for row in connection.execute("PRAGMA index_list(mcp_tasks)")}
            assert {
                "ix_mcp_tasks_tenant_parent_run",
                "ix_mcp_tasks_tenant_parent_receipt",
                "ix_mcp_tasks_tenant_notification_run",
            }.issubset(indexes)
            table_sql = connection.execute("SELECT sql FROM sqlite_master WHERE type = 'table' AND name = 'mcp_tasks'").fetchone()[0]
            assert "uq_mcp_tasks_tenant_lineage" in table_sql
            assert "uq_mcp_tasks_tenant_user_server_remote" in table_sql
            assert "ck_mcp_tasks_writer_lineage_required" in table_sql
            assert "ck_mcp_tasks_cancel_intent_pair" in table_sql
            assert "ck_mcp_tasks_writer_cancel_intent" in table_sql
            plan = " ".join(
                str(row)
                for row in connection.execute(
                    "EXPLAIN QUERY PLAN SELECT id FROM mcp_tasks WHERE tenant_digest = ? AND parent_run_id = ? ORDER BY created_at DESC, id DESC LIMIT 101",
                    ("a" * 64, "run-parent"),
                )
            )
            assert "ix_mcp_tasks_tenant_parent_run" in plan

        await asyncio.to_thread(command.downgrade, config, _PREVIOUS_REVISION)
        with sqlite3.connect(path) as connection:
            columns_after_rollback = {row[1] for row in connection.execute("PRAGMA table_info(mcp_tasks)")}
            assert "lineage_digest" in columns_after_rollback
            assert "tenant_digest" in columns_after_rollback
            rolled_back_table_sql = connection.execute("SELECT sql FROM sqlite_master WHERE type = 'table' AND name = 'mcp_tasks'").fetchone()[0]
            assert "uq_mcp_tasks_user_server_remote" in rolled_back_table_sql

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
                    created_at, updated_at
                ) VALUES (
                    'task-v2', 2, 'tenant-aaaaaaaaaaaaaaaa', :digest,
                    '{}', :digest, 'user-1', 'thread-1', 'reports', 'ordinary',
                    'remote-1', 'report', 'working', 0, '{}', 'none',
                    0, 0, 0, 0, 0, 0, 0, :now, :now
                )
                """,
                {
                    "digest": "a" * 64,
                    "now": "2026-08-31T00:00:00+00:00",
                },
            )
            connection.commit()

        with pytest.raises(RuntimeError, match="mcp_task_schema_writer_rollback_blocked"):
            await asyncio.to_thread(command.downgrade, config, _PREVIOUS_REVISION)

        with sqlite3.connect(path) as connection:
            version = connection.execute("SELECT version_num FROM alembic_version").fetchone()[0]
            assert version == _REVISION
    finally:
        await engine.dispose()
