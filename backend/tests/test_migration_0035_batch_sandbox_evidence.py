from __future__ import annotations

import asyncio
import sqlite3
from pathlib import Path

import pytest
from alembic import command
from sqlalchemy.ext.asyncio import create_async_engine

from deerflow.persistence.bootstrap import _get_alembic_config

_REVISION = "0035_batch_sandbox_evidence"
_PREVIOUS_REVISION = "0034_tool_plane_revisions"


@pytest.mark.asyncio
async def test_migration_adds_attempt_sandbox_evidence_columns_and_downgrades_unused(
    tmp_path: Path,
) -> None:
    path = tmp_path / "batch-sandbox-evidence.db"
    engine = create_async_engine(f"sqlite+aiosqlite:///{path}")
    config = _get_alembic_config(engine)
    try:
        await asyncio.to_thread(command.upgrade, config, _PREVIOUS_REVISION)
        await asyncio.to_thread(command.upgrade, config, _REVISION)

        with sqlite3.connect(path) as connection:
            columns = {
                row[1]
                for row in connection.execute(
                    "PRAGMA table_info(subagent_batch_attempts)",
                )
            }
            assert {
                "accepted_material_request_json",
                "accepted_material_request_digest",
                "accepted_execution_evidence_json",
                "accepted_execution_evidence_digest",
                "accepted_sandbox_lifecycle_json",
            } <= columns

        await asyncio.to_thread(command.downgrade, config, _PREVIOUS_REVISION)
        with sqlite3.connect(path) as connection:
            columns = {
                row[1]
                for row in connection.execute(
                    "PRAGMA table_info(subagent_batch_attempts)",
                )
            }
            assert "accepted_execution_evidence_json" not in columns
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_migration_refuses_to_discard_persisted_sandbox_evidence(
    tmp_path: Path,
) -> None:
    path = tmp_path / "batch-sandbox-evidence-used.db"
    engine = create_async_engine(f"sqlite+aiosqlite:///{path}")
    config = _get_alembic_config(engine)
    try:
        await asyncio.to_thread(command.upgrade, config, _REVISION)
        with sqlite3.connect(path) as connection:
            connection.execute("PRAGMA foreign_keys = OFF")
            connection.execute(
                "INSERT INTO subagent_batch_attempts "
                "(id, batch_id, item_id, tenant_digest, attempt_number, "
                "lease_epoch, worker_ref, status, consumed, claimed_at, "
                "accepted_execution_evidence_json, "
                "accepted_execution_evidence_digest) VALUES "
                "('attempt-1', 'batch-1', 'item-1', ?, 1, 1, ?, 'terminal', "
                "1, CURRENT_TIMESTAMP, '{}', ?)",
                ("a" * 64, "b" * 64, "c" * 64),
            )
            connection.commit()

        with pytest.raises(
            RuntimeError,
            match="batch_sandbox_evidence_downgrade_blocked",
        ):
            await asyncio.to_thread(
                command.downgrade,
                config,
                _PREVIOUS_REVISION,
            )
    finally:
        await engine.dispose()
