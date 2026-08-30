from __future__ import annotations

import asyncio
import sqlite3
from pathlib import Path

import pytest
from alembic import command
from sqlalchemy.ext.asyncio import create_async_engine

from deerflow.persistence.bootstrap import _get_alembic_config
from deerflow.persistence.models.run_event import RunEventRow

_REVISION = "0024_tool_receipt_idempotency"
_PREVIOUS_REVISION = "0023_agent_assembly_evidence"
_INDEX = "uq_run_events_receipt_idempotency"


def test_orm_metadata_declares_nullable_idempotency_column_and_unique_index() -> None:
    table = RunEventRow.__table__
    assert table.c.idempotency_key.nullable is True
    index = next(index for index in table.indexes if index.name == _INDEX)
    assert index.unique is True
    assert [column.name for column in index.columns] == ["run_id", "event_type", "idempotency_key"]


@pytest.mark.asyncio
async def test_sqlite_upgrade_enforces_receipt_key_but_allows_null_legacy_rows(tmp_path: Path) -> None:
    path = tmp_path / "tool-receipt-migration.db"
    engine = create_async_engine(f"sqlite+aiosqlite:///{path}")
    config = _get_alembic_config(engine)
    try:
        await asyncio.to_thread(command.upgrade, config, _PREVIOUS_REVISION)
        await asyncio.to_thread(command.upgrade, config, "head")
        with sqlite3.connect(path) as connection:
            columns = {row[1] for row in connection.execute("PRAGMA table_info(run_events)")}
            indexes = {row[1] for row in connection.execute("PRAGMA index_list(run_events)")}
            assert "idempotency_key" in columns
            assert _INDEX in indexes
            base = "INSERT INTO run_events (thread_id, run_id, event_type, category, content, event_metadata, seq, created_at, idempotency_key) VALUES (?, ?, ?, 'tool', '{}', '{}', ?, CURRENT_TIMESTAMP, ?)"
            connection.execute(base, ("thread-1", "run-1", "tool_receipt.started.v1", 1, None))
            connection.execute(base, ("thread-1", "run-1", "tool_receipt.started.v1", 2, None))
            connection.execute(base, ("thread-1", "run-1", "tool_receipt.started.v1", 3, "key-1"))
            with pytest.raises(sqlite3.IntegrityError):
                connection.execute(base, ("thread-2", "run-1", "tool_receipt.started.v1", 1, "key-1"))
            # The same key remains legal for another event type by contract.
            connection.execute(base, ("thread-1", "run-1", "tool_receipt.outcome.v1", 4, "key-1"))

        await asyncio.to_thread(command.downgrade, config, _PREVIOUS_REVISION)
        with sqlite3.connect(path) as connection:
            assert "idempotency_key" not in {row[1] for row in connection.execute("PRAGMA table_info(run_events)")}
        await asyncio.to_thread(command.upgrade, config, _REVISION)
    finally:
        await engine.dispose()
