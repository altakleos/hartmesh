"""What ``disable`` held for an identity: the shape, and that dropping the record leaves what it held off."""

from __future__ import annotations

import asyncio
import sqlite3
from pathlib import Path

import pytest
from alembic import command
from sqlalchemy.ext.asyncio import create_async_engine

from deerflow.persistence.bootstrap import _get_alembic_config

_REVISION = "0047_identity_holds"
_PREVIOUS = "0046_mcp_task_disable_reason"


def _tables(path: Path) -> set[str]:
    with sqlite3.connect(path) as connection:
        return {row[0] for row in connection.execute("SELECT name FROM sqlite_master WHERE type = 'table'")}


@pytest.mark.asyncio
async def test_migration_adds_the_record_keyed_by_identity_and_what_it_held(tmp_path: Path) -> None:
    path = tmp_path / "identity-holds.db"
    engine = create_async_engine(f"sqlite+aiosqlite:///{path}")
    config = _get_alembic_config(engine)
    try:
        await asyncio.to_thread(command.upgrade, config, _PREVIOUS)
        assert "identity_holds" not in _tables(path)
        await asyncio.to_thread(command.upgrade, config, _REVISION)
        with sqlite3.connect(path) as connection:
            columns = list(connection.execute("PRAGMA table_info(identity_holds)"))
            assert {row[1] for row in columns} == {"issuer", "subject", "kind", "target_id", "user_id", "held_at"}
            assert [row[1] for row in sorted(columns, key=lambda row: row[5]) if row[5]] == ["issuer", "subject", "kind", "target_id"]
            connection.execute("INSERT INTO identity_holds (issuer, subject, kind, target_id, user_id, held_at) VALUES ('https://login.example.com', 'sub-1', 'schedule', 'task-1', 'user-1', '2026-09-26 10:00:00')")
        # The record only says what a restore may turn back on; without it
        # everything it held stays off, which is the safe direction.
        await asyncio.to_thread(command.downgrade, config, _PREVIOUS)
        assert "identity_holds" not in _tables(path)
        # Idempotent on a database that already has the table (a fresh one gets it from create_all).
        await asyncio.to_thread(command.upgrade, config, _REVISION)
        await asyncio.to_thread(command.upgrade, config, _REVISION)
    finally:
        await engine.dispose()
