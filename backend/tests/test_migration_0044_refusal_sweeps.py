"""The tables through which Gateway processes confirm a refusal: their shape, and a clean downgrade."""

from __future__ import annotations

import asyncio
import sqlite3
from pathlib import Path

import pytest
from alembic import command
from sqlalchemy.ext.asyncio import create_async_engine

from deerflow.persistence.bootstrap import _get_alembic_config

_REVISION = "0044_refusal_sweeps"
_PREVIOUS = "0043_role_limits"
_TABLES = {"refusal_checks", "gateway_processes", "surface_endings"}


def _tables(path: Path) -> set[str]:
    with sqlite3.connect(path) as connection:
        return {row[0] for row in connection.execute("SELECT name FROM sqlite_master WHERE type = 'table'")}


def _columns(path: Path, table: str) -> set[str]:
    with sqlite3.connect(path) as connection:
        return {row[1] for row in connection.execute(f"PRAGMA table_info({table})")}


@pytest.mark.asyncio
async def test_migration_adds_the_three_tables_and_downgrades_cleanly(tmp_path: Path) -> None:
    path = tmp_path / "refusal-sweeps.db"
    engine = create_async_engine(f"sqlite+aiosqlite:///{path}")
    config = _get_alembic_config(engine)
    try:
        await asyncio.to_thread(command.upgrade, config, _PREVIOUS)
        assert not _TABLES & _tables(path)
        await asyncio.to_thread(command.upgrade, config, _REVISION)
        assert _TABLES <= _tables(path)
        assert _columns(path, "refusal_checks") == {"id", "requested_at"}
        assert _columns(path, "gateway_processes") == {"process_id", "started_at", "heartbeat_at", "checked_through"}
        assert _columns(path, "surface_endings") == {"id", "process_id", "user_id", "surface", "count", "check_id", "ended_at"}
        # Nothing here outlives the processes that wrote it, so the downgrade drops it.
        await asyncio.to_thread(command.downgrade, config, _PREVIOUS)
        assert not _TABLES & _tables(path)
        # Idempotent on a database that already has the tables (a fresh one gets them from create_all).
        await asyncio.to_thread(command.upgrade, config, _REVISION)
        await asyncio.to_thread(command.downgrade, config, _PREVIOUS)
        await asyncio.to_thread(command.upgrade, config, _REVISION)
        await asyncio.to_thread(command.upgrade, config, _REVISION)
    finally:
        await engine.dispose()
