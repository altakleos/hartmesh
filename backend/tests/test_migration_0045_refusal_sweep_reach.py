"""What a process could not confirm ended, and which surfaces it cannot reach: two columns, and a clean downgrade."""

from __future__ import annotations

import asyncio
import sqlite3
from pathlib import Path

import pytest
from alembic import command
from sqlalchemy.ext.asyncio import create_async_engine

from deerflow.persistence.bootstrap import _get_alembic_config

_REVISION = "0045_refusal_sweep_reach"
_PREVIOUS = "0044_refusal_sweeps"


def _columns(path: Path, table: str) -> set[str]:
    with sqlite3.connect(path) as connection:
        return {row[1] for row in connection.execute(f"PRAGMA table_info({table})")}


@pytest.mark.asyncio
async def test_migration_adds_failed_and_unreached_and_downgrades_cleanly(tmp_path: Path) -> None:
    path = tmp_path / "refusal-sweep-reach.db"
    engine = create_async_engine(f"sqlite+aiosqlite:///{path}")
    config = _get_alembic_config(engine)
    try:
        await asyncio.to_thread(command.upgrade, config, _PREVIOUS)
        with sqlite3.connect(path) as connection:
            connection.execute("INSERT INTO surface_endings (process_id, user_id, surface, count, check_id, ended_at) VALUES ('gw', 'u', 'sse_streams', 1, 1, '2026-09-26 00:00:00')")
        await asyncio.to_thread(command.upgrade, config, _REVISION)
        assert "failed" in _columns(path, "surface_endings")
        assert "unreached" in _columns(path, "gateway_processes")
        with sqlite3.connect(path) as connection:
            assert connection.execute("SELECT failed FROM surface_endings").fetchall() == [(0,)], "an ending recorded before reads as confirmed"
        await asyncio.to_thread(command.downgrade, config, _PREVIOUS)
        assert "failed" not in _columns(path, "surface_endings")
        assert "unreached" not in _columns(path, "gateway_processes")
        # Idempotent on a database that already has the columns (a fresh one gets them from create_all).
        await asyncio.to_thread(command.upgrade, config, _REVISION)
        await asyncio.to_thread(command.downgrade, config, _PREVIOUS)
        await asyncio.to_thread(command.upgrade, config, _REVISION)
        await asyncio.to_thread(command.upgrade, config, _REVISION)
    finally:
        await engine.dispose()
