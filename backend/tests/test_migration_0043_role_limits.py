"""Role limits the deployer holds: the shape, and that a limit cannot be dropped while any stand."""

from __future__ import annotations

import asyncio
import sqlite3
from pathlib import Path

import pytest
from alembic import command
from sqlalchemy.ext.asyncio import create_async_engine

from deerflow.persistence.bootstrap import _get_alembic_config

_REVISION = "0043_role_limits"
_PREVIOUS = "0042_provider_keys"


def _tables(path: Path) -> set[str]:
    with sqlite3.connect(path) as connection:
        return {row[0] for row in connection.execute("SELECT name FROM sqlite_master WHERE type = 'table'")}


@pytest.mark.asyncio
async def test_migration_adds_the_table_and_downgrades_only_while_no_limit_holds(tmp_path: Path) -> None:
    path = tmp_path / "role-limits.db"
    engine = create_async_engine(f"sqlite+aiosqlite:///{path}")
    config = _get_alembic_config(engine)
    try:
        await asyncio.to_thread(command.upgrade, config, _PREVIOUS)
        assert "role_limits" not in _tables(path)
        await asyncio.to_thread(command.upgrade, config, _REVISION)
        with sqlite3.connect(path) as connection:
            columns = list(connection.execute("PRAGMA table_info(role_limits)"))
            assert {row[1] for row in columns} == {"issuer", "subject", "role", "limited_at"}
            assert [row[1] for row in columns if row[5]] == ["issuer", "subject"], "keyed by issuer and subject, like the refusal"
            connection.execute("INSERT INTO role_limits (issuer, subject, role, limited_at) VALUES ('https://login.example.com', 'sub-1', 'user', '2026-09-26 10:00:00')")
        # Dropping the table would hand a demoted administrator the role back at their next sign-in.
        with pytest.raises(Exception, match="Refusing to drop role_limits"):
            await asyncio.to_thread(command.downgrade, config, _PREVIOUS)
        with sqlite3.connect(path) as connection:
            assert connection.execute("SELECT count(*) FROM role_limits").fetchone() == (1,), "the limit stands"
            connection.execute("DELETE FROM role_limits")
        await asyncio.to_thread(command.downgrade, config, _PREVIOUS)
        assert "role_limits" not in _tables(path)
        # Idempotent on a database that already has the table (a fresh one gets it from create_all).
        await asyncio.to_thread(command.upgrade, config, _REVISION)
        await asyncio.to_thread(command.upgrade, config, _REVISION)
    finally:
        await engine.dispose()
