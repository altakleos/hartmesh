"""Identities the deployer turned off, and the last sign-in: the shape, and that the refusals cannot be dropped while any stand."""

from __future__ import annotations

import asyncio
import sqlite3
from pathlib import Path

import pytest
from alembic import command
from sqlalchemy.ext.asyncio import create_async_engine

from deerflow.persistence.bootstrap import _get_alembic_config

_REVISION = "0040_account_access"
_PREVIOUS = "0039_users_oauth_issuer"


@pytest.mark.asyncio
async def test_migration_adds_the_table_and_the_column_and_downgrades_only_while_nobody_is_turned_off(tmp_path: Path) -> None:
    path = tmp_path / "account-access.db"
    engine = create_async_engine(f"sqlite+aiosqlite:///{path}")
    config = _get_alembic_config(engine)
    try:
        await asyncio.to_thread(command.upgrade, config, _PREVIOUS)
        with sqlite3.connect(path) as connection:
            assert "disabled_identities" not in {row[0] for row in connection.execute("SELECT name FROM sqlite_master WHERE type = 'table'")}
        await asyncio.to_thread(command.upgrade, config, _REVISION)
        with sqlite3.connect(path) as connection:
            assert {row[1] for row in connection.execute("PRAGMA table_info(disabled_identities)")} == {"issuer", "subject", "disabled_at"}
            assert [row[1] for row in connection.execute("PRAGMA table_info(disabled_identities)") if row[5]] == ["issuer", "subject"], "keyed by issuer and subject"
            assert "last_sign_in_at" in {row[1] for row in connection.execute("PRAGMA table_info(users)")}
            connection.execute("INSERT INTO disabled_identities (issuer, subject, disabled_at) VALUES ('https://login.example.com', 'sub-1', '2026-09-21 10:00:00')")
        with pytest.raises(Exception, match="Refusing to drop disabled_identities"):
            await asyncio.to_thread(command.downgrade, config, _PREVIOUS)
        with sqlite3.connect(path) as connection:
            assert connection.execute("SELECT count(*) FROM disabled_identities").fetchone() == (1,), "the refusal stands"
            connection.execute("DELETE FROM disabled_identities")
        await asyncio.to_thread(command.downgrade, config, _PREVIOUS)
        with sqlite3.connect(path) as connection:
            assert "disabled_identities" not in {row[0] for row in connection.execute("SELECT name FROM sqlite_master WHERE type = 'table'")}
            assert "last_sign_in_at" not in {row[1] for row in connection.execute("PRAGMA table_info(users)")}
        # Idempotent on a database that already has the table (a fresh one gets it from create_all).
        await asyncio.to_thread(command.upgrade, config, _REVISION)
        await asyncio.to_thread(command.upgrade, config, _REVISION)
    finally:
        await engine.dispose()
