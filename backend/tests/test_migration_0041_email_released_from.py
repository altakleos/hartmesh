"""The column that records what a released account's address used to be: the shape, and that a release survives the round trip."""

from __future__ import annotations

import asyncio
import sqlite3
from pathlib import Path

import pytest
from alembic import command
from sqlalchemy.ext.asyncio import create_async_engine

from deerflow.persistence.bootstrap import _get_alembic_config

_REVISION = "0041_email_released_from"
_PREVIOUS = "0040_account_access"


@pytest.mark.asyncio
async def test_migration_adds_the_column_and_a_downgrade_keeps_the_release_itself(tmp_path: Path) -> None:
    path = tmp_path / "email-released-from.db"
    engine = create_async_engine(f"sqlite+aiosqlite:///{path}")
    config = _get_alembic_config(engine)
    try:
        await asyncio.to_thread(command.upgrade, config, _PREVIOUS)
        with sqlite3.connect(path) as connection:
            assert "email_released_from" not in {row[1] for row in connection.execute("PRAGMA table_info(users)")}

        await asyncio.to_thread(command.upgrade, config, _REVISION)
        with sqlite3.connect(path) as connection:
            column = next(row for row in connection.execute("PRAGMA table_info(users)") if row[1] == "email_released_from")
            assert column[2].upper().startswith("VARCHAR"), "an address, not a flag"
            assert column[3] == 0, "nullable: almost every account has never been released"
            # A released account: the address it holds now is nobody's, and the column says what it held.
            connection.execute(
                "INSERT INTO users (id, email, system_role, created_at, needs_setup, token_version, email_released_from) VALUES ('user-1', 'released-user-1@released.example', 'user', '2026-09-22 10:00:00', 0, 0, 'pat@example.com')"
            )
            # And the address it released is free for another account to hold.
            connection.execute("INSERT INTO users (id, email, system_role, created_at, needs_setup, token_version) VALUES ('user-2', 'pat@example.com', 'user', '2026-09-22 10:00:00', 0, 0)")

        await asyncio.to_thread(command.downgrade, config, _PREVIOUS)
        with sqlite3.connect(path) as connection:
            assert "email_released_from" not in {row[1] for row in connection.execute("PRAGMA table_info(users)")}
            held = dict(connection.execute("SELECT id, email FROM users"))
        assert held == {"user-1": "released-user-1@released.example", "user-2": "pat@example.com"}, "the release survives; only the record of what it held is lost"

        # Idempotent on a database that already has the column (a fresh one gets it from create_all).
        await asyncio.to_thread(command.upgrade, config, _REVISION)
        await asyncio.to_thread(command.upgrade, config, _REVISION)
    finally:
        await engine.dispose()
