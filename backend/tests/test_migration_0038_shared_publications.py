"""The Shared area's publication records: the shape, and that the account cannot be dropped once it exists."""

from __future__ import annotations

import asyncio
import sqlite3
from pathlib import Path

import pytest
from alembic import command
from sqlalchemy.ext.asyncio import create_async_engine

from deerflow.persistence.bootstrap import _get_alembic_config

_REVISION = "0041_email_released_from"
_PREVIOUS = "0037_merge_upstream_0018"


def _insert_one(connection: sqlite3.Connection, publication_id: str = "pub-1") -> None:
    connection.execute(
        "INSERT INTO shared_publications (publication_id, path, size, sha256, published_by, published_at) VALUES (?, 'Reports/august.pdf', 12, 'abc', 'owner-a', '2026-09-19 10:00:00')",
        (publication_id,),
    )


@pytest.mark.asyncio
async def test_migration_adds_the_record_and_downgrades_only_before_first_use(tmp_path: Path) -> None:
    path = tmp_path / "shared-publications.db"
    engine = create_async_engine(f"sqlite+aiosqlite:///{path}")
    config = _get_alembic_config(engine)
    try:
        await asyncio.to_thread(command.upgrade, config, _PREVIOUS)
        await asyncio.to_thread(command.upgrade, config, _REVISION)
        with sqlite3.connect(path) as connection:
            columns = {row[1] for row in connection.execute("PRAGMA table_info(shared_publications)")}
            assert columns == {
                "publication_id",
                "path",
                "size",
                "sha256",
                "published_by",
                "published_at",
                "from_thread_id",
                "from_path",
                "removed_by",
                "removed_at",
            }
            indexes = {row[1] for row in connection.execute("PRAGMA index_list(shared_publications)")}
            assert {"ix_shared_publications_path", "ix_shared_publications_published_by"} <= indexes

        # Nothing published yet, so the revision is still reversible.
        await asyncio.to_thread(command.downgrade, config, _PREVIOUS)
        with sqlite3.connect(path) as connection:
            assert connection.execute("SELECT name FROM sqlite_master WHERE name = 'shared_publications'").fetchone() is None

        await asyncio.to_thread(command.upgrade, config, _REVISION)
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_migration_refuses_to_erase_the_account_of_what_was_shared(tmp_path: Path) -> None:
    """A removed file's row is the only thing that outlives it; a downgrade must not take it."""
    path = tmp_path / "shared-publications-used.db"
    engine = create_async_engine(f"sqlite+aiosqlite:///{path}")
    config = _get_alembic_config(engine)
    try:
        await asyncio.to_thread(command.upgrade, config, _REVISION)
        with sqlite3.connect(path) as connection:
            _insert_one(connection)
            connection.commit()

        with pytest.raises(Exception, match="publication records"):
            await asyncio.to_thread(command.downgrade, config, _PREVIOUS)

        with sqlite3.connect(path) as connection:
            assert connection.execute("SELECT COUNT(*) FROM shared_publications").fetchone()[0] == 1
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_a_database_that_already_has_the_table_is_left_alone(tmp_path: Path) -> None:
    """A fresh database gets the table from ``create_all`` before being stamped; the revision must not fight it."""
    path = tmp_path / "shared-publications-fresh.db"
    engine = create_async_engine(f"sqlite+aiosqlite:///{path}")
    config = _get_alembic_config(engine)
    try:
        await asyncio.to_thread(command.upgrade, config, _PREVIOUS)
        with sqlite3.connect(path) as connection:
            connection.execute(
                "CREATE TABLE shared_publications (publication_id VARCHAR(64) PRIMARY KEY, path VARCHAR(1024) NOT NULL, "
                "size INTEGER NOT NULL, sha256 VARCHAR(64) NOT NULL, published_by VARCHAR(64) NOT NULL, "
                "published_at DATETIME NOT NULL, from_thread_id VARCHAR(64), from_path VARCHAR(4096), "
                "removed_by VARCHAR(64), removed_at DATETIME)"
            )
            _insert_one(connection)
            connection.commit()

        await asyncio.to_thread(command.upgrade, config, _REVISION)

        with sqlite3.connect(path) as connection:
            assert connection.execute("SELECT COUNT(*) FROM shared_publications").fetchone()[0] == 1
    finally:
        await engine.dispose()
