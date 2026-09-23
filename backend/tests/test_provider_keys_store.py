"""Provider keys an administrator set in the product: the stored rows, their history and the migration.

The repository stores ciphertext and never sees a key; what it owns is the
row per catalog variable, the record of every add, replace and remove (who
and when, never the value), and the compare-and-swap a wrapping-key
rotation rewrites a row with.
"""

from __future__ import annotations

import asyncio
import sqlite3
from pathlib import Path

import pytest
import pytest_asyncio
from alembic import command
from sqlalchemy.ext.asyncio import create_async_engine

from deerflow.persistence.bootstrap import _get_alembic_config

_REVISION = "0042_provider_keys"
_PREVIOUS = "0041_email_released_from"


@pytest_asyncio.fixture
async def repo(tmp_path):
    from deerflow.persistence.engine import close_engine, get_session_factory, init_engine
    from deerflow.persistence.provider_keys import ProviderKeyRepository

    await init_engine("sqlite", url=f"sqlite+aiosqlite:///{tmp_path / 'keys.db'}", sqlite_dir=str(tmp_path))
    try:
        yield ProviderKeyRepository(get_session_factory())
    finally:
        await close_engine()


@pytest.mark.asyncio
async def test_a_put_adds_then_replaces_and_each_change_is_recorded_without_the_value(repo) -> None:
    assert await repo.stored() == {}

    assert await repo.put("OPENAI_API_KEY", "fernet:v1:first", actor_id="admin-1", actor_email="admin@example.com") == "added"
    assert await repo.put("OPENAI_API_KEY", "fernet:v1:second", actor_id="admin-2", actor_email="other@example.com") == "replaced"

    stored = await repo.stored()
    assert list(stored) == ["OPENAI_API_KEY"]
    row = stored["OPENAI_API_KEY"]
    assert row.ciphertext == "fernet:v1:second"
    assert row.changed_by == "other@example.com"
    assert row.changed_at.tzinfo is not None

    events = await repo.events()
    assert [(event["variable"], event["action"], event["actor_email"]) for event in events] == [
        ("OPENAI_API_KEY", "replaced", "other@example.com"),
        ("OPENAI_API_KEY", "added", "admin@example.com"),
    ]
    assert all(set(event) == {"event_id", "variable", "action", "actor_id", "actor_email", "occurred_at"} for event in events)
    assert "fernet" not in repr(events)


@pytest.mark.asyncio
async def test_a_remove_deletes_the_row_and_records_it_and_removing_nothing_records_nothing(repo) -> None:
    await repo.put("TAVILY_API_KEY", "fernet:v1:x", actor_id="admin-1", actor_email="admin@example.com")

    assert await repo.remove("TAVILY_API_KEY", actor_id="admin-1", actor_email="admin@example.com") is True
    assert await repo.stored() == {}
    assert await repo.remove("TAVILY_API_KEY", actor_id="admin-1", actor_email="admin@example.com") is False

    assert [event["action"] for event in await repo.events()] == ["removed", "added"]


@pytest.mark.asyncio
async def test_a_key_change_that_cannot_be_recorded_does_not_happen(repo, monkeypatch: pytest.MonkeyPatch) -> None:
    """The change and its record are one transaction: no change without its record."""
    import uuid

    fixed = uuid.UUID(int=1)
    monkeypatch.setattr(uuid, "uuid4", lambda: fixed)
    await repo.put("OPENAI_API_KEY", "fernet:v1:first", actor_id="admin-1", actor_email="admin@example.com")
    # A second record with the same id cannot be written, so neither can the change.
    with pytest.raises(Exception):
        await repo.put("OPENAI_API_KEY", "fernet:v1:second", actor_id="admin-1", actor_email="admin@example.com")
    assert (await repo.stored())["OPENAI_API_KEY"].ciphertext == "fernet:v1:first"
    with pytest.raises(Exception):
        await repo.remove("OPENAI_API_KEY", actor_id="admin-1", actor_email="admin@example.com")
    assert "OPENAI_API_KEY" in await repo.stored()


@pytest.mark.asyncio
async def test_a_rotation_rewrites_only_the_ciphertext_it_read(repo) -> None:
    """Re-wrapping at start must not overwrite a key an administrator replaced meanwhile."""
    await repo.put("OPENAI_API_KEY", "fernet:v1:old", actor_id="admin-1", actor_email="admin@example.com")
    before = (await repo.stored())["OPENAI_API_KEY"]

    assert await repo.rewrap("OPENAI_API_KEY", expected="fernet:v1:stale", ciphertext="fernet:v1:new") is False
    assert await repo.rewrap("OPENAI_API_KEY", expected="fernet:v1:old", ciphertext="fernet:v1:new") is True

    after = (await repo.stored())["OPENAI_API_KEY"]
    assert after.ciphertext == "fernet:v1:new"
    # Not a change of key: the history and the last-changed facts stay as they were.
    assert (after.changed_at, after.changed_by) == (before.changed_at, before.changed_by)
    assert [event["action"] for event in await repo.events()] == ["added"]


@pytest.mark.asyncio
async def test_the_event_list_is_bounded(repo) -> None:
    with pytest.raises(ValueError):
        await repo.events(limit=0)
    with pytest.raises(ValueError):
        await repo.events(limit=201)


@pytest.mark.asyncio
async def test_the_migration_adds_both_tables_and_downgrades_only_while_no_key_is_stored(tmp_path: Path) -> None:
    path = tmp_path / "provider-keys.db"
    engine = create_async_engine(f"sqlite+aiosqlite:///{path}")
    config = _get_alembic_config(engine)
    try:
        await asyncio.to_thread(command.upgrade, config, _PREVIOUS)
        await asyncio.to_thread(command.upgrade, config, _REVISION)
        with sqlite3.connect(path) as connection:
            keys = {row[1] for row in connection.execute("PRAGMA table_info(provider_keys)")}
            events = {row[1] for row in connection.execute("PRAGMA table_info(provider_key_events)")}
        assert keys == {"variable", "ciphertext", "changed_at", "changed_by"}
        assert events == {"event_id", "variable", "action", "actor_id", "actor_email", "occurred_at"}

        await asyncio.to_thread(command.downgrade, config, _PREVIOUS)
        await asyncio.to_thread(command.upgrade, config, _REVISION)
        with sqlite3.connect(path) as connection:
            connection.execute("INSERT INTO provider_keys (variable, ciphertext, changed_at, changed_by) VALUES ('OPENAI_API_KEY', 'fernet:v1:x', '2026-09-23 10:00:00', 'admin@example.com')")
            connection.commit()

        # Dropping the table would put every provider back on the key the
        # deployment was seeded with, which is the one thing this table exists to prevent.
        with pytest.raises(Exception, match="provider keys"):
            await asyncio.to_thread(command.downgrade, config, _PREVIOUS)
        with sqlite3.connect(path) as connection:
            assert connection.execute("SELECT COUNT(*) FROM provider_keys").fetchone()[0] == 1
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_a_database_that_already_has_the_tables_is_left_alone(tmp_path: Path) -> None:
    """A fresh database gets the tables from ``create_all`` before being stamped; the revision must not fight it."""
    path = tmp_path / "provider-keys-fresh.db"
    engine = create_async_engine(f"sqlite+aiosqlite:///{path}")
    config = _get_alembic_config(engine)
    try:
        await asyncio.to_thread(command.upgrade, config, _PREVIOUS)
        with sqlite3.connect(path) as connection:
            connection.execute("CREATE TABLE provider_keys (variable VARCHAR(64) PRIMARY KEY, ciphertext TEXT NOT NULL, changed_at DATETIME NOT NULL, changed_by VARCHAR(320) NOT NULL)")
            connection.execute(
                "CREATE TABLE provider_key_events (event_id VARCHAR(36) PRIMARY KEY, variable VARCHAR(64) NOT NULL, action VARCHAR(16) NOT NULL, actor_id VARCHAR(64) NOT NULL, actor_email VARCHAR(320), occurred_at DATETIME NOT NULL)"
            )
            connection.execute("INSERT INTO provider_keys VALUES ('OPENAI_API_KEY', 'fernet:v1:x', '2026-09-23 10:00:00', 'admin@example.com')")
            connection.commit()

        await asyncio.to_thread(command.upgrade, config, _REVISION)

        with sqlite3.connect(path) as connection:
            assert connection.execute("SELECT COUNT(*) FROM provider_keys").fetchone()[0] == 1
    finally:
        await engine.dispose()
