"""The record of what ``disable`` held for an identity, which ``enable --restore-held`` reads and every ``enable`` discards."""

from __future__ import annotations

import pytest

from app.gateway.auth.repositories.sqlite import SQLiteUserRepository

ISSUER = "https://login.example.com/realms/tenant"


@pytest.fixture
async def users(tmp_path):
    from deerflow.persistence.engine import close_engine, get_session_factory, init_engine

    await init_engine("sqlite", url=f"sqlite+aiosqlite:///{tmp_path / 'holds.db'}", sqlite_dir=str(tmp_path))
    try:
        yield SQLiteUserRepository(get_session_factory())
    finally:
        await close_engine()


@pytest.mark.anyio
async def test_a_hold_is_recorded_once_per_target_and_listed_for_its_identity(users):
    await users.record_holds(ISSUER, "sub-1", [("schedule", "task-1", "user-1"), ("channel_binding", "conn-1", "user-1")])
    # A re-run of disable records what it holds again; nothing doubles.
    await users.record_holds(ISSUER + "/", "sub-1", [("schedule", "task-1", "user-1"), ("schedule", "task-2", "user-2")])
    await users.record_holds(ISSUER, "sub-other", [("schedule", "task-9", "user-9")])

    held = await users.list_holds(ISSUER, "sub-1")

    assert sorted(held) == [("channel_binding", "conn-1", "user-1"), ("schedule", "task-1", "user-1"), ("schedule", "task-2", "user-2")]


@pytest.mark.anyio
async def test_discarding_removes_the_named_targets_or_the_whole_record(users):
    await users.record_holds(ISSUER, "sub-1", [("schedule", "task-1", "user-1"), ("schedule", "task-2", "user-1"), ("channel_binding", "conn-1", "user-1")])
    await users.record_holds(ISSUER, "sub-other", [("schedule", "task-9", "user-9")])

    await users.discard_holds(ISSUER, "sub-1", [("schedule", "task-1")])
    assert sorted(await users.list_holds(ISSUER, "sub-1")) == [("channel_binding", "conn-1", "user-1"), ("schedule", "task-2", "user-1")]

    assert await users.discard_holds(ISSUER, "sub-1") == 2
    assert await users.list_holds(ISSUER, "sub-1") == []
    assert await users.list_holds(ISSUER, "sub-other") == [("schedule", "task-9", "user-9")]
