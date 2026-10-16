"""Operator lifetime counts use committed host resolution times, never intent age."""

from contextlib import asynccontextmanager
from datetime import UTC, datetime

import pytest
from _storage_spaces_test_support import home
from test_storage_spaces_maintenance import intent, outcome
from test_storage_spaces_service import space_file_storage as space_file_storage

from deerflow.persistence.spaces.files import SpaceFileOperationRow


@pytest.mark.asyncio
@pytest.mark.parametrize("complete", [False, True])
async def test_host_settlement_stamps_the_new_outcome_and_preserves_replay(space_file_storage, complete):
    files, sf, _catalog = space_file_storage
    space = await home(files)
    request = {"action": "remove", "path": "absent"} if complete else {"action": "write", "path": "absent", "sha256": "a" * 64}
    identifier = await intent(sf, space, request)
    async with sf() as session, session.begin():
        (await session.get(SpaceFileOperationRow, (space.id, identifier))).created_at = datetime(2020, 1, 1, tzinfo=UTC)
    before = datetime.now(UTC)
    assert await files.settle_interrupted(space_id=space.id, operation_id=identifier) == 1
    phase, result = await outcome(sf, space, identifier)
    timestamp = datetime.fromisoformat(result["host_settled_at"])
    assert phase == ("complete" if complete else "failed")
    assert timestamp.utcoffset() == UTC.utcoffset(timestamp)
    assert before <= timestamp <= datetime.now(UTC)
    assert await files.settle_interrupted(space_id=space.id, operation_id=identifier) == 0
    assert await outcome(sf, space, identifier) == (phase, result)


@pytest.mark.asyncio
async def test_failed_settlement_commit_publishes_neither_timestamp_nor_outcome(space_file_storage, monkeypatch):
    files, sf, _catalog = space_file_storage
    space = await home(files)
    identifier = await intent(sf, space, {"action": "remove", "path": "absent"})
    original = files.registry.host_locked

    @asynccontextmanager
    async def rollback(identifiers):
        async with original(identifiers) as locked:
            yield locked
            raise OSError("offline SQL outcome was not committed")

    with monkeypatch.context() as fault:
        fault.setattr(files.registry, "host_locked", rollback)
        assert await files.settle_interrupted(space_id=space.id, operation_id=identifier) == 0
    assert await outcome(sf, space, identifier) == ("pending", None)
    before = datetime.now(UTC)
    assert await files.settle_interrupted(space_id=space.id, operation_id=identifier) == 1
    assert datetime.fromisoformat((await outcome(sf, space, identifier))[1]["host_settled_at"]) >= before


@pytest.mark.asyncio
async def test_host_discard_stamps_only_its_transaction_and_never_redates_terminal_history(space_file_storage):
    from deerflow.spaces.host_release import _discard

    files, sf, _catalog = space_file_storage
    space = await home(files)
    identifier = await intent(sf, space, {"action": "remove", "path": "absent"})
    with pytest.raises(OSError):
        async with sf() as session, session.begin():
            await _discard(session, space.id)
            raise OSError("offline transaction rollback")
    assert await outcome(sf, space, identifier) == ("pending", None)
    before = datetime.now(UTC)
    async with sf() as session, session.begin():
        await _discard(session, space.id)
    phase, result = await outcome(sf, space, identifier)
    assert phase == "failed" and result["resolution"] == "host-discarded"
    assert before <= datetime.fromisoformat(result["host_settled_at"]) <= datetime.now(UTC)
    async with sf() as session, session.begin():
        await _discard(session, space.id)
    assert await outcome(sf, space, identifier) == (phase, result)
