"""Trusted release callers retain uncertainty and prove absence outside SQL."""

import asyncio

import pytest
from _storage_spaces_test_support import ALICE, home, operation
from sqlalchemy import select, text
from test_storage_spaces_release import deleted
from test_storage_spaces_service import space_file_storage as space_file_storage

from deerflow.persistence.spaces.files import SpaceBackingRow, SpaceFileOperationRow
from deerflow.persistence.spaces.model import SpaceRow
from deerflow.spaces.backings import BackingUnavailable
from deerflow.spaces.recovery import SpaceRecovery
from deerflow.spaces.service import SpaceOperationPending


@pytest.mark.asyncio
async def test_retaining_unverified_release_keeps_binding_intent_and_claim(space_file_storage, monkeypatch):
    files, sf, catalog = space_file_storage
    space = await home(files)
    slot = await deleted(sf, space)
    volume = catalog.verified[slot]
    (volume.data_path / "retained").write_bytes(b"unverified data")
    pending_id = operation()
    async with sf() as session, session.begin():
        session.add(SpaceFileOperationRow(space_id=space.id, operation_id=pending_id, actor_kind="human", actor_id=ALICE.subject_id, generation=1, phase="pending", request={"action": "write", "path": "uncertain"}))
    claims = files._claim_snapshot()
    published = []

    async def publication(session):
        published.append(session)

    def unverified(*_args, **_kwargs):
        raise BackingUnavailable("The physical binding is unverified")

    monkeypatch.setattr(catalog, "verify", unverified)
    result = await SpaceRecovery(files).host_release(space.id, retain_unverified=True, publication=publication)
    assert result == {"complete": False, "slot_released": False}
    assert not published
    assert files._claim_snapshot() == claims
    assert (volume.data_path / "retained").read_bytes() == b"unverified data"
    async with sf() as session:
        assert await session.get(SpaceBackingRow, space.backing_handle) is not None
        pending = await session.get(SpaceFileOperationRow, (space.id, pending_id))
        assert pending.phase == "pending" and pending.result is None
        assert len((await session.scalars(select(SpaceFileOperationRow))).all()) == 1


async def inspect_completed_release_without_retaining_sql(sf, space):
    """An independent writer must acquire the parent before publication."""
    async with sf() as session:
        if session.get_bind().dialect.name == "sqlite":
            await session.execute(text("BEGIN IMMEDIATE"))
        else:
            await session.begin()
        row = await session.get(SpaceRow, space.id, with_for_update=True)
        assert row.status == "deleted"
        assert await session.get(SpaceBackingRow, space.backing_handle) is not None
        pending = (await session.scalars(select(SpaceFileOperationRow))).one()
        assert pending.phase == "pending" and pending.result["stage"] == "filesystem-complete"
        await session.rollback()


@pytest.mark.asyncio
async def test_final_proof_runs_after_checkpoint_outside_sql_before_publication(space_file_storage):
    files, sf, _catalog = space_file_storage
    space = await home(files)
    await deleted(sf, space)
    calls = []

    async def before_publication():
        await asyncio.wait_for(inspect_completed_release_without_retaining_sql(sf, space), 3)
        calls.append("proof")

    async def publication(session):
        assert session.in_transaction()
        assert calls == ["proof"]
        assert await session.get(SpaceBackingRow, space.backing_handle) is not None
        calls.append("publication")

    result = await SpaceRecovery(files).host_release(space.id, before_publication=before_publication, publication=publication)
    assert result["newly_reclaimed"]
    assert calls == ["proof", "publication"]
    async with sf() as session:
        assert await session.get(SpaceBackingRow, space.backing_handle) is None


@pytest.mark.asyncio
async def test_failed_final_proof_retains_checkpoint_and_is_repeated_on_retry(space_file_storage):
    files, sf, catalog = space_file_storage
    space = await home(files)
    slot = await deleted(sf, space)
    volume = catalog.verified[slot]
    original_roots = volume.root_inode, volume.control_inode
    calls = []

    async def before_publication():
        await asyncio.wait_for(inspect_completed_release_without_retaining_sql(sf, space), 3)
        calls.append("proof")
        if len(calls) == 1:
            raise RuntimeError("Native absence is unconfirmed")

    async def publication(session):
        calls.append("publication")

    recovery = SpaceRecovery(files)
    with pytest.raises(SpaceOperationPending):
        await recovery.host_release(space.id, before_publication=before_publication, publication=publication)
    assert calls == ["proof"]
    assert (volume.root_inode, volume.control_inode) != original_roots
    async with sf() as session:
        assert await session.get(SpaceBackingRow, space.backing_handle) is not None
        pending = (await session.scalars(select(SpaceFileOperationRow))).one()
        saved = pending.operation_id, pending.request, pending.result
        assert pending.phase == "pending" and pending.result["stage"] == "filesystem-complete"
    result = await recovery.host_release(space.id, before_publication=before_publication, publication=publication)
    assert result["newly_reclaimed"]
    assert calls == ["proof", "proof", "publication"]
    async with sf() as session:
        completed = await session.get(SpaceFileOperationRow, (space.id, saved[0]))
        assert completed.phase == "complete" and completed.request == saved[1]
        assert completed.result == {**saved[2], "outcome": "reclaimed"}
        assert await session.get(SpaceBackingRow, space.backing_handle) is None
