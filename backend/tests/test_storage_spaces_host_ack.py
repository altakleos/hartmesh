"""Confirmed host replay repairs an old charge without touching new ownership."""

import pytest
from _storage_spaces_test_support import ALICE, home, operation
from sqlalchemy import event, select
from sqlalchemy.orm import Session
from test_storage_spaces_release import deleted
from test_storage_spaces_service import space_file_storage as space_file_storage

from deerflow.persistence.spaces.files import SpaceBackingRow, SpaceFileOperationRow
from deerflow.persistence.spaces.model import SpaceEventRow
from deerflow.spaces.recovery import SpaceRecovery
from deerflow.spaces.registry import SpaceRegistry
from deerflow.spaces.service import SpaceFiles, SpaceOperationPending


@pytest.mark.asyncio
@pytest.mark.parametrize(("race", "damage"), [(None, None), ("sql-only", None), ("observed", None), (None, "generation"), (None, "operation-id"), (None, "checkpoint"), (None, "deletion")])
async def test_host_replay_after_lost_commit_ack_repairs_only_a_confirmed_original_claim(space_file_storage, race, damage):
    files, sf, catalog = space_file_storage
    space = await home(files)
    slot = await deleted(sf, space)
    lost = False

    def select_ack(session):
        for row in (*session.new, *session.dirty):
            if isinstance(row, SpaceFileOperationRow) and (row.space_id, row.actor_id, row.phase) == (space.id, "host:space-release", "complete"):
                session.info["host_release_ack_failure"] = True

    def lose_ack(session):
        nonlocal lost
        if session.info.pop("host_release_ack_failure", False) and not lost:
            lost = True
            raise OSError("commit succeeded but acknowledgment was lost")

    event.listen(Session, "before_commit", select_ack)
    event.listen(Session, "after_commit", lose_ack)
    try:
        with pytest.raises(SpaceOperationPending):
            await SpaceRecovery(files).host_release(space.id)
    finally:
        event.remove(Session, "before_commit", select_ack)
        event.remove(Session, "after_commit", lose_ack)
    assert lost and slot in files.bound_slots[None]
    async with sf() as session:
        assert await session.get(SpaceBackingRow, space.backing_handle) is None
        operation_row = await session.scalar(select(SpaceFileOperationRow).where(SpaceFileOperationRow.space_id == space.id))
        assert operation_row.phase == "complete"
    claim = files._claims[slot]
    if damage:
        async with sf() as session, session.begin():
            receipt = await session.scalar(select(SpaceFileOperationRow).where(SpaceFileOperationRow.space_id == space.id))
            if damage == "generation":
                receipt.generation -= 1
            elif damage == "operation-id":
                receipt.operation_id = "invalid"
            elif damage == "checkpoint":
                receipt.result = {**receipt.result, "request_sha256": "0" * 64}
            else:
                deletion = await session.get(SpaceEventRow, (space.id, 2))
                deletion.details = {**deletion.details, "cause": "changed"}
        with pytest.raises(SpaceOperationPending):
            await SpaceRecovery(files).host_release(space.id)
        assert files._claims[slot] is claim and slot not in files.assignable_slots(None)
        return
    if race:
        fresh = SpaceFiles(SpaceRegistry(sf, files.registry._resolver), catalog)
        replacement = await home(fresh, "New allocation")
        async with sf() as session:
            assert (await session.get(SpaceBackingRow, replacement.backing_handle)).slot_id == slot
        await fresh.write(actor=ALICE, space_id=replacement.id, expected_generation=replacement.generation, operation_id=operation(), path="new-owner", content=b"preserve", create=True)
        if race == "observed":
            claim = files._publish_claim(slot, claim, replacement.backing_handle, "bound")
    result = await SpaceRecovery(files).host_release(space.id)
    assert result["complete"] and result["slot_released"] and result["newly_reclaimed"] is False
    if race:
        assert slot not in files.assignable_slots(None) and files._claims[slot] is claim
        assert await fresh.read(actor=ALICE, space_id=replacement.id, path="new-owner", max_bytes=100) == b"preserve"
    else:
        assert slot in files.assignable_slots(None) and slot not in files.bound_slots[None]
