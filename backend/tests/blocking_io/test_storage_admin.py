"""Metadata fallback verifies unavailable backing without blocking the loop."""

import asyncio
from types import SimpleNamespace

import pytest
from _storage_spaces_test_support import ALICE, home, make_storage_fixture, operation

from deerflow.persistence.spaces.files import SpaceBackingRow, SpaceFileOperationRow
from deerflow.spaces.contract import Permission


@pytest.mark.asyncio
async def test_unverified_admin_metadata_verification_stays_off_loop(tmp_path):
    async for files, sf, catalog in make_storage_fixture(tmp_path, SimpleNamespace(param="sqlite")):

        def qualify():
            for slot, volume in list(catalog.verified.items()):
                catalog.verified[slot] = SimpleNamespace(
                    spec=volume.spec, data_path=volume.data_path, control_path=volume.control_path, device=volume.device, root_inode=volume.root_inode, control_inode=volume.control_inode, filesystem=volume.filesystem
                )

        await asyncio.to_thread(qualify)
        space = await home(files)
        identifier = operation()
        async with sf() as session, session.begin():
            bound = await session.get(SpaceBackingRow, space.backing_handle)
            original = bound.max_bytes, bound.max_inodes
            session.add(SpaceFileOperationRow(space_id=space.id, operation_id=identifier, actor_kind=ALICE.kind, actor_id=ALICE.subject_id, generation=1, phase="pending", request={"action": "write"}))
        volume = catalog.verified[bound.slot_id]
        volume.spec.max_bytes *= 2
        volume.spec.max_inodes *= 2
        await asyncio.to_thread((volume.control_path / ".space-binding").unlink)
        renamed = await files.registry.rename(actor=ALICE, space_id=space.id, expected_generation=1, name="Metadata available")
        assert renamed.name == "Metadata available" and renamed.generation == 2 and renamed.permissions & Permission.ADMIN
        async with sf() as session:
            current = await session.get(SpaceBackingRow, space.backing_handle)
            assert (current.max_bytes, current.max_inodes) == original
            assert (await session.get(SpaceFileOperationRow, (space.id, identifier))).phase == "pending"
