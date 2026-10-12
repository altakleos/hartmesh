"""Real SQL retirement keeps descriptor verification and purge off the loop."""

import asyncio
from types import SimpleNamespace

import pytest
from _storage_spaces_test_support import ALICE, home, make_storage_fixture

from deerflow.persistence.spaces.files import SpaceBackingRow
from deerflow.persistence.spaces.model import SpaceEventRow, SpaceRow
from deerflow.spaces.recovery import SpaceRecovery


@pytest.mark.asyncio
async def test_host_release_filesystem_phases_do_not_block_the_event_loop(tmp_path):
    async for files, sf, catalog in make_storage_fixture(tmp_path, SimpleNamespace(param="sqlite")):

        def qualify():
            for slot, volume in list(catalog.verified.items()):
                catalog.verified[slot] = SimpleNamespace(
                    spec=volume.spec, data_path=volume.data_path, control_path=volume.control_path, device=volume.device, root_inode=volume.root_inode, control_inode=volume.control_inode, filesystem=volume.filesystem
                )

        await asyncio.to_thread(qualify)
        space = await home(files)
        async with sf() as session, session.begin():
            row = await session.get(SpaceRow, space.id)
            row.status, row.generation = "deleted", 2
            binding = await session.get(SpaceBackingRow, space.backing_handle)
            session.add(SpaceEventRow(space_id=space.id, generation=2, action="delete", actor_kind="human", actor_id=ALICE.subject_id, details={"retained_backups": False}))
        volume = catalog.verified[binding.slot_id]
        await asyncio.to_thread((volume.data_path / "private").write_bytes, b"private")
        before = await asyncio.to_thread(lambda: (volume.data_path.stat().st_ino, volume.control_path.stat().st_ino))
        result = await SpaceRecovery(files).host_release(space.id)
        assert result["newly_reclaimed"]
        after = await asyncio.to_thread(lambda: (volume.data_path.stat().st_ino, volume.control_path.stat().st_ino))
        assert before[0] != after[0] and before[1] != after[1]
        async with sf() as session:
            assert await session.get(SpaceBackingRow, space.backing_handle) is None
