"""Real SQL/file recovery keeps qualification, proofs and cleanup off-loop."""

import asyncio
from types import SimpleNamespace

import pytest
from _storage_spaces_test_support import ALICE, home, make_storage_fixture, operation

from deerflow.persistence.spaces.files import SpaceFileOperationRow
from deerflow.spaces.workflows import WorkflowReceipt


@pytest.mark.asyncio
async def test_direct_and_receipt_settlement_do_not_block_the_event_loop(tmp_path):
    async for files, sf, _catalog in make_storage_fixture(tmp_path, SimpleNamespace(param="sqlite")):

        def qualify():
            # Production VerifiedVolume carries prepared inode facts; the broad
            # directory fixture computes them lazily for replacement tests.
            for slot, volume in list(_catalog.verified.items()):
                _catalog.verified[slot] = SimpleNamespace(
                    spec=volume.spec, data_path=volume.data_path, control_path=volume.control_path, device=volume.device, root_inode=volume.root_inode, control_inode=volume.control_inode, filesystem=volume.filesystem
                )

        await asyncio.to_thread(qualify)
        space = await home(files)
        direct = operation()
        digest = await files.write(actor=ALICE, space_id=space.id, expected_generation=1, operation_id=direct, path="item", content=b"confirmed", create=True)
        async with sf() as session, session.begin():
            (await session.get(SpaceFileOperationRow, (space.id, direct))).phase = "pending"
        assert await files.settle_interrupted() == 1
        async with sf() as session:
            row = await session.get(SpaceFileOperationRow, (space.id, direct))
            assert row.phase == "complete" and row.result["value"] == digest

        workflow = operation()
        request = {"action": "feature-workflow", "namespace": "fixture-domain"}
        async with sf() as session, session.begin():
            session.add(SpaceFileOperationRow(space_id=space.id, operation_id=workflow, actor_kind="human", actor_id="alice", generation=1, phase="pending", request=request))
        receipt = WorkflowReceipt(next(iter(files._verified.values())), workflow, request)
        await asyncio.to_thread(receipt.save)

        async def settle(_intent, *, session):
            return "host-settled"

        files.workflow_settlers["fixture-domain"] = settle
        assert await files.settle_interrupted() == 1
        assert not await asyncio.to_thread(receipt.path.exists)

        from deerflow.spaces.filesystem import FilesystemUnavailable

        physical = next(iter(files._verified.values()))
        marker = physical.control_path / ".space-binding"
        await asyncio.to_thread(marker.unlink)
        with pytest.raises(FilesystemUnavailable):
            await files.list_directory(actor=ALICE, space_id=space.id)
        assert await files.backfill_binding_markers() == {}
        assert await asyncio.to_thread(marker.exists)
        assert await files.list_directory(actor=ALICE, space_id=space.id) != ([], False)
