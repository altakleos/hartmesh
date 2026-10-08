import asyncio

import pytest
import pytest_asyncio
from _storage_spaces_test_support import ALICE, home, make_storage_fixture, operation

from deerflow.spaces.contract import Permission
from deerflow.spaces.recovery import SpaceRecovery
from deerflow.spaces.service import SpaceOperationPending


@pytest_asyncio.fixture(params=["sqlite", "postgres"])
async def storage(tmp_path, request):
    async for value in make_storage_fixture(tmp_path, request):
        yield value


@pytest.mark.asyncio
async def test_running_domain_workflow_blocks_reads_grants_and_recovery(storage):
    files, _, _ = storage
    space = await home(files)
    started, release = asyncio.Event(), asyncio.Event()
    op = operation()

    async def domain(volumes):
        started.set()
        await release.wait()
        return {"finished": True}

    task = asyncio.create_task(files.run_workflow(actor=ALICE, requests={space.id: (Permission.WRITE, 1)}, destination_id=space.id, operation_id=op, request={"action": "workflow"}, call=domain))
    await asyncio.wait_for(started.wait(), 10)
    try:
        with pytest.raises(SpaceOperationPending):
            await files.list_directory(actor=ALICE, space_id=space.id)
        with pytest.raises(SpaceOperationPending):
            await SpaceRecovery(files).accept_current_state(actor=ALICE, space_id=space.id, expected_generation=1, operation_id=op, acknowledge_uncertain_outcome=True)
        task.cancel()
        await asyncio.sleep(0.02)
        assert not task.done()
    finally:
        release.set()
        with pytest.raises(asyncio.CancelledError):
            await task
    assert await files.list_directory(actor=ALICE, space_id=space.id) == ([], False)


@pytest.mark.asyncio
async def test_domain_failure_keeps_intent_and_bytes_for_explicit_recovery(storage):
    files, _, _ = storage
    space = await home(files)

    async def domain(volumes):
        (volumes[space.id].data_path / "retained").write_bytes(b"uncertain")
        raise OSError("lost database acknowledgement")

    with pytest.raises(SpaceOperationPending):
        await files.run_workflow(actor=ALICE, requests={space.id: (Permission.WRITE, 1)}, destination_id=space.id, operation_id=operation(), request={"action": "workflow"}, call=domain)
    with pytest.raises(SpaceOperationPending):
        await files.list_directory(actor=ALICE, space_id=space.id)
    facts = await SpaceRecovery(files).status(actor=ALICE, space_id=space.id)
    assert len(facts["operations"]) == 1
