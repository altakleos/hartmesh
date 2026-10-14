"""Optional host scope precedes parent locking and finalizes after fenced commit."""

from contextlib import asynccontextmanager

import pytest
from _storage_spaces_test_support import ALICE, home, operation
from test_storage_spaces_attachments import storage as storage

from deerflow.persistence.spaces.lifecycle import SpaceAttachmentRow
from deerflow.spaces.attachments import ResourceMount


@pytest.mark.asyncio
@pytest.mark.parametrize("method", ["host", "human", "confirmed"])
async def test_containment_scope_owns_physical_work_then_finalizes_after_sql_commit(storage, method):
    files, sf, _catalog, mounts, provider = storage
    space = await home(files)
    attached = await mounts.attach(actor=ALICE, incarnation=operation(), resources=[ResourceMount(space.id, 1, "home", writable=True)])
    events = []
    if method == "confirmed":
        await mounts.contain_host(attached.id, "operator")

    class Scope:
        parent_ids = (space.id,)

        async def admit(self, session, records):
            assert events[0] == "entered"
            assert set(records) == {attached.id}
            events.append("admitted")

        async def contain(self, record):
            assert record.id == attached.id
            provider.fence(record.container_id, record.id)
            events.append("contained")

        async def finalize(self):
            async with sf() as session:
                assert (await session.get(SpaceAttachmentRow, attached.id)).phase == "fenced"
            events.append("finalized")

    @asynccontextmanager
    async def acquire(_mounts, captured):
        assert set(captured) == {attached.id}
        events.append("entered")
        try:
            yield Scope()
        finally:
            events.append("released")

    provider._containment_scope = acquire
    if method == "host":
        assert await mounts.contain_host(attached.id, "operator")
    elif method == "human":
        await mounts.retire(actor=ALICE, space_id=space.id, expected_generation=1, reason="storage-recovery", attachment_ids=[attached.id])
    else:
        async with sf() as session:
            row = await session.get(SpaceAttachmentRow, attached.id)
            incarnation = row.incarnation
        assert await mounts.confirm_fenced_preparation(actor=ALICE, incarnation=incarnation, resources=[ResourceMount(space.id, 1, "home", True)], host_id=provider.host_id, attachment_id=attached.id, container_id=attached.container_id)
    assert events[0] == "entered" and "admitted" in events
    assert events[-2:] == ["finalized", "released"]
    assert not provider.containers
