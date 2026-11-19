"""Host containment admits domain conditions before recording any closure."""

from contextlib import asynccontextmanager

import pytest
from _storage_spaces_test_support import ALICE, home, operation
from test_storage_spaces_attachments import storage as storage

from deerflow.persistence.spaces.lifecycle import SpaceAttachmentRow
from deerflow.spaces.attachments import ResourceMount


@pytest.mark.asyncio
async def test_host_containment_admission_refusal_rolls_back_before_effects(storage):
    files, sf, _catalog, attachments, provider = storage
    space = await home(files)
    attached = await attachments.attach(actor=ALICE, incarnation=operation(), resources=[ResourceMount(space.id, 1, "home", True)])

    class DomainRefusal(Exception):
        pass

    async def admission(session, identities):
        assert session.in_transaction() and identities == ((attached.id, attached.container_id),)
        assert (await session.get(SpaceAttachmentRow, attached.id)).phase == "active"
        raise DomainRefusal("Work appeared")

    with pytest.raises(DomainRefusal, match="Work appeared"):
        await attachments.contain_host(attached.id, "operator", admission=admission)
    async with sf() as session:
        row = await session.get(SpaceAttachmentRow, attached.id)
        assert row.phase == "active" and row.fenced_at is row.fence_reason is None
    assert attached.container_id in provider.containers and attachments.uncontained == {}


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", ["before-commit", "lost-ack", "after-commit"])
async def test_domain_containment_enqueues_retry_only_after_confirmed_intent(storage, monkeypatch, failure):
    files, sf, _catalog, attachments, provider = storage
    space = await home(files)
    attached = await attachments.attach(actor=ALICE, incarnation=operation(), resources=[ResourceMount(space.id, 1, "home", True)])
    lock = files.registry.host_locked
    transactions = 0

    @asynccontextmanager
    async def interrupted(parents):
        nonlocal transactions
        transactions += 1
        if transactions == 2 and failure == "after-commit":
            raise OSError("Second transaction unavailable")
        async with lock(parents) as state:
            yield state
            if failure == "before-commit":
                raise OSError("Intent rolled back")
        if failure == "lost-ack":
            raise OSError("Commit acknowledgement lost")

    async def admit(session, identities):
        assert session.in_transaction() and identities == ((attached.id, attached.container_id),)

    monkeypatch.setattr(files.registry, "host_locked", interrupted)
    assert not await attachments.contain_host(attached.id, "outcome-uncertain", admission=admit)
    async with sf() as session:
        row = await session.get(SpaceAttachmentRow, attached.id)
        assert row.phase == ("active" if failure == "before-commit" else "fence_pending")
    assert attached.container_id in provider.containers
    assert attachments.uncontained == ({attached.id: "outcome-uncertain"} if failure == "after-commit" else {})
