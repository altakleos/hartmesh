"""Exact preparation correlation on SQL, without physical proof or effects."""

from datetime import UTC, datetime
from unittest.mock import Mock

import pytest
from _storage_spaces_test_support import ALICE, BOB, home, operation
from test_storage_spaces_attachments import storage as storage

from deerflow.persistence.spaces.lifecycle import SpaceAttachmentRow
from deerflow.persistence.spaces.model import SpaceGrantRow
from deerflow.spaces.attachments import AttachmentPending, ResourceMount


@pytest.mark.parametrize("changed", ["actor", "extra"])
@pytest.mark.asyncio
async def test_scoped_unfenced_read_refuses_pending_row_with_foreign_actor_or_complete_mount_mismatch(storage, changed):
    files, sf, _catalog, mounts, provider = storage
    space = await home(files)
    resources = [ResourceMount(space.id, 1, "home", writable=True)]
    attached = await mounts.attach(actor=ALICE, incarnation=operation(), resources=resources)
    async with sf() as session, session.begin():
        (await session.get(SpaceAttachmentRow, attached.id)).phase = "pending"
    requested = resources if changed == "actor" else resources + [ResourceMount(operation(), 1, "extra")]
    actor = BOB if changed == "actor" else ALICE
    with pytest.raises(AttachmentPending):
        await mounts.unfenced(space.id, actor=actor, resources=requested, host_id=provider.host_id)


@pytest.mark.asyncio
async def test_missing_preparation_is_distinct_from_a_fenced_historical_row(storage):
    files, _sf, _catalog, mounts, provider = storage
    space = await home(files)
    resources = [ResourceMount(space.id, 1, "home", writable=True)]
    incarnation = operation()
    assert await mounts.inspect_preparation(actor=ALICE, incarnation=incarnation, resources=resources, host_id=provider.host_id) is None
    attached = await mounts.attach(actor=ALICE, incarnation=incarnation, resources=resources)
    assert await mounts.contain_host(attached.id, "idle")
    facts = await mounts.inspect_preparation(actor=ALICE, incarnation=incarnation, resources=resources, host_id=provider.host_id)
    assert facts.id == attached.id and facts.phase == "fenced" and facts.container_id == attached.container_id


@pytest.mark.parametrize("phase", ["pending", "active", "fence_pending", "fenced"])
@pytest.mark.asyncio
async def test_preparation_inspection_returns_detached_phase_facts_without_provider_calls(storage, phase):
    files, sf, _catalog, mounts, provider = storage
    space = await home(files)
    resources = [ResourceMount(space.id, 1, "home", writable=True)]
    attached = await mounts.attach(actor=ALICE, incarnation=operation(), resources=resources)
    async with sf() as session, session.begin():
        durable = await session.get(SpaceAttachmentRow, attached.id)
        durable.phase = phase
        if phase in ("fence_pending", "fenced"):
            durable.fenced_by_kind, durable.fenced_by_id, durable.fence_reason = "host", provider.host_id, "idle"
            durable.fenced_at = datetime.now(UTC) if phase == "fenced" else None
    for name in ("prepare", "start", "fence", "discover", "cleanup", "verify_active"):
        setattr(provider, name, Mock(side_effect=AssertionError("Inspection is metadata-only")))
    facts = await mounts.inspect_preparation(actor=ALICE, incarnation=attached.incarnation, resources=resources, host_id=provider.host_id)
    assert (facts.id, facts.phase, facts.host_id, facts.container_id) == (attached.id, phase, provider.host_id, attached.container_id)
    with pytest.raises(AttributeError):
        facts.phase = "pending"


@pytest.mark.parametrize("changed", ["actor", "host", "generation", "alias", "mode", "extra"])
@pytest.mark.asyncio
async def test_existing_incarnation_with_wrong_scope_is_never_reported_absent(storage, changed):
    files, _sf, _catalog, mounts, provider = storage
    space = await home(files)
    resources = [ResourceMount(space.id, 1, "home", writable=True)]
    attached = await mounts.attach(actor=ALICE, incarnation=operation(), resources=resources)
    actor, host_id = ALICE, provider.host_id
    if changed == "actor":
        actor = BOB
    elif changed == "host":
        host_id = "another-host"
    elif changed == "generation":
        resources = [ResourceMount(space.id, 2, "home", writable=True)]
    elif changed == "alias":
        resources = [ResourceMount(space.id, 1, "different", writable=True)]
    elif changed == "mode":
        resources = [ResourceMount(space.id, 1, "home")]
    else:
        resources.append(ResourceMount(operation(), 1, "extra"))
    with pytest.raises(AttachmentPending):
        await mounts.inspect_preparation(actor=actor, incarnation=attached.incarnation, resources=resources, host_id=host_id)


@pytest.mark.asyncio
async def test_revoked_actor_can_inspect_exact_cleanup_correlation(storage):
    files, sf, _catalog, mounts, provider = storage
    space = await home(files)
    resources = [ResourceMount(space.id, 1, "home", writable=True)]
    attached = await mounts.attach(actor=ALICE, incarnation=operation(), resources=resources)
    async with sf() as session, session.begin():
        await session.delete(await session.get(SpaceGrantRow, (space.id, "human", "alice")))
    facts = await mounts.inspect_preparation(actor=ALICE, incarnation=attached.incarnation, resources=resources, host_id=provider.host_id)
    assert facts.id == attached.id


@pytest.mark.asyncio
async def test_failed_sql_inspection_propagates_instead_of_returning_absence(storage, monkeypatch):
    _files, _sf, _catalog, mounts, provider = storage
    failure = RuntimeError("injected unavailable metadata")
    monkeypatch.setattr(mounts.registry, "_sf", Mock(side_effect=failure))
    with pytest.raises(RuntimeError) as raised:
        await mounts.inspect_preparation(actor=ALICE, incarnation=operation(), resources=[ResourceMount(operation(), 1, "home", writable=True)], host_id=provider.host_id)
    assert raised.value is failure


@pytest.mark.parametrize("changed", ["partial", "closer", "reason", "timestamp"])
@pytest.mark.asyncio
async def test_malformed_closure_metadata_never_becomes_a_preparation_fact(storage, changed):
    files, sf, _catalog, mounts, provider = storage
    space = await home(files)
    resources = [ResourceMount(space.id, 1, "home", writable=True)]
    attached = await mounts.attach(actor=ALICE, incarnation=operation(), resources=resources)
    async with sf() as session, session.begin():
        durable = await session.get(SpaceAttachmentRow, attached.id)
        durable.phase = "fence_pending"
        durable.fenced_by_kind, durable.fenced_by_id, durable.fence_reason = "host", provider.host_id, "idle"
        if changed == "partial":
            durable.fenced_by_kind = None
        elif changed == "closer":
            durable.fenced_by_kind = "unsupported"
        elif changed == "reason":
            durable.fence_reason = "unsupported"
        else:
            durable.fenced_at = datetime.now(UTC)
    with pytest.raises(AttachmentPending):
        await mounts.inspect_preparation(actor=ALICE, incarnation=attached.incarnation, resources=resources, host_id=provider.host_id)
