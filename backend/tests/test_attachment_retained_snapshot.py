"""Complete bounded attachment accounting snapshots on actual SQL."""

from unittest.mock import Mock

import pytest
from _storage_spaces_test_support import ALICE, home, operation
from test_storage_spaces_attachments import storage as storage

from deerflow.persistence.spaces.lifecycle import SpaceAttachmentRow, SpaceMountRow
from deerflow.spaces.attachments import AttachmentPending, ResourceMount


@pytest.mark.asyncio
async def test_snapshot_returns_all_distinct_attachments_without_provider_effects(storage):
    files, _sf, _catalog, mounts, provider = storage
    first, second = await home(files), await home(files, name="Second")
    resources = [ResourceMount(first.id, 1, "home", True), ResourceMount(second.id, 1, "shared", True)]
    attached = await mounts.attach(actor=ALICE, incarnation=operation(), resources=resources)
    for name in ("prepare", "start", "fence", "discover", "cleanup", "verify_active"):
        setattr(provider, name, Mock(side_effect=AssertionError("Snapshots read metadata only")))
    facts = await mounts.retained_claims()
    assert len(facts) == 1 and facts[0].id == attached.id and facts[0].incarnation == attached.incarnation
    assert facts[0].actor == ALICE and set(facts[0].resources) == set(resources)
    with pytest.raises(AttributeError):
        facts[0].phase = "pending"


@pytest.mark.asyncio
async def test_global_limit_plus_one_refuses_truncated_complete_snapshot(storage):
    files, _sf, _catalog, mounts, _provider = storage
    first, second = await home(files), await home(files, name="Second")
    for space in (first, second):
        await mounts.attach(actor=ALICE, incarnation=operation(), resources=[ResourceMount(space.id, 1, "home", True)])
    with pytest.raises(AttachmentPending, match="limit"):
        await mounts.retained_claims(limit=1)
    assert len(await mounts.retained_claims(limit=2)) == 2


@pytest.mark.asyncio
@pytest.mark.parametrize("changed", ["missing_mounts", "identity", "closure", "container"])
async def test_malformed_snapshot_is_never_returned_as_ready_or_empty(storage, changed):
    files, sf, _catalog, mounts, _provider = storage
    space = await home(files)
    attached = await mounts.attach(actor=ALICE, incarnation=operation(), resources=[ResourceMount(space.id, 1, "home", True)])
    async with sf() as session, session.begin():
        row = await session.get(SpaceAttachmentRow, attached.id)
        if changed == "missing_mounts":
            await session.delete(await session.get(SpaceMountRow, (attached.id, space.id)))
        elif changed == "identity":
            row.incarnation = "invalid"
        elif changed == "closure":
            row.fenced_by_kind = "host"
        else:
            row.container_id = None
    with pytest.raises((AttachmentPending, ValueError)):
        await mounts.retained_claims()


@pytest.mark.asyncio
async def test_sql_failure_propagates_and_fenced_history_is_excluded(storage, monkeypatch):
    files, _sf, _catalog, mounts, _provider = storage
    space = await home(files)
    attached = await mounts.attach(actor=ALICE, incarnation=operation(), resources=[ResourceMount(space.id, 1, "home", True)])
    assert await mounts.contain_host(attached.id, "idle")
    assert await mounts.retained_claims() == ()
    monkeypatch.setattr(mounts.registry, "_sf", Mock(side_effect=RuntimeError("SQL unavailable")))
    with pytest.raises(RuntimeError, match="SQL unavailable"):
        await mounts.retained_claims()


@pytest.mark.asyncio
async def test_exact_retained_read_shares_complete_validated_snapshot_facts(storage):
    files, _sf, _catalog, mounts, provider = storage
    space = await home(files)
    attached = await mounts.attach(actor=ALICE, incarnation=operation(), resources=[ResourceMount(space.id, 1, "home", True)])
    for name in ("prepare", "start", "fence", "discover", "cleanup", "verify_active"):
        setattr(provider, name, Mock(side_effect=AssertionError("Exact accounting is metadata only")))
    current = await mounts.retained_attachment(attached.id)
    assert current == (await mounts.retained_claims())[0]
    assert current.incarnation == attached.incarnation and current.container_id == attached.container_id
    assert await mounts.retained_attachment(operation()) is None


@pytest.mark.asyncio
@pytest.mark.parametrize("changed", ["missing_mounts", "identity", "closure", "container"])
async def test_exact_retained_read_rejects_the_same_invalid_facts(storage, changed):
    files, sf, _catalog, mounts, _provider = storage
    space = await home(files)
    attached = await mounts.attach(actor=ALICE, incarnation=operation(), resources=[ResourceMount(space.id, 1, "home", True)])
    async with sf() as session, session.begin():
        row = await session.get(SpaceAttachmentRow, attached.id)
        if changed == "missing_mounts":
            await session.delete(await session.get(SpaceMountRow, (attached.id, space.id)))
        elif changed == "identity":
            row.incarnation = "invalid"
        elif changed == "closure":
            row.fenced_by_kind = "host"
        else:
            row.container_id = None
    with pytest.raises((AttachmentPending, ValueError)):
        await mounts.retained_attachment(attached.id)


@pytest.mark.asyncio
async def test_exact_retained_read_never_turns_sql_failure_into_absence(storage, monkeypatch):
    _files, _sf, _catalog, mounts, _provider = storage
    monkeypatch.setattr(mounts.registry, "_sf", Mock(side_effect=RuntimeError("SQL unavailable")))
    with pytest.raises(RuntimeError, match="SQL unavailable"):
        await mounts.retained_attachment(operation())


@pytest.mark.asyncio
async def test_fenced_preparation_confirmation_repeats_physical_proof_and_preserves_history(storage):
    files, sf, _catalog, mounts, provider = storage
    space = await home(files)
    resources = [ResourceMount(space.id, 1, "home", True)]
    attached = await mounts.attach(actor=ALICE, incarnation=operation(), resources=resources)
    assert await mounts.contain_host(attached.id, "idle")
    async with sf() as session:
        row = await session.get(SpaceAttachmentRow, attached.id)
        original = row.fence_reason, row.fenced_at
    provider.fence = Mock(wraps=provider.fence)
    assert await mounts.confirm_fenced_preparation(
        actor=ALICE,
        incarnation=attached.incarnation,
        resources=resources,
        host_id=attached.host_id,
        attachment_id=attached.id,
        container_id=attached.container_id,
    )
    provider.fence.assert_called_once_with(attached.container_id, attached.id)
    async with sf() as session:
        row = await session.get(SpaceAttachmentRow, attached.id)
        assert (row.fence_reason, row.fenced_at) == original
    assert await mounts.retained_attachment(attached.id) is None
