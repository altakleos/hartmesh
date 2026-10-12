"""Unverified administrative metadata never grants filesystem authority."""

import pytest
from _storage_spaces_test_support import ALICE, BOB, home, operation
from sqlalchemy import select
from test_storage_spaces_service import space_file_storage as space_file_storage

from deerflow.persistence.spaces.files import SpaceBackingRow, SpaceFileOperationRow
from deerflow.persistence.spaces.lifecycle import SpaceAttachmentRow, SpaceMountRow
from deerflow.persistence.spaces.model import SpaceGrantRow, SpaceRow
from deerflow.spaces.attachments import AttachmentPending
from deerflow.spaces.backings import BackingUnavailable
from deerflow.spaces.contract import Custody, FeatureBinding, MutationMode, Permission, SpaceConflict, SpaceDenied, SpaceNotFound
from deerflow.spaces.incarnations import BoundVolume
from deerflow.spaces.recovery import SpaceRecovery
from deerflow.spaces.service import SpaceOperationPending


async def backing(sf, space):
    async with sf() as session:
        return await session.get(SpaceBackingRow, space.backing_handle)


async def pending(sf, space, action="write"):
    identifier = operation()
    async with sf() as session, session.begin():
        session.add(SpaceFileOperationRow(space_id=space.id, operation_id=identifier, actor_kind=ALICE.kind, actor_id=ALICE.subject_id, generation=1, phase="pending", request={"action": action}))
    return identifier


def invalidate(catalog, binding, marker="missing"):
    path = catalog.verified[binding.slot_id].control_path / ".space-binding"
    if marker == "missing":
        path.unlink()
    else:
        path.write_text("invalid binding", encoding="utf-8")


@pytest.mark.asyncio
@pytest.mark.parametrize("mutation", ["rename", "grant"])
@pytest.mark.parametrize("marker", ["missing", "invalid"])
async def test_unverified_admin_changes_metadata_without_publishing_unverified_growth(space_file_storage, mutation, marker):
    files, sf, catalog = space_file_storage
    space = await home(files)
    original = await backing(sf, space)
    spec = catalog.volumes[original.slot_id]
    spec.max_bytes *= 2
    spec.max_inodes *= 2
    invalidate(catalog, original, marker)
    identifier = await pending(sf, space)
    if mutation == "rename":
        result = await files.registry.rename(actor=ALICE, space_id=space.id, expected_generation=1, name="Metadata survives")
        assert result.name == "Metadata survives"
    else:
        result = await files.registry.set_grant(actor=ALICE, space_id=space.id, expected_generation=1, subject=BOB, permissions=Permission.READ, acknowledge_existing_data=True)
    assert result.generation == 2
    unchanged = await backing(sf, space)
    assert (unchanged.max_bytes, unchanged.max_inodes) == (original.max_bytes, original.max_inodes)
    async with sf() as session:
        assert (await session.get(SpaceFileOperationRow, (space.id, identifier))).phase == "pending"
    assert not any(catalog.verified[original.slot_id].data_path.iterdir())


@pytest.mark.asyncio
async def test_verified_admin_still_records_qualified_inventory_growth(space_file_storage):
    files, sf, catalog = space_file_storage
    space = await home(files)
    original = await backing(sf, space)
    spec = catalog.volumes[original.slot_id]
    spec.max_bytes *= 2
    spec.max_inodes *= 2
    await files.registry.rename(actor=ALICE, space_id=space.id, expected_generation=1, name="Verified growth")
    current = await backing(sf, space)
    assert (current.max_bytes, current.max_inodes) == (spec.max_bytes, spec.max_inodes)


@pytest.mark.asyncio
@pytest.mark.parametrize("extra", [Permission.READ, Permission.WRITE, Permission.OPERATE, Permission.EXPORT])
async def test_combined_admin_requires_qualified_backing(space_file_storage, extra):
    files, sf, catalog = space_file_storage
    space = await files.create(actor=ALICE, name="Mediated", custody=Custody.personal(ALICE), mode=MutationMode.MEDIATED, feature=FeatureBinding("example.feature", "api", 1)) if extra == Permission.OPERATE else await home(files)
    invalidate(catalog, await backing(sf, space))
    with pytest.raises(BackingUnavailable):
        async with files.registry.admitted(actor=ALICE, requests={space.id: (Permission.ADMIN | extra, 1)}):
            pytest.fail("Combined file authority used unverified backing")
    assert (await files.registry.get(actor=ALICE, space_id=space.id)).generation == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("action", ["write", "backup"])
async def test_unverified_admin_can_inspect_metadata_with_any_pending_action(space_file_storage, action):
    files, sf, catalog = space_file_storage
    space = await home(files)
    invalidate(catalog, await backing(sf, space))
    identifier = await pending(sf, space, action)
    async with files.registry.admitted(actor=ALICE, requests={space.id: (Permission.ADMIN, 1)}) as (session, rows):
        assert rows[space.id][0].name == "Home"
        assert (await session.get(SpaceFileOperationRow, (space.id, identifier))).phase == "pending"
    with pytest.raises((BackingUnavailable, SpaceOperationPending)):
        await files.read(actor=ALICE, space_id=space.id, path="unavailable", max_bytes=100)


@pytest.mark.asyncio
async def test_admin_fallback_preserves_actor_grant_and_generation_checks(space_file_storage):
    files, sf, catalog = space_file_storage
    space = await home(files)
    await files.registry.set_grant(actor=ALICE, space_id=space.id, expected_generation=1, subject=BOB, permissions=Permission.READ, acknowledge_existing_data=True)
    invalidate(catalog, await backing(sf, space))
    await pending(sf, space)
    with pytest.raises(SpaceDenied):
        await files.registry.rename(actor=BOB, space_id=space.id, expected_generation=2, name="Denied")
    with pytest.raises(SpaceConflict, match="generation"):
        await files.registry.rename(actor=ALICE, space_id=space.id, expected_generation=1, name="Stale")
    async with sf() as session, session.begin():
        await session.delete(await session.get(SpaceGrantRow, (space.id, ALICE.kind, ALICE.subject_id)))
    with pytest.raises(SpaceNotFound):
        await files.registry.rename(actor=ALICE, space_id=space.id, expected_generation=2, name="Revoked")
    async with sf() as session:
        row = await session.get(SpaceRow, space.id)
        assert (row.name, row.generation) == ("Home", 2)


@pytest.mark.asyncio
@pytest.mark.parametrize("phase", ["pending", "fence_pending"])
@pytest.mark.parametrize("installed", [False, True])
async def test_unverified_admin_never_bypasses_attachment_guard(space_file_storage, phase, installed):
    files, sf, catalog = space_file_storage
    if installed:
        from test_storage_spaces_attachments import ContainedProvider

        from deerflow.spaces.attachments import SpaceAttachments

        SpaceAttachments(files, ContainedProvider())
    space = await home(files)
    invalidate(catalog, await backing(sf, space))
    await pending(sf, space)
    identifier = operation()
    async with sf() as session, session.begin():
        session.add(SpaceAttachmentRow(id=identifier, incarnation=operation(), actor_kind=ALICE.kind, actor_id=ALICE.subject_id, host_id="fixture-host", phase=phase))
        await session.flush()
        session.add(SpaceMountRow(attachment_id=identifier, space_id=space.id, generation=1, alias="home", writable=1))
    with pytest.raises(AttachmentPending if installed else SpaceOperationPending, match="attachment"):
        await files.registry.rename(actor=ALICE, space_id=space.id, expected_generation=1, name="Unsafe")
    async with sf() as session:
        assert (await session.get(SpaceAttachmentRow, identifier)).phase == phase
        assert (await session.get(SpaceRow, space.id)).generation == 1


@pytest.mark.asyncio
async def test_one_unverified_resource_does_not_skip_other_resources_pending_gate(space_file_storage):
    files, sf, catalog = space_file_storage
    first, second = await home(files, "Unverified"), await home(files, "Verified")
    invalidate(catalog, await backing(sf, first))
    await pending(sf, first)
    await pending(sf, second)
    with pytest.raises(SpaceOperationPending, match="unconfirmed"):
        async with files.registry.admitted(actor=ALICE, requests={first.id: (Permission.ADMIN, 1), second.id: (Permission.ADMIN, 1)}):
            pytest.fail("Another resource's pending operation was bypassed")


@pytest.mark.asyncio
async def test_verified_workflow_contention_remains_pending(space_file_storage):
    from deerflow.spaces.workflows import acquire, close

    files, sf, catalog = space_file_storage
    space = await home(files)
    bound = await backing(sf, space)
    descriptors = acquire({space.id: BoundVolume(catalog.verified[bound.slot_id], bound.backing_handle)})
    try:
        with pytest.raises(SpaceOperationPending, match="workflow"):
            await files.registry.rename(actor=ALICE, space_id=space.id, expected_generation=1, name="Held")
    finally:
        close(descriptors)


@pytest.mark.asyncio
@pytest.mark.parametrize("action", ["backup", "archive", "delete"])
async def test_unverified_lifecycle_never_writes_intent_or_touches_files(space_file_storage, action):
    files, sf, catalog = space_file_storage
    space = await home(files)
    bound = await backing(sf, space)
    document = catalog.verified[bound.slot_id].data_path / "document"
    document.write_bytes(b"preserved")
    invalidate(catalog, bound)
    with pytest.raises(BackingUnavailable):
        await getattr(SpaceRecovery(files), action)(actor=ALICE, space_id=space.id, expected_generation=1, operation_id=operation())
    assert document.read_bytes() == b"preserved"
    async with sf() as session:
        assert not (await session.scalars(select(SpaceFileOperationRow))).all()
        row = await session.get(SpaceRow, space.id)
        assert (row.status, row.generation) == ("active", 1)


@pytest.mark.asyncio
async def test_unverified_restore_keeps_verified_backup_and_writes_no_new_intent(space_file_storage):
    files, sf, catalog = space_file_storage
    space = await home(files)
    bound = await backing(sf, space)
    document = catalog.verified[bound.slot_id].data_path / "document"
    document.write_bytes(b"backup contents")
    saved = await SpaceRecovery(files).backup(actor=ALICE, space_id=space.id, expected_generation=1, operation_id=operation())
    invalidate(catalog, bound)
    with pytest.raises(BackingUnavailable):
        await SpaceRecovery(files).restore(actor=ALICE, space_id=space.id, expected_generation=1, operation_id=operation(), backup_id=saved.id)
    assert document.read_bytes() == b"backup contents"
    async with sf() as session:
        operations = list(await session.scalars(select(SpaceFileOperationRow)))
        assert len(operations) == 1 and operations[0].phase == "complete"


@pytest.mark.asyncio
async def test_unverified_accept_closes_workspace_then_preserves_pending_outcome(space_file_storage, monkeypatch):
    files, sf, catalog = space_file_storage
    space = await home(files)
    invalidate(catalog, await backing(sf, space))
    identifier = await pending(sf, space)
    calls = []
    recovery = SpaceRecovery(files)

    async def quiesce(*args):
        calls.append(args)

    monkeypatch.setattr(recovery, "_quiesce", quiesce)
    with pytest.raises(BackingUnavailable):
        await recovery.accept_current_state(actor=ALICE, space_id=space.id, expected_generation=1, operation_id=identifier, acknowledge_uncertain_outcome=True)
    assert calls == [(ALICE, space.id, 1, identifier, [])]
    async with sf() as session:
        assert (await session.get(SpaceFileOperationRow, (space.id, identifier))).phase == "pending"
        assert (await session.get(SpaceRow, space.id)).generation == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", ["missing-binding", "absent-catalog", "kernel-proof"])
async def test_admin_metadata_survives_other_unavailable_backing_states(space_file_storage, monkeypatch, failure):
    files, sf, catalog = space_file_storage
    space = await home(files)
    bound = await backing(sf, space)
    document = catalog.verified[bound.slot_id].data_path / "document"
    document.write_bytes(b"untouched")
    if failure == "missing-binding":
        async with sf() as session, session.begin():
            await session.delete(await session.get(SpaceBackingRow, space.backing_handle))
    elif failure == "absent-catalog":
        catalog.volumes.pop(bound.slot_id)
    else:

        def unavailable(*args, **kwargs):
            raise BackingUnavailable("Fixture kernel proof unavailable")

        monkeypatch.setattr(catalog, "verify", unavailable)
    await pending(sf, space)
    assert (await files.registry.rename(actor=ALICE, space_id=space.id, expected_generation=1, name="Available metadata")).generation == 2
    assert document.read_bytes() == b"untouched"
