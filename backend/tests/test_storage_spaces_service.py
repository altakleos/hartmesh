"""Admission/journaling uses real SQL and files; this tier does not qualify quotas."""

from __future__ import annotations

import asyncio
import hashlib

import pytest
import pytest_asyncio
from _storage_spaces_test_support import ALICE, BOB, home, make_storage_fixture, operation
from sqlalchemy import select

from deerflow.spaces.contract import Custody, MutationMode, Permission, ResolvedPrincipal, SpaceConflict, SpaceDenied, SpaceNotFound
from deerflow.spaces.filesystem import ConfinedFilesystem
from deerflow.spaces.principals import HostPrincipalResolver
from deerflow.spaces.service import SpaceFiles, SpaceOperationPending


@pytest_asyncio.fixture(params=["sqlite", "postgres"])
async def space_file_storage(tmp_path, request):
    async for value in make_storage_fixture(tmp_path, request):
        yield value


@pytest.mark.asyncio
async def test_concurrent_creation_cannot_claim_one_volume_twice(space_file_storage):
    service, _, _ = space_file_storage
    resources = await asyncio.gather(*(home(service, str(i)) for i in range(4)))
    assert len({space.backing_handle for space in resources}) == 4
    with pytest.raises(SpaceConflict, match="capacity"):
        await home(service)


@pytest.mark.asyncio
async def test_company_provisioning_authority_is_rechecked_inside_create_transaction(space_file_storage):
    service, _, _ = space_file_storage
    calls = 0

    async def retiring_admin(reference):
        nonlocal calls
        calls += 1
        return ResolvedPrincipal(reference, calls == 1)

    service.registry._resolver = HostPrincipalResolver(human=retiring_admin)
    with pytest.raises(SpaceDenied, match="Company provisioning"):
        await service.create(actor=ALICE, name="company", custody=Custody.company(), mode=MutationMode.NATIVE)


@pytest.mark.asyncio
async def test_binding_and_files_survive_service_restart_without_chat(space_file_storage):
    service, sf, catalog = space_file_storage
    space = await home(service)
    op = operation()
    result = await service.write(actor=ALICE, space_id=space.id, expected_generation=1, operation_id=op, path=".page", content=b"first", create=True)
    assert result == hashlib.sha256(b"first").hexdigest()
    restarted = SpaceFiles(service.registry, catalog)
    assert (await restarted.read(actor=ALICE, space_id=space.id, path=".page", max_bytes=100)) == b"first"
    assert await restarted.write(actor=ALICE, space_id=space.id, expected_generation=1, operation_id=op, path=".page", content=b"first", create=True) == result
    assert (await service.registry.get(actor=ALICE, space_id=space.id)).generation == 1
    from deerflow.persistence.spaces.files import SpaceBackingRow

    async with sf() as session:
        binding = (await session.execute(select(SpaceBackingRow))).scalar_one()
        assert binding.backing_handle == space.backing_handle


@pytest.mark.asyncio
async def test_creation_never_claims_nonempty_or_already_bound_root(space_file_storage):
    service, _, catalog = space_file_storage
    first_slot = sorted(catalog.volumes)[0]
    (catalog.verified[first_slot].data_path / "operator-data").write_bytes(b"retain")
    spaces = [await home(service, str(i)) for i in range(3)]
    assert len({s.backing_handle for s in spaces}) == 3
    with pytest.raises(SpaceConflict, match="capacity"):
        await home(service)
    assert (catalog.verified[first_slot].data_path / "operator-data").read_bytes() == b"retain"


@pytest.mark.asyncio
async def test_mandatory_grants_modes_and_generation_before_io(space_file_storage):
    service, _, _ = space_file_storage
    space = await home(service)
    with pytest.raises(SpaceNotFound):
        await service.read(actor=BOB, space_id=space.id, path="no-file", max_bytes=100)
    space = await service.registry.set_grant(actor=ALICE, space_id=space.id, subject=BOB, permissions=Permission.READ, expected_generation=1, acknowledge_existing_data=True)
    with pytest.raises(SpaceDenied):
        await service.write(actor=BOB, space_id=space.id, expected_generation=2, operation_id=operation(), path="no-file", content=b"bad", create=True)
    with pytest.raises(SpaceConflict, match="generation"):
        await service.write(actor=ALICE, space_id=space.id, expected_generation=1, operation_id=operation(), path="no-file", content=b"bad", create=True)
    assert await service.list_directory(actor=ALICE, space_id=space.id) == ([], False)


@pytest.mark.asyncio
async def test_operation_id_cannot_alias_different_bytes_or_actor(space_file_storage):
    service, _, _ = space_file_storage
    space = await home(service)
    op = operation()
    await service.write(actor=ALICE, space_id=space.id, expected_generation=1, operation_id=op, path="x", content=b"a", create=True)
    with pytest.raises(SpaceConflict, match="operation"):
        await service.write(actor=ALICE, space_id=space.id, expected_generation=1, operation_id=op, path="x", content=b"b", create=True)
    space = await service.registry.set_grant(actor=ALICE, space_id=space.id, subject=BOB, permissions=Permission.READ | Permission.WRITE, expected_generation=1, acknowledge_existing_data=True)
    with pytest.raises(SpaceConflict, match="operation"):
        await service.write(actor=BOB, space_id=space.id, expected_generation=2, operation_id=op, path="x", content=b"a", create=True)


@pytest.mark.asyncio
async def test_uncertain_outcome_is_retained_and_blocks_new_edits_and_grants(space_file_storage, monkeypatch):
    service, sf, _ = space_file_storage
    space = await home(service)
    original = ConfinedFilesystem.write_atomic

    def publish_then_fail(self, *args, **kwargs):
        original(self, *args, **kwargs)
        raise OSError("simulated post-publication fsync failure")

    monkeypatch.setattr(ConfinedFilesystem, "write_atomic", publish_then_fail)
    with pytest.raises(SpaceOperationPending):
        await service.write(actor=ALICE, space_id=space.id, expected_generation=1, operation_id=operation(), path="x", content=b"published", create=True)
    with pytest.raises(SpaceOperationPending):
        await service.write(actor=ALICE, space_id=space.id, expected_generation=1, operation_id=operation(), path="y", content=b"blocked", create=True)
    with pytest.raises(SpaceOperationPending):
        await service.registry.set_grant(actor=ALICE, space_id=space.id, subject=BOB, permissions=Permission.READ, expected_generation=1, acknowledge_existing_data=True)
    from deerflow.persistence.spaces.files import SpaceFileOperationRow

    async with sf() as session:
        assert (await session.execute(select(SpaceFileOperationRow))).scalar_one().phase == "pending"


@pytest.mark.asyncio
async def test_cancel_drains_started_writer_before_another_mutation(space_file_storage, monkeypatch):
    import threading

    service, _, _ = space_file_storage
    space = await home(service)
    started, release, finished = threading.Event(), threading.Event(), threading.Event()
    original = ConfinedFilesystem.write_atomic

    def delayed(self, *args, **kwargs):
        started.set()
        assert release.wait(5)
        result = original(self, *args, **kwargs)
        finished.set()
        return result

    monkeypatch.setattr(ConfinedFilesystem, "write_atomic", delayed)
    task = asyncio.create_task(service.write(actor=ALICE, space_id=space.id, expected_generation=1, operation_id=operation(), path="x", content=b"a", create=True))
    assert await asyncio.to_thread(started.wait, 5)
    task.cancel()
    await asyncio.sleep(0.02)
    assert not task.done()
    release.set()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert finished.is_set()
    assert await service.read(actor=ALICE, space_id=space.id, path="x", max_bytes=100) == b"a"


@pytest.mark.asyncio
async def test_cross_audience_copy_requires_export_and_explicit_disclosure(space_file_storage):
    service, _, _ = space_file_storage
    source, destination = await home(service, "private"), await home(service, "broader")
    destination = await service.registry.set_grant(actor=ALICE, space_id=destination.id, subject=BOB, permissions=Permission.READ, expected_generation=1, acknowledge_existing_data=True)
    await service.write(actor=ALICE, space_id=source.id, expected_generation=1, operation_id=operation(), path="secret", content=b"private", create=True)
    kwargs = dict(actor=ALICE, source_id=source.id, destination_id=destination.id, source_generation=1, destination_generation=2, source_path="secret", destination_path="copy", operation_id=operation())
    with pytest.raises(SpaceDenied, match="disclosure"):
        await service.copy(**kwargs)
    await service.copy(**kwargs, acknowledge_disclosure=True)
    assert await service.read(actor=BOB, space_id=destination.id, path="copy", max_bytes=100) == b"private"
    source = await service.registry.set_grant(actor=ALICE, space_id=source.id, subject=ALICE, permissions=Permission.READ | Permission.WRITE | Permission.ADMIN, expected_generation=1)
    with pytest.raises(SpaceDenied):
        await service.copy(**{**kwargs, "source_generation": source.generation, "operation_id": operation()}, acknowledge_disclosure=True)


@pytest.mark.asyncio
async def test_pending_operation_id_in_another_resource_is_not_an_admission_bypass(space_file_storage):
    from deerflow.persistence.spaces.files import SpaceFileOperationRow

    service, sf, _ = space_file_storage
    source, destination = await home(service), await home(service)
    op = operation()
    async with sf() as session:
        session.add(SpaceFileOperationRow(space_id=source.id, operation_id=op, actor_kind=ALICE.kind, actor_id=ALICE.subject_id, generation=1, phase="pending", request={"action": "write"}))
        await session.commit()
    with pytest.raises(SpaceOperationPending):
        await service.copy(actor=ALICE, source_id=source.id, destination_id=destination.id, source_generation=1, destination_generation=1, source_path="unknown", destination_path="x", operation_id=op)


@pytest.mark.asyncio
async def test_expected_folder_conflicts_do_not_freeze_the_resource(space_file_storage):
    service, _, _ = space_file_storage
    space = await home(service)
    await service.mkdir(actor=ALICE, space_id=space.id, expected_generation=1, operation_id=operation(), path="folder")
    with pytest.raises(SpaceConflict, match="already exists"):
        await service.mkdir(actor=ALICE, space_id=space.id, expected_generation=1, operation_id=operation(), path="folder")
    await service.write(actor=ALICE, space_id=space.id, expected_generation=1, operation_id=operation(), path="folder/page", content=b"keep", create=True)
    with pytest.raises(SpaceConflict, match="not empty"):
        await service.remove(actor=ALICE, space_id=space.id, expected_generation=1, operation_id=operation(), path="folder")
    assert await service.read(actor=ALICE, space_id=space.id, path="folder/page", max_bytes=10) == b"keep"
