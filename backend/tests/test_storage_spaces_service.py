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


@pytest.mark.parametrize("kind", ["user", "company", "assistant", "employee"])
@pytest.mark.asyncio
async def test_typed_assignment_creates_layout_and_atomic_role_link(space_file_storage, kind):
    import stat

    from deerflow.features.resources import StorageFeatureLinkRow
    from deerflow.persistence.spaces.files import SpaceBackingRow
    from deerflow.spaces.provision import LAYOUT

    files, sf, catalog = space_file_storage
    async with sf.kw["bind"].begin() as connection:
        await connection.run_sync(StorageFeatureLinkRow.__table__.create)
    for spec, slot_kind in zip(catalog.volumes.values(), ["employee", "assistant", "company", "user"], strict=True):
        spec.kind = slot_kind
    space_id = await files.assign(kind=kind, actor=ALICE, name="Assigned", custody=Custody.company() if kind == "company" else Custody.personal(ALICE), mode=MutationMode.NATIVE)
    async with sf() as session:
        binding = (await session.execute(select(SpaceBackingRow).where(SpaceBackingRow.space_id == space_id))).scalar_one()
        links = (await session.execute(select(StorageFeatureLinkRow))).scalars().all()
    volume = catalog.verified[binding.slot_id]
    assert volume.spec.kind == kind
    for root, names in zip((volume.data_path, volume.control_path), LAYOUT[kind], strict=True):
        assert {p.name for p in root.iterdir()} == set(names) | ({".space-binding"} if root == volume.control_path else set())
        for name in names:
            metadata = (root / name).stat()
            assert stat.S_IMODE(metadata.st_mode) == 0o700
            assert (metadata.st_uid, metadata.st_gid) == (root.stat().st_uid, root.stat().st_gid)
    expected = {"user": [("hm.user-space", "human:alice")], "company": [("hm.company-space", "company")]}.get(kind, [])
    assert [(row.namespace, row.key) for row in links] == expected
    assert all(row.space_id == space_id for row in links)
    assert binding.slot_id in files.bound_slots[kind]
    assert binding.slot_id not in files.unclaimed_slots[kind]


@pytest.mark.asyncio
async def test_provisioning_skips_wrong_kind_failed_and_nonempty_slots(space_file_storage):
    from deerflow.spaces.backings import BackingUnavailable
    from deerflow.spaces.service import SpaceCapacityExhausted

    files, _, catalog = space_file_storage
    slots = list(catalog.volumes)
    for slot, kind in zip(slots, ["company", "assistant", "assistant", "assistant"], strict=True):
        catalog.volumes[slot].kind = kind
    (catalog.verified[slots[2]].control_path / "retained").write_bytes(b"keep")
    original = catalog.verify

    def verify(slot, **kwargs):
        if slot == slots[1]:
            raise BackingUnavailable("unverified fixture slot")
        return original(slot, **kwargs)

    catalog.verify = verify
    async with files.provisioning(actor=ALICE, name="Assistant", custody=Custody.personal(ALICE), mode=MutationMode.NATIVE, kind="assistant"):
        pass
    assert files.bound_slots["assistant"] == {slots[3]}
    assert files.unclaimed_slots["assistant"] == {slots[1], slots[2]}
    with pytest.raises(SpaceCapacityExhausted):
        async with files.provisioning(actor=ALICE, name="Another", custody=Custody.personal(ALICE), mode=MutationMode.NATIVE, kind="assistant"):
            pytest.fail("claimed another kind or retained/unverified slot")


@pytest.mark.asyncio
async def test_untyped_inventory_can_assign_each_kind_and_reload_bound_slots(space_file_storage):
    files, _, catalog = space_file_storage
    for spec in catalog.volumes.values():
        spec.kind = None
    for kind in ("user", "assistant", "employee", "company"):
        async with files.provisioning(actor=ALICE, name=kind, custody=Custody.personal(ALICE), mode=MutationMode.NATIVE, kind=kind):
            pass
    restarted = SpaceFiles(files.registry, catalog)
    await restarted.load_bound_slots()
    await restarted.load_unclaimed_slots()
    assert set().union(*restarted.bound_slots.values()) == set(catalog.volumes)
    assert not set().union(*restarted.unclaimed_slots.values())


@pytest.mark.asyncio
async def test_unclaimed_slots_are_recomputed_without_claiming_or_removing_files(space_file_storage):
    from deerflow.spaces.backings import BackingUnavailable

    files, _, catalog = space_file_storage
    slots = list(catalog.volumes)
    for spec in catalog.volumes.values():
        spec.kind = "user"
    async with files.provisioning(actor=ALICE, name="Bound", custody=Custody.personal(ALICE), mode=MutationMode.NATIVE, kind="user"):
        pass
    retained = catalog.verified[slots[1]].data_path / "retained"
    retained.write_bytes(b"preserve")
    original = catalog.verify

    def verify(slot, **kwargs):
        if slot == slots[2]:
            raise BackingUnavailable("unverified fixture slot")
        return original(slot, **kwargs)

    catalog.verify = verify
    restarted = SpaceFiles(files.registry, catalog)
    await restarted.load_bound_slots()
    await restarted.load_unclaimed_slots()
    assert restarted.bound_slots["user"] == {slots[0]}
    assert restarted.unclaimed_slots["user"] == {slots[1], slots[2]}
    retained.unlink()
    catalog.verify = original
    await restarted.load_unclaimed_slots()
    assert not restarted.unclaimed_slots["user"]


@pytest.mark.parametrize("change", ["grow", "shrink-bytes", "shrink-inodes", "new-uuid", "new-root", "new-control"])
@pytest.mark.asyncio
async def test_grown_binding_is_recorded_only_for_same_incarnation(space_file_storage, change):
    from types import SimpleNamespace
    from uuid import uuid4

    from _storage_spaces_test_support import FixtureVolume

    from deerflow.persistence.spaces.files import SpaceBackingRow
    from deerflow.spaces.backings import BackingUnavailable

    files, sf, catalog = space_file_storage
    space = await home(files)
    async with sf() as session:
        binding = await session.get(SpaceBackingRow, space.backing_handle)
        slot, original_bytes, original_inodes = binding.slot_id, binding.max_bytes, binding.max_inodes
    previous = catalog.verified[slot]
    spec = SimpleNamespace(**vars(previous.spec))
    spec.max_bytes *= 2
    spec.max_inodes *= 2
    if change == "shrink-bytes":
        spec.max_bytes = original_bytes - 1
    if change == "shrink-inodes":
        spec.max_inodes = original_inodes - 1
    if change == "new-uuid":
        spec.filesystem_uuid = str(uuid4())
    data, control = previous.data_path, previous.control_path
    if change == "new-root":
        data = data.parent / "replacement-data"
        data.mkdir()
    if change == "new-control":
        control = control.parent / "replacement-control"
        control.mkdir()
    catalog.volumes[slot] = spec
    catalog.verified[slot] = FixtureVolume(spec, data, control)
    restarted = SpaceFiles(files.registry, catalog)
    if change == "grow":
        assert await restarted.list_directory(actor=ALICE, space_id=space.id) == ([], False)
    else:
        with pytest.raises(BackingUnavailable):
            await restarted.list_directory(actor=ALICE, space_id=space.id)
    async with sf() as session:
        binding = await session.get(SpaceBackingRow, space.backing_handle)
        assert (binding.max_bytes, binding.max_inodes) == ((spec.max_bytes, spec.max_inodes) if change == "grow" else (original_bytes, original_inodes))


@pytest.mark.parametrize("export", [False, True])
@pytest.mark.asyncio
async def test_bounded_read_retries_one_file_change_and_no_other_conflict(space_file_storage, monkeypatch, export):
    from deerflow.spaces.filesystem import FileConflict

    files, _, _ = space_file_storage
    space = await home(files)
    calls = []

    def changing(_self, path, **kwargs):
        calls.append(path)
        if len(calls) == 1:
            raise FileConflict("File changed during its bounded read; retry")
        return b"new version"

    monkeypatch.setattr(ConfinedFilesystem, "read_bytes", changing)
    read = files.export_file if export else files.read
    assert await read(actor=ALICE, space_id=space.id, path="changing", max_bytes=100) == b"new version"
    assert len(calls) == 2

    def always_changing(*args, **kwargs):
        calls.append("again")
        raise FileConflict("File changed during its bounded read; retry")

    monkeypatch.setattr(ConfinedFilesystem, "read_bytes", always_changing)
    with pytest.raises(FileConflict):
        await read(actor=ALICE, space_id=space.id, path="changing", max_bytes=100)
    assert len(calls) == 4


@pytest.mark.parametrize("assign", [False, True])
@pytest.mark.asyncio
async def test_cancelled_layout_keeps_allocation_ownership_until_worker_finishes(space_file_storage, monkeypatch, assign):
    import threading

    from deerflow.features.resources import StorageFeatureLinkRow
    from deerflow.persistence.spaces.files import SpaceBackingRow

    files, sf, catalog = space_file_storage
    async with sf.kw["bind"].begin() as connection:
        await connection.run_sync(StorageFeatureLinkRow.__table__.create)
    for spec in catalog.volumes.values():
        spec.kind = "user"
    entered, finish = threading.Event(), threading.Event()
    layout = files._layout

    def blocked(volume, kind):
        entered.set()
        assert finish.wait(timeout=10)
        layout(volume, kind)

    monkeypatch.setattr(files, "_layout", blocked)

    async def allocate():
        if assign:
            return await files.assign(kind="user", actor=ALICE, name="Assigned", custody=Custody.personal(ALICE), mode=MutationMode.NATIVE)
        async with files.provisioning(actor=ALICE, name="Assigned", custody=Custody.personal(ALICE), mode=MutationMode.NATIVE, kind="user"):
            pass

    task = asyncio.create_task(allocate())
    try:
        for _ in range(200):
            if entered.is_set():
                break
            await asyncio.sleep(0.01)
        assert entered.is_set()
        task.cancel()
        await asyncio.sleep(0.02)
        assert not task.done(), "allocation unlocked while its layout worker still ran"
        finish.set()
        with pytest.raises(asyncio.CancelledError):
            await task
        async with sf() as session:
            rows = (await session.execute(select(SpaceBackingRow))).scalars().all()
        assert len(rows) == (1 if assign else 0)
        first = next(iter(catalog.volumes))
        assert (catalog.verified[first].data_path / "files").is_dir()
        assert first in (files.bound_slots["user"] if assign else files.unclaimed_slots["user"])
    finally:
        finish.set()
        if not task.done():
            await asyncio.gather(task, return_exceptions=True)


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
        session.add(SpaceFileOperationRow(space_id=source.id, operation_id=op, actor_kind=ALICE.kind, actor_id=ALICE.subject_id, generation=1, phase="pending", request={"action": "copy"}))
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
