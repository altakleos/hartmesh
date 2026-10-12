"""Delayed capacity snapshots cannot overwrite a newer allocation."""

import asyncio

import pytest
from _storage_spaces_test_support import home
from sqlalchemy.ext.asyncio import AsyncSession
from test_storage_spaces_service import space_file_storage as space_file_storage

from deerflow.persistence.spaces.files import SpaceBackingRow


@pytest.mark.asyncio
async def test_old_empty_binding_snapshot_cannot_clear_a_new_allocation(space_file_storage, monkeypatch):
    files, sf, _catalog = space_file_storage
    entered, finish = asyncio.Event(), asyncio.Event()
    execute = AsyncSession.execute
    loading = None

    async def held(session, statement, *args, **kwargs):
        result = await execute(session, statement, *args, **kwargs)
        if asyncio.current_task() is loading and SpaceBackingRow.__tablename__ in str(statement):
            entered.set()
            await finish.wait()
        return result

    monkeypatch.setattr(AsyncSession, "execute", held)
    loading = asyncio.create_task(files.load_bound_slots())
    try:
        async with asyncio.timeout(5):
            await entered.wait()
        resource = await home(files)
        async with sf() as session:
            slot = (await session.get(SpaceBackingRow, resource.backing_handle)).slot_id
        assert slot in files.bound_slots[None]
        finish.set()
        await loading
        assert slot in files.bound_slots[None]
        assert slot not in files.unclaimed_slots[None]
    finally:
        finish.set()
        await asyncio.gather(loading, return_exceptions=True)


@pytest.mark.asyncio
async def test_old_uncertainty_probe_cannot_mark_a_new_bound_slot_unclaimed(space_file_storage, monkeypatch):
    from deerflow.spaces import service

    files, _sf, catalog = space_file_storage
    slot = next(iter(catalog.volumes))
    volume = catalog.verified[slot]
    residue = volume.control_path / "old-private-stage"
    residue.write_bytes(b"old retained uncertainty")
    entered, finish = asyncio.Event(), asyncio.Event()
    original = service.run_file_io
    loading = None
    captured = False

    async def held(function, *args, **kwargs):
        nonlocal captured
        result = await original(function, *args, **kwargs)
        if asyncio.current_task() is loading and function is files._empty and not captured:
            captured = True
            assert result is False
            entered.set()
            await finish.wait()
        return result

    monkeypatch.setattr(service, "run_file_io", held)
    loading = asyncio.create_task(files.load_unclaimed_slots())
    try:
        async with asyncio.timeout(5):
            await entered.wait()
        residue.unlink()
        resource = await home(files)
        assert slot in files.bound_slots[None]
        finish.set()
        await loading
        assert slot in files.bound_slots[None]
        assert slot not in files.unclaimed_slots[None]
        assert (volume.control_path / ".space-binding").read_bytes() == resource.backing_handle.encode("ascii")
    finally:
        finish.set()
        await asyncio.gather(loading, return_exceptions=True)


@pytest.mark.asyncio
async def test_empty_probe_and_metadata_cannot_clear_an_owned_allocation(space_file_storage, monkeypatch):
    from _storage_spaces_test_support import ALICE

    from deerflow.spaces import service
    from deerflow.spaces.contract import Custody, MutationMode

    files, _sf, catalog = space_file_storage
    slot = next(iter(catalog.volumes))
    entered, marker, yielded, rollback = (asyncio.Event() for _ in range(4))
    original = service.run_file_io

    async def held(function, *args, **kwargs):
        if function is service.prepare_marker:
            entered.set()
            await marker.wait()
        return await original(function, *args, **kwargs)

    async def allocate():
        async with files.provisioning(actor=ALICE, name="Interrupted", custody=Custody.personal(ALICE), mode=MutationMode.NATIVE):
            yielded.set()
            await rollback.wait()
            raise RuntimeError("fixture caller rollback")

    monkeypatch.setattr(service, "run_file_io", held)
    task = asyncio.create_task(allocate())
    try:
        async with asyncio.timeout(5):
            await entered.wait()
        claim = files._claim(slot)
        assert claim.phase == "allocating"
        assert not any(catalog.verified[slot].control_path.iterdir())
        await files.load_unclaimed_slots()
        await files.load_bound_slots()
        assert files._claim(slot) is claim
        assert slot not in files.assignable_slots(None)
        marker.set()
        async with asyncio.timeout(5):
            await yielded.wait()
        await files.load_bound_slots()
        assert files._claim(slot) is claim
        assert slot not in files.bound_slots[None]
        rollback.set()
        with pytest.raises(RuntimeError, match="fixture caller rollback"):
            await task
        assert files._claim(slot).phase == "unclaimed"
        assert files._claim(slot).handle == claim.handle
        assert slot not in files.assignable_slots(None)
        assert (catalog.verified[slot].control_path / ".space-binding").read_bytes() == claim.handle.encode("ascii")
    finally:
        marker.set()
        rollback.set()
        await asyncio.gather(task, return_exceptions=True)


@pytest.mark.asyncio
async def test_free_slot_tombstones_reject_a_previous_free_claim(space_file_storage):
    files, _sf, catalog = space_file_storage
    slot = next(iter(catalog.volumes))
    await files.load_unclaimed_slots()
    old = files._claim(slot)
    current = files._publish_claim(slot, old, "a" * 32, "allocating")
    freed = files._publish_claim(slot, current, None, "free")
    assert freed is not old
    assert files._publish_claim(slot, old, "b" * 32, "unclaimed") is None
    assert not files._cache_bound(slot, old, "b" * 32, catalog.verified[slot])
    assert files._claim(slot) is freed
    assert slot in files.assignable_slots(None)
    assert slot not in files._verified


@pytest.mark.asyncio
async def test_refresh_commit_failure_publishes_no_cache_facts(space_file_storage):
    from sqlalchemy import event
    from sqlalchemy.orm import Session

    files, sf, _catalog = space_file_storage
    resource = await home(files)
    async with sf() as session:
        binding = await session.get(SpaceBackingRow, resource.backing_handle)
        slot = binding.slot_id
    claim = files._claim(slot)
    sentinel = object()
    files._verified[slot] = sentinel
    owner = asyncio.current_task()

    def failed_commit(session):
        if asyncio.current_task() is owner:
            raise OSError("fixture refresh commit failure")

    event.listen(Session, "before_commit", failed_commit)
    try:
        with pytest.raises(OSError, match="fixture refresh commit failure"):
            await files.refresh_bound_volumes()
    finally:
        event.remove(Session, "before_commit", failed_commit)
    assert files._claim(slot) is claim
    assert files._verified[slot] is sentinel
    assert slot in files.bound_slots[None]


@pytest.mark.asyncio
async def test_old_refresh_proof_cannot_replace_a_new_claim(space_file_storage, monkeypatch):
    from deerflow.spaces import service

    files, sf, catalog = space_file_storage
    resource = await home(files)
    async with sf() as session:
        slot = (await session.get(SpaceBackingRow, resource.backing_handle)).slot_id
    entered, finish = asyncio.Event(), asyncio.Event()
    original = service.run_file_io
    task = None

    async def held(function, *args, **kwargs):
        result = await original(function, *args, **kwargs)
        if asyncio.current_task() is task and function is service.verify_bound:
            entered.set()
            await finish.wait()
        return result

    monkeypatch.setattr(service, "run_file_io", held)
    task = asyncio.create_task(files.refresh_bound_volumes())
    try:
        async with asyncio.timeout(5):
            await entered.wait()
        old = files._claim(slot)
        freed = files._publish_claim(slot, old, None, "free")
        new = files._publish_claim(slot, freed, "b" * 32, "allocating")
        finish.set()
        assert await task == {}
        assert files._claim(slot) is new
        assert slot not in files._verified
        assert slot not in files.bound_slots[None]
        assert slot in files.unclaimed_slots[None]
        assert slot not in files.assignable_slots(None)
    finally:
        finish.set()
        await asyncio.gather(task, return_exceptions=True)


@pytest.mark.asyncio
async def test_stale_bound_cache_cannot_override_current_empty_sql_and_files(space_file_storage):
    from deerflow.spaces.registry import SpaceRegistry
    from deerflow.spaces.service import SpaceFiles

    files, sf, catalog = space_file_storage
    old = await home(files)
    async with sf() as session, session.begin():
        binding = await session.get(SpaceBackingRow, old.backing_handle)
        slot = binding.slot_id
        await session.delete(binding)
    # Model independently completed retirement, leaving this instance's cache old.
    independent = SpaceFiles(SpaceRegistry(sf, files.registry._resolver), catalog)
    (catalog.verified[slot].control_path / ".space-binding").unlink()
    await independent.load_bound_slots()
    await independent.load_unclaimed_slots()
    assert slot in independent.assignable_slots(None)
    assert files._claim(slot).handle == old.backing_handle
    assert slot in files.bound_slots[None]
    for other, volume in catalog.verified.items():
        if other != slot:
            (volume.control_path / "retained").write_bytes(b"unavailable")
    new = await home(files)
    async with sf() as session:
        assert (await session.get(SpaceBackingRow, new.backing_handle)).slot_id == slot
    assert files._claim(slot).handle == new.backing_handle
    assert slot in files.bound_slots[None]
    assert slot not in files.unclaimed_slots[None]


@pytest.mark.asyncio
async def test_binding_missing_from_inventory_remains_bound_and_unverified(space_file_storage):
    from deerflow.spaces.registry import SpaceRegistry
    from deerflow.spaces.service import SpaceFiles

    files, sf, catalog = space_file_storage
    resource = await home(files)
    async with sf() as session:
        slot = (await session.get(SpaceBackingRow, resource.backing_handle)).slot_id
    del catalog.volumes[slot]
    restarted = SpaceFiles(SpaceRegistry(sf, files.registry._resolver), catalog)
    await restarted.load_bound_slots()
    assert slot in restarted.bound_slots[None]
    errors = await restarted.refresh_bound_volumes()
    assert slot in errors
    assert "inventory" in errors[slot]
    assert slot in restarted.bound_slots[None]
    assert slot not in restarted._verified
    await restarted.load_unclaimed_slots()
    assert slot in restarted.bound_slots[None]
