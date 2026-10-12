"""Bound filesystem identity survives catalog refresh and numeric inode reuse."""

import asyncio
import os
import threading
from contextlib import asynccontextmanager

import pytest
from _storage_spaces_test_support import ALICE, home
from sqlalchemy import select
from test_agent_instances import instances as instances
from test_storage_spaces_attachments import storage as storage
from test_storage_spaces_service import space_file_storage as space_file_storage
from test_storage_spaces_startup import startup as startup

from deerflow.persistence.spaces.files import SpaceBackingRow, SpaceFileOperationRow
from deerflow.persistence.spaces.model import SpaceRow
from deerflow.spaces.filesystem import FilesystemUnavailable

MARKER = ".space-binding"


async def bound(files, sf, space):
    async with sf() as session:
        row = await session.get(SpaceRow, space.id)
        return await files._volume(session, row)


@pytest.mark.asyncio
async def test_allocation_publishes_private_marker_of_exact_durable_binding(space_file_storage):
    files, sf, _ = space_file_storage
    space = await home(files)
    volume = await bound(files, sf, space)
    marker = volume.control_path / MARKER
    assert marker.read_bytes() == space.backing_handle.encode("ascii")
    assert marker.stat().st_mode & 0o777 == 0o600
    assert not (volume.data_path / MARKER).exists()
    with volume.filesystem() as filesystem:
        assert filesystem.list_directory() == ([], False)


@pytest.mark.asyncio
@pytest.mark.parametrize("changed", ["missing", "different", "oversized", "fifo", "link", "hardlink", "public-mode"])
async def test_bound_filesystem_refuses_unproved_private_incarnation(space_file_storage, changed, tmp_path):
    files, sf, _ = space_file_storage
    space = await home(files)
    volume = await bound(files, sf, space)
    marker = volume.control_path / MARKER
    marker.unlink()
    if changed == "different":
        marker.write_bytes(b"0" * 32)
        marker.chmod(0o600)
    elif changed == "oversized":
        marker.write_bytes(space.backing_handle.encode("ascii") + b"x" * 65536)
        marker.chmod(0o600)
    elif changed == "fifo":
        os.mkfifo(marker, 0o600)
    elif changed in ("link", "hardlink"):
        unrelated = tmp_path / "foreign-marker"
        unrelated.write_bytes(space.backing_handle.encode("ascii"))
        unrelated.chmod(0o600)
        if changed == "link":
            marker.symlink_to(unrelated)
        else:
            os.link(unrelated, marker)
    elif changed == "public-mode":
        marker.write_bytes(space.backing_handle.encode("ascii"))
        marker.chmod(0o644)
    with pytest.raises(FilesystemUnavailable):
        with volume.filesystem():
            pytest.fail("A bound view admitted an unproved incarnation")


@pytest.mark.asyncio
async def test_stale_view_refuses_a_different_binding_even_with_identical_inode_facts(space_file_storage):
    files, sf, catalog = space_file_storage
    space = await home(files)
    old = await bound(files, sf, space)
    from deerflow.spaces.incarnations import BoundVolume

    old_facts = old.root_inode, old.control_inode, old.device
    replacement_handle = "f" * 32 if space.backing_handle != "f" * 32 else "e" * 32
    (old.control_path / MARKER).write_bytes(replacement_handle.encode("ascii"))
    current = BoundVolume(catalog.verify(old.spec.slot_id), replacement_handle)
    assert (current.root_inode, current.control_inode, current.device) == old_facts
    from deerflow.config.paths import Paths
    from deerflow.spaces.chat_paths import ChatStoragePaths

    base = Paths(old.data_path.parent / "operational")
    previous_paths = ChatStoragePaths(base, user_id="alice", user=old)
    current_paths = ChatStoragePaths(base, user_id="alice", user=current)
    assert previous_paths.routing_signature() != current_paths.routing_signature()
    with current.filesystem():
        pass
    with pytest.raises(FilesystemUnavailable):
        with old.filesystem():
            pytest.fail("Old numeric inode facts cannot admit another resource")


@pytest.mark.asyncio
async def test_runtime_admission_never_backfills_missing_marker(space_file_storage):
    files, sf, _ = space_file_storage
    space = await home(files)
    volume = await bound(files, sf, space)
    (volume.control_path / MARKER).unlink()
    with pytest.raises(FilesystemUnavailable):
        await files.list_directory(actor=ALICE, space_id=space.id)
    assert not (volume.control_path / MARKER).exists()


@pytest.mark.asyncio
async def test_startup_backfill_only_repairs_absence_for_existing_binding(space_file_storage):
    files, sf, _ = space_file_storage
    space = await home(files)
    volume = await bound(files, sf, space)
    marker = volume.control_path / MARKER
    marker.unlink()
    receipt = volume.control_path / "retained-receipt"
    receipt.write_bytes(b"retain pending evidence")
    assert await files.backfill_binding_markers() == {}
    assert marker.read_bytes() == space.backing_handle.encode("ascii")
    assert receipt.read_bytes() == b"retain pending evidence"
    marker.write_bytes(b"0" * 32)
    assert volume.spec.slot_id in await files.backfill_binding_markers()
    assert marker.read_bytes() == b"0" * 32


@pytest.mark.asyncio
async def test_bound_refresh_preserves_incarnation_enforcement(space_file_storage):
    files, sf, _ = space_file_storage
    space = await home(files)
    volume = await bound(files, sf, space)
    assert await files.refresh_bound_volumes() == {}
    refreshed = await bound(files, sf, space)
    assert refreshed.backing_handle == space.backing_handle
    (volume.control_path / MARKER).write_bytes(b"0" * 32)
    with pytest.raises(FilesystemUnavailable):
        with refreshed.filesystem():
            pytest.fail("Catalog refresh stripped the binding expectation")


@pytest.mark.asyncio
@pytest.mark.parametrize("changed", ["missing", "different"])
@pytest.mark.parametrize("reuse", [False, True])
async def test_unproved_marker_never_reaches_attachment_provider(storage, monkeypatch, changed, reuse):
    from deerflow.spaces.attachments import AttachmentPending, ResourceMount
    from deerflow.spaces.backings import BackingUnavailable

    files, sf, _catalog, attachments, provider = storage
    space = await home(files)
    volume = await bound(files, sf, space)
    from _storage_spaces_test_support import operation

    incarnation = operation()
    resources = [ResourceMount(space.id, space.generation, "home", writable=True)]
    if reuse:
        await attachments.attach(actor=ALICE, incarnation=incarnation, resources=resources)
    marker = volume.control_path / MARKER
    if changed == "missing":
        marker.unlink()
    else:
        marker.write_bytes(b"0" * 32)

    def forbidden(*args, **kwargs):
        pytest.fail("Unproved binding reached the attachment provider")

    monkeypatch.setattr(provider, "prepare", forbidden)
    monkeypatch.setattr(provider, "start", forbidden)
    monkeypatch.setattr(provider, "verify_active", forbidden)
    with pytest.raises(AttachmentPending) as refusal:
        if reuse:
            await attachments.resume(actor=ALICE, incarnation=incarnation, resources=resources)
        else:
            await attachments.attach(actor=ALICE, incarnation=incarnation, resources=resources)

    assert isinstance(refusal.value.__cause__, BackingUnavailable)


@pytest.mark.asyncio
async def test_backfill_waits_for_initial_mount_completion_and_runs_once(startup, monkeypatch):
    from deerflow.spaces.contract import Custody, MutationMode
    from deerflow.spaces.service import SpaceFiles

    storage, app, config, sf, _catalog, _adapter, agents = startup
    original_files = agents.files
    async with original_files.provisioning(actor=ALICE, name="Home", custody=Custody.personal(ALICE), mode=MutationMode.NATIVE, kind="user") as (_session, space):
        pass
    volume = await bound(original_files, sf, space)
    marker = volume.control_path / MARKER
    marker.unlink()
    entered, finish = asyncio.Event(), asyncio.Event()
    backfill_calls = []
    original = SpaceFiles.backfill_binding_markers

    async def held_mount(_verb):
        entered.set()
        await finish.wait()
        return {"returncode": 0}

    async def counted(files):
        backfill_calls.append(files)
        assert finish.is_set(), "Backfill started before mount completion"
        return await original(files)

    monkeypatch.setenv("HARTMESH_STORAGE_INIT_IMAGE", "unused-fixture-image")
    monkeypatch.setattr(storage, "run_storage_init", held_mount)
    monkeypatch.setattr(storage, "STORAGE_INIT_VERBS", {"mount-all": 0.01})
    monkeypatch.setattr(storage, "RECHECK_SECONDS", 3600)
    monkeypatch.setattr(SpaceFiles, "backfill_binding_markers", counted)
    try:
        await storage.initialize_storage_spaces(app, config, session_factory=sf)
        assert entered.is_set() and app.state.agent_execution.mount_pending
        await asyncio.sleep(0)
        assert backfill_calls == [] and not marker.exists()
        finish.set()
        await app.state.agent_execution.mount_task
        await storage.storage_recheck(app)
        assert len(backfill_calls) == 1
        assert marker.read_bytes() == space.backing_handle.encode("ascii")
        assert await app.state.storage_spaces.binding_verified(space.id)
        marker.unlink()
        app.state.agent_execution.metadata_task = asyncio.create_task(storage._load_metadata(app))
        await app.state.agent_execution.metadata_task
        assert len(backfill_calls) == 1 and not marker.exists()
    finally:
        finish.set()


@pytest.mark.asyncio
async def test_live_metadata_recheck_never_recreates_removed_marker(startup):
    import asyncio

    from deerflow.features.resources import StorageFeatureLinkRow

    storage, app, config, sf, _catalog, _adapter, _agents = startup
    await storage.initialize_storage_spaces(app, config, session_factory=sf)
    files = app.state.storage_spaces
    async with sf() as session:
        identifier = (await session.get(StorageFeatureLinkRow, ("hm.company-space", "company"))).space_id
        row = await session.get(SpaceRow, identifier)
        volume = await files._volume(session, row)
    marker = volume.control_path / MARKER
    marker.unlink()
    app.state.agent_execution.storage_reason = "admission-failed"
    app.state.agent_execution.metadata_task = asyncio.create_task(storage._load_metadata(app))
    await app.state.agent_execution.metadata_task
    await storage.storage_recheck(app)
    assert not marker.exists()
    assert not await files.binding_verified(identifier)


@pytest.mark.asyncio
async def test_allocation_rollback_retains_unclaimed_marker_without_binding(space_file_storage):
    from deerflow.spaces.contract import Custody, MutationMode

    files, sf, catalog = space_file_storage
    with pytest.raises(RuntimeError, match="fixture publication rollback"):
        async with files.provisioning(actor=ALICE, name="Attempted", custody=Custody.personal(ALICE), mode=MutationMode.NATIVE) as (_session, _space):
            raise RuntimeError("fixture publication rollback")
    async with sf() as session:
        assert not (await session.scalars(select(SpaceBackingRow))).all()
        assert not (await session.scalars(select(SpaceRow))).all()
    retained = {slot for slot, volume in catalog.verified.items() if (volume.control_path / MARKER).exists()}
    assert len(retained) == 1 and retained <= files.unclaimed_slots[None]
    space = await home(files)
    volume = await bound(files, sf, space)
    assert volume.spec.slot_id not in retained


@pytest.mark.asyncio
async def test_backfill_never_adopts_an_unbound_marker_or_changes_conflict(space_file_storage):
    files, sf, catalog = space_file_storage
    unbound = next(iter(catalog.verified.values()))
    (unbound.control_path / MARKER).write_bytes(b"0" * 32)
    assert await files.backfill_binding_markers() == {}
    space = await home(files)
    volume = await bound(files, sf, space)
    (volume.control_path / MARKER).write_bytes(b"0" * 32)
    before = {path.name: path.read_bytes() for path in volume.control_path.iterdir()}
    assert volume.spec.slot_id in await files.backfill_binding_markers()
    assert {path.name: path.read_bytes() for path in volume.control_path.iterdir()} == before


@pytest.mark.asyncio
async def test_backfill_rechecks_exact_binding_before_any_private_effect(space_file_storage, monkeypatch):
    from deerflow.spaces import incarnation_backfill

    files, sf, _catalog = space_file_storage
    space = await home(files)
    volume = await bound(files, sf, space)
    marker = volume.control_path / MARKER
    marker.unlink()
    original = incarnation_backfill.run_file_io

    async def change_after_inspection(function, *args, **kwargs):
        result = await original(function, *args, **kwargs)
        if function is incarnation_backfill._inspect:
            async with sf() as session, session.begin():
                (await session.get(SpaceBackingRow, space.backing_handle)).max_inodes -= 1
        return result

    monkeypatch.setattr(incarnation_backfill, "run_file_io", change_after_inspection)
    assert volume.spec.slot_id in await files.backfill_binding_markers()
    assert not marker.exists()
    assert not (volume.control_path / ".host-workflow.lock").exists()


@pytest.mark.asyncio
async def test_backfill_cancellation_drains_worker_and_private_ownership(space_file_storage, monkeypatch):
    from deerflow.spaces import incarnation_backfill
    from deerflow.spaces.service import SpaceOperationPending
    from deerflow.spaces.workflows import acquire, close

    files, sf, catalog = space_file_storage
    space = await home(files)
    volume = await bound(files, sf, space)
    (volume.control_path / MARKER).unlink()
    entered, resume = threading.Event(), threading.Event()
    original = incarnation_backfill.publish_marker

    def blocked(*args):
        entered.set()
        assert resume.wait(10)
        return original(*args)

    monkeypatch.setattr(incarnation_backfill, "publish_marker", blocked)
    task = asyncio.create_task(files.backfill_binding_markers())
    try:
        assert await asyncio.to_thread(entered.wait, 5)
        task.cancel()
        await asyncio.sleep(0.02)
        assert not task.done()
        with pytest.raises(SpaceOperationPending):
            await asyncio.to_thread(acquire, {space.id: catalog.verified[volume.spec.slot_id]})
        resume.set()
        with pytest.raises(asyncio.CancelledError):
            await task
        with volume.filesystem():
            pass
        descriptors = await asyncio.to_thread(acquire, {space.id: catalog.verified[volume.spec.slot_id]})
        await asyncio.to_thread(close, descriptors)
    finally:
        resume.set()
        await asyncio.gather(task, return_exceptions=True)


@pytest.mark.asyncio
async def test_delayed_settlement_never_creates_lock_in_retired_unbound_slot(space_file_storage, monkeypatch):
    from _storage_spaces_test_support import operation

    from deerflow.spaces import maintenance

    files, sf, _catalog = space_file_storage
    space = await home(files)
    volume = await bound(files, sf, space)
    async with sf() as session, session.begin():
        session.add(SpaceFileOperationRow(space_id=space.id, operation_id=operation(), actor_kind="human", actor_id="alice", generation=1, phase="pending", request={"action": "remove", "path": "absent"}))
    original = maintenance.run_file_io

    async def retire_after_ownership(function, *args, **kwargs):
        result = await original(function, *args, **kwargs)
        if function is maintenance._owned:
            async with sf() as session, session.begin():
                (await session.get(SpaceRow, space.id)).status = "deleted"
                await session.delete(await session.get(SpaceBackingRow, space.backing_handle))
        return result

    monkeypatch.setattr(maintenance, "run_file_io", retire_after_ownership)
    assert await files.settle_interrupted() == 0
    assert not (volume.control_path / ".host-workflow.lock").exists()


@pytest.mark.asyncio
@pytest.mark.parametrize("mode,status", [("native", "archived"), ("native", "deleted"), ("mediated", "active"), ("mediated", "deleted")])
async def test_backfill_is_generic_private_metadata_for_existing_bindings(space_file_storage, mode, status):
    from deerflow.spaces.contract import Custody, FeatureBinding, MutationMode

    files, sf, _catalog = space_file_storage
    space = await files.create(actor=ALICE, name="Existing", custody=Custody.personal(ALICE), mode=MutationMode(mode), feature=FeatureBinding("plugin", "controller", 1) if mode == "mediated" else None)
    volume = await bound(files, sf, space)
    async with sf() as session, session.begin():
        (await session.get(SpaceRow, space.id)).status = status
    (volume.control_path / MARKER).unlink()
    (volume.data_path / "retained").write_bytes(b"ordinary data")
    assert await files.backfill_binding_markers() == {}
    assert (volume.control_path / MARKER).read_bytes() == space.backing_handle.encode("ascii")
    assert (volume.data_path / "retained").read_bytes() == b"ordinary data"


@pytest.mark.asyncio
async def test_backfill_commit_failure_retains_existing_binding_and_evidence(space_file_storage, monkeypatch):
    from deerflow.spaces.workflows import acquire, close

    files, sf, catalog = space_file_storage
    space = await home(files)
    volume = await bound(files, sf, space)
    marker = volume.control_path / MARKER
    marker.unlink()
    receipt = volume.control_path / "retained-receipt"
    receipt.write_bytes(b"pending evidence")
    original = files.registry.host_locked

    @asynccontextmanager
    async def lost_commit(identifiers):
        async with original(identifiers) as admission:
            yield admission
            raise OSError("fixture backfill commit failure")

    monkeypatch.setattr(files.registry, "host_locked", lost_commit)
    assert volume.spec.slot_id in await files.backfill_binding_markers()
    async with sf() as session:
        binding = await session.get(SpaceBackingRow, space.backing_handle)
        assert binding is not None and binding.space_id == space.id
    assert marker.read_bytes() == space.backing_handle.encode("ascii")
    assert receipt.read_bytes() == b"pending evidence"
    descriptors = await asyncio.to_thread(acquire, {space.id: catalog.verified[volume.spec.slot_id]})
    await asyncio.to_thread(close, descriptors)
