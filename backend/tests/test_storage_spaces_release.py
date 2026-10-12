"""Host reclamation retains durable authority and fences retired roots."""

import os

import pytest
from _storage_spaces_test_support import ALICE, home, operation
from sqlalchemy import select, update
from test_storage_spaces_service import space_file_storage as space_file_storage

from deerflow.persistence.spaces.files import SpaceBackingRow, SpaceFileOperationRow
from deerflow.persistence.spaces.model import SpaceEventRow, SpaceGrantRow, SpaceRow
from deerflow.spaces.filesystem import FilesystemUnavailable
from deerflow.spaces.incarnations import BoundVolume
from deerflow.spaces.recovery import SpaceRecovery


async def deleted(sf, space, *, retained=False, status="deleted", event=True):
    async with sf() as session, session.begin():
        row = await session.get(SpaceRow, space.id)
        row.status, row.generation = status, 2
        if event:
            session.add(SpaceEventRow(space_id=space.id, generation=2, action="delete", actor_kind="human", actor_id=ALICE.subject_id, details={"retained_backups": retained, "retained_binding": True, "cause": "seat-released"}))
        binding = await session.get(SpaceBackingRow, space.backing_handle)
        return binding.slot_id


@pytest.mark.asyncio
@pytest.mark.parametrize("status,retained,event", [("active", False, True), ("archived", False, True), ("deleted", True, True), ("deleted", 0, True), ("deleted", "false", True), ("deleted", None, True), ("deleted", False, False)])
async def test_release_requires_deleted_status_and_exact_false_delete_event(space_file_storage, status, retained, event):
    files, sf, catalog = space_file_storage
    space = await home(files)
    slot = await deleted(sf, space, retained=retained, status=status, event=event)
    volume = catalog.verified[slot]
    (volume.data_path / "retained").write_bytes(b"safe")
    result = await SpaceRecovery(files).host_release(space.id)
    assert result == {"complete": False, "slot_released": False}
    assert (volume.data_path / "retained").read_bytes() == b"safe"
    async with sf() as session:
        assert await session.get(SpaceBackingRow, space.backing_handle) is not None
        assert not (await session.scalars(select(SpaceFileOperationRow))).all()


@pytest.mark.asyncio
async def test_release_renews_roots_preserves_tombstone_and_recycles_only_after_commit(space_file_storage, monkeypatch):
    files, sf, catalog = space_file_storage
    space = await home(files)
    slot = await deleted(sf, space)
    volume = catalog.verified[slot]
    stale = BoundVolume(volume, space.backing_handle)
    owned = stale.filesystem()
    old_roots = volume.root_inode, volume.control_inode
    (volume.data_path / "old-document").write_bytes(b"private")
    (volume.control_path / "retained-backup").write_bytes(b"private")
    pending_id = operation()
    async with sf() as session, session.begin():
        session.add(SpaceFileOperationRow(space_id=space.id, operation_id=pending_id, actor_kind="human", actor_id=ALICE.subject_id, generation=1, phase="pending", request={"action": "write", "path": "uncertain"}))

    async def no_principal(*_args, **_kwargs):
        pytest.fail("Host release resolved a principal")

    monkeypatch.setattr(files.registry, "_actor", no_principal)
    try:
        result = await SpaceRecovery(files).host_release(space.id)
        assert result["complete"] and result["slot_released"] and result["newly_reclaimed"]
        assert result["cause"] == "seat-released"
        assert (volume.root_inode, volume.control_inode) != old_roots
        assert not any(volume.data_path.iterdir()) and not any(volume.control_path.iterdir())
        assert not any(p.name.startswith(".space-release-") for p in volume.data_path.parent.iterdir())
        with pytest.raises(OSError):
            os.mkdir("late-write", dir_fd=owned._data_fd)
        with pytest.raises((OSError, FilesystemUnavailable)):
            with stale.filesystem():
                pytest.fail("Retired bound view reopened")
        async with sf() as session:
            assert await session.get(SpaceBackingRow, space.backing_handle) is None
            row = await session.get(SpaceRow, space.id)
            assert (row.status, row.generation) == ("deleted", 2)
            assert await session.get(SpaceGrantRow, (space.id, "human", ALICE.subject_id)) is not None
            events = (await session.scalars(select(SpaceEventRow).where(SpaceEventRow.space_id == space.id))).all()
            assert len(events) == 2
            pending = await session.get(SpaceFileOperationRow, (space.id, pending_id))
            assert pending.phase == "failed" and pending.result == {"resolution": "host-discarded"}
            intents = (await session.scalars(select(SpaceFileOperationRow).where(SpaceFileOperationRow.actor_id == "host:space-release"))).all()
            assert len(intents) == 1 and intents[0].phase == "complete"
            assert intents[0].actor_kind == "nonhuman" and intents[0].request["action"] == "host-release"
        assert slot in files.assignable_slots(None)
        assert slot not in files._verified
        again = await SpaceRecovery(files).host_release(space.id)
        assert again["complete"] and again["slot_released"] and not again["newly_reclaimed"]
    finally:
        owned.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("boundary", ["initialize", "rotate-data", "rotate-control", "purge-data", "purge-control", "finish", "unlink-state", "unlink-lock", "rmdir-journal"])
async def test_release_retries_after_each_filesystem_boundary_with_fresh_caches(space_file_storage, monkeypatch, boundary):
    from deerflow.spaces import release_filesystem
    from deerflow.spaces.registry import SpaceRegistry
    from deerflow.spaces.service import SpaceFiles, SpaceOperationPending

    files, sf, catalog = space_file_storage
    space = await home(files)
    slot = await deleted(sf, space)
    volume = catalog.verified[slot]
    (volume.data_path / "document").write_bytes(b"private")
    (volume.control_path / "backup").write_bytes(b"private")
    triggered = False

    def failed():
        nonlocal triggered
        if not triggered:
            triggered = True
            raise OSError("fixture interrupted release boundary")

    if boundary in ("initialize", "finish"):
        original = getattr(release_filesystem.ReleaseFilesystem, boundary)

        def held(owner, *args, **kwargs):
            result = original(owner, *args, **kwargs)
            failed()
            return result

        monkeypatch.setattr(release_filesystem.ReleaseFilesystem, boundary, held)
    elif boundary.startswith(("rotate-", "purge-")):
        action, target = boundary.split("-")
        method = "rotate" if action == "rotate" else "purge_retired"
        original = getattr(release_filesystem.ReleaseFilesystem, method)

        def held(owner, name):
            result = original(owner, name)
            if name == target:
                failed()
            return result

        monkeypatch.setattr(release_filesystem.ReleaseFilesystem, method, held)
    elif boundary.startswith("unlink-"):
        target = "state.json" if boundary == "unlink-state" else "lock"
        original = os.unlink

        def held(name, *args, **kwargs):
            result = original(name, *args, **kwargs)
            if name == target:
                failed()
            return result

        monkeypatch.setattr(os, "unlink", held)
    else:
        original = os.rmdir

        def held(name, *args, **kwargs):
            result = original(name, *args, **kwargs)
            if str(name).startswith(release_filesystem.PREFIX):
                failed()
            return result

        monkeypatch.setattr(os, "rmdir", held)
    with pytest.raises(SpaceOperationPending, match="Release is incomplete"):
        await SpaceRecovery(files).host_release(space.id)
    assert triggered
    async with sf() as session:
        assert await session.get(SpaceBackingRow, space.backing_handle) is not None
        intents = (await session.scalars(select(SpaceFileOperationRow).where(SpaceFileOperationRow.actor_id == "host:space-release"))).all()
        assert len(intents) == 1 and intents[0].phase == "pending"
        intent_id = intents[0].operation_id
    restarted = SpaceFiles(SpaceRegistry(sf, files.registry._resolver), catalog)
    result = await SpaceRecovery(restarted).host_release(space.id)
    assert result["newly_reclaimed"] and result["slot_released"]
    assert not any(volume.data_path.iterdir()) and not any(volume.control_path.iterdir())
    async with sf() as session:
        assert await session.get(SpaceBackingRow, space.backing_handle) is None
        intents = (await session.scalars(select(SpaceFileOperationRow).where(SpaceFileOperationRow.actor_id == "host:space-release"))).all()
        assert len(intents) == 1 and intents[0].operation_id == intent_id and intents[0].phase == "complete"


@pytest.mark.asyncio
@pytest.mark.parametrize("target", ["journal", "lock", "replacement", "state"])
async def test_control_only_full_slot_bootstraps_before_journal_publication(space_file_storage, monkeypatch, target):
    import errno

    from deerflow.spaces import release_filesystem

    files, sf, catalog = space_file_storage
    space = await home(files)
    slot = await deleted(sf, space)
    volume = catalog.verified[slot]
    (volume.control_path / "large-retained-backup").write_bytes(b"private")
    assert not any(volume.data_path.iterdir())
    original_mkdir, original_open, original_write = os.mkdir, os.open, os.write
    triggered = False

    def enospc():
        nonlocal triggered
        if triggered:
            return
        triggered = True
        assert (volume.control_path / "large-retained-backup").exists()
        raise OSError(errno.ENOSPC, "fixture full control capacity")

    def mkdir(name, *args, **kwargs):
        if target == "journal" and str(name).startswith(release_filesystem.PREFIX):
            enospc()
        if target == "replacement" and name == "data":
            enospc()
        return original_mkdir(name, *args, **kwargs)

    def opened(name, flags, *args, **kwargs):
        if target == "lock" and name == "lock" and flags & os.O_CREAT:
            enospc()
        return original_open(name, flags, *args, **kwargs)

    def written(fd, raw):
        if target == "state" and b"request_sha256" in raw:
            enospc()
        return original_write(fd, raw)

    monkeypatch.setattr(os, "mkdir", mkdir)
    monkeypatch.setattr(os, "open", opened)
    monkeypatch.setattr(os, "write", written)
    result = await SpaceRecovery(files).host_release(space.id)
    assert triggered and result["newly_reclaimed"]
    assert not any(volume.data_path.iterdir()) and not any(volume.control_path.iterdir())
    # ENOSPC injection exercises retry ownership; this fixture does not attest quotas.


@pytest.mark.asyncio
async def test_final_domain_publication_failure_retries_without_journal(space_file_storage):
    from deerflow.features.resources import StorageFeatureLinkRow
    from deerflow.spaces.registry import SpaceRegistry
    from deerflow.spaces.service import SpaceFiles, SpaceOperationPending

    files, sf, catalog = space_file_storage
    async with sf.kw["bind"].begin() as connection:
        await connection.run_sync(StorageFeatureLinkRow.__table__.create)
    space = await home(files)
    slot = await deleted(sf, space)
    async with sf() as session, session.begin():
        session.add(StorageFeatureLinkRow(namespace="example.owner", key="one", space_id=space.id))
    failed = False

    async def publish(session):
        nonlocal failed
        assert session.in_transaction()
        link = await session.get(StorageFeatureLinkRow, ("example.owner", "one"))
        assert link.space_id == space.id
        await session.delete(link)
        if not failed:
            failed = True
            raise OSError("fixture domain publication failure")

    with pytest.raises(SpaceOperationPending):
        await SpaceRecovery(files).host_release(space.id, publication=publish)
    volume = catalog.verified[slot]
    assert not any(p.name.startswith(".space-release-") for p in volume.data_path.parent.iterdir())
    async with sf() as session:
        assert await session.get(SpaceBackingRow, space.backing_handle) is not None
        assert await session.get(StorageFeatureLinkRow, ("example.owner", "one")) is not None
        intent = (await session.scalars(select(SpaceFileOperationRow))).one()
        assert intent.result["stage"] == "filesystem-complete" and intent.phase == "pending"
    restarted = SpaceFiles(SpaceRegistry(sf, files.registry._resolver), catalog)
    result = await SpaceRecovery(restarted).host_release(space.id, publication=publish)
    assert result["newly_reclaimed"]
    async with sf() as session:
        assert await session.get(StorageFeatureLinkRow, ("example.owner", "one")) is None
        assert await session.get(SpaceBackingRow, space.backing_handle) is None


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ["directory", "file", "symlink"])
async def test_unresolved_sibling_journal_keeps_empty_roots_unassignable(space_file_storage, tmp_path, kind):
    files, sf, catalog = space_file_storage
    slot = next(iter(catalog.volumes))
    volume = catalog.verified[slot]
    journal = volume.data_path.parent / (".space-release-" + "a" * 32)
    if kind == "directory":
        journal.mkdir(mode=0o700)
        (journal / "uncertain").write_bytes(b"preserve")
    elif kind == "file":
        journal.write_bytes(b"preserve")
    else:
        outside = tmp_path / "outside"
        outside.mkdir()
        journal.symlink_to(outside, target_is_directory=True)
    await files.load_bound_slots()
    await files.load_unclaimed_slots()
    assert slot not in files.assignable_slots(None)
    space = await home(files)
    async with sf() as session:
        assert (await session.get(SpaceBackingRow, space.backing_handle)).slot_id != slot
    assert journal.lstat()


@pytest.mark.asyncio
@pytest.mark.parametrize("phase", ["pending", "active", "fence_pending", "fenced"])
async def test_release_requires_every_attachment_fenced(space_file_storage, phase):
    from deerflow.persistence.spaces.lifecycle import SpaceAttachmentRow, SpaceMountRow

    files, sf, catalog = space_file_storage
    space = await home(files)
    slot = await deleted(sf, space)
    volume = catalog.verified[slot]
    (volume.data_path / "retained").write_bytes(b"private")
    async with sf() as session, session.begin():
        attachment = SpaceAttachmentRow(id=operation(), incarnation=operation(), actor_kind="human", actor_id="alice", host_id="qualified-host", phase=phase)
        session.add(attachment)
        await session.flush()
        session.add(SpaceMountRow(attachment_id=attachment.id, space_id=space.id, generation=2, alias="files", writable=0))
    result = await SpaceRecovery(files).host_release(space.id)
    if phase == "fenced":
        assert result["newly_reclaimed"]
    else:
        assert result == {"complete": False, "slot_released": False}
        assert (volume.data_path / "retained").read_bytes() == b"private"
        async with sf() as session:
            assert await session.get(SpaceBackingRow, space.backing_handle) is not None
            assert not (await session.scalars(select(SpaceFileOperationRow))).all()


@pytest.mark.asyncio
@pytest.mark.parametrize("stage", ["intent", "checkpoint", "final"])
async def test_release_commit_failure_keeps_binding_and_retries_from_durable_progress(space_file_storage, stage):
    from sqlalchemy import event
    from sqlalchemy.orm import Session

    from deerflow.spaces.registry import SpaceRegistry
    from deerflow.spaces.service import SpaceFiles, SpaceOperationPending

    files, sf, catalog = space_file_storage
    space = await home(files)
    slot = await deleted(sf, space)
    volume = catalog.verified[slot]
    (volume.data_path / "private").write_bytes(b"private")
    triggered = False

    def failed_commit(session):
        nonlocal triggered
        if triggered:
            return
        for row in (*session.new, *session.dirty):
            if not isinstance(row, SpaceFileOperationRow) or row.actor_id != "host:space-release":
                continue
            selected = (stage == "intent" and row in session.new) or (stage == "checkpoint" and row.phase == "pending" and row.result.get("stage") == "filesystem-complete") or (stage == "final" and row.phase == "complete")
            if selected:
                triggered = True
                raise OSError("fixture release commit failure")

    event.listen(Session, "before_commit", failed_commit)
    try:
        with pytest.raises(SpaceOperationPending):
            await SpaceRecovery(files).host_release(space.id)
    finally:
        event.remove(Session, "before_commit", failed_commit)
    assert triggered
    async with sf() as session:
        assert await session.get(SpaceBackingRow, space.backing_handle) is not None
        intents = (await session.scalars(select(SpaceFileOperationRow).where(SpaceFileOperationRow.actor_id == "host:space-release"))).all()
        if stage == "intent":
            assert not intents and (volume.data_path / "private").read_bytes() == b"private"
        else:
            assert len(intents) == 1 and intents[0].phase == "pending"
            assert intents[0].result["stage"] == ("filesystem-complete" if stage == "final" else "intent")
    restarted = SpaceFiles(SpaceRegistry(sf, files.registry._resolver), catalog)
    result = await SpaceRecovery(restarted).host_release(space.id)
    assert result["newly_reclaimed"]
    async with sf() as session:
        assert await session.get(SpaceBackingRow, space.backing_handle) is None
        intents = (await session.scalars(select(SpaceFileOperationRow).where(SpaceFileOperationRow.actor_id == "host:space-release"))).all()
        assert len(intents) == 1 and intents[0].phase == "complete"


@pytest.mark.asyncio
async def test_newer_retained_delete_event_refuses_host_release(space_file_storage):
    files, sf, catalog = space_file_storage
    space = await home(files)
    slot = await deleted(sf, space)
    async with sf() as session, session.begin():
        row = await session.get(SpaceRow, space.id)
        row.generation = 3
        session.add(SpaceEventRow(space_id=space.id, generation=3, action="delete", actor_kind="human", actor_id=ALICE.subject_id, details={"retained_backups": True}))
    result = await SpaceRecovery(files).host_release(space.id)
    assert result == {"complete": False, "slot_released": False}
    assert (catalog.verified[slot].control_path / ".space-binding").exists()


@pytest.mark.asyncio
@pytest.mark.parametrize("phase", ["bootstrap", "purge_retired"])
async def test_root_replaced_between_observation_and_destructive_open_is_preserved(space_file_storage, monkeypatch, phase):
    import errno

    from deerflow.spaces import release_filesystem
    from deerflow.spaces.service import SpaceOperationPending

    files, sf, catalog = space_file_storage
    space = await home(files)
    slot = await deleted(sf, space)
    volume = catalog.verified[slot]
    active, observed, triggered = False, False, False
    owner_ref = None
    original_method = getattr(release_filesystem.ReleaseFilesystem, phase)
    original_at = release_filesystem.ReleaseFilesystem._at
    original_child, original_mkdir = release_filesystem._child, os.mkdir

    def method(owner, *args):
        nonlocal active, owner_ref
        active, owner_ref = True, owner
        try:
            return original_method(owner, *args)
        finally:
            active = False

    def inspected(owner, parent, name):
        nonlocal observed
        result = original_at(owner, parent, name)
        target = owner.parent if phase == "bootstrap" else owner.journal
        if active and parent == target and name == "data" and result is not None:
            observed = True
        return result

    def opened(parent, name, *args, **kwargs):
        nonlocal triggered, observed
        target = (owner_ref.parent if phase == "bootstrap" else owner_ref.journal) if owner_ref is not None else None
        if active and observed and not triggered and parent == target and name == "data":
            triggered, observed = True, False
            os.rename("data", "preserved-original-root", src_dir_fd=target, dst_dir_fd=owner_ref.parent)
            original_mkdir("data", dir_fd=target)
            fd = original_child(target, "data")
            try:
                file = os.open("innocent", os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600, dir_fd=fd)
                try:
                    os.write(file, b"replacement bytes must survive")
                finally:
                    os.close(file)
            finally:
                os.close(fd)
        return original_child(parent, name, *args, **kwargs)

    def mkdir(name, *args, **kwargs):
        if phase == "bootstrap" and str(name).startswith(release_filesystem.PREFIX) and not triggered:
            raise OSError(errno.ENOSPC, "fixture requires bootstrap")
        return original_mkdir(name, *args, **kwargs)

    monkeypatch.setattr(release_filesystem.ReleaseFilesystem, phase, method)
    monkeypatch.setattr(release_filesystem.ReleaseFilesystem, "_at", inspected)
    monkeypatch.setattr(release_filesystem, "_child", opened)
    monkeypatch.setattr(os, "mkdir", mkdir)
    with pytest.raises(SpaceOperationPending):
        await SpaceRecovery(files).host_release(space.id)
    assert triggered
    if phase == "bootstrap":
        replacement = volume.data_path / "innocent"
    else:
        journal = next(p for p in volume.data_path.parent.iterdir() if p.name.startswith(release_filesystem.PREFIX))
        replacement = journal / "data" / "innocent"
    assert replacement.read_bytes() == b"replacement bytes must survive"
    async with sf() as session:
        assert await session.get(SpaceBackingRow, space.backing_handle) is not None


@pytest.mark.asyncio
async def test_unwritable_parent_refuses_before_intent_then_operator_repair_allows_release(space_file_storage, monkeypatch):
    import errno

    from deerflow.spaces import release_filesystem
    from deerflow.spaces.service import SpaceOperationPending

    files, sf, catalog = space_file_storage
    space = await home(files)
    slot = await deleted(sf, space)
    volume = catalog.verified[slot]
    parent_inode = volume.data_path.parent.stat().st_ino
    original_facts, original_mkdir = release_filesystem._facts, os.mkdir
    repaired = False

    def facts(metadata):
        result = original_facts(metadata)
        if metadata.st_ino == parent_inode and metadata.st_dev == volume.device:
            # Model an operator-owned parent inaccessible to this host identity.
            result.update(uid=os.geteuid() + 1, gid=os.getegid() if repaired else 65534, mode=0o770 if repaired else 0o755)
        return result

    def mkdir(name, *args, **kwargs):
        if not repaired and str(name).startswith(release_filesystem.PREFIX):
            raise OSError(errno.EACCES, "fixture operator parent needs repair")
        return original_mkdir(name, *args, **kwargs)

    monkeypatch.setattr(release_filesystem, "_facts", facts)
    monkeypatch.setattr(os, "mkdir", mkdir)
    with pytest.raises(SpaceOperationPending):
        await SpaceRecovery(files).host_release(space.id)
    async with sf() as session:
        assert await session.get(SpaceBackingRow, space.backing_handle) is not None
        assert not (await session.scalars(select(SpaceFileOperationRow))).all()
    assert (volume.control_path / ".space-binding").exists()
    repaired = True
    result = await SpaceRecovery(files).host_release(space.id)
    assert result["newly_reclaimed"]


async def interrupted_before_rotation(files, space_id, monkeypatch):
    from deerflow.spaces import release_filesystem
    from deerflow.spaces.service import SpaceOperationPending

    initialize = release_filesystem.ReleaseFilesystem.initialize

    def stopped(owner):
        initialize(owner)
        raise OSError("fixture stop before rotation")

    with monkeypatch.context() as patch:
        patch.setattr(release_filesystem.ReleaseFilesystem, "initialize", stopped)
        with pytest.raises(SpaceOperationPending):
            await SpaceRecovery(files).host_release(space_id)


@pytest.mark.asyncio
@pytest.mark.parametrize("damage", ["json", "large", "symlink", "fifo", "hardlink", "mode"])
async def test_malformed_journal_never_purges_original_roots(space_file_storage, monkeypatch, tmp_path, damage):
    from deerflow.spaces.service import SpaceOperationPending

    files, sf, catalog = space_file_storage
    space = await home(files)
    slot = await deleted(sf, space)
    volume = catalog.verified[slot]
    (volume.data_path / "safe").write_bytes(b"original")
    await interrupted_before_rotation(files, space.id, monkeypatch)
    journal = next(p for p in volume.data_path.parent.iterdir() if p.name.startswith(".space-release-"))
    state = journal / "state.json"
    outside = tmp_path / "outside-proof"
    outside.write_bytes(b"preserve outside")
    if damage == "json":
        state.write_bytes(b"null")
    elif damage == "large":
        state.write_bytes(b"x" * 65537)
    elif damage == "symlink":
        state.unlink()
        state.symlink_to(outside)
    elif damage == "fifo":
        state.unlink()
        os.mkfifo(state, mode=0o600)
    elif damage == "hardlink":
        os.link(state, journal / "unexpected-link")
    else:
        state.chmod(0o644)
    with pytest.raises(SpaceOperationPending):
        await SpaceRecovery(files).host_release(space.id)
    assert (volume.data_path / "safe").read_bytes() == b"original"
    assert outside.read_bytes() == b"preserve outside"
    async with sf() as session:
        assert await session.get(SpaceBackingRow, space.backing_handle) is not None
        intent = (await session.scalars(select(SpaceFileOperationRow))).one()
        assert intent.phase == "pending" and intent.result["stage"] == "intent"


@pytest.mark.asyncio
@pytest.mark.parametrize("damage", ["operation-id", "request-version", "result-version", "checkpoint"])
async def test_malformed_durable_intent_refuses_before_effects(space_file_storage, monkeypatch, damage):
    from deerflow.spaces.service import SpaceOperationPending

    files, sf, catalog = space_file_storage
    space = await home(files)
    slot = await deleted(sf, space)
    volume = catalog.verified[slot]
    (volume.data_path / "safe").write_bytes(b"original")
    await interrupted_before_rotation(files, space.id, monkeypatch)
    async with sf() as session, session.begin():
        intent = (await session.scalars(select(SpaceFileOperationRow))).one()
        if damage == "operation-id":
            intent.operation_id = "../" + "a" * 29
        elif damage == "request-version":
            await session.execute(update(SpaceFileOperationRow).where(SpaceFileOperationRow.space_id == space.id, SpaceFileOperationRow.operation_id == intent.operation_id).values(request={**intent.request, "schema_version": True}))
        elif damage == "result-version":
            await session.execute(update(SpaceFileOperationRow).where(SpaceFileOperationRow.space_id == space.id, SpaceFileOperationRow.operation_id == intent.operation_id).values(result={"schema_version": True, "stage": "intent"}))
        else:
            intent.result = {"schema_version": 1, "stage": "filesystem-complete"}
    with pytest.raises(SpaceOperationPending):
        await SpaceRecovery(files).host_release(space.id)
    assert (volume.data_path / "safe").read_bytes() == b"original"
    async with sf() as session:
        assert await session.get(SpaceBackingRow, space.backing_handle) is not None


@pytest.mark.asyncio
async def test_ordinary_settlement_never_interprets_host_release_intent(space_file_storage, monkeypatch):
    files, sf, _catalog = space_file_storage
    space = await home(files)
    await deleted(sf, space)
    await interrupted_before_rotation(files, space.id, monkeypatch)
    async with sf() as session:
        old = (await session.scalars(select(SpaceFileOperationRow))).one()
        facts = old.operation_id, old.phase, old.request, old.result
    assert await files.settle_interrupted() == 0
    async with sf() as session:
        current = (await session.scalars(select(SpaceFileOperationRow))).one()
        assert (current.operation_id, current.phase, current.request, current.result) == facts


@pytest.mark.asyncio
async def test_repaired_parent_metadata_can_resume_an_existing_intent(space_file_storage, monkeypatch):
    from deerflow.spaces import release_filesystem

    files, sf, catalog = space_file_storage
    space = await home(files)
    slot = await deleted(sf, space)
    volume = catalog.verified[slot]
    await interrupted_before_rotation(files, space.id, monkeypatch)
    parent_inode = volume.data_path.parent.stat().st_ino
    original = release_filesystem._facts

    def repaired(metadata):
        result = original(metadata)
        if metadata.st_ino == parent_inode:
            result["mode"] = 0o770
        return result

    monkeypatch.setattr(release_filesystem, "_facts", repaired)
    result = await SpaceRecovery(files).host_release(space.id)
    assert result["newly_reclaimed"]


@pytest.mark.asyncio
async def test_lost_eligibility_after_intent_keeps_original_binding_and_data(space_file_storage, monkeypatch):
    files, sf, catalog = space_file_storage
    space = await home(files)
    slot = await deleted(sf, space)
    volume = catalog.verified[slot]
    (volume.data_path / "safe").write_bytes(b"original")
    await interrupted_before_rotation(files, space.id, monkeypatch)
    async with sf() as session, session.begin():
        event = await session.get(SpaceEventRow, (space.id, 2))
        event.details = {**event.details, "retained_backups": True}
    assert await SpaceRecovery(files).host_release(space.id) == {"complete": False, "slot_released": False}
    assert (volume.data_path / "safe").read_bytes() == b"original"
    async with sf() as session:
        assert await session.get(SpaceBackingRow, space.backing_handle) is not None
        intent = (await session.scalars(select(SpaceFileOperationRow))).one()
        assert intent.phase == "pending"


@pytest.mark.asyncio
async def test_journal_path_replacement_after_read_keeps_original_data(space_file_storage, monkeypatch):
    from deerflow.spaces import release_filesystem
    from deerflow.spaces.service import SpaceOperationPending

    files, sf, catalog = space_file_storage
    space = await home(files)
    slot = await deleted(sf, space)
    volume = catalog.verified[slot]
    (volume.data_path / "safe").write_bytes(b"original")
    await interrupted_before_rotation(files, space.id, monkeypatch)
    original = release_filesystem.ReleaseFilesystem._read_state
    triggered = False

    def changed(owner):
        nonlocal triggered
        original(owner)
        if not triggered and owner.state is not None:
            triggered = True
            os.rename(owner.name, "preserved-journal", src_dir_fd=owner.parent, dst_dir_fd=owner.parent)
            os.mkdir(owner.name, mode=0o700, dir_fd=owner.parent)
            replacement = volume.data_path.parent / owner.name
            (replacement / "innocent").write_bytes(b"preserve replacement")

    monkeypatch.setattr(release_filesystem.ReleaseFilesystem, "_read_state", changed)
    with pytest.raises(SpaceOperationPending):
        await SpaceRecovery(files).host_release(space.id)
    assert triggered
    assert (volume.data_path / "safe").read_bytes() == b"original"
    replacement = next(p for p in volume.data_path.parent.iterdir() if p.name.startswith(".space-release-"))
    assert (replacement / "innocent").read_bytes() == b"preserve replacement"
    async with sf() as session:
        assert await session.get(SpaceBackingRow, space.backing_handle) is not None


@pytest.mark.asyncio
@pytest.mark.parametrize("rollback", [False, True])
async def test_new_allocation_after_release_commit_keeps_its_claim(space_file_storage, monkeypatch, rollback):
    import asyncio
    from contextlib import asynccontextmanager

    from deerflow.spaces.contract import Custody, MutationMode

    files, sf, catalog = space_file_storage
    space = await home(files)
    slot = await deleted(sf, space)
    committed, finish = asyncio.Event(), asyncio.Event()
    original = files.registry.host_locked

    @asynccontextmanager
    async def held(identifiers, **kwargs):
        final = False
        async with original(identifiers, **kwargs) as (session, rows):
            yield session, rows
            final = any(isinstance(row, SpaceFileOperationRow) and row.space_id == space.id and row.actor_id == "host:space-release" and row.phase == "complete" for row in session.identity_map.values())
        if final:
            committed.set()
            await finish.wait()

    monkeypatch.setattr(files.registry, "host_locked", held)
    task = asyncio.create_task(SpaceRecovery(files).host_release(space.id))
    try:
        async with asyncio.timeout(5):
            await committed.wait()
        if rollback:
            with pytest.raises(RuntimeError, match="fixture next allocation rollback"):
                async with files.provisioning(actor=ALICE, name="Next", custody=Custody.personal(ALICE), mode=MutationMode.NATIVE) as (_session, next_space):
                    raise RuntimeError("fixture next allocation rollback")
        else:
            next_space = await home(files)
        newer = files._claim(slot)
        assert newer.handle == next_space.backing_handle
        finish.set()
        result = await task
        assert result["newly_reclaimed"]
        assert files._claim(slot) is newer
        assert (catalog.verified[slot].control_path / ".space-binding").read_bytes() == next_space.backing_handle.encode("ascii")
        if rollback:
            assert newer.phase == "unclaimed" and slot in files.unclaimed_slots[None]
            assert slot not in files._verified
        else:
            assert newer.phase == "bound" and slot in files.bound_slots[None]
            assert slot in files._verified
        assert slot not in files.assignable_slots(None)
    finally:
        finish.set()
        await asyncio.gather(task, return_exceptions=True)


@pytest.mark.asyncio
async def test_cancelled_release_drains_workers_and_transaction_before_return(space_file_storage, monkeypatch):
    import asyncio
    import threading

    from deerflow.spaces import release_filesystem

    files, sf, _catalog = space_file_storage
    space = await home(files)
    await deleted(sf, space)
    entered, finish = threading.Event(), threading.Event()
    original = release_filesystem.ReleaseFilesystem.rotate

    def held(owner, name):
        if name == "data":
            entered.set()
            assert finish.wait(5), "fixture release worker not resumed"
        return original(owner, name)

    monkeypatch.setattr(release_filesystem.ReleaseFilesystem, "rotate", held)
    task = asyncio.create_task(SpaceRecovery(files).host_release(space.id))
    try:
        assert await asyncio.to_thread(entered.wait, 5)
        task.cancel()
        async with sf() as session:
            assert await session.get(SpaceBackingRow, space.backing_handle) is not None
        assert not task.done()
        finish.set()
        with pytest.raises(asyncio.CancelledError):
            await task
        async with sf() as session:
            assert await session.get(SpaceBackingRow, space.backing_handle) is None
            intent = (await session.scalars(select(SpaceFileOperationRow))).one()
            assert intent.phase == "complete"
    finally:
        finish.set()
        await asyncio.gather(task, return_exceptions=True)


@pytest.mark.asyncio
@pytest.mark.parametrize("boundary", ["journal", "lock", "data", "control", "temp-write", "journal-sync", "parent-sync"])
async def test_initialization_crash_retries_without_adopting_uncertain_entries(space_file_storage, monkeypatch, boundary):
    from deerflow.spaces import release_filesystem
    from deerflow.spaces.registry import SpaceRegistry
    from deerflow.spaces.service import SpaceFiles, SpaceOperationPending

    files, sf, catalog = space_file_storage
    space = await home(files)
    slot = await deleted(sf, space)
    volume = catalog.verified[slot]
    (volume.data_path / "original").write_bytes(b"safe")
    original_mkdir, original_open, original_write, original_sync = os.mkdir, os.open, os.write, os.fsync
    triggered = False
    parent_inode = volume.data_path.parent.stat().st_ino

    def interrupted():
        nonlocal triggered
        if not triggered:
            triggered = True
            raise OSError("fixture interrupted journal initialization")

    def mkdir(name, *args, **kwargs):
        result = original_mkdir(name, *args, **kwargs)
        if boundary == "journal" and str(name).startswith(release_filesystem.PREFIX):
            interrupted()
        elif boundary in ("data", "control") and name == boundary:
            interrupted()
        return result

    def opened(name, flags, *args, **kwargs):
        fd = original_open(name, flags, *args, **kwargs)
        if boundary == "lock" and name == "lock" and flags & os.O_CREAT and not triggered:
            # Model crash after closed lock publication, not an unowned FD leak.
            os.close(fd)
            interrupted()
        return fd

    def written(fd, raw):
        if boundary == "temp-write" and b"request_sha256" in raw and not triggered:
            original_write(fd, raw[:8])
            interrupted()
        return original_write(fd, raw)

    def synced(fd):
        result = original_sync(fd)
        if not triggered and boundary == "parent-sync" and os.fstat(fd).st_ino == parent_inode:
            interrupted()
        if not triggered and boundary == "journal-sync":
            try:
                name = os.readlink(f"/proc/self/fd/{fd}")
            except OSError:
                name = ""
            if Path(name).name.startswith(release_filesystem.PREFIX):
                interrupted()
        return result

    from pathlib import Path

    with monkeypatch.context() as patch:
        patch.setattr(os, "mkdir", mkdir)
        patch.setattr(os, "open", opened)
        patch.setattr(os, "write", written)
        patch.setattr(os, "fsync", synced)
        with pytest.raises(SpaceOperationPending):
            await SpaceRecovery(files).host_release(space.id)
    assert triggered
    assert (volume.data_path / "original").read_bytes() == b"safe"
    async with sf() as session:
        assert await session.get(SpaceBackingRow, space.backing_handle) is not None
        intent = (await session.scalars(select(SpaceFileOperationRow))).one()
        assert intent.phase == "pending" and intent.result["stage"] == "intent"
    restarted = SpaceFiles(SpaceRegistry(sf, files.registry._resolver), catalog)
    result = await SpaceRecovery(restarted).host_release(space.id)
    assert result["newly_reclaimed"]
    assert not any(volume.data_path.iterdir()) and not any(volume.control_path.iterdir())


@pytest.mark.asyncio
async def test_concurrent_full_slot_retry_has_one_owned_bootstrap(space_file_storage, monkeypatch):
    import asyncio
    import errno
    import threading

    from deerflow.spaces import release_filesystem
    from deerflow.spaces.registry import SpaceRegistry
    from deerflow.spaces.service import SpaceFiles, SpaceOperationPending

    files, sf, catalog = space_file_storage
    space = await home(files)
    slot = await deleted(sf, space)
    volume = catalog.verified[slot]
    (volume.control_path / "full-private-backup").write_bytes(b"private")
    independent = SpaceFiles(SpaceRegistry(sf, files.registry._resolver), catalog)
    proof = threading.Barrier(2)
    entered, finish = threading.Event(), threading.Event()
    original_facts = release_filesystem.ReleaseFilesystem.original_facts
    original_bootstrap = release_filesystem.ReleaseFilesystem.bootstrap
    original_mkdir = os.mkdir
    faulted, bootstraps = False, 0

    def qualified(owner, handle):
        result = original_facts(owner, handle)
        proof.wait(timeout=5)
        return result

    def bootstrap(owner):
        nonlocal bootstraps
        bootstraps += 1
        assert owner.lock is None and owner.journal is None
        entered.set()
        assert finish.wait(5), "fixture bootstrap did not resume"
        return original_bootstrap(owner)

    def mkdir(name, *args, **kwargs):
        nonlocal faulted
        if str(name).startswith(release_filesystem.PREFIX) and not faulted:
            faulted = True
            raise OSError(errno.ENOSPC, "fixture no release ownership capacity")
        return original_mkdir(name, *args, **kwargs)

    monkeypatch.setattr(release_filesystem.ReleaseFilesystem, "original_facts", qualified)
    monkeypatch.setattr(release_filesystem.ReleaseFilesystem, "bootstrap", bootstrap)
    monkeypatch.setattr(os, "mkdir", mkdir)
    tasks = [asyncio.create_task(SpaceRecovery(instance).host_release(space.id)) for instance in (files, independent)]
    try:
        assert await asyncio.to_thread(entered.wait, 5)
        assert bootstraps == 1
        assert not any(task.done() for task in tasks)
        finish.set()
        results = await asyncio.gather(*tasks, return_exceptions=True)
        successes = [result for result in results if isinstance(result, dict) and result.get("newly_reclaimed")]
        assert len(successes) == 1
        assert all(isinstance(result, (dict, SpaceOperationPending)) for result in results)
        assert bootstraps == 1
        async with sf() as session:
            assert await session.get(SpaceBackingRow, space.backing_handle) is None
            intents = (await session.scalars(select(SpaceFileOperationRow))).all()
            assert len(intents) == 1 and intents[0].phase == "complete"
    finally:
        finish.set()
        await asyncio.gather(*tasks, return_exceptions=True)


@pytest.mark.asyncio
async def test_unverified_effect_free_release_preserves_bytes_and_uncertain_capacity(space_file_storage, monkeypatch):
    from deerflow.spaces.backings import BackingUnavailable

    files, sf, catalog = space_file_storage
    space = await home(files)
    slot = await deleted(sf, space)
    volume = catalog.verified[slot]
    (volume.data_path / "safe").write_bytes(b"private")

    def unavailable(*_args, **_kwargs):
        raise BackingUnavailable("fixture physical verification unavailable")

    monkeypatch.setattr(catalog, "verify", unavailable)
    result = await SpaceRecovery(files).host_release(space.id)
    assert result["complete"] and not result["slot_released"] and not result["newly_reclaimed"]
    assert (volume.data_path / "safe").read_bytes() == b"private"
    assert slot in files.unclaimed_slots[None] and slot not in files.bound_slots[None]
    assert slot not in files.assignable_slots(None)
    async with sf() as session:
        assert await session.get(SpaceBackingRow, space.backing_handle) is None
        assert not (await session.scalars(select(SpaceFileOperationRow))).all()


@pytest.mark.asyncio
async def test_unknown_private_intent_cannot_take_unverified_forget_binding_branch(space_file_storage, monkeypatch):
    from deerflow.spaces.backings import BackingUnavailable
    from deerflow.spaces.service import SpaceOperationPending

    files, sf, catalog = space_file_storage
    space = await home(files)
    slot = await deleted(sf, space)
    volume = catalog.verified[slot]
    await interrupted_before_rotation(files, space.id, monkeypatch)
    async with sf() as session, session.begin():
        intent = (await session.scalars(select(SpaceFileOperationRow))).one()
        intent.request = {**intent.request, "action": "unknown"}

    def unavailable(*_args, **_kwargs):
        raise BackingUnavailable("fixture lost physical proof after intent")

    monkeypatch.setattr(catalog, "verify", unavailable)
    with pytest.raises(SpaceOperationPending):
        await SpaceRecovery(files).host_release(space.id)
    assert (volume.control_path / ".space-binding").exists()
    async with sf() as session:
        assert await session.get(SpaceBackingRow, space.backing_handle) is not None
        intent = (await session.scalars(select(SpaceFileOperationRow))).one()
        assert intent.phase == "pending"
