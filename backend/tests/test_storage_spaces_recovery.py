"""Quiesced files, current grants and durable lifecycle uncertainty."""

import hashlib
import sqlite3

import pytest
import pytest_asyncio
from _storage_spaces_test_support import ALICE, BOB, home, make_storage_fixture, operation
from sqlalchemy import select

from deerflow.persistence.spaces.files import SpaceFileOperationRow
from deerflow.persistence.spaces.model import SpaceRow
from deerflow.spaces.contract import Permission, SpaceConflict, SpaceNotFound
from deerflow.spaces.recovery import SpaceRecovery


@pytest_asyncio.fixture(params=["sqlite", "postgres"])
async def storage(tmp_path, request):
    async for files, sf, catalog in make_storage_fixture(tmp_path, request):
        yield files, sf, catalog, SpaceRecovery(files)


@pytest.mark.asyncio
async def test_backup_restore_preserves_root_and_applies_current_grants(storage):
    files, _, catalog, recovery = storage
    space = await home(files)
    await files.write(actor=ALICE, space_id=space.id, expected_generation=1, operation_id=operation(), path=".wiki", content=b"version one", create=True)
    await files.registry.set_grant(actor=ALICE, space_id=space.id, expected_generation=1, subject=BOB, permissions=Permission.READ, acknowledge_existing_data=True)
    backup = await recovery.backup(actor=ALICE, space_id=space.id, expected_generation=2, operation_id=operation())
    assert backup.consistency == "quiesced-filesystem" and backup.size_bytes > 0
    volume = next(v for v in catalog.verified.values() if (v.data_path / ".wiki").exists())
    inode = volume.root_inode
    await files.registry.set_grant(actor=ALICE, space_id=space.id, expected_generation=2, subject=BOB, permissions=Permission(0))
    await files.write(actor=ALICE, space_id=space.id, expected_generation=3, operation_id=operation(), path=".wiki", content=b"version two", expected_sha256=hashlib.sha256(b"version one").hexdigest())
    restored = await recovery.restore(actor=ALICE, space_id=space.id, expected_generation=3, operation_id=operation(), backup_id=backup.id)
    assert restored.generation == 4 and volume.root_inode == inode
    assert await files.read(actor=ALICE, space_id=space.id, path=".wiki", max_bytes=100) == b"version one"
    with pytest.raises(SpaceNotFound):
        await files.read(actor=BOB, space_id=space.id, path=".wiki", max_bytes=100)
    with pytest.raises(SpaceConflict):
        await files.write(actor=ALICE, space_id=space.id, expected_generation=3, operation_id=operation(), path="old", content=b"stale", create=True)


@pytest.mark.asyncio
async def test_quiesced_backup_restores_sqlite_repository_links_and_binary_data(storage):
    files, _, catalog, recovery = storage
    space = await home(files)
    async with files.registry.admitted(actor=ALICE, requests={space.id: (Permission.WRITE, 1)}) as (session, rows):
        volume = await files._volume(session, rows[space.id][0])
    root = volume.data_path
    (root / ".git").mkdir()
    (root / ".git" / "HEAD").write_bytes(b"ref: refs/heads/main\n")
    (root / "binary").write_bytes(bytes(range(256)))
    (root / "link").symlink_to("binary")
    db = sqlite3.connect(root / "index.sqlite")
    db.execute("PRAGMA journal_mode=WAL")
    db.execute("CREATE TABLE pages (body TEXT)")
    db.execute("INSERT INTO pages VALUES ('retained')")
    db.commit()
    db.close()
    backup = await recovery.backup(actor=ALICE, space_id=space.id, expected_generation=1, operation_id=operation())
    (root / "binary").unlink()
    (root / "index.sqlite").unlink()
    await recovery.restore(actor=ALICE, space_id=space.id, expected_generation=1, operation_id=operation(), backup_id=backup.id)
    assert (root / "link").is_symlink() and (root / "link").read_bytes() == bytes(range(256))
    assert (root / ".git" / "HEAD").read_bytes().startswith(b"ref:")
    with sqlite3.connect(root / "index.sqlite") as restored:
        assert restored.execute("PRAGMA integrity_check").fetchone() == ("ok",)
        assert restored.execute("SELECT body FROM pages").fetchone() == ("retained",)


@pytest.mark.asyncio
async def test_archive_is_readonly_and_delete_keeps_tombstone_and_backups(storage):
    files, sf, catalog, recovery = storage
    space = await home(files)
    await files.write(actor=ALICE, space_id=space.id, expected_generation=1, operation_id=operation(), path="page", content=b"retained", create=True)
    backup = await recovery.backup(actor=ALICE, space_id=space.id, expected_generation=1, operation_id=operation())
    archived = await recovery.archive(actor=ALICE, space_id=space.id, expected_generation=1, operation_id=operation())
    assert archived.status == "archived" and archived.generation == 2
    assert await files.read(actor=ALICE, space_id=space.id, path="page", max_bytes=20) == b"retained"
    with pytest.raises(PermissionError):
        await files.write(actor=ALICE, space_id=space.id, expected_generation=2, operation_id=operation(), path="other", content=b"no", create=True)
    await recovery.delete(actor=ALICE, space_id=space.id, expected_generation=2, operation_id=operation())
    with pytest.raises(SpaceNotFound):
        await files.registry.get(actor=ALICE, space_id=space.id)
    async with sf() as session:
        row = await session.get(SpaceRow, space.id)
        assert row.status == "deleted" and row.generation == 3
    assert any((v.control_path / ("backup-" + backup.id + ".tar")).exists() for v in catalog.verified.values())
    # Tombstoned bindings are never recycled even though visible bytes are gone.
    remaining = [await home(files) for _ in range(3)]
    assert all(r.backing_handle != space.backing_handle for r in remaining)
    with pytest.raises(SpaceConflict):
        await home(files)


@pytest.mark.asyncio
async def test_backup_corruption_never_replaces_visible_files(storage):
    files, _, catalog, recovery = storage
    space = await home(files)
    await files.write(actor=ALICE, space_id=space.id, expected_generation=1, operation_id=operation(), path="page", content=b"safe", create=True)
    backup = await recovery.backup(actor=ALICE, space_id=space.id, expected_generation=1, operation_id=operation())
    for volume in catalog.verified.values():
        path = volume.control_path / ("backup-" + backup.id + ".tar")
        if path.exists():
            path.write_bytes(b"corrupt")
    with pytest.raises(SpaceConflict):
        await recovery.restore(actor=ALICE, space_id=space.id, expected_generation=1, operation_id=operation(), backup_id=backup.id)
    assert await files.read(actor=ALICE, space_id=space.id, path="page", max_bytes=20) == b"safe"


@pytest.mark.asyncio
async def test_pending_file_operation_requires_explicit_owner_reconciliation(storage):
    files, sf, _, recovery = storage
    space = await home(files)
    op = operation()
    async with sf() as session, session.begin():
        session.add(SpaceFileOperationRow(space_id=space.id, operation_id=op, actor_kind=ALICE.kind, actor_id=ALICE.subject_id, generation=1, phase="pending", request={"action": "write", "path": "page"}))
    with pytest.raises(SpaceConflict):
        await files.read(actor=ALICE, space_id=space.id, path="page", max_bytes=20)
    with pytest.raises(ValueError):
        await recovery.accept_current_state(actor=ALICE, space_id=space.id, expected_generation=1, operation_id=op, acknowledge_uncertain_outcome=False)
    await recovery.accept_current_state(actor=ALICE, space_id=space.id, expected_generation=1, operation_id=op, acknowledge_uncertain_outcome=True)
    await files.write(actor=ALICE, space_id=space.id, expected_generation=1, operation_id=operation(), path="page", content=b"corrected", create=True)
    async with sf() as session:
        row = await session.get(SpaceFileOperationRow, (space.id, op))
        assert row.phase == "failed" and row.result["resolution"] == "owner-accepted-current-state"


@pytest.mark.asyncio
async def test_restore_commit_failure_retains_displaced_bytes_and_pending_intent(storage):
    from sqlalchemy import event

    from deerflow.spaces.service import SpaceOperationPending

    files, sf, catalog, recovery = storage
    space = await home(files)
    await files.write(actor=ALICE, space_id=space.id, expected_generation=1, operation_id=operation(), path="page", content=b"backup", create=True)
    backup = await recovery.backup(actor=ALICE, space_id=space.id, expected_generation=1, operation_id=operation())
    await files.write(actor=ALICE, space_id=space.id, expected_generation=1, operation_id=operation(), path="page", content=b"displaced", expected_sha256=hashlib.sha256(b"backup").hexdigest())
    op = operation()

    def fail_completion(session):
        for item in session.identity_map.values():
            if isinstance(item, SpaceFileOperationRow) and item.operation_id == op and item.phase == "complete":
                raise RuntimeError("Completion commit rejected")

    event.listen(sf.class_.sync_session_class, "before_commit", fail_completion)
    try:
        with pytest.raises(SpaceOperationPending):
            await recovery.restore(actor=ALICE, space_id=space.id, expected_generation=1, operation_id=op, backup_id=backup.id)
    finally:
        event.remove(sf.class_.sync_session_class, "before_commit", fail_completion)
    volume = next(v for v in catalog.verified.values() if (v.data_path / "page").exists())
    assert (volume.data_path / "page").read_bytes() == b"backup"
    assert (volume.control_path / ("restore-" + op) / "old" / "page").read_bytes() == b"displaced"
    async with sf() as session:
        assert (await session.get(SpaceFileOperationRow, (space.id, op))).phase == "pending"


@pytest.mark.asyncio
async def test_repeated_completed_backup_never_retires_a_new_environment(storage):
    from test_storage_spaces_attachments import ContainedProvider

    from deerflow.spaces.attachments import ResourceMount, SpaceAttachments

    files, _, _, _ = storage
    provider = ContainedProvider()
    mounts = SpaceAttachments(files, provider)
    recovery = SpaceRecovery(files, attachments=mounts)
    space = await home(files)
    op = operation()
    backup = await recovery.backup(actor=ALICE, space_id=space.id, expected_generation=1, operation_id=op)
    attached = await mounts.attach(actor=ALICE, incarnation=operation(), resources=[ResourceMount(space.id, 1, "home", writable=True)])
    try:
        repeated = await recovery.backup(actor=ALICE, space_id=space.id, expected_generation=1, operation_id=op)
        assert repeated.id == backup.id
    except SpaceConflict:
        pass  # rejecting a durable retry before containment also preserves it
    assert attached.container_id in provider.containers


@pytest.mark.asyncio
async def test_invalid_lifecycle_generation_never_performs_containment(storage):
    from test_storage_spaces_attachments import ContainedProvider

    from deerflow.spaces.attachments import ResourceMount, SpaceAttachments

    files, sf, _, _ = storage
    provider = ContainedProvider()
    mounts = SpaceAttachments(files, provider)
    recovery = SpaceRecovery(files, attachments=mounts)
    space = await home(files)
    attached = await mounts.attach(actor=ALICE, incarnation=operation(), resources=[ResourceMount(space.id, 1, "home", writable=True)])
    for invalid in (None, True, 0, "1", 2**31):
        for action in (recovery.backup, recovery.archive, mounts.retire):
            kwargs = {"actor": ALICE, "space_id": space.id, "expected_generation": invalid}
            if action != mounts.retire:
                kwargs["operation_id"] = operation()
            with pytest.raises(ValueError):
                await action(**kwargs)
        assert attached.container_id in provider.containers
    async with sf() as session:
        assert (await session.get(SpaceRow, space.id)).generation == 1
        assert not (await session.execute(select(SpaceFileOperationRow))).first()


@pytest.mark.asyncio
async def test_backup_commit_failure_retains_exact_owned_artifact_in_intent(storage):
    from sqlalchemy import event

    from deerflow.spaces.service import SpaceOperationPending

    files, sf, catalog, recovery = storage
    space = await home(files)
    op = operation()

    def fail_completion(session):
        for item in session.identity_map.values():
            if isinstance(item, SpaceFileOperationRow) and item.operation_id == op and item.phase == "complete":
                raise RuntimeError("Backup completion commit rejected")

    event.listen(sf.class_.sync_session_class, "before_commit", fail_completion)
    try:
        with pytest.raises(SpaceOperationPending):
            await recovery.backup(actor=ALICE, space_id=space.id, expected_generation=1, operation_id=op)
    finally:
        event.remove(sf.class_.sync_session_class, "before_commit", fail_completion)
    status = await SpaceRecovery(files).status(actor=ALICE, space_id=space.id)
    assert status["operations"][0]["request"] == {"action": "backup", "backup_id": op}
    assert status["operations"][0]["phase"] == "pending" and status["backups"] == []
    assert any((v.control_path / ("backup-" + op + ".tar")).exists() for v in catalog.verified.values())


@pytest.mark.asyncio
async def test_concurrent_duplicate_precheck_never_retires_replacement(storage):
    import asyncio
    from contextvars import ContextVar

    from test_storage_spaces_attachments import ContainedProvider

    from deerflow.spaces.attachments import ResourceMount, SpaceAttachments

    files, sf, _, _ = storage
    provider = ContainedProvider()
    mounts = SpaceAttachments(files, provider)
    recovery = SpaceRecovery(files, attachments=mounts)
    space = await home(files)
    op = operation()
    paused, resume = asyncio.Event(), asyncio.Event()
    original_get = sf.class_.get
    observed = False
    delayed_context = ContextVar("delayed_backup_regression", default=False)

    async def delayed_get(session, model, identity, **kwargs):
        nonlocal observed
        result = await original_get(session, model, identity, **kwargs)
        if delayed_context.get() and model is SpaceFileOperationRow and not observed:
            observed = True
            paused.set()
            await resume.wait()
        return result

    sf.class_.get = delayed_get

    async def delayed_request():
        token = delayed_context.set(True)
        try:
            return await recovery.backup(actor=ALICE, space_id=space.id, expected_generation=1, operation_id=op)
        finally:
            delayed_context.reset(token)

    delayed = asyncio.create_task(delayed_request(), name="delayed-backup")
    try:
        await asyncio.wait_for(paused.wait(), 5)
        completed = await recovery.backup(actor=ALICE, space_id=space.id, expected_generation=1, operation_id=op)
        replacement = await mounts.attach(actor=ALICE, incarnation=operation(), resources=[ResourceMount(space.id, 1, "home", writable=True)])
        resume.set()
        try:
            result = await delayed
            assert result.id == completed.id
        except SpaceConflict:
            pass
        assert replacement.container_id in provider.containers
    finally:
        resume.set()
        if not delayed.done():
            delayed.cancel()
            await asyncio.gather(delayed, return_exceptions=True)
        sf.class_.get = original_get
