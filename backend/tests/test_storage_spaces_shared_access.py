"""Concurrent filesystem admission retains durable uncertainty and lifecycle fences."""

import asyncio
import errno

import pytest
import pytest_asyncio
from _storage_spaces_test_support import ALICE, home, make_storage_fixture, operation
from sqlalchemy import select
from test_storage_spaces_attachments import ContainedProvider

from deerflow.persistence.spaces.files import SpaceFileOperationRow
from deerflow.persistence.spaces.lifecycle import SpaceAttachmentRow
from deerflow.spaces.attachments import ResourceMount, SpaceAttachments
from deerflow.spaces.contract import Permission, SpaceConflict
from deerflow.spaces.filesystem import ConfinedFilesystem
from deerflow.spaces.recovery import SpaceRecovery
from deerflow.spaces.service import SpaceFiles, SpaceOperationPending


@pytest_asyncio.fixture(params=["sqlite", "postgres"])
async def storage(tmp_path, request):
    async for value in make_storage_fixture(tmp_path, request):
        yield value


@pytest.mark.asyncio
@pytest.mark.parametrize("adapter", [True, False])
@pytest.mark.parametrize("phase", ["active", "pending", "fence_pending"])
async def test_attachment_admission_allows_file_access_and_reserves_unconfirmed_admin(storage, phase, adapter):
    files, sf, catalog = storage
    space = await home(files)
    mounts = SpaceAttachments(files, ContainedProvider())
    attachment = await mounts.attach(actor=ALICE, incarnation=operation(), resources=[ResourceMount(space.id, 1, "files", writable=True)])
    async with sf() as session:
        row = await session.get(SpaceAttachmentRow, attachment.id)
        row.phase = phase
        await session.commit()
    if not adapter:
        files = SpaceFiles(files.registry, catalog)
    for permission in (Permission.READ, Permission.READ | Permission.EXPORT, Permission.WRITE):
        async with files.registry.admitted(actor=ALICE, requests={space.id: (permission, 1)}):
            pass
    if phase == "active":
        async with files.registry.admitted(actor=ALICE, requests={space.id: (Permission.ADMIN | Permission.WRITE, 1)}):
            pass
    else:
        with pytest.raises(SpaceConflict):
            async with files.registry.admitted(actor=ALICE, requests={space.id: (Permission.ADMIN, 1)}):
                pytest.fail("Unconfirmed attachment admitted ADMIN")
    await files.write(actor=ALICE, space_id=space.id, expected_generation=1, operation_id=operation(), path="host.txt", content=b"host beside container", create=True)


@pytest.mark.asyncio
@pytest.mark.parametrize("action", ["write", "put", "mkdir", "rename", "remove", "feature-workflow", "copy", "controller", "backup", "restore", "archive", "delete", "accept", None, []])
async def test_pending_row_blocks_only_its_required_permissions(storage, action):
    files, sf, _ = storage
    space = await home(files)
    async with sf() as session:
        session.add(SpaceFileOperationRow(space_id=space.id, operation_id=operation(), actor_kind="human", actor_id="alice", generation=1, phase="pending", request={"action": action}))
        await session.commit()
    file_action = isinstance(action, str) and action in {"write", "put", "mkdir", "rename", "remove", "feature-workflow"}
    for permission in (Permission.READ, Permission.READ | Permission.EXPORT, Permission.WRITE, Permission.ADMIN):
        if file_action and not permission & (Permission.WRITE | Permission.OPERATE | Permission.ADMIN):
            async with files.registry.admitted(actor=ALICE, requests={space.id: (permission, 1)}):
                pass
        else:
            with pytest.raises(SpaceOperationPending) as caught:
                async with files.registry.admitted(actor=ALICE, requests={space.id: (permission, 1)}):
                    pytest.fail("Uncertain operation admitted a conflicting request")
            assert caught.value.cause == ("lifecycle" if action in ("backup", "restore", "archive", "delete", "accept") else "workflow")


@pytest.mark.asyncio
@pytest.mark.parametrize("action", ["backup", "restore", "delete", "archive", "accept"])
async def test_adapter_loss_keeps_lifecycle_effects_fenced(storage, action):
    files, sf, catalog = storage
    space = await home(files)
    mounts = SpaceAttachments(files, ContainedProvider())
    attachment = await mounts.attach(actor=ALICE, incarnation=operation(), resources=[ResourceMount(space.id, 1, "files", writable=True)])
    volume = catalog.verify(next(iter(files.bound_slots[None])))
    (volume.data_path / "sentinel").write_bytes(b"retain")
    files = SpaceFiles(files.registry, catalog)
    recovery = SpaceRecovery(files)
    op = operation()
    if action == "accept":
        async with sf() as session:
            session.add(SpaceFileOperationRow(space_id=space.id, operation_id=op, actor_kind="human", actor_id="alice", generation=1, phase="pending", request={"action": "write"}))
            await session.commit()
        call = recovery.accept_current_state(actor=ALICE, space_id=space.id, expected_generation=1, operation_id=op, acknowledge_uncertain_outcome=True)
    else:
        values = {"backup_id": operation()} if action == "restore" else {}
        call = getattr(recovery, action)(actor=ALICE, space_id=space.id, expected_generation=1, operation_id=op, **values)
    with pytest.raises(SpaceConflict):
        await call
    async with sf() as session:
        assert (await session.get(SpaceAttachmentRow, attachment.id)).phase == "active"
        from deerflow.persistence.spaces.model import SpaceRow

        row = await session.get(SpaceRow, space.id)
        assert (row.status, row.generation) == ("active", 1)
        intent = await session.get(SpaceFileOperationRow, (space.id, op))
        assert intent is not None and intent.phase == "pending"
    assert (volume.data_path / "sentinel").read_bytes() == b"retain"


@pytest.mark.asyncio
async def test_waiting_reader_does_not_hold_sql_and_rechecks_the_workflow(storage, monkeypatch):
    files, _, _ = storage
    space = await home(files)
    started, release, read_started = asyncio.Event(), asyncio.Event(), asyncio.Event()

    async def write(volumes):
        started.set()
        await release.wait()
        return "written"

    async def read(volumes):
        read_started.set()
        return "read"

    writer = asyncio.create_task(files.run_workflow(actor=ALICE, requests={space.id: (Permission.WRITE, 1)}, destination_id=space.id, operation_id=operation(), request={"action": "feature-workflow"}, call=write))
    import threading

    from deerflow.spaces import workflows

    contended = threading.Event()
    original = workflows._open

    def observed(*args, **kwargs):
        try:
            return original(*args, **kwargs)
        except SpaceOperationPending:
            contended.set()
            raise

    monkeypatch.setattr(workflows, "_open", observed)
    reader = None
    try:
        await asyncio.wait_for(started.wait(), 5)
        reader = asyncio.create_task(files.run_read_workflow(actor=ALICE, requests={space.id: (Permission.READ, 1)}, call=read))
        assert await asyncio.to_thread(contended.wait, 2)
        assert not read_started.is_set() and not reader.done()
        release.set()
        assert await asyncio.wait_for(writer, 2) == "written"
        assert await asyncio.wait_for(reader, 2) == "read"
    finally:
        release.set()
        await asyncio.gather(writer, *([reader] if reader else []), return_exceptions=True)


@pytest.mark.asyncio
async def test_two_read_workflows_hold_shared_locks_at_once(storage):
    files, _, _ = storage
    space = await home(files)
    entered = [asyncio.Event(), asyncio.Event()]
    release = asyncio.Event()

    async def read(index, volumes):
        entered[index].set()
        await release.wait()

    tasks = [asyncio.create_task(files.run_read_workflow(actor=ALICE, requests={space.id: (Permission.READ, 1)}, call=lambda volumes, i=i: read(i, volumes))) for i in range(2)]
    try:
        await asyncio.wait_for(asyncio.gather(*(event.wait() for event in entered)), 2)
    finally:
        release.set()
        await asyncio.gather(*tasks)


@pytest.mark.parametrize("capacity_errno", [errno.ENOSPC, errno.EDQUOT])
def test_prepublication_capacity_failure_leaves_no_entry_or_stage(tmp_path, monkeypatch, capacity_errno):
    from deerflow.spaces.filesystem import SpaceFull

    data, control = tmp_path / "data", tmp_path / "control"
    data.mkdir()
    control.mkdir(mode=0o700)
    with ConfinedFilesystem(data, control) as fs:

        def full(*args, **kwargs):
            raise OSError(capacity_errno, "full")

        with monkeypatch.context() as patch:
            patch.setattr("os.write", full)
            with pytest.raises(SpaceFull):
                fs.write_atomic("new", b"content", expected_sha256=None, create=True)
        with monkeypatch.context() as patch:
            patch.setattr("os.mkdir", full)
            with pytest.raises(SpaceFull):
                fs.mkdir("folder")
        assert not list(data.iterdir()) and not list(control.iterdir())


@pytest.mark.asyncio
@pytest.mark.parametrize("action", ["write", "mkdir"])
async def test_postpublication_capacity_failure_preserves_uncertain_intent(storage, monkeypatch, action):
    files, sf, catalog = storage
    space = await home(files)
    volume = catalog.verify(next(iter(files.bound_slots[None])))
    import os

    original = os.fsync

    def fail_published(fd):
        if (volume.data_path / "new").exists():
            raise OSError(errno.ENOSPC, "directory sync lost")
        return original(fd)

    monkeypatch.setattr(os, "fsync", fail_published)
    with pytest.raises(SpaceOperationPending):
        if action == "write":
            await files.write(actor=ALICE, space_id=space.id, expected_generation=1, operation_id=operation(), path="new", content=b"published", create=True)
        else:
            await files.mkdir(actor=ALICE, space_id=space.id, expected_generation=1, operation_id=operation(), path="new")
    async with sf() as session:
        assert (await session.execute(select(SpaceFileOperationRow))).scalar_one().phase == "pending"
    assert (volume.data_path / "new").exists()


@pytest.mark.parametrize("capacity_errno", [errno.ENOSPC, errno.EDQUOT])
def test_failed_no_replace_capacity_is_space_full(tmp_path, monkeypatch, capacity_errno):
    import ctypes

    from deerflow.spaces.filesystem import SpaceFull

    class Lib:
        @staticmethod
        def renameat2(*args):
            ctypes.set_errno(capacity_errno)
            return -1

    monkeypatch.setattr(ctypes, "CDLL", lambda *args, **kwargs: Lib())
    with pytest.raises(SpaceFull):
        ConfinedFilesystem._rename_no_replace(1, "stage", 2, "new")


@pytest.mark.asyncio
async def test_shared_lock_deadline_and_partial_lock_cleanup(storage, monkeypatch):
    from deerflow.spaces import workflows
    from deerflow.utils.file_io import run_file_io

    files, _, catalog = storage
    first, second = await home(files), await home(files)
    async with files.registry.admitted(actor=ALICE, requests={first.id: (Permission.READ, 1), second.id: (Permission.READ, 1)}) as (session, rows):
        volumes = {key: await files._volume(session, row) for key, (row, _) in rows.items()}
    blocked = sorted(volumes)[-1]
    descriptors = await run_file_io(workflows.acquire, {blocked: volumes[blocked]})
    monkeypatch.setattr(workflows, "READ_LOCK_WAIT_SECONDS", 0.1)
    try:
        with pytest.raises(SpaceOperationPending, match="workflow"):
            await asyncio.wait_for(run_file_io(workflows.acquire, volumes, shared=True), 1)
        # The first shared descriptor must be closed after the second timed out.
        first_key = sorted(volumes)[0]
        check = await run_file_io(workflows.acquire, {first_key: volumes[first_key]})
        await run_file_io(workflows.close, check)
    finally:
        await run_file_io(workflows.close, descriptors)


@pytest.mark.asyncio
async def test_waiting_read_rechecks_revoked_grant_before_callback(storage, monkeypatch):
    import threading

    from deerflow.persistence.spaces.model import SpaceGrantRow
    from deerflow.spaces import workflows
    from deerflow.spaces.contract import SpaceNotFound
    from deerflow.utils.file_io import run_file_io

    files, sf, _ = storage
    space = await home(files)
    async with files.registry.admitted(actor=ALICE, requests={space.id: (Permission.READ, 1)}) as (session, rows):
        volume = await files._volume(session, rows[space.id][0])
    descriptors = await run_file_io(workflows.acquire, {space.id: volume})
    contended = threading.Event()
    original = workflows._open

    def observed(*args, **kwargs):
        try:
            return original(*args, **kwargs)
        except SpaceOperationPending:
            contended.set()
            raise

    monkeypatch.setattr(workflows, "_open", observed)

    async def forbidden(volumes):
        pytest.fail("Revoked reader reached callback")

    reader = asyncio.create_task(files.run_read_workflow(actor=ALICE, requests={space.id: (Permission.READ, 1)}, call=forbidden))
    try:
        assert await asyncio.to_thread(contended.wait, 2)
        async with sf() as session:
            await session.delete(await session.get(SpaceGrantRow, (space.id, "human", "alice")))
            await session.commit()
        await run_file_io(workflows.close, descriptors)
        descriptors = []
        with pytest.raises(SpaceNotFound):
            await asyncio.wait_for(reader, 2)
    finally:
        await run_file_io(workflows.close, descriptors)
        await asyncio.gather(reader, return_exceptions=True)


@pytest.mark.asyncio
async def test_attachment_guard_rejects_invalid_phase_for_every_permission(storage, monkeypatch):
    from types import SimpleNamespace

    from deerflow.spaces.attachments import AttachmentPending

    files, _, _ = storage
    mounts = SpaceAttachments(files, ContainedProvider())

    async def current(*args, **kwargs):
        return [(SimpleNamespace(id=operation(), phase="invalid"), SimpleNamespace(space_id="one", writable=True))]

    monkeypatch.setattr(mounts, "_current", current)
    for permission in (Permission.READ, Permission.WRITE, Permission.EXPORT, Permission.OPERATE, Permission.ADMIN):
        with pytest.raises(AttachmentPending, match="Invalid persisted"):
            await mounts._guard(None, {"one": (None, None)}, {"one": (permission, 1)})


@pytest.mark.asyncio
async def test_cancelled_waiting_read_drains_and_releases_every_lock(storage, monkeypatch):
    import threading

    from deerflow.spaces import workflows
    from deerflow.utils.file_io import run_file_io

    files, _, _ = storage
    first, second = await home(files), await home(files)
    requests = {first.id: (Permission.READ, 1), second.id: (Permission.READ, 1)}
    async with files.registry.admitted(actor=ALICE, requests=requests) as (session, rows):
        volumes = {key: await files._volume(session, row) for key, (row, _) in rows.items()}
    blocked = sorted(volumes)[-1]
    descriptors = await run_file_io(workflows.acquire, {blocked: volumes[blocked]})
    contended = threading.Event()
    original = workflows._open

    def observed(*args, **kwargs):
        try:
            return original(*args, **kwargs)
        except SpaceOperationPending:
            contended.set()
            raise

    monkeypatch.setattr(workflows, "_open", observed)

    async def read(volumes):
        return "drained"

    reader = asyncio.create_task(files.run_read_workflow(actor=ALICE, requests=requests, call=read))
    try:
        assert await asyncio.to_thread(contended.wait, 2)
        reader.cancel()
        await asyncio.sleep(0)
        assert not reader.done()
        await run_file_io(workflows.close, descriptors)
        descriptors = []
        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(reader, 2)
        check = await run_file_io(workflows.acquire, volumes)
        await run_file_io(workflows.close, check)
    finally:
        await run_file_io(workflows.close, descriptors)
        await asyncio.gather(reader, return_exceptions=True)


@pytest.mark.asyncio
async def test_attachment_capacity_is_bounded_and_lifecycle_fences_the_full_scope(storage):
    from deerflow.persistence.spaces.lifecycle import SpaceMountRow
    from deerflow.spaces.attachments import AttachmentPlan

    files, sf, _ = storage
    space = await home(files)
    provider = ContainedProvider()
    mounts = SpaceAttachments(files, provider)
    # Seed 31 independent proven rows rather than paying 31 lifecycle roundtrips;
    # the real API admits the 32nd and must refuse the 33rd under the parent lock.
    async with sf() as session:
        for _ in range(31):
            identifier = operation()
            plan = AttachmentPlan(identifier, operation(), ALICE, provider.host_id, ())
            container = provider.prepare(plan)
            provider.start(container, plan)
            session.add(SpaceAttachmentRow(id=identifier, incarnation=plan.incarnation, actor_kind="human", actor_id="alice", host_id=provider.host_id, container_id=container, phase="active"))
            await session.flush()
            session.add(SpaceMountRow(attachment_id=identifier, space_id=space.id, generation=1, alias="files", writable=1))
        await session.commit()
    await mounts.attach(actor=ALICE, incarnation=operation(), resources=[ResourceMount(space.id, 1, "files", writable=True)])
    with pytest.raises(SpaceConflict, match="capacity"):
        await mounts.attach(actor=ALICE, incarnation=operation(), resources=[ResourceMount(space.id, 1, "files", writable=True)])
    assert len(provider.containers) == 32
    await SpaceRecovery(files).backup(actor=ALICE, space_id=space.id, expected_generation=1, operation_id=operation())
    assert not provider.containers
    async with sf() as session:
        rows = (await session.execute(select(SpaceAttachmentRow))).scalars().all()
        assert len(rows) == 32 and all(row.phase == "fenced" for row in rows)


@pytest.mark.asyncio
@pytest.mark.parametrize("adapter", [True, False])
async def test_invalid_persisted_phase_refuses_adapter_and_fallback(storage, adapter):
    from sqlalchemy import text

    files, sf, catalog = storage
    space = await home(files)
    mounts = SpaceAttachments(files, ContainedProvider())
    attached = await mounts.attach(actor=ALICE, incarnation=operation(), resources=[ResourceMount(space.id, 1, "files", writable=True)])
    async with sf() as session:
        # Model a drifted/corrupt persisted row: normal SQL constraints prevent it.
        if session.bind.dialect.name == "sqlite":
            await session.execute(text("PRAGMA ignore_check_constraints=ON"))
        else:
            await session.execute(text("ALTER TABLE storage_space_attachments DROP CONSTRAINT ck_storage_space_attachments_phase"))
        row = await session.get(SpaceAttachmentRow, attached.id)
        row.phase = "invalid"
        await session.commit()
    if not adapter:
        files = SpaceFiles(files.registry, catalog)
    for permission in (Permission.READ, Permission.WRITE, Permission.EXPORT, Permission.ADMIN):
        with pytest.raises(SpaceConflict, match="Invalid persisted"):
            async with files.registry.admitted(actor=ALICE, requests={space.id: (permission, 1)}):
                pytest.fail("Invalid phase admitted access")


@pytest.mark.asyncio
@pytest.mark.parametrize("action", ["write", "feature-workflow", "copy"])
async def test_pending_file_and_workflow_rows_refuse_operate(storage, action):
    from deerflow.spaces.contract import Custody, FeatureBinding, MutationMode

    files, sf, _ = storage
    space = await files.create(actor=ALICE, name="Controller", custody=Custody.personal(ALICE), mode=MutationMode.MEDIATED, feature=FeatureBinding("test.feature", "operation", 1))
    async with sf() as session:
        session.add(SpaceFileOperationRow(space_id=space.id, operation_id=operation(), actor_kind="human", actor_id="alice", generation=1, phase="pending", request={"action": action}))
        await session.commit()
    with pytest.raises(SpaceOperationPending) as caught:
        async with files.registry.admitted(actor=ALICE, requests={space.id: (Permission.OPERATE, 1)}):
            pytest.fail("Pending row admitted OPERATE")
    assert caught.value.cause == "workflow"


@pytest.mark.asyncio
async def test_restore_cleanup_retains_private_state_while_attachment_is_active(storage, monkeypatch):
    files, sf, _ = storage
    space = await home(files)
    op = operation()
    async with sf() as session:
        session.add(SpaceFileOperationRow(space_id=space.id, operation_id=op, actor_kind="human", actor_id="alice", generation=1, phase="complete", request={"action": "restore"}, result={"cleanup": "pending"}))
        await session.commit()
    mounts = SpaceAttachments(files, ContainedProvider())
    await mounts.attach(actor=ALICE, incarnation=operation(), resources=[ResourceMount(space.id, 1, "files", writable=True)])

    def forbidden(*args):
        pytest.fail("Cleanup touched an active mount")

    monkeypatch.setattr("deerflow.spaces.snapshots.cleanup_restore", forbidden)
    await SpaceRecovery(files)._cleanup_restore(ALICE, space.id, op)
    async with sf() as session:
        assert (await session.get(SpaceFileOperationRow, (space.id, op))).result == {"cleanup": "pending"}
