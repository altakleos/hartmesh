"""Explicit current ADMIN release reuses durable whole-slot cleanup."""

import asyncio

import pytest
import pytest_asyncio
from _storage_spaces_test_support import ALICE, BOB, home, operation
from sqlalchemy import select
from test_storage_spaces_attachments import ContainedProvider
from test_storage_spaces_release import deleted
from test_storage_spaces_service import space_file_storage as space_file_storage

from deerflow.persistence.spaces.files import SpaceBackingRow, SpaceFileOperationRow
from deerflow.persistence.spaces.model import SpaceEventRow, SpaceGrantRow
from deerflow.spaces.attachments import ResourceMount, SpaceAttachments
from deerflow.spaces.backings import BackingUnavailable
from deerflow.spaces.contract import Custody, MutationMode, Permission, SpaceConflict, SpaceDenied, SpaceNotFound
from deerflow.spaces.recovery import SpaceRecovery
from deerflow.spaces.release_filesystem import ReleaseFilesystem
from deerflow.spaces.service import SpaceOperationPending


@pytest_asyncio.fixture(params=["sqlite", "postgres"])
async def single_pool_storage(tmp_path, request, monkeypatch):
    import _storage_spaces_test_support as support

    create_engine = support.create_async_engine

    def limited(*args, **kwargs):
        return create_engine(*args, **kwargs, pool_size=1, max_overflow=0, pool_timeout=0.2)

    monkeypatch.setattr(support, "create_async_engine", limited)
    async for value in support.make_storage_fixture(tmp_path, request):
        yield value


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ["assistant", "employee"])
async def test_release_empties_data_and_control_and_returns_the_slot_only_to_its_kind(space_file_storage, kind):
    files, sf, catalog = space_file_storage
    for spec in catalog.volumes.values():
        spec.kind = kind
    async with files.provisioning(actor=ALICE, name="Deleted files", custody=Custody.personal(ALICE), mode=MutationMode.NATIVE, kind=kind) as (_session, space):
        pass
    slot = await deleted(sf, space, retained=True)
    volume = catalog.verified[slot]
    filesystem_uuid = volume.spec.filesystem_uuid
    (volume.data_path / "document").write_bytes(b"private")
    (volume.control_path / "backup").write_bytes(b"kept by ordinary deletion")
    recovery = SpaceRecovery(files)
    assert await recovery.host_release(space.id) == {"complete": False, "slot_released": False}
    key = operation()
    result = await recovery.release(actor=ALICE, space_id=space.id, expected_generation=2, operation_id=key)
    assert result == {"complete": True, "slot_released": True}
    assert not any(volume.data_path.iterdir()) and not any(volume.control_path.iterdir())
    assert slot not in files.bound_slots[kind] and slot in files.assignable_slots(kind)
    other = "employee" if kind == "assistant" else "assistant"
    assert slot not in files.assignable_slots(other)
    async with sf() as session:
        assert await session.get(SpaceBackingRow, space.backing_handle) is None
        intent = await session.get(SpaceFileOperationRow, (space.id, key))
        assert intent.phase == "complete" and intent.request == {"action": "release"}
        assert (await session.get(SpaceEventRow, (space.id, 2))).details["retained_backups"] is True
    assert await recovery.release(actor=ALICE, space_id=space.id, expected_generation=2, operation_id=key) == result
    async with files.provisioning(actor=ALICE, name="Replacement", custody=Custody.personal(ALICE), mode=MutationMode.NATIVE, kind=kind) as (session, replacement):
        binding = await session.get(SpaceBackingRow, replacement.backing_handle)
        assert binding.slot_id == slot and binding.filesystem_uuid == filesystem_uuid


@pytest.mark.asyncio
@pytest.mark.parametrize("reason", ["active", "archived", "no-admin", "attachment"])
async def test_admitted_deleted_requires_current_admin_deleted_status_and_fenced_attachments(space_file_storage, reason):
    files, sf, catalog = space_file_storage
    space = await home(files)
    if reason == "attachment":
        attachments = SpaceAttachments(files, ContainedProvider())
        await attachments.attach(actor=ALICE, incarnation=operation(), resources=[ResourceMount(space.id, space.generation, "home", writable=True)])
    status = reason if reason in {"active", "archived"} else "deleted"
    slot = await deleted(sf, space, retained=True, status=status)
    (catalog.verified[slot].data_path / "keep").write_bytes(b"untouched")
    if reason == "no-admin":
        async with sf() as session, session.begin():
            (await session.get(SpaceGrantRow, (space.id, "human", "alice"))).permissions = int(Permission.READ)
    expected = SpaceNotFound if reason in {"active", "archived"} else SpaceDenied if reason == "no-admin" else SpaceConflict
    with pytest.raises(expected):
        async with files.registry.admitted_deleted(actor=ALICE, space_id=space.id):
            pytest.fail("An unauthorized deleted-Space admission yielded")
    with pytest.raises(expected):
        await SpaceRecovery(files).release(actor=ALICE, space_id=space.id, expected_generation=2, operation_id=operation())
    assert (catalog.verified[slot].data_path / "keep").read_bytes() == b"untouched"


@pytest.mark.asyncio
async def test_unverified_release_preserves_bytes_and_replays_its_false_outcome(space_file_storage, monkeypatch):
    files, sf, catalog = space_file_storage
    space = await home(files)
    slot = await deleted(sf, space, retained=True)
    volume = catalog.verified[slot]
    (volume.data_path / "keep").write_bytes(b"uncertain private data")
    verify = catalog.verify

    def unavailable(*_args, **_kwargs):
        raise BackingUnavailable("offline unverified volume")

    monkeypatch.setattr(catalog, "verify", unavailable)
    key = operation()
    recovery = SpaceRecovery(files)
    result = await recovery.release(actor=ALICE, space_id=space.id, expected_generation=2, operation_id=key)
    assert result == {"complete": True, "slot_released": False}
    assert (volume.data_path / "keep").read_bytes() == b"uncertain private data"
    assert slot in files.unclaimed_slots[None] and slot not in files.assignable_slots(None)
    monkeypatch.setattr(catalog, "verify", verify)
    assert await recovery.release(actor=ALICE, space_id=space.id, expected_generation=2, operation_id=key) == result
    async with sf() as session:
        assert await session.get(SpaceBackingRow, space.backing_handle) is None
        intent = await session.get(SpaceFileOperationRow, (space.id, key))
        assert intent.phase == "complete" and intent.request == {"action": "release"}


@pytest.mark.asyncio
@pytest.mark.parametrize("boundary", ["initialize", "rotate", "checkpoint", "finish"])
async def test_caller_release_retries_durable_physical_boundaries(space_file_storage, monkeypatch, boundary):
    files, sf, catalog = space_file_storage
    space = await home(files)
    slot = await deleted(sf, space, retained=True)
    volume = catalog.verified[slot]
    (volume.data_path / "document").write_bytes(b"private")
    original = getattr(ReleaseFilesystem, boundary)
    interrupted = False

    def fail_once(owner, *args, **kwargs):
        nonlocal interrupted
        result = original(owner, *args, **kwargs)
        if not interrupted:
            interrupted = True
            raise OSError("interrupted release boundary")
        return result

    monkeypatch.setattr(ReleaseFilesystem, boundary, fail_once)
    recovery, key = SpaceRecovery(files), operation()
    with pytest.raises(SpaceOperationPending):
        await recovery.release(actor=ALICE, space_id=space.id, expected_generation=2, operation_id=key)
    async with sf() as session:
        assert await session.get(SpaceBackingRow, space.backing_handle) is not None
        intent = await session.get(SpaceFileOperationRow, (space.id, key))
        assert intent.phase == "pending" and intent.request == {"action": "release"}
    assert await recovery.release(actor=ALICE, space_id=space.id, expected_generation=2, operation_id=key) == {"complete": True, "slot_released": True}
    assert not any(volume.data_path.iterdir()) and not any(volume.control_path.iterdir())


@pytest.mark.asyncio
async def test_release_identity_binds_actor_generation_and_request(space_file_storage):
    files, sf, _catalog = space_file_storage
    space = await home(files)
    async with sf() as session, session.begin():
        session.add(SpaceGrantRow(space_id=space.id, principal_kind="human", subject_id="bob", permissions=int(Permission.ADMIN)))
    await deleted(sf, space, retained=True)
    key = operation()
    recovery = SpaceRecovery(files)
    assert (await recovery.release(actor=ALICE, space_id=space.id, expected_generation=2, operation_id=key))["complete"]
    for actor, generation in ((BOB, 2), (ALICE, 1)):
        with pytest.raises(SpaceConflict):
            await recovery.release(actor=actor, space_id=space.id, expected_generation=generation, operation_id=key)


@pytest.mark.asyncio
async def test_host_or_different_caller_release_cannot_discard_a_pending_caller_intent(space_file_storage, monkeypatch):
    files, sf, catalog = space_file_storage
    space = await home(files)
    slot = await deleted(sf, space, retained=False)
    (catalog.verified[slot].data_path / "keep").write_bytes(b"original")
    initialize = ReleaseFilesystem.initialize

    def interrupted(_owner):
        raise OSError("before physical initialization")

    monkeypatch.setattr(ReleaseFilesystem, "initialize", interrupted)
    recovery, key = SpaceRecovery(files), operation()
    with pytest.raises(SpaceOperationPending):
        await recovery.release(actor=ALICE, space_id=space.id, expected_generation=2, operation_id=key)
    with pytest.raises(SpaceOperationPending):
        await recovery.host_release(space.id)
    with pytest.raises(SpaceOperationPending):
        await recovery.release(actor=ALICE, space_id=space.id, expected_generation=2, operation_id=operation())
    async with sf() as session:
        assert (await session.get(SpaceFileOperationRow, (space.id, key))).phase == "pending"
        assert len((await session.scalars(select(SpaceFileOperationRow).where(SpaceFileOperationRow.space_id == space.id))).all()) == 1
    assert (catalog.verified[slot].data_path / "keep").read_bytes() == b"original"
    monkeypatch.setattr(ReleaseFilesystem, "initialize", initialize)
    assert (await recovery.release(actor=ALICE, space_id=space.id, expected_generation=2, operation_id=key))["slot_released"]


@pytest.mark.asyncio
async def test_admin_revocation_after_intent_refuses_before_physical_effects(space_file_storage, monkeypatch):
    from deerflow.spaces import host_release as engine

    files, sf, catalog = space_file_storage
    space = await home(files)
    slot = await deleted(sf, space, retained=True)
    (catalog.verified[slot].data_path / "keep").write_bytes(b"private")
    run_io = engine.run_file_io
    revoked = False

    async def revoke_after_ownership(func, *args, **kwargs):
        nonlocal revoked
        result = await run_io(func, *args, **kwargs)
        if getattr(func, "__name__", None) == "acquire_existing" and not revoked:
            revoked = True
            async with sf() as session, session.begin():
                (await session.get(SpaceGrantRow, (space.id, "human", "alice"))).permissions = int(Permission.READ)
        return result

    monkeypatch.setattr(engine, "run_file_io", revoke_after_ownership)
    key = operation()
    with pytest.raises(SpaceDenied):
        await SpaceRecovery(files).release(actor=ALICE, space_id=space.id, expected_generation=2, operation_id=key)
    assert revoked and (catalog.verified[slot].data_path / "keep").read_bytes() == b"private"
    async with sf() as session, session.begin():
        assert (await session.get(SpaceFileOperationRow, (space.id, key))).phase == "pending"
        (await session.get(SpaceGrantRow, (space.id, "human", "alice"))).permissions = 27
    assert (await SpaceRecovery(files).release(actor=ALICE, space_id=space.id, expected_generation=2, operation_id=key))["slot_released"]


@pytest.mark.asyncio
async def test_cancellation_drains_release_before_returning_or_publishing_capacity(space_file_storage, monkeypatch):
    from deerflow.spaces import host_release as engine

    files, sf, catalog = space_file_storage
    space = await home(files)
    slot = await deleted(sf, space, retained=True)
    (catalog.verified[slot].data_path / "old").write_bytes(b"private")
    run_io, entered, release = engine.run_file_io, asyncio.Event(), asyncio.Event()

    async def hold_after_rotation(func, *args, **kwargs):
        result = await run_io(func, *args, **kwargs)
        if getattr(func, "__name__", None) == "rotate" and args == ("data",):
            entered.set()
            await release.wait()
        return result

    monkeypatch.setattr(engine, "run_file_io", hold_after_rotation)
    key = operation()
    task = asyncio.create_task(SpaceRecovery(files).release(actor=ALICE, space_id=space.id, expected_generation=2, operation_id=key))
    try:
        await asyncio.wait_for(entered.wait(), 10)
        task.cancel()
        await asyncio.sleep(0)
        assert not task.done() and slot not in files.assignable_slots(None)
        release.set()
        with pytest.raises(asyncio.CancelledError):
            await task
        async with sf() as session:
            assert await session.get(SpaceBackingRow, space.backing_handle) is None
            assert (await session.get(SpaceFileOperationRow, (space.id, key))).phase == "complete"
        assert slot in files.assignable_slots(None)
    finally:
        release.set()
        await asyncio.gather(task, return_exceptions=True)


@pytest.mark.asyncio
@pytest.mark.parametrize("stage", ["intent", "checkpoint", "final", "unverified"])
async def test_commit_failure_retains_claim_until_a_fresh_caller_retry_commits(space_file_storage, stage, monkeypatch, caplog):
    from sqlalchemy import event
    from sqlalchemy.orm import Session

    from deerflow.spaces.registry import SpaceRegistry
    from deerflow.spaces.service import SpaceFiles

    files, sf, catalog = space_file_storage
    space = await home(files)
    slot = await deleted(sf, space, retained=True)
    (catalog.verified[slot].data_path / "private").write_bytes(b"private")
    triggered = False
    verify = catalog.verify
    if stage == "unverified":

        def unavailable(*_args, **_kwargs):
            raise BackingUnavailable("Offline verification failure")

        monkeypatch.setattr(catalog, "verify", unavailable)

    def failed_commit(session):
        nonlocal triggered
        if triggered:
            return
        for row in (*session.new, *session.dirty):
            if not isinstance(row, SpaceFileOperationRow) or row.space_id != space.id or row.request != {"action": "release"}:
                continue
            progress = (row.result or {}).get("progress") or {}
            if (stage == "intent" and row in session.new) or (stage == "checkpoint" and row.phase == "pending" and progress.get("stage") == "filesystem-complete") or (stage in {"final", "unverified"} and row.phase == "complete"):
                triggered = True
                raise OSError("caller release commit failed")

    key = operation()
    event.listen(Session, "before_commit", failed_commit)
    try:
        with pytest.raises(SpaceOperationPending):
            await SpaceRecovery(files).release(actor=ALICE, space_id=space.id, expected_generation=2, operation_id=key)
    finally:
        event.remove(Session, "before_commit", failed_commit)
    assert triggered and slot not in files.assignable_slots(None)
    if stage == "unverified":
        assert "released without emptying" not in caplog.text
        monkeypatch.setattr(catalog, "verify", verify)
    async with sf() as session:
        assert await session.get(SpaceBackingRow, space.backing_handle) is not None
    restarted = SpaceFiles(SpaceRegistry(sf, files.registry._resolver), catalog)
    assert (await SpaceRecovery(restarted).release(actor=ALICE, space_id=space.id, expected_generation=2, operation_id=key))["slot_released"]


@pytest.mark.asyncio
@pytest.mark.parametrize("unverified", [False, True])
async def test_malformed_caller_request_keeps_its_release_proof_from_host_discard(space_file_storage, monkeypatch, unverified):
    files, sf, catalog = space_file_storage
    space = await home(files)
    slot = await deleted(sf, space, retained=False)
    (catalog.verified[slot].data_path / "keep").write_bytes(b"original")

    def interrupted(_owner):
        raise OSError("before physical initialization")

    initialize = ReleaseFilesystem.initialize
    monkeypatch.setattr(ReleaseFilesystem, "initialize", interrupted)
    key = operation()
    with pytest.raises(SpaceOperationPending):
        await SpaceRecovery(files).release(actor=ALICE, space_id=space.id, expected_generation=2, operation_id=key)
    async with sf() as session, session.begin():
        (await session.get(SpaceFileOperationRow, (space.id, key))).request = {"action": "write"}
    monkeypatch.setattr(ReleaseFilesystem, "initialize", initialize)
    if unverified:

        def unavailable(*_args, **_kwargs):
            raise BackingUnavailable("unverified physical proof")

        monkeypatch.setattr(catalog, "verify", unavailable)
    with pytest.raises(SpaceOperationPending):
        await SpaceRecovery(files).host_release(space.id)
    with pytest.raises(SpaceOperationPending):
        await SpaceRecovery(files).release(actor=ALICE, space_id=space.id, expected_generation=2, operation_id=operation())
    assert (catalog.verified[slot].data_path / "keep").read_bytes() == b"original"
    async with sf() as session:
        assert (await session.get(SpaceFileOperationRow, (space.id, key))).phase == "pending"
        assert await session.get(SpaceBackingRow, space.backing_handle) is not None


@pytest.mark.asyncio
@pytest.mark.parametrize("released,race", [(True, None), (False, None), (True, "sql-only"), (True, "observed")])
async def test_lost_commit_ack_replay_repairs_only_the_original_slot_claim(space_file_storage, monkeypatch, released, race):
    from sqlalchemy import event
    from sqlalchemy.orm import Session

    from deerflow.spaces.registry import SpaceRegistry
    from deerflow.spaces.service import SpaceFiles

    files, sf, catalog = space_file_storage
    space = await home(files)
    slot = await deleted(sf, space, retained=True)
    key = operation()
    verify = catalog.verify
    if not released:

        def unavailable(*_args, **_kwargs):
            raise BackingUnavailable("unverified volume")

        monkeypatch.setattr(catalog, "verify", unavailable)
    lost = False

    def select_ack(session):
        for row in (*session.new, *session.dirty):
            if isinstance(row, SpaceFileOperationRow) and (row.space_id, row.operation_id, row.phase) == (space.id, key, "complete"):
                session.info["release_ack_failure"] = True

    def lose_ack(session):
        nonlocal lost
        if session.info.pop("release_ack_failure", False) and not lost:
            lost = True
            raise OSError("commit succeeded but acknowledgment was lost")

    event.listen(Session, "before_commit", select_ack)
    event.listen(Session, "after_commit", lose_ack)
    try:
        with pytest.raises(SpaceOperationPending):
            await SpaceRecovery(files).release(actor=ALICE, space_id=space.id, expected_generation=2, operation_id=key)
    finally:
        event.remove(Session, "before_commit", select_ack)
        event.remove(Session, "after_commit", lose_ack)
    assert lost and slot in files.bound_slots[None]
    async with sf() as session:
        assert await session.get(SpaceBackingRow, space.backing_handle) is None
        assert (await session.get(SpaceFileOperationRow, (space.id, key))).phase == "complete"
    monkeypatch.setattr(catalog, "verify", verify)
    claim = files._claims[slot]
    if race:
        fresh = SpaceFiles(SpaceRegistry(sf, files.registry._resolver), catalog)
        replacement = await home(fresh, "New allocation")
        async with sf() as session:
            assert (await session.get(SpaceBackingRow, replacement.backing_handle)).slot_id == slot
        await fresh.write(actor=ALICE, space_id=replacement.id, expected_generation=replacement.generation, operation_id=operation(), path="new-owner", content=b"preserve", create=True)
        if race == "observed":
            claim = files._publish_claim(slot, claim, replacement.backing_handle, "bound")
    result = await SpaceRecovery(files).release(actor=ALICE, space_id=space.id, expected_generation=2, operation_id=key)
    assert result == {"complete": True, "slot_released": released}
    if race:
        assert slot not in files.assignable_slots(None) and files._claims[slot] is claim
        assert await fresh.read(actor=ALICE, space_id=replacement.id, path="new-owner", max_bytes=100) == b"preserve"
    elif released:
        assert slot in files.assignable_slots(None) and slot not in files.bound_slots[None]
    else:
        assert slot in files.unclaimed_slots[None] and slot not in files.bound_slots[None]


@pytest.mark.asyncio
async def test_release_and_completed_replay_borrow_identity_reads_with_one_connection(single_pool_storage):
    from deerflow.spaces.principals import HostPrincipalResolver

    files, sf, _catalog = single_pool_storage
    space = await home(files)
    await deleted(sf, space, retained=True)
    original = files.registry._resolver._human
    borrowed = []

    async def current(reference):
        async with sf() as session:
            await session.execute(select(1))
            return await original(reference)

    async def in_session(reference, session):
        await session.execute(select(1))
        borrowed.append(reference)
        return await original(reference)

    current.in_session = in_session
    files.registry._resolver = HostPrincipalResolver(human=current)
    key = operation()
    recovery = SpaceRecovery(files)
    assert (await recovery.release(actor=ALICE, space_id=space.id, expected_generation=2, operation_id=key))["slot_released"]
    assert (await recovery.release(actor=ALICE, space_id=space.id, expected_generation=2, operation_id=key))["slot_released"]
    assert len(borrowed) > 1


@pytest.mark.asyncio
async def test_completed_release_replay_requires_current_admin(space_file_storage):
    files, sf, _catalog = space_file_storage
    space = await home(files)
    await deleted(sf, space, retained=True)
    key = operation()
    recovery = SpaceRecovery(files)
    assert (await recovery.release(actor=ALICE, space_id=space.id, expected_generation=2, operation_id=key))["complete"]
    async with sf() as session, session.begin():
        (await session.get(SpaceGrantRow, (space.id, "human", "alice"))).permissions = int(Permission.READ)
    with pytest.raises(SpaceDenied):
        await recovery.release(actor=ALICE, space_id=space.id, expected_generation=2, operation_id=key)
