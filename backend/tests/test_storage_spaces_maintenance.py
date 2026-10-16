"""Actorless recovery proves effects; it never races a live host mutation."""

import asyncio
import hashlib
import os
import threading
from contextlib import asynccontextmanager

import pytest
from _storage_spaces_test_support import ALICE, assert_host_settlement, home, operation
from test_storage_spaces_service import space_file_storage as space_file_storage

from deerflow.persistence.spaces.files import SpaceFileOperationRow
from deerflow.persistence.spaces.lifecycle import SpaceAttachmentRow, SpaceMountRow
from deerflow.spaces.service import SpaceOperationPending
from deerflow.spaces.workflows import WorkflowReceipt, acquire, close


async def intent(sf, space, request):
    identifier = operation()
    async with sf() as session, session.begin():
        session.add(SpaceFileOperationRow(space_id=space.id, operation_id=identifier, actor_kind="human", actor_id="alice", generation=space.generation, phase="pending", request=request))
    return identifier


async def outcome(sf, space, identifier):
    async with sf() as session:
        row = await session.get(SpaceFileOperationRow, (space.id, identifier))
        return row.phase, row.result


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "action,present,complete", [("write", True, True), ("write", False, False), ("mkdir", True, True), ("mkdir", False, False), ("rename", True, True), ("rename", False, False), ("remove", True, False), ("remove", False, True)]
)
async def test_settlement_proves_direct_effects(space_file_storage, action, present, complete):
    files, sf, catalog = space_file_storage
    space = await home(files)
    volume = next(iter(files._verified.values()))
    request = {"action": action, "path": "item"}
    digest = hashlib.sha256(b"published").hexdigest()
    if action == "write":
        request["sha256"] = digest
    if action == "rename":
        request["destination"] = "renamed"
    if present:
        if action == "mkdir":
            (volume.data_path / "item").mkdir()
        else:
            (volume.data_path / ("renamed" if action == "rename" else "item")).write_bytes(b"published")
    identifier = await intent(sf, space, request)
    assert await files.settle_interrupted() == 1
    phase, result = await outcome(sf, space, identifier)
    assert phase == ("complete" if complete else "failed")
    assert_host_settlement(result, {"resolution": "host-settled", "value": digest if action == "write" else None} if complete else {"resolution": "host-settled"})
    assert await files.settle_interrupted() == 0


@pytest.mark.asyncio
async def test_settlement_preserves_unknown_actions_and_missing_receipts(space_file_storage):
    files, sf, _ = space_file_storage
    space = await home(files)
    for request in ({"action": "copy"}, {"action": "restore"}, {"action": "unknown"}, {"action": "feature-workflow", "namespace": "hm.my-files"}):
        identifier = await intent(sf, space, request)
        assert await files.settle_interrupted() == 0
        assert await outcome(sf, space, identifier) == ("pending", None)


@pytest.mark.asyncio
async def test_receipt_settlement_retains_facts_and_only_removes_owned_stages(space_file_storage):
    files, sf, _ = space_file_storage
    space = await home(files)
    volume = next(iter(files._verified.values()))
    request = {"action": "feature-workflow", "namespace": "hm.projects"}
    identifier = await intent(sf, space, request)
    receipt = WorkflowReceipt(volume, identifier, request)
    receipt.value["facts"] = {"published": "opaque-domain-value"}
    receipt.save()

    async def settle(_intent, *, session):
        return "host-settled"

    files.workflow_settlers["hm.projects"] = settle
    stage = volume.control_path / ("stage-" + operation())
    stage.write_bytes(b"abandoned")
    unrelated = volume.control_path / "upload-retained"
    unrelated.write_bytes(b"keep")
    directory = volume.control_path / ("stage-" + operation())
    directory.mkdir()
    link = volume.control_path / ("stage-" + operation())
    link.symlink_to(unrelated)
    assert await files.settle_interrupted() == 1
    phase, result = await outcome(sf, space, identifier)
    assert phase == "complete"
    assert_host_settlement(result, {"resolution": "host-settled", "value": None, "facts": receipt.value["facts"]})
    assert not receipt.path.exists()
    assert not stage.exists()
    assert unrelated.read_bytes() == b"keep" and directory.is_dir() and link.is_symlink()


@pytest.mark.asyncio
async def test_settlement_retains_live_lock_and_unsafe_path_uncertainty(space_file_storage):
    files, sf, _ = space_file_storage
    space = await home(files)
    volume = next(iter(files._verified.values()))
    identifier = await intent(sf, space, {"action": "remove", "path": "../outside"})
    assert await files.settle_interrupted() == 0
    assert await outcome(sf, space, identifier) == ("pending", None)
    descriptors = acquire({space.id: volume})
    try:
        assert await files.settle_interrupted() == 0
    finally:
        close(descriptors)


@pytest.mark.asyncio
async def test_live_mutation_is_owned_across_intent_commit(space_file_storage, monkeypatch):
    files, sf, _ = space_file_storage
    space = await home(files)
    committed, resume = asyncio.Event(), asyncio.Event()
    original = files.registry.admitted
    calls = 0

    @asynccontextmanager
    async def pause_after_intent(**kwargs):
        nonlocal calls
        calls += 1
        first = calls == 1
        async with original(**kwargs) as admission:
            yield admission
        if first:
            committed.set()
            await resume.wait()

    monkeypatch.setattr(files.registry, "admitted", pause_after_intent)
    identifier = operation()
    task = asyncio.create_task(files.write(actor=ALICE, space_id=space.id, expected_generation=space.generation, operation_id=identifier, path="item", content=b"live", create=True))
    try:
        await asyncio.wait_for(committed.wait(), 5)
        assert await files.settle_interrupted() == 0
        assert await outcome(sf, space, identifier) == ("pending", None)
    finally:
        resume.set()
        await task
    assert await outcome(sf, space, identifier) == ("complete", {"value": hashlib.sha256(b"live").hexdigest()})


@pytest.mark.asyncio
async def test_provenance_is_recorded_without_changing_authority(space_file_storage):
    files, sf, _ = space_file_storage
    space = await home(files)
    identifier = operation()
    await files.mkdir(actor=ALICE, space_id=space.id, expected_generation=space.generation, operation_id=identifier, path="folder", on_behalf_of="human-request-id")
    async with sf() as session:
        row = await session.get(SpaceFileOperationRow, (space.id, identifier))
        assert row.request == {"action": "mkdir", "path": "folder", "destination": None, "sha256": None, "size": None, "on_behalf_of": "human-request-id"}
    from _storage_spaces_test_support import BOB

    from deerflow.spaces.contract import SpaceDenied

    with pytest.raises(SpaceDenied):
        await files.mkdir(actor=BOB, space_id=space.id, expected_generation=space.generation, operation_id=operation(), path="other", on_behalf_of="alice")


def test_http_busy_includes_specific_cause():
    from app.gateway.routers.spaces import _http_error

    for cause in ("workflow", "lifecycle"):
        error = _http_error(SpaceOperationPending("busy", cause=cause))
        assert error.status_code == 409
        assert error.detail["code"] == "storage-busy"
        assert error.detail["cause"] == cause


@pytest.mark.asyncio
@pytest.mark.parametrize("phase", ["pending", "active", "fence_pending", "fenced"])
async def test_settlement_requires_confirmed_attachment_fencing(space_file_storage, phase):
    files, sf, _ = space_file_storage
    space = await home(files)
    identifier = await intent(sf, space, {"action": "remove", "path": "absent"})
    attachment_id = operation()
    async with sf() as session, session.begin():
        session.add(SpaceAttachmentRow(id=attachment_id, incarnation=operation(), actor_kind="human", actor_id="alice", host_id="fixture-host", phase=phase))
        await session.flush()
        session.add(SpaceMountRow(attachment_id=attachment_id, space_id=space.id, generation=space.generation, alias="data", writable=1))
    assert await files.settle_interrupted() == (1 if phase == "fenced" else 0)
    assert (await outcome(sf, space, identifier))[0] == ("complete" if phase == "fenced" else "pending")


@pytest.mark.asyncio
async def test_failed_commit_preserves_pending_receipt_and_stage(space_file_storage, monkeypatch):
    files, sf, _ = space_file_storage
    space = await home(files)
    volume = next(iter(files._verified.values()))
    request = {"action": "feature-workflow", "namespace": "fixture-domain"}
    identifier = await intent(sf, space, request)
    receipt = WorkflowReceipt(volume, identifier, request)
    receipt.save()
    stage = volume.control_path / ("stage-" + operation())
    stage.write_bytes(b"retain")

    async def settle(_intent, *, session):
        return "host-settled"

    files.workflow_settlers["fixture-domain"] = settle
    original = files.registry.host_locked

    @asynccontextmanager
    async def failed_commit(identifiers):
        async with original(identifiers) as admission:
            yield admission
            raise OSError("fixture commit lost")

    monkeypatch.setattr(files.registry, "host_locked", failed_commit)
    assert await files.settle_interrupted() == 0
    assert await outcome(sf, space, identifier) == ("pending", None)
    assert receipt.path.exists() and stage.read_bytes() == b"retain"


@pytest.mark.asyncio
async def test_cancelled_settler_drains_worker_and_owns_lock_until_commit(space_file_storage, monkeypatch):
    from deerflow.spaces import maintenance

    files, sf, _ = space_file_storage
    space = await home(files)
    identifier = await intent(sf, space, {"action": "remove", "path": "absent"})
    entered, resume, finished = threading.Event(), threading.Event(), threading.Event()
    original = maintenance._direct

    def blocked(*args):
        entered.set()
        try:
            assert resume.wait(10)
            return original(*args)
        finally:
            finished.set()

    monkeypatch.setattr(maintenance, "_direct", blocked)
    task = asyncio.create_task(files.settle_interrupted())
    try:
        assert await asyncio.to_thread(entered.wait, 5)
        assert await files.settle_interrupted() == 0  # A competing settler cannot own it.
        task.cancel()
        await asyncio.sleep(0.02)
        assert not task.done()
        resume.set()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert finished.is_set()
        assert (await outcome(sf, space, identifier))[0] == "complete"
    finally:
        resume.set()
        await asyncio.gather(task, return_exceptions=True)


@pytest.mark.asyncio
async def test_inspection_errors_do_not_prove_absence(space_file_storage, monkeypatch):
    from deerflow.spaces.filesystem import ConfinedFilesystem

    files, sf, _ = space_file_storage
    space = await home(files)
    identifier = await intent(sf, space, {"action": "remove", "path": "absent"})

    def denied(*args, **kwargs):
        raise PermissionError("fixture denied metadata")

    monkeypatch.setattr(ConfinedFilesystem, "_open", denied)
    assert await files.settle_interrupted() == 0
    assert await outcome(sf, space, identifier) == ("pending", None)


@pytest.mark.asyncio
async def test_settlement_does_not_require_historical_intent_generation_to_be_current(space_file_storage):
    from deerflow.persistence.spaces.model import SpaceRow

    files, sf, _ = space_file_storage
    space = await home(files)
    identifier = await intent(sf, space, {"action": "remove", "path": "absent"})
    async with sf() as session, session.begin():
        (await session.get(SpaceRow, space.id)).generation += 1
    assert await files.settle_interrupted() == 1
    assert (await outcome(sf, space, identifier))[0] == "complete"


@pytest.mark.asyncio
async def test_unverified_device_never_qualifies_itself_from_current_paths(space_file_storage):
    files, sf, _ = space_file_storage
    space = await home(files)
    volume = next(iter(files._verified.values()))
    identifier = await intent(sf, space, {"action": "remove", "path": "absent"})
    volume.device += 1  # Directory paths cannot replace the catalog's device proof.
    assert await files.settle_interrupted() == 0
    assert await outcome(sf, space, identifier) == ("pending", None)


@pytest.mark.asyncio
@pytest.mark.parametrize("root", ["data_path", "control_path"])
async def test_replaced_root_after_lock_preserves_all_evidence(space_file_storage, monkeypatch, root):
    from deerflow.spaces import maintenance

    files, sf, _ = space_file_storage
    space = await home(files)
    volume = next(iter(files._verified.values()))
    request = {"action": "feature-workflow", "namespace": "fixture-domain"}
    identifier = await intent(sf, space, request)
    receipt = WorkflowReceipt(volume, identifier, request)
    receipt.save()
    original = maintenance._owned
    original_path = getattr(volume, root)
    displaced = original_path.with_name("retained-" + root)

    def replace(*args):
        owned = original(*args)
        original_path.rename(displaced)
        original_path.mkdir(mode=0o700)
        (original_path / "foreign-evidence").write_bytes(b"keep")
        return owned

    async def settle(_intent, *, session):
        raise AssertionError("Domain settlement must not see an unverified root")

    files.workflow_settlers["fixture-domain"] = settle
    monkeypatch.setattr(maintenance, "_owned", replace)
    assert await files.settle_interrupted() == 0
    assert await outcome(sf, space, identifier) == ("pending", None)
    assert (original_path / "foreign-evidence").read_bytes() == b"keep"
    assert (displaced / receipt.path.name).exists() if root == "control_path" else receipt.path.exists()


@pytest.mark.asyncio
async def test_pinned_receipt_never_reads_or_removes_replacement_control_root(space_file_storage):
    files, _sf, _ = space_file_storage
    await home(files)
    volume = next(iter(files._verified.values()))
    identifier, request = operation(), {"action": "feature-workflow", "namespace": "fixture-domain"}
    receipt = WorkflowReceipt(volume, identifier, request)
    receipt.value["facts"] = {"proof": "owned"}
    receipt.save()
    with volume.filesystem() as filesystem:
        pinned = WorkflowReceipt(volume, identifier, request, control_fd=filesystem._control_fd)
        displaced = volume.control_path.with_name("retained-control")
        volume.control_path.rename(displaced)
        volume.control_path.mkdir(mode=0o700)
        foreign = WorkflowReceipt(volume, identifier, request)
        foreign.value["facts"] = {"proof": "foreign"}
        foreign.save()
        pinned.load()
        assert pinned.value["facts"] == {"proof": "owned"}
        pinned.remove()
        assert not (displaced / receipt.path.name).exists()
        foreign.load()
        assert foreign.value["facts"] == {"proof": "foreign"}


@pytest.mark.asyncio
async def test_uninspectable_pending_operation_keeps_shared_generic_stages(space_file_storage):
    files, sf, _ = space_file_storage
    space = await home(files)
    volume = next(iter(files._verified.values()))
    identifier = await intent(sf, space, {"action": "copy"})
    await intent(sf, space, {"action": "remove", "path": "absent"})
    stage = volume.control_path / ("stage-" + operation())
    stage.write_bytes(b"unknown copy evidence")
    assert await files.settle_interrupted() == 1
    assert await outcome(sf, space, identifier) == ("pending", None)
    assert stage.read_bytes() == b"unknown copy evidence"


@pytest.mark.asyncio
@pytest.mark.parametrize("changed", ["binding", "intent"])
async def test_settlement_rechecks_snapshot_after_qualification(space_file_storage, monkeypatch, changed):
    from deerflow.persistence.spaces.files import SpaceBackingRow
    from deerflow.spaces import maintenance

    files, sf, _ = space_file_storage
    space = await home(files)
    identifier = await intent(sf, space, {"action": "remove", "path": "absent"})
    original = maintenance.run_file_io

    async def replace_after_ownership(function, *args, **kwargs):
        result = await original(function, *args, **kwargs)
        if function is maintenance._owned:
            async with sf() as session, session.begin():
                if changed == "binding":
                    (await session.get(SpaceBackingRow, space.backing_handle)).root_inode += 1
                else:
                    (await session.get(SpaceFileOperationRow, (space.id, identifier))).request = {"action": "remove", "path": "different"}
        return result

    monkeypatch.setattr(maintenance, "run_file_io", replace_after_ownership)
    assert await files.settle_interrupted() == 0
    assert await outcome(sf, space, identifier) == ("pending", None)


@pytest.mark.asyncio
async def test_pending_put_retains_the_proven_file_result(space_file_storage):
    files, sf, _ = space_file_storage
    space = await home(files)
    volume = next(iter(files._verified.values()))
    (volume.data_path / "item").write_bytes(b"published")
    digest = hashlib.sha256(b"published").hexdigest()
    identifier = await intent(sf, space, {"action": "put", "path": "item", "sha256": digest, "size": 9, "on_behalf_of": "alice"})
    assert await files.settle_interrupted() == 1
    phase, result = await outcome(sf, space, identifier)
    assert phase == "complete"
    assert_host_settlement(result, {"resolution": "host-settled", "value": {"path": "item", "size": 9, "sha256": digest, "deduplicated": False}})


@pytest.mark.asyncio
@pytest.mark.parametrize("malformed", [[], {}])
async def test_malformed_action_does_not_starve_later_valid_operations(space_file_storage, malformed):
    files, sf, _ = space_file_storage
    space = await home(files)
    bad = await intent(sf, space, {"action": malformed})
    good = await intent(sf, space, {"action": "remove", "path": "absent"})
    assert await files.settle_interrupted() == 1
    assert await outcome(sf, space, bad) == ("pending", None)
    assert (await outcome(sf, space, good))[0] == "complete"


@pytest.mark.asyncio
async def test_private_fifo_receipt_is_rejected_without_blocking(space_file_storage):
    files, _sf, _ = space_file_storage
    await home(files)
    volume = next(iter(files._verified.values()))
    receipt = WorkflowReceipt(volume, operation(), {"action": "feature-workflow"})
    os.mkfifo(receipt.path, 0o600)
    task = asyncio.create_task(asyncio.to_thread(receipt.load))
    done = set()
    try:
        done, _pending = await asyncio.wait({task}, timeout=1)
    finally:
        if not task.done():
            # Release an old blocking implementation so a failing test cannot
            # leave its worker or executor shutdown hung.
            def unblock():
                try:
                    fd = os.open(receipt.path, os.O_WRONLY | os.O_NONBLOCK)
                except OSError:
                    return
                os.close(fd)

            await asyncio.to_thread(unblock)
        await asyncio.gather(task, return_exceptions=True)
    assert task in done, "Special receipt nodes must refuse without waiting for a writer"
    assert isinstance(task.exception(), ValueError)


@pytest.mark.asyncio
async def test_malformed_durable_operation_id_is_preserved_without_filesystem_effect(space_file_storage):
    files, sf, _ = space_file_storage
    space = await home(files)
    async with sf() as session, session.begin():
        session.add(SpaceFileOperationRow(space_id=space.id, operation_id="invalid-operation-id", actor_kind="human", actor_id="alice", generation=1, phase="pending", request={"action": "remove", "path": "absent"}))
    assert await files.settle_interrupted() == 0
    assert await outcome(sf, space, "invalid-operation-id") == ("pending", None)
