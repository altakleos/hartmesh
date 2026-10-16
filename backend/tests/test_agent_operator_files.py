"""Operator archives and targeted recovery preserve unrelated host state."""

import hashlib
import os
import stat
import tarfile
from types import SimpleNamespace

import pytest
from _storage_spaces_test_support import assert_host_settlement, home
from test_storage_spaces_maintenance import intent, outcome
from test_storage_spaces_service import space_file_storage as space_file_storage

from deerflow.spaces import snapshots


def test_external_archive_keeps_links_private_and_outside_the_storage_budget(tmp_path):
    data, control, exports = (tmp_path / name for name in ("data", "control", "exports"))
    for path in (data, control, exports):
        path.mkdir(mode=0o700 if path == control else 0o755)
    (data / "document").write_bytes(b"retained")
    secret = tmp_path / "outside"
    secret.write_bytes(b"outside")
    (data / "link").symlink_to(secret)
    target = exports / "agent.tar"
    volume = SimpleNamespace(data_path=data, control_path=control)

    digest, size = snapshots.backup(volume, "unused", target=target)

    assert digest == hashlib.sha256(target.read_bytes()).hexdigest() and size == target.stat().st_size
    assert stat.S_IMODE(target.stat().st_mode) == 0o600
    assert list(control.iterdir()) == []
    with tarfile.open(target) as archive:
        assert {member.name for member in archive} == {"document", "link"}
        assert archive.getmember("link").issym()
        assert archive.extractfile("document").read() == b"retained"
    with pytest.raises(FileExistsError):
        snapshots.backup(volume, "unused", target=target)
    assert target.read_bytes() and secret.read_bytes() == b"outside"


def test_external_archive_failure_removes_only_its_owned_target(tmp_path):
    data, control = tmp_path / "data", tmp_path / "control"
    data.mkdir()
    control.mkdir(mode=0o700)
    os.mkfifo(data / "unsupported")
    target = tmp_path / "agent.tar"
    with pytest.raises(snapshots.FileConflict, match="special"):
        snapshots.backup(SimpleNamespace(data_path=data, control_path=control), "unused", target=target)
    assert not target.exists()
    assert stat.S_ISFIFO((data / "unsupported").lstat().st_mode)


def test_external_archive_uses_the_callers_pinned_directory_after_a_parent_rename(tmp_path):
    data, control, exports = (tmp_path / name for name in ("data", "control", "exports"))
    for path in (data, control, exports):
        path.mkdir(mode=0o700 if path == control else 0o755)
    (data / "document").write_bytes(b"retained")
    descriptor = os.open(exports, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    retained = exports.with_name("retained-directory")
    try:
        exports.rename(retained)
        exports.mkdir()
        sentinel = exports / "agent.tar"
        sentinel.write_bytes(b"unrelated")
        digest, size = snapshots.backup(SimpleNamespace(data_path=data, control_path=control), "unused", target=exports / "agent.tar", target_directory_fd=descriptor)
        assert sentinel.read_bytes() == b"unrelated"
        archive = retained / "agent.tar"
        assert digest == hashlib.sha256(archive.read_bytes()).hexdigest() and size == archive.stat().st_size
        with pytest.raises(FileExistsError):
            snapshots.backup(SimpleNamespace(data_path=data, control_path=control), "unused", target=exports / "agent.tar", target_directory_fd=descriptor)
    finally:
        os.close(descriptor)


def test_archive_refuses_a_queued_directory_replaced_after_its_header(tmp_path, monkeypatch):
    data, control, outside = (tmp_path / name for name in ("data", "control", "outside"))
    for path in (data, control, outside):
        path.mkdir(mode=0o700 if path == control else 0o755)
    folder = data / "folder"
    folder.mkdir()
    (folder / "document").write_bytes(b"Admitted")
    (outside / "private").write_bytes(b"Not in Home")
    original, swapped = tarfile.TarFile.addfile, []

    def changed(archive, info, fileobj=None):
        result = original(archive, info, fileobj)
        if info.name == "folder" and not swapped:
            swapped.append(True)
            folder.rename(data / "retained-folder")
            folder.symlink_to(outside, target_is_directory=True)
        return result

    monkeypatch.setattr(tarfile.TarFile, "addfile", changed)
    target = tmp_path / "archive.tar"
    with pytest.raises(snapshots.FileConflict, match="changed"):
        snapshots.backup(SimpleNamespace(data_path=data, control_path=control), "unused", target=target)
    assert swapped and not target.exists() and (outside / "private").read_bytes() == b"Not in Home"


def test_archive_refuses_a_regular_leaf_replaced_before_its_content_is_read(tmp_path, monkeypatch):
    import builtins

    data, control = tmp_path / "data", tmp_path / "control"
    data.mkdir()
    control.mkdir(mode=0o700)
    leaf, outside = data / "document", tmp_path / "outside"
    leaf.write_bytes(b"Admitted")
    outside.write_bytes(b"Not in Home")
    opened, descriptor_open, swapped = builtins.open, os.open, []

    def replace():
        if not swapped:
            swapped.append(True)
            leaf.unlink()
            leaf.symlink_to(outside)

    def path_read(path, mode="r", *args, **kwargs):
        if str(path) == str(leaf) and mode == "rb":
            replace()
        return opened(path, mode, *args, **kwargs)

    def pinned_read(path, flags, *args, **kwargs):
        if str(path).startswith("/proc/self/fd/") and not flags & os.O_PATH:
            replace()
        return descriptor_open(path, flags, *args, **kwargs)

    monkeypatch.setattr(builtins, "open", path_read)
    monkeypatch.setattr(os, "open", pinned_read)
    target = tmp_path / "archive.tar"
    with pytest.raises(snapshots.FileConflict, match="changed"):
        snapshots.backup(SimpleNamespace(data_path=data, control_path=control), "unused", target=target)
    assert swapped and not target.exists() and outside.read_bytes() == b"Not in Home"


@pytest.mark.asyncio
async def test_targeted_settlement_cannot_settle_another_pending_operation(space_file_storage):
    files, sf, _catalog = space_file_storage
    first, second = await home(files), await home(files)
    wanted = await intent(sf, first, {"action": "remove", "path": "absent"})
    sibling = await intent(sf, first, {"action": "remove", "path": "other"})
    unrelated = await intent(sf, second, {"action": "remove", "path": "absent"})

    assert await files.settle_interrupted(space_id=first.id, operation_id=wanted) == 1
    phase, result = await outcome(sf, first, wanted)
    assert phase == "complete"
    assert_host_settlement(result, {"resolution": "host-settled", "value": None})
    assert await outcome(sf, first, sibling) == ("pending", None)
    assert await outcome(sf, second, unrelated) == ("pending", None)
    assert await files.settle_interrupted(space_id=second.id, operation_id=wanted) == 0
    assert await files.settle_interrupted() == 2


@pytest.mark.asyncio
@pytest.mark.parametrize("selection", [{"space_id": "a" * 32}, {"operation_id": "a" * 32}, {"space_id": "bad", "operation_id": "a" * 32}, {"space_id": "a" * 32, "operation_id": "bad"}])
async def test_targeted_settlement_requires_both_valid_identifiers(space_file_storage, selection):
    files, _sf, _catalog = space_file_storage
    with pytest.raises(ValueError):
        await files.settle_interrupted(**selection)


@pytest.mark.asyncio
async def test_targeted_discard_never_replays_or_removes_files_receipts_or_other_intents(space_file_storage):
    from deerflow.spaces.maintenance import discard_interrupted
    from deerflow.spaces.workflows import WorkflowReceipt

    files, sf, catalog = space_file_storage
    first, second = await home(files), await home(files)
    target = await intent(sf, first, {"action": "remove", "path": "keep-this"})
    sibling = await intent(sf, first, {"action": "remove", "path": "also-keep"})
    other = await intent(sf, second, {"action": "remove", "path": "unrelated"})
    async with files.registry.host_locked([first.id]) as (session, rows):
        volume = await files._volume(session, rows[first.id])
    path = volume.data_path / "keep-this"
    path.write_bytes(b"No replay")
    receipt = WorkflowReceipt(volume, target, {"action": "remove", "path": "keep-this"})
    receipt.save()
    before = receipt.path.read_bytes(), receipt.path.stat().st_ino, receipt.path.stat().st_mtime_ns
    assert await discard_interrupted(files, space_id=first.id, operation_id=target, host_id="exact-daemon")
    phase, result = await outcome(sf, first, target)
    assert phase == "failed"
    assert_host_settlement(result, {"resolution": "host-discarded", "by": "operator", "host_id": "exact-daemon"})
    assert path.read_bytes() == b"No replay" and (receipt.path.read_bytes(), receipt.path.stat().st_ino, receipt.path.stat().st_mtime_ns) == before
    assert await outcome(sf, first, sibling) == await outcome(sf, second, other) == ("pending", None)
