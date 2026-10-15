"""Operator archives and targeted recovery preserve unrelated host state."""

import hashlib
import os
import stat
import tarfile
from types import SimpleNamespace

import pytest
from _storage_spaces_test_support import home
from test_storage_spaces_maintenance import intent, outcome
from test_storage_spaces_service import space_file_storage as space_file_storage

from deerflow.spaces import snapshots


def test_external_archive_keeps_links_private_and_outside_the_storage_budget(tmp_path):
    data, control, exports = (tmp_path / name for name in ("data", "control", "exports"))
    for path in (data, control, exports):
        path.mkdir()
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
    control.mkdir()
    os.mkfifo(data / "unsupported")
    target = tmp_path / "agent.tar"
    with pytest.raises(snapshots.FileConflict, match="special"):
        snapshots.backup(SimpleNamespace(data_path=data, control_path=control), "unused", target=target)
    assert not target.exists()
    assert stat.S_ISFIFO((data / "unsupported").lstat().st_mode)


@pytest.mark.asyncio
async def test_targeted_settlement_cannot_settle_another_pending_operation(space_file_storage):
    files, sf, _catalog = space_file_storage
    first, second = await home(files), await home(files)
    wanted = await intent(sf, first, {"action": "remove", "path": "absent"})
    sibling = await intent(sf, first, {"action": "remove", "path": "other"})
    unrelated = await intent(sf, second, {"action": "remove", "path": "absent"})

    assert await files.settle_interrupted(space_id=first.id, operation_id=wanted) == 1
    assert await outcome(sf, first, wanted) == ("complete", {"resolution": "host-settled", "value": None})
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
