"""Operator ownership-file preparation preserves live locks and unknown bytes."""

import fcntl
import os
from types import SimpleNamespace

import pytest
from test_storage_init import operator as operator

from app.gateway import storage_init as si


@pytest.fixture
def runtime_operator(operator, monkeypatch):
    tool, mounted, commands = operator
    original_fstat, original_stat = os.fstat, os.stat
    owners, mutations = {}, []

    def projected(metadata):
        key = metadata.st_dev, metadata.st_ino
        if key not in owners:
            return metadata
        values = {name: getattr(metadata, name) for name in dir(metadata) if name.startswith("st_")}
        values["st_uid"], values["st_gid"] = owners[key]
        return SimpleNamespace(**values)

    def chown(fd, uid, gid):
        metadata = original_fstat(fd)
        key = metadata.st_dev, metadata.st_ino
        owners[key] = uid, gid
        mutations.append((key, uid, gid))

    monkeypatch.setattr(si.os, "fchown", chown)
    monkeypatch.setattr(si.os, "fstat", lambda fd: projected(original_fstat(fd)))
    monkeypatch.setattr(si.os, "stat", lambda *args, **kwargs: projected(original_stat(*args, **kwargs)))
    return tool, owners, mutations


def test_prepare_creates_stable_empty_uid_1000_runtime_lock(runtime_operator):
    tool, _owners, _mutations = runtime_operator
    tool.prepare()
    path = tool.pool / ".gateway-owner.lock"
    metadata = path.stat()
    assert (metadata.st_uid, metadata.st_gid, metadata.st_mode & 0o777, metadata.st_nlink, metadata.st_size) == (1000, 1000, 0o600, 1, 0)
    tool.prepare()
    assert path.stat().st_ino == metadata.st_ino


def test_mount_all_leaves_a_valid_live_owner_untouched(runtime_operator):
    tool, _owners, mutations = runtime_operator
    tool.prepare()
    path = tool.pool / ".gateway-owner.lock"
    descriptor = os.open(path, os.O_RDWR)
    metadata = os.fstat(descriptor)
    key = metadata.st_dev, metadata.st_ino
    fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
    try:
        mutations.clear()
        assert tool.mount_all()["returncode"] == 0
        assert path.stat().st_ino == metadata.st_ino
        assert not [item for item in mutations if item[0] == key]
    finally:
        os.close(descriptor)


@pytest.mark.parametrize("kind", ["symlink", "hardlink", "fifo", "directory", "nonempty"])
def test_operator_never_adopts_or_erases_unrecognized_runtime_lock(runtime_operator, kind):
    tool, _owners, _mutations = runtime_operator
    tool.prepare()
    path = tool.pool / ".gateway-owner.lock"
    if kind in ("symlink", "fifo", "directory"):
        path.unlink()
    if kind == "symlink":
        target = tool.pool / "retain"
        target.write_bytes(b"retained")
        path.symlink_to(target)
    elif kind == "hardlink":
        os.link(path, tool.pool / "retain")
    elif kind == "fifo":
        os.mkfifo(path)
    elif kind == "directory":
        path.mkdir()
    else:
        path.write_bytes(b"unknown evidence")
    with pytest.raises(si.Refused):
        tool.mount_all()
    if kind in ("symlink", "nonempty"):
        assert path.read_bytes() == (b"retained" if kind == "symlink" else b"unknown evidence")
    assert path.exists()


@pytest.mark.parametrize("held", [False, True])
def test_malformed_metadata_repair_requires_exclusive_file_ownership(runtime_operator, held):
    tool, owners, mutations = runtime_operator
    tool.prepare()
    path = tool.pool / ".gateway-owner.lock"
    descriptor = os.open(path, os.O_RDWR)
    metadata = os.fstat(descriptor)
    key = metadata.st_dev, metadata.st_ino
    owners[key] = (0, 0)
    path.chmod(0o660)
    if held:
        fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
    try:
        mutations.clear()
        if held:
            with pytest.raises(si.Refused):
                tool.mount_all()
            assert owners[key] == (0, 0) and path.stat().st_mode & 0o777 == 0o660
            assert not [item for item in mutations if item[0] == key]
        else:
            assert tool.mount_all()["returncode"] == 0
            repaired = path.stat()
            assert (repaired.st_uid, repaired.st_gid, repaired.st_mode & 0o777) == (1000, 1000, 0o600)
            assert repaired.st_ino == metadata.st_ino
    finally:
        os.close(descriptor)
