"""Operator publishes host evidence privately without adopting retained bytes."""

import os

import pytest
from test_storage_init import operator as operator
from test_storage_init_runtime_owner import runtime_operator as runtime_operator

from app.gateway import storage_init as si


def test_operator_atomically_publishes_private_evidence_directory(runtime_operator):
    tool, _owners, mutations = runtime_operator
    tool.pool.mkdir(parents=True, exist_ok=True)
    tool._prepare_native_creates()
    directory = tool.pool / ".native-creates"
    metadata = directory.stat()
    assert (metadata.st_uid, metadata.st_gid, metadata.st_mode & 0o777) == (1000, 1000, 0o700)
    retained = directory / "unknown-record"
    retained.write_bytes(b"retained uncertainty")
    mutations.clear()
    tool._prepare_native_creates()
    assert directory.stat().st_ino == metadata.st_ino
    assert retained.read_bytes() == b"retained uncertainty" and not mutations


@pytest.mark.parametrize("kind", ["symlink", "file", "fifo", "mode", "owner"])
def test_operator_never_repairs_or_replaces_published_evidence(runtime_operator, kind):
    tool, owners, mutations = runtime_operator
    tool.pool.mkdir(parents=True, exist_ok=True)
    tool._prepare_native_creates()
    directory = tool.pool / ".native-creates"
    if kind in ("symlink", "file", "fifo"):
        directory.rmdir()
    if kind == "symlink":
        target = tool.pool / "retained"
        target.mkdir()
        directory.symlink_to(target, target_is_directory=True)
    elif kind == "file":
        directory.write_bytes(b"unknown")
    elif kind == "fifo":
        os.mkfifo(directory)
    elif kind == "mode":
        directory.chmod(0o770)
    else:
        metadata = directory.stat()
        owners[metadata.st_dev, metadata.st_ino] = (0, 0)
    metadata = directory.lstat()
    mutations.clear()
    with pytest.raises(si.Refused):
        tool._prepare_native_creates()
    assert directory.lstat().st_ino == metadata.st_ino and not mutations
    if kind == "file":
        assert directory.read_bytes() == b"unknown"


def test_operator_failed_publication_preserves_private_stage_and_can_retry(runtime_operator, monkeypatch):
    from deerflow.spaces.filesystem import ConfinedFilesystem

    tool, _owners, _mutations = runtime_operator
    tool.pool.mkdir(parents=True, exist_ok=True)
    rename = ConfinedFilesystem._rename_no_replace

    def fail(*args):
        raise OSError("Publication interrupted")

    monkeypatch.setattr(ConfinedFilesystem, "_rename_no_replace", fail)
    with pytest.raises(si.Refused):
        tool._prepare_native_creates()
    assert not (tool.pool / ".native-creates").exists()
    stages = tuple(tool.pool.glob(".native-creates-stage-*"))
    assert len(stages) == 1 and stages[0].stat().st_mode & 0o777 == 0o700
    monkeypatch.setattr(ConfinedFilesystem, "_rename_no_replace", rename)
    tool._prepare_native_creates()
    assert (tool.pool / ".native-creates").is_dir() and stages[0].is_dir()


def test_operator_stage_path_replacement_is_not_published(runtime_operator, monkeypatch):
    tool, _owners, _mutations = runtime_operator
    tool.pool.mkdir(parents=True, exist_ok=True)
    fsync = os.fsync
    swapped = []

    def replace_before_publication(fd):
        fsync(fd)
        target = os.readlink(f"/proc/self/fd/{fd}")
        if ".native-creates-stage-" in target and not swapped:
            source = tool.pool / target.rsplit("/", 1)[-1]
            source.rename(tool.pool / "retained-stage")
            source.mkdir(mode=0o700)
            swapped.append(True)

    monkeypatch.setattr(os, "fsync", replace_before_publication)
    with pytest.raises(si.Refused):
        tool._prepare_native_creates()
    assert swapped and not (tool.pool / ".native-creates").exists()
    assert (tool.pool / "retained-stage").is_dir()


def test_full_prepare_creates_native_evidence_namespace(runtime_operator):
    tool, _owners, _mutations = runtime_operator
    tool.prepare()
    directory = tool.pool / ".native-creates"
    assert directory.is_dir()
    metadata = directory.stat()
    assert (metadata.st_uid, metadata.st_gid, metadata.st_mode & 0o777) == (1000, 1000, 0o700)


def test_mount_all_prepares_missing_namespace_without_erasing_stage(runtime_operator):
    tool, _owners, _mutations = runtime_operator
    tool.prepare()
    directory = tool.pool / ".native-creates"
    if directory.exists():
        directory.rmdir()
    stage = tool.pool / ".native-creates-stage-unknown"
    stage.mkdir(mode=0o700)
    (stage / "retained").write_bytes(b"uncertainty")
    assert tool.mount_all()["returncode"] == 0
    assert directory.is_dir() and (stage / "retained").read_bytes() == b"uncertainty"


def test_operator_refuses_shared_writable_pool_before_staging(runtime_operator):
    tool, _owners, _mutations = runtime_operator
    tool.pool.chmod(0o770)
    with pytest.raises(si.Refused):
        tool._prepare_native_creates()
    assert not tuple(tool.pool.glob(".native-creates*"))


def test_operator_retries_durability_on_same_published_directory(runtime_operator, monkeypatch):
    tool, _owners, _mutations = runtime_operator
    fsync = os.fsync
    attempts = []
    fail_parent = True

    def interrupted_parent_sync(fd):
        if os.readlink(f"/proc/self/fd/{fd}") == str(tool.pool):
            attempts.append(True)
            if fail_parent:
                raise OSError("Pool directory fsync interrupted")
        fsync(fd)

    monkeypatch.setattr(os, "fsync", interrupted_parent_sync)
    with pytest.raises(si.Refused):
        tool._prepare_native_creates()
    directory = tool.pool / ".native-creates"
    inode = directory.stat().st_ino
    with pytest.raises(si.Refused):
        tool._prepare_native_creates()
    assert len(attempts) == 2 and directory.stat().st_ino == inode
    fail_parent = False
    tool._prepare_native_creates()
    assert len(attempts) == 3 and directory.stat().st_ino == inode
