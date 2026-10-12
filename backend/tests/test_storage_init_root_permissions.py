"""Qualified parent repair uses small syscall doubles, never native acceptance."""

import os
from pathlib import Path

import pytest
from test_storage_init import operator as operator
from test_storage_init import verified_operator as verified_operator

from app.gateway import storage_init as si


@pytest.fixture
def parent_operator(operator, monkeypatch):
    tool, mounted, commands = operator
    tool.prepare()
    specs = tool.load().volumes
    identities = {spec.mount_path.stat().st_ino: spec.filesystem_uuid for spec in specs.values()}
    mutations = []
    monkeypatch.setattr(si, "read_ext4_identity", lambda path: specs[path.stem].filesystem_uuid)
    monkeypatch.setattr(si, "mounted_ext4_identity", lambda fd: identities[os.fstat(fd).st_ino])
    monkeypatch.setattr(si, "verify_image_extents", lambda *_args: None)

    def facts(_self, spec, root, **kwargs):
        assert os.fstat(root).st_ino in identities
        return dict(type="ext4", root="/", path=str(spec.mount_path), options={"rw", "nodev", "nosuid"})

    monkeypatch.setattr(si.StorageInitializer, "_mounted_root_facts", facts, raising=False)
    monkeypatch.setattr(si.os, "fchown", lambda fd, uid, gid: mutations.append((os.fstat(fd).st_ino, uid, gid)))
    return tool, mounted, commands, specs, mutations


def test_prepare_gives_gateway_group_parent_write_permission(operator, monkeypatch):
    tool, _, _ = operator
    owners = {}
    chown = si.os.fchown

    def record_chown(fd, uid, gid):
        chown(fd, uid, gid)
        owners.setdefault(Path(os.readlink(f"/proc/self/fd/{fd}")), (uid, gid))

    monkeypatch.setattr(si.os, "fchown", record_chown)
    tool.prepare()
    for spec in tool.load().volumes.values():
        assert owners[spec.mount_path] == (0, 1000)
        assert spec.mount_path.stat().st_mode & 0o777 == 0o770
        assert owners[spec.image_path] == (0, 1000)
        assert owners[spec.mount_path / "data"] == owners[spec.mount_path / "control"] == (1000, 1000)


@pytest.mark.parametrize("already", [False, True])
def test_mount_all_repairs_only_the_qualified_parent(parent_operator, already):
    tool, mounted, commands, specs, mutations = parent_operator
    spec = next(iter(specs.values()))
    inode = spec.mount_path.stat().st_ino
    spec.mount_path.chmod(0o755)
    (spec.mount_path / "data").rmdir()
    (spec.mount_path / "control").chmod(0o777)
    if not already:
        mounted.remove(spec.mount_path)
    commands.clear()
    document = tool.mount_all()
    assert document["returncode"] == 0, document
    assert spec.mount_path.stat().st_mode & 0o777 == 0o770
    assert (inode, 0, 1000) in mutations
    assert not (spec.mount_path / "data").exists()
    assert (spec.mount_path / "control").stat().st_mode & 0o777 == 0o777
    assert not [command for command in commands if command[0] in ("e2fsck", "sh")]


@pytest.mark.parametrize("failure", ["size", "hardlink", "fifo", "directory", "allocation", "image-uuid", "inventory-uuid", "mounted-uuid", "source", "geometry", "read-only", "nodev", "nosuid"])
def test_unqualified_slot_has_zero_parent_permission_mutations(parent_operator, monkeypatch, failure):
    tool, _, commands, specs, mutations = parent_operator
    spec = next(iter(specs.values()))
    inode = spec.mount_path.stat().st_ino
    spec.mount_path.chmod(0o755)
    if failure == "size":
        spec.image_path.write_bytes(b"too short")
    elif failure == "hardlink":
        os.link(spec.image_path, tool.pool / "extra-link")
    elif failure in ("fifo", "directory"):
        spec.image_path.unlink()
        if failure == "fifo":
            os.mkfifo(spec.image_path)
        else:
            spec.image_path.mkdir()
    elif failure == "inventory-uuid":
        from dataclasses import replace
        from types import SimpleNamespace
        from uuid import uuid4

        monkeypatch.setattr(tool, "load", lambda **_kwargs: SimpleNamespace(volumes={**specs, spec.slot_id: replace(spec, filesystem_uuid=str(uuid4()))}))
    elif failure == "allocation":
        original = si.verify_image_extents

        def extents(fd, size):
            if os.fstat(fd).st_ino == spec.image_path.stat().st_ino:
                raise si.BackingUnavailable("unallocated image")
            return original(fd, size)

        monkeypatch.setattr(si, "verify_image_extents", extents)
    elif failure == "image-uuid":
        original = si._ext4_identity
        monkeypatch.setattr(si, "_ext4_identity", lambda fd: "different" if os.fstat(fd).st_ino == spec.image_path.stat().st_ino else original(fd))
    elif failure == "mounted-uuid":
        original = si.mounted_ext4_identity
        monkeypatch.setattr(si, "mounted_ext4_identity", lambda fd: "different" if os.fstat(fd).st_ino == inode else original(fd))
    else:
        original = si.StorageInitializer._mounted_root_facts

        def facts(self, value, root, **kwargs):
            result = original(self, value, root, **kwargs)
            if value.slot_id == spec.slot_id:
                if failure in ("source", "geometry"):
                    raise si.Refused("foreign loop image or geometry")
                result["options"] = {"nodev", "nosuid", "ro"} if failure == "read-only" else {"rw", "nodev", "nosuid"} - {failure}
            return result

        monkeypatch.setattr(si.StorageInitializer, "_mounted_root_facts", facts)
    commands.clear()
    document = tool.mount_all()
    assert document["returncode"] == 2
    assert [entry["slot_id"] for entry in document["failed"]] == [spec.slot_id]
    assert not [change for change in mutations if change[0] == inode]
    assert spec.mount_path.stat().st_mode & 0o777 == 0o755
    assert not [command for command in commands if command[0] in ("e2fsck", "sh")]


def test_parent_replacement_after_proof_cannot_redirect_repair(parent_operator, monkeypatch):
    tool, _, _, specs, mutations = parent_operator
    spec = next(iter(specs.values()))
    inode = spec.mount_path.stat().st_ino
    spec.mount_path.chmod(0o755)
    original = si.StorageInitializer._mounted_root_facts

    def replaced(self, value, root, **kwargs):
        result = original(self, value, root, **kwargs)
        if value.slot_id == spec.slot_id:
            value.mount_path.rename(value.mount_path.with_name("retired-root"))
            value.mount_path.mkdir(mode=0o755)
        return result

    monkeypatch.setattr(si.StorageInitializer, "_mounted_root_facts", replaced)
    document = tool.mount_all()
    assert document["returncode"] == 2
    replacement = spec.mount_path.stat()
    assert replacement.st_ino != inode and replacement.st_mode & 0o777 == 0o755
    assert not [change for change in mutations if change[0] == replacement.st_ino]


def test_symlinked_parent_is_refused_without_mutation(parent_operator):
    tool, _, _, specs, mutations = parent_operator
    spec = next(iter(specs.values()))
    spec.mount_path.rename(spec.mount_path.with_name("retired-root"))
    spec.mount_path.symlink_to(spec.mount_path.with_name("retired-root"), target_is_directory=True)
    with pytest.raises(si.Refused):
        tool.mount_all()
    assert mutations == []


def test_explicit_verify_repairs_the_old_parent_layout(verified_operator):
    tool, spec, _, _ = verified_operator
    spec.mount_path.chmod(0o755)
    assert tool.slot_state(spec)["state"] == "wrong-modes"
    assert tool.slot_state(spec, repair=True)["state"] == "repaired"
    assert spec.mount_path.stat().st_mode & 0o777 == 0o770
    assert (spec.mount_path / "data").stat().st_mode & 0o777 == 0o770
    assert (spec.mount_path / "control").stat().st_mode & 0o777 == 0o700
