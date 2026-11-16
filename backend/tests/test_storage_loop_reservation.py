"""Offline ownership/queue guards; real loop behavior belongs to native CI."""

import os
from dataclasses import replace
from types import SimpleNamespace
from uuid import uuid4

import pytest

from deerflow.spaces import loop_reservation as lr
from deerflow.spaces.backings import BackingUnavailable, FixedVolumeSpec


@pytest.fixture
def held_loop(tmp_path, monkeypatch):
    root, image = tmp_path / "volume", tmp_path / "image"
    root.mkdir()
    image.write_bytes(bytes(4096))
    image.chmod(0o600)
    spec = FixedVolumeSpec(uuid4().hex, root, image, str(uuid4()), 4096, 128)
    sysfs = tmp_path / "sysfs"
    loop = sysfs / "loop"
    loop.mkdir(parents=True)
    queue = sysfs / "queue"
    queue.mkdir()
    limit = queue / "discard_max_bytes"
    limit.write_text("4096\n", encoding="utf-8")
    for name, value in {"backing_file": str(image), "offset": "0", "sizelimit": "0", "autoclear": "1"}.items():
        (loop / name).write_text(value + "\n", encoding="utf-8")
    root_fd = os.open(root, os.O_RDONLY | os.O_DIRECTORY)
    image_fd = os.open(image, os.O_RDONLY)
    meta = os.fstat(root_fd)
    device = f"{os.major(meta.st_dev)}:{os.minor(meta.st_dev)}"
    mount = dict(type="ext4", root="/", path=str(root), device=device, mount_id=17, options={"rw", "nodev", "nosuid"})
    monkeypatch.setattr(lr, "_device_path", lambda value: sysfs if value == device else pytest.fail("another device was used"))
    monkeypatch.setattr(lr, "_mount_record", lambda fd: dict(mount))
    monkeypatch.setattr(lr, "_ext4_identity", lambda fd: spec.filesystem_uuid)
    monkeypatch.setattr(lr, "mounted_ext4_identity", lambda fd: spec.filesystem_uuid)
    try:
        yield spec, root_fd, image_fd, limit, loop, mount
    finally:
        os.close(image_fd)
        os.close(root_fd)


def test_only_qualified_loop_is_configured_and_both_descriptors_stay_open(held_loop):
    spec, root, image, limit, _loop, _mount = held_loop
    writes = []

    def write(path, *, pass_fds):
        assert path == limit
        assert pass_fds == (root, image)
        assert os.fstat(root).st_ino == spec.mount_path.stat().st_ino
        assert os.fstat(image).st_ino == spec.image_path.stat().st_ino
        writes.append(path)
        path.write_text("0\n", encoding="utf-8")

    lr.protect_loop_reservation(spec, root, image, writer=write)
    assert writes == [limit]
    lr.verify_loop_reservation(_mount["device"])


@pytest.mark.parametrize("failure", ["source", "offset", "sizelimit", "autoclear", "mount", "readonly", "uuid", "image-link", "image-name", "root-name"])
def test_unqualified_identity_has_no_queue_mutation(held_loop, monkeypatch, failure):
    spec, root, image, limit, loop, mount = held_loop
    if failure == "source":
        (loop / "backing_file").write_text("/another.img\n", encoding="utf-8")
    elif failure in {"offset", "sizelimit", "autoclear"}:
        (loop / failure).write_text("4096\n", encoding="utf-8")
    elif failure == "mount":
        mount["device"] = "7:999999"
    elif failure == "readonly":
        mount["options"] = {"ro", "nodev", "nosuid"}
    elif failure == "uuid":
        spec = replace(spec, filesystem_uuid=str(uuid4()))
    elif failure == "image-link":
        os.link(spec.image_path, spec.image_path.with_suffix(".link"))
    elif failure == "image-name":
        spec.image_path.rename(spec.image_path.with_suffix(".old"))
        spec.image_path.write_bytes(bytes(4096))
    else:
        spec.mount_path.rename(spec.mount_path.with_suffix(".old"))
        spec.mount_path.mkdir()
    writes = []
    with pytest.raises(BackingUnavailable):
        lr.protect_loop_reservation(spec, root, image, writer=lambda *a, **kw: writes.append(a))
    assert not writes and limit.read_text(encoding="utf-8") == "4096\n"


@pytest.mark.parametrize("value", ["4096\n", "", "00\n", "-1\n", "unknown\n"])
def test_nonzero_or_malformed_readback_never_qualifies(held_loop, value):
    spec, root, image, limit, _loop, _mount = held_loop
    with pytest.raises(BackingUnavailable, match="reservation"):
        lr.protect_loop_reservation(spec, root, image, writer=lambda path, **kw: path.write_text(value, encoding="utf-8"))


def test_unavailable_protection_cannot_qualify_and_does_not_repair(held_loop):
    _spec, _root, _image, limit, _loop, mount = held_loop
    limit.unlink()
    with pytest.raises(BackingUnavailable):
        lr.verify_loop_reservation(mount["device"])
    assert not limit.exists()


@pytest.mark.parametrize("changed", ["mount", "image", "source"])
def test_identity_is_rechecked_after_configuration(held_loop, changed):
    spec, root, image, limit, loop, mount = held_loop

    def write(path, **kwargs):
        path.write_text("0\n", encoding="utf-8")
        if changed == "mount":
            mount["mount_id"] += 1
        elif changed == "source":
            (loop / "backing_file").write_text("/different.img\n", encoding="utf-8")
        else:
            spec.image_path.rename(spec.image_path.with_suffix(".old"))
            spec.image_path.write_bytes(bytes(4096))

    with pytest.raises(BackingUnavailable):
        lr.protect_loop_reservation(spec, root, image, writer=write)


def test_qualification_rechecks_protection_instead_of_caching_success(held_loop):
    spec, root, image, limit, _loop, mount = held_loop
    lr.protect_loop_reservation(spec, root, image)
    lr.verify_loop_reservation(mount["device"])
    limit.write_text("4096\n", encoding="utf-8")
    with pytest.raises(BackingUnavailable):
        lr.verify_loop_reservation(mount["device"])
    assert limit.read_text(encoding="utf-8") == "4096\n"


def test_host_writer_inherits_mount_image_and_pool_ownership(held_loop, monkeypatch):
    from app.gateway.storage_init import Host

    _spec, root, image, _limit, _loop, _mount = held_loop
    host = Host()
    host.lock_fd = root
    calls = []

    def run(args, **kwargs):
        calls.append((args, kwargs))
        assert set(kwargs["pass_fds"]) == {root, image}
        assert all(os.fstat(fd) for fd in kwargs["pass_fds"])
        return SimpleNamespace(returncode=0, stdout="", stderr="")

    monkeypatch.setattr("app.gateway.storage_init.subprocess.run", run)
    host.run("true", pass_fds=(root, image))
    assert len(calls) == 1 and calls[0][0][:6] == ["nsenter", "-m", "-t", "1", "--", "true"]


def test_explicit_layout_repairs_held_image_mode_before_protecting_loop(held_loop, tmp_path, monkeypatch):
    from app.gateway import storage_init as si

    spec, _root, _image, limit, _loop, mount = held_loop
    tool = si.StorageInitializer(tmp_path / "operator", None)
    monkeypatch.setattr(si, "verify_image_extents", lambda *args: None)
    monkeypatch.setattr(si, "_ext4_identity", lambda fd: spec.filesystem_uuid)
    monkeypatch.setattr(si, "mounted_ext4_identity", lambda fd: spec.filesystem_uuid)
    monkeypatch.setattr(tool, "_mounted_root_facts", lambda *args, **kwargs: dict(mount))
    # Exercise real modes and the real guard; ownership is a syscall double.
    monkeypatch.setattr(tool, "_set_mode", lambda fd, uid, gid, mode: os.fchmod(fd, mode))
    writes = []

    def host_run(*args, **kwargs):
        assert args[:3] == ("sh", "-eu", "-c")
        root, image = kwargs["pass_fds"]
        assert os.fstat(root).st_ino == spec.mount_path.stat().st_ino
        assert os.fstat(image).st_ino == spec.image_path.stat().st_ino
        assert os.fstat(image).st_mode & 0o777 == 0o640
        writes.append(args)
        limit.write_text("0\n", encoding="ascii")

    monkeypatch.setattr(tool.host, "run", host_run)
    spec.image_path.chmod(0o666)
    tool.layout(spec)
    assert len(writes) == 1 and spec.image_path.stat().st_mode & 0o777 == 0o640
    assert (spec.mount_path / "data").is_dir() and (spec.mount_path / "control").is_dir()
