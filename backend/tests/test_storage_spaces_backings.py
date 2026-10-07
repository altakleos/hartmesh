"""Provider facts must establish hard bounds; directories never self-attest."""

from __future__ import annotations

import ctypes
import json
import os
import struct
import subprocess
import sys
import uuid
from pathlib import Path

import pytest

from deerflow.spaces.backings import BackingUnavailable, FixedVolumeSpec, PreparedVolumeCatalog, read_ext4_identity

pytestmark = pytest.mark.skipif(sys.platform != "linux", reason="The first qualified backing adapter is Linux-only")


def test_normal_directory_is_not_a_hard_bound_volume(tmp_path: Path):
    root = tmp_path / "volume"
    root.mkdir()
    (root / "data").mkdir()
    (root / "control").mkdir(mode=0o700)
    image = tmp_path / "image"
    image.write_bytes(b"not a filesystem")
    spec = FixedVolumeSpec(uuid.uuid4().hex, root, image, str(uuid.uuid4()), 128 << 20, 1024)
    with pytest.raises(BackingUnavailable):
        PreparedVolumeCatalog([spec], data_disk=tmp_path, reserve_bytes=1 << 20, reserve_inodes=10).verify(spec.slot_id)


@pytest.mark.parametrize("field,value", [("max_bytes", True), ("max_bytes", 0), ("max_inodes", True), ("max_inodes", 0), ("slot_id", "../other"), ("filesystem_uuid", "unknown")])
def test_provider_spec_is_strict_and_not_a_browser_path(field, value, tmp_path):
    values = {"slot_id": uuid.uuid4().hex, "mount_path": tmp_path / "volume", "image_path": tmp_path / "image", "filesystem_uuid": str(uuid.uuid4()), "max_bytes": 128 << 20, "max_inodes": 1024}
    values[field] = value
    with pytest.raises(ValueError):
        FixedVolumeSpec(**values)


def test_duplicate_or_overlapping_inventory_is_rejected_before_use(tmp_path):
    first = FixedVolumeSpec(uuid.uuid4().hex, tmp_path / "volume", tmp_path / "first.img", str(uuid.uuid4()), 128 << 20, 1024)
    second = FixedVolumeSpec(uuid.uuid4().hex, first.mount_path / "nested", tmp_path / "second.img", str(uuid.uuid4()), 128 << 20, 1024)
    with pytest.raises(ValueError):
        PreparedVolumeCatalog([first, second], data_disk=tmp_path, reserve_bytes=1024, reserve_inodes=10)
    with pytest.raises(ValueError):
        PreparedVolumeCatalog([first, first], data_disk=tmp_path, reserve_bytes=1024, reserve_inodes=10)


def test_image_cannot_be_inside_another_resources_visible_mount(tmp_path):
    first = FixedVolumeSpec(uuid.uuid4().hex, tmp_path / "first", tmp_path / "first.img", str(uuid.uuid4()), 128 << 20, 1024)
    second = FixedVolumeSpec(uuid.uuid4().hex, tmp_path / "second", first.mount_path / "data" / "second.img", str(uuid.uuid4()), 128 << 20, 1024)
    with pytest.raises(ValueError):
        PreparedVolumeCatalog([first, second], data_disk=tmp_path, reserve_bytes=1024, reserve_inodes=10)


def test_real_ext4_header_identity_is_read_without_mount_or_install(tmp_path):
    tool = Path("/usr/sbin/mkfs.ext4")
    if not tool.is_file():
        pytest.skip("Host has no preinstalled ext4 formatting tool; real-volume CI requires it")
    image = tmp_path / "fresh.img"
    fd = os.open(image, os.O_CREAT | os.O_EXCL | os.O_RDWR, 0o600)
    try:
        os.posix_fallocate(fd, 0, 32 << 20)
    finally:
        os.close(fd)
    identity = str(uuid.uuid4())
    subprocess.run([str(tool), "-q", "-F", "-U", identity, "-N", "256", "-m", "0", str(image)], check=True, capture_output=True)
    assert read_ext4_identity(image) == identity
    image.write_bytes(b"not ext4")
    with pytest.raises(BackingUnavailable):
        read_ext4_identity(image)


def test_inventory_refuses_unknown_fields_and_relative_paths(tmp_path):
    manifest = tmp_path / "inventory.json"
    manifest.write_text(json.dumps({"schema_version": 1, "data_disk": str(tmp_path), "reserve_bytes": 1024, "reserve_inodes": 10, "volumes": [], "browser_actor": "invented"}), encoding="utf-8")
    with pytest.raises(ValueError):
        PreparedVolumeCatalog.from_manifest(manifest)
    manifest.write_text(json.dumps({"schema_version": 1, "data_disk": "relative", "reserve_bytes": 1024, "reserve_inodes": 10, "volumes": []}), encoding="utf-8")
    with pytest.raises(ValueError):
        PreparedVolumeCatalog.from_manifest(manifest)


def test_sparse_image_and_platform_reserve_are_not_claimed_as_reserved_capacity(tmp_path):
    root = tmp_path / "volume"
    root.mkdir()
    image = tmp_path / "sparse.img"
    with image.open("wb") as output:
        output.truncate(128 << 20)
    spec = FixedVolumeSpec(uuid.uuid4().hex, root, image, str(uuid.uuid4()), 128 << 20, 1024)
    catalog = PreparedVolumeCatalog([spec], data_disk=tmp_path, reserve_bytes=1 << 62, reserve_inodes=10)
    with pytest.raises(BackingUnavailable):
        catalog.verify_reserve()
    with pytest.raises(BackingUnavailable):
        catalog.verify(spec.slot_id)


def test_blocks_beyond_eof_cannot_mask_a_hole_inside_the_image(tmp_path):
    from deerflow.spaces.backings import verify_image_extents

    path = tmp_path / "aggregate-trap"
    fd = os.open(path, os.O_CREAT | os.O_EXCL | os.O_RDWR, 0o600)
    try:
        os.ftruncate(fd, 65536)
        libc = ctypes.CDLL(None, use_errno=True)
        assert libc.fallocate(fd, 1, ctypes.c_longlong(1 << 20), ctypes.c_longlong(65536)) == 0
        assert os.fstat(fd).st_blocks * 512 >= 65536
        with pytest.raises(BackingUnavailable):
            verify_image_extents(fd, 65536)
    finally:
        os.close(fd)


def test_real_reserved_unwritten_extents_are_supported(tmp_path):
    from deerflow.spaces.backings import verify_image_extents

    fd = os.open(tmp_path / "allocated", os.O_CREAT | os.O_EXCL | os.O_RDWR, 0o600)
    try:
        os.posix_fallocate(fd, 0, 65536)
        verify_image_extents(fd, 65536)
    finally:
        os.close(fd)


@pytest.mark.parametrize(
    "facts",
    [
        {"offset": 4096, "sizelimit": 0, "autoclear": 1},
        {"offset": 0, "sizelimit": 4096, "autoclear": 1},
        {"offset": 0, "sizelimit": 0, "autoclear": 0},
    ],
)
def test_loop_range_must_bind_the_whole_provider_image(facts):
    from deerflow.spaces.backings import validate_loop_range

    with pytest.raises(BackingUnavailable):
        validate_loop_range(facts)


def test_shared_extent_is_refused_even_with_complete_logical_coverage(tmp_path, monkeypatch):
    import fcntl

    from deerflow.spaces.backings import verify_image_extents

    def shared_map(fd, command, buffer, mutate):
        struct.pack_into("=QQIIII", buffer, 0, 0, 65536, 1, 1, 128, 0)
        struct.pack_into("=QQQQQIIII", buffer, 32, 0, 4096, 65536, 0, 0, 0x2001, 0, 0, 0)
        return 0

    monkeypatch.setattr(fcntl, "ioctl", shared_map)
    fd = os.open(tmp_path / "image", os.O_CREAT | os.O_EXCL | os.O_RDWR, 0o600)
    try:
        with pytest.raises(BackingUnavailable, match="shared"):
            verify_image_extents(fd, 65536)
    finally:
        os.close(fd)
