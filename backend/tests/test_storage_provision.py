"""Whole-plan arithmetic and typed inventory compatibility, without mount claims."""

import json
from types import SimpleNamespace
from uuid import uuid4

import pytest

from deerflow.config.storage_spaces_config import StoragePlan
from deerflow.spaces.backings import FixedVolumeSpec, PreparedVolumeCatalog
from deerflow.spaces.provision import INODE_BYTES, MAX_SPACES, storage_fit


def plan(**overrides):
    return StoragePlan(**{**dict(users=10, user_gib=1, ai_assistants_per_user=1, ai_assistant_gib=1, ai_employees=10, ai_employee_gib=1, company_gib=1), **overrides})


@pytest.mark.parametrize("disk_gib,headroom_gib", [(30, 4), (50, 5), (100, 10)])
def test_fit_preserves_larger_of_four_gib_and_ten_percent(tmp_path, monkeypatch, disk_gib, headroom_gib):
    def capacity(free):
        return SimpleNamespace(f_blocks=disk_gib << 30, f_frsize=1, f_bavail=free, f_favail=1024 + 31 * 4)

    monkeypatch.setattr("deerflow.spaces.provision.os.statvfs", lambda p: capacity((31 + headroom_gib) << 30))
    fit = storage_fit(tmp_path, plan(), None)
    assert fit.required == {"user": 10, "assistant": 10, "employee": 10, "company": 1}
    assert fit.missing_bytes == 31 << 30 and fit.headroom_bytes == headroom_gib << 30
    assert fit.fits
    monkeypatch.setattr("deerflow.spaces.provision.os.statvfs", lambda p: capacity(((31 + headroom_gib) << 30) - 1))
    assert not storage_fit(tmp_path, plan(), None).fits


def test_fit_preserves_four_host_inodes_per_missing_space(tmp_path, monkeypatch):
    monkeypatch.setattr("deerflow.spaces.provision.os.statvfs", lambda p: SimpleNamespace(f_blocks=50 << 30, f_frsize=1, f_bavail=48 << 30, f_favail=1024 + 31 * 4 - 1))
    assert not storage_fit(tmp_path, plan(), None).fits


def test_existing_slots_and_surplus_remove_only_their_kind_from_missing(tmp_path, monkeypatch):
    volumes = [FixedVolumeSpec(uuid4().hex, tmp_path / f"{i}.volume", tmp_path / f"{i}.img", str(uuid4()), 1 << 30, (1 << 30) // INODE_BYTES, kind="employee") for i in range(15)]
    inventory = PreparedVolumeCatalog(volumes, data_disk=tmp_path, reserve_bytes=1 << 30, reserve_inodes=1024)
    monkeypatch.setattr("deerflow.spaces.provision.os.statvfs", lambda p: SimpleNamespace(f_blocks=50 << 30, f_frsize=1, f_bavail=26 << 30, f_favail=2000))
    fit = storage_fit(tmp_path, plan(), inventory)
    assert fit.present["employee"] == 15 and fit.missing["employee"] == 0
    assert fit.missing_bytes == 21 << 30 and fit.fits


def test_fit_propagates_unreadable_pool(tmp_path, monkeypatch):
    def denied(path):
        raise OSError("unreadable disk")

    monkeypatch.setattr("deerflow.spaces.provision.os.statvfs", denied)
    with pytest.raises(OSError, match="unreadable disk"):
        storage_fit(tmp_path, plan(), None)


def test_max_spaces_and_inode_ratio():
    assert MAX_SPACES == 256 and INODE_BYTES == 8192


@pytest.mark.parametrize("version", [1, 2])
def test_catalogue_loads_legacy_untyped_and_new_typed_entries(tmp_path, version):
    slot = uuid4().hex
    entry = dict(slot_id=slot, mount_path=str(tmp_path / f"{slot}.volume"), image_path=str(tmp_path / f"{slot}.img"), filesystem_uuid=str(uuid4()), max_bytes=1 << 30, max_inodes=131072)
    if version == 2:
        entry["kind"] = "user"
    path = tmp_path / "inventory.json"
    path.write_text(json.dumps(dict(schema_version=version, data_disk=str(tmp_path), reserve_bytes=1 << 30, reserve_inodes=1024, volumes=[entry])), encoding="utf-8")
    path.chmod(0o440)
    assert PreparedVolumeCatalog.from_manifest(path).volumes[slot].kind == ("user" if version == 2 else None)


@pytest.mark.parametrize("kind", [None, "unknown", 1, True])
def test_v2_catalogue_requires_known_kind(tmp_path, kind):
    slot = uuid4().hex
    entry = dict(slot_id=slot, mount_path=str(tmp_path / f"{slot}.volume"), image_path=str(tmp_path / f"{slot}.img"), filesystem_uuid=str(uuid4()), max_bytes=1 << 30, max_inodes=131072, kind=kind)
    path = tmp_path / "inventory.json"
    path.write_text(json.dumps(dict(schema_version=2, data_disk=str(tmp_path), reserve_bytes=1 << 30, reserve_inodes=1024, volumes=[entry])), encoding="utf-8")
    path.chmod(0o440)
    with pytest.raises(ValueError):
        PreparedVolumeCatalog.from_manifest(path)
