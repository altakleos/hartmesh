"""The provider helper remains explicit, offline and separate from ordinary APIs."""

from __future__ import annotations

import importlib.util
import os
import subprocess
import sys
from pathlib import Path

import pytest

SCRIPT = Path(__file__).resolve().parents[2] / "scripts" / "storage_spaces_volume.py"


def _module():
    spec = importlib.util.spec_from_file_location("storage_volume_operator_test", SCRIPT)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_operator_help_has_no_dependency_install_or_runtime_boot():
    result = subprocess.run([sys.executable, str(SCRIPT), "--help"], check=False, capture_output=True, text=True)
    assert result.returncode == 0, result.stderr
    assert "data-disk" in result.stdout and "reserve" in result.stdout


def test_prepare_requires_mount_authority_before_creating_artifacts(tmp_path, monkeypatch):
    module = _module()
    monkeypatch.setattr(module.os, "geteuid", lambda: 1002)
    with pytest.raises(PermissionError):
        module.prepare_volumes(tmp_path, count=1, size_bytes=64 << 20, inodes=256, reserve_bytes=16 << 20, reserve_inodes=64, data_uid=1000)
    assert not list(tmp_path.iterdir())


def test_provider_failure_does_not_erase_preexisting_data(tmp_path, monkeypatch):
    module = _module()
    existing = tmp_path / "existing"
    existing.write_bytes(b"preserve existing provider data")
    monkeypatch.setattr(module.os, "geteuid", lambda: 0)
    with pytest.raises((ValueError, PermissionError, OSError)):
        module.prepare_volumes(tmp_path, count=1, size_bytes=64 << 20, inodes=256, reserve_bytes=1 << 62, reserve_inodes=64, data_uid=1000)
    assert existing.read_bytes() == b"preserve existing provider data"


def test_successful_publication_then_exception_preserves_published_backings(tmp_path, monkeypatch):
    # Preparation syscall doubles isolate publication ownership; this test
    # makes no mounted-filesystem qualification claim.
    module = _module()
    original_stat, original_link = Path.stat, os.link

    def provider_stat(path, *args, **kwargs):
        value = original_stat(path, *args, **kwargs)
        if path == tmp_path:
            fields = list(value)
            fields[4] = 0
            return os.stat_result(fields)
        return value

    def link_then_interrupt(source, destination, *args, **kwargs):
        original_link(source, destination, *args, **kwargs)
        raise KeyboardInterrupt("fixture interruption after successful link")

    monkeypatch.setattr(Path, "stat", provider_stat)
    monkeypatch.setattr(module.os, "geteuid", lambda: 0)
    monkeypatch.setattr(module.os, "chown", lambda *args, **kwargs: None)
    monkeypatch.setattr(module.shutil, "which", lambda name: "/sbin/" + name)
    monkeypatch.setattr(module.subprocess, "run", lambda *args, **kwargs: subprocess.CompletedProcess(args[0], 0))
    monkeypatch.setattr(module.PreparedVolumeCatalog, "verify", lambda *args, **kwargs: None)
    monkeypatch.setattr(module.os, "link", link_then_interrupt)
    with pytest.raises(KeyboardInterrupt):
        module.prepare_volumes(tmp_path, count=1, size_bytes=32 << 20, inodes=256, reserve_bytes=16 << 20, reserve_inodes=64, data_uid=1000)
    assert (tmp_path / "inventory.v1.json").is_file()
    assert len(list(tmp_path.glob("*.img"))) == 1
    assert len(list(tmp_path.glob("*.volume/data"))) == 1
