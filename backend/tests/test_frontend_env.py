"""The UI migration retains local settings without overwriting either app."""

from __future__ import annotations

import importlib.util
import os
from pathlib import Path

import pytest


def _helper():
    path = Path(__file__).resolve().parents[2] / "scripts/frontend_env.py"
    spec = importlib.util.spec_from_file_location("hartmesh_frontend_env", path)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module.ensure_frontend_env


@pytest.mark.parametrize("legacy", [False, True])
def test_bootstrap_prefers_legacy_settings_and_never_prints_values(tmp_path: Path, capsys, legacy: bool):
    project = tmp_path / "frontend-hm"
    project.mkdir()
    (project / ".env.example").write_bytes(b"VALUE=template\n")
    old = tmp_path / "frontend/.env"
    old.parent.mkdir()
    if legacy:
        old.write_bytes(b"VALUE=private\r\n")
    _helper()(tmp_path)
    expected = b"VALUE=private\r\n" if legacy else b"VALUE=template\n"
    assert (project / ".env").read_bytes() == expected
    if legacy:
        assert old.read_bytes() == expected
    output = capsys.readouterr()
    assert "VALUE=" not in output.out + output.err
    if os.name != "nt":
        assert (project / ".env").stat().st_mode & 0o777 == 0o600


def test_existing_destination_is_never_overwritten(tmp_path: Path):
    project = tmp_path / "frontend-hm"
    project.mkdir()
    target = project / ".env"
    target.write_bytes(b"existing")
    _helper()(tmp_path)
    assert target.read_bytes() == b"existing"


def test_missing_template_fails_without_creating_a_file(tmp_path: Path):
    (tmp_path / "frontend-hm").mkdir()
    with pytest.raises(FileNotFoundError):
        _helper()(tmp_path)
    assert not (tmp_path / "frontend-hm/.env").exists()


def test_broken_destination_symlink_is_not_followed(tmp_path: Path):
    if os.name == "nt":
        pytest.skip("symlink creation needs privileges on Windows")
    project = tmp_path / "frontend-hm"
    project.mkdir()
    (project / ".env.example").write_text("template", encoding="utf-8")
    other = tmp_path / "unrelated"
    (project / ".env").symlink_to(other)
    with pytest.raises(OSError):
        _helper()(tmp_path)
    assert not other.exists()
