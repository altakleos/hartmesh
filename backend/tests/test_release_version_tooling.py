"""Behavioral contracts for the release version helper scripts."""

from __future__ import annotations

import os
import shutil
import subprocess
import tomllib
from pathlib import Path

import pytest

_REPO_ROOT = Path(__file__).resolve().parents[2]


@pytest.fixture
def release_tree(tmp_path: Path) -> Path:
    """Create the smallest release tree accepted by both helper scripts."""
    backend = tmp_path / "backend"
    frontend = tmp_path / "frontend"
    chart = tmp_path / "deploy/helm/deer-flow"
    scripts = tmp_path / "scripts"
    for directory in (backend, frontend, chart, scripts):
        directory.mkdir(parents=True)

    (backend / "pyproject.toml").write_text(
        """[project]
name = "deer-flow"
version = "2.1.0"
requires-python = ">=3.12"
dependencies = []
""",
        encoding="utf-8",
    )
    (backend / "uv.lock").write_text(
        """version = 1
revision = 3
requires-python = ">=3.12"

[[package]]
name = "deer-flow"
version = "2.1.0"
source = { virtual = "." }
""",
        encoding="utf-8",
    )
    (frontend / "package.json").write_text(
        '{\n  "name": "deer-flow-frontend",\n  "version": "2.1.0"\n}\n',
        encoding="utf-8",
    )
    (chart / "Chart.yaml").write_text(
        'apiVersion: v2\nname: deer-flow\nversion: 2.1.0\nappVersion: "2.1.0"\n',
        encoding="utf-8",
    )
    for name in ("bump_version.sh", "verify_versions.sh"):
        shutil.copy2(_REPO_ROOT / "scripts" / name, scripts / name)
    return tmp_path


def _run_helper(release_tree: Path, name: str, version: str, *, path: str | None = None) -> subprocess.CompletedProcess[str]:
    env = os.environ.copy()
    if path is not None:
        env["PATH"] = path
    return subprocess.run(
        ["bash", str(release_tree / "scripts" / name), version],
        cwd=release_tree,
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )


def _locked_root_version(release_tree: Path) -> str:
    lock = tomllib.loads((release_tree / "backend/uv.lock").read_text(encoding="utf-8"))
    return next(package["version"] for package in lock["package"] if package["name"] == "deer-flow")


def test_verify_versions_rejects_only_a_stale_root_lock_version(release_tree: Path) -> None:
    lock_path = release_tree / "backend/uv.lock"
    lock_path.write_text(lock_path.read_text(encoding="utf-8").replace('version = "2.1.0"', 'version = "2.0.0"'), encoding="utf-8")

    stale = _run_helper(release_tree, "verify_versions.sh", "2.1.0")

    assert stale.returncode == 1
    assert "backend/uv.lock is '2.0.0' but expected '2.1.0'." in stale.stderr

    lock_path.write_text(lock_path.read_text(encoding="utf-8").replace('version = "2.0.0"', 'version = "2.1.0"'), encoding="utf-8")
    aligned = _run_helper(release_tree, "verify_versions.sh", "2.1.0")

    assert aligned.returncode == 0
    assert "OK — all version sources agree on 2.1.0." in aligned.stdout


def test_bump_version_accepts_hartmesh_build_metadata_and_updates_lock(release_tree: Path) -> None:
    uv = shutil.which("uv")
    if uv is None:
        pytest.skip("uv is required for the live lock update contract")

    bumped = _run_helper(release_tree, "bump_version.sh", "2.1.0+hartmesh.0")

    assert bumped.returncode == 0, bumped.stderr
    assert _locked_root_version(release_tree) == "2.1.0+hartmesh.0"
    lock_check = subprocess.run([uv, "lock", "--check"], cwd=release_tree / "backend", capture_output=True, text=True, check=False)
    assert lock_check.returncode == 0, lock_check.stderr


def test_bump_version_refuses_to_edit_sources_without_uv(release_tree: Path) -> None:
    system_path = "/usr/bin:/bin"
    if shutil.which("uv", path=system_path) is not None:
        pytest.skip(f"test PATH unexpectedly contains uv: {system_path}")

    refused = _run_helper(release_tree, "bump_version.sh", "2.1.0+hartmesh.0", path=system_path)

    assert refused.returncode == 1
    assert "error: uv is required to update backend/uv.lock" in refused.stderr
    assert 'version = "2.1.0"' in (release_tree / "backend/pyproject.toml").read_text(encoding="utf-8")
