from __future__ import annotations

import json
import os
import runpy
import shutil
import subprocess
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
PNPM_SCRIPT = REPO_ROOT / "scripts" / "pnpm.py"
FRONTEND_DIR = REPO_ROOT / "frontend"
PNPM_MODULE = runpy.run_path(str(PNPM_SCRIPT))
HARTMESH_DIR = REPO_ROOT / "frontend-hm"


@pytest.mark.parametrize("executable", ["pnpm", "corepack"])
def test_runner_selects_hartmesh_and_preserves_pnpm_arguments(tmp_path: Path, executable: str):
    _write_fake_command(tmp_path, executable, executable)
    result = _run_pnpm(tmp_path, "--project", "frontend-hm", "--", "run", "dev", "--port", "3010", cwd=REPO_ROOT)
    prefix = "pnpm " if executable == "corepack" else ""
    assert result.returncode == 0
    assert result.stdout.strip() == f"{executable}|{HARTMESH_DIR}|{prefix}run dev --port 3010"


@pytest.mark.parametrize("project", ["../frontend", "missing", "", "--version"])
def test_runner_rejects_invalid_project_without_running_pnpm(tmp_path: Path, project: str):
    _write_fake_command(tmp_path, "pnpm", "must-not-run")
    result = _run_pnpm(tmp_path, "--project", project, "--", "--version")
    assert result.returncode == 2
    assert "project" in result.stderr.lower()
    assert result.stdout == ""


def test_runner_rejects_missing_selected_project(tmp_path: Path):
    import shutil

    scripts = tmp_path / "scripts"
    scripts.mkdir()
    shutil.copy2(PNPM_SCRIPT, scripts / "pnpm.py")
    result = subprocess.run([sys.executable, str(scripts / "pnpm.py"), "--project", "frontend-hm", "--", "--version"], capture_output=True, text=True)
    assert result.returncode == 2
    assert "frontend-hm/package.json" in result.stderr


def test_relative_path_executable_is_resolved_before_switching_project(tmp_path: Path):
    binary = tmp_path / "bin"
    binary.mkdir()
    _write_fake_command(binary, "pnpm", "relative")
    result = _run_pnpm(Path("bin"), "--project", "frontend-hm", "--", "--version", cwd=tmp_path)
    assert result.returncode == 0, result.stderr
    assert result.stdout.strip() == f"relative|{HARTMESH_DIR}|--version"


def _write_fake_command(bin_dir: Path, name: str, label: str, exit_code: int = 0) -> Path:
    if os.name == "nt":
        path = bin_dir / f"{name}.cmd"
        path.write_text(
            f"@echo off\r\necho {label}^|%CD%^|%*\r\nexit /b {exit_code}\r\n",
            encoding="utf-8",
        )
    else:
        path = bin_dir / name
        path.write_text(
            f"#!/bin/sh\nprintf '%s|%s|%s\\n' '{label}' \"$PWD\" \"$*\"\nexit {exit_code}\n",
            encoding="utf-8",
        )
        path.chmod(0o755)
    return path


def _run_pnpm(
    path: Path,
    *args: str,
    cwd: Path = FRONTEND_DIR,
) -> subprocess.CompletedProcess[str]:
    env = os.environ.copy()
    env["PATH"] = str(path)
    return subprocess.run(
        [sys.executable, str(PNPM_SCRIPT), *args],
        cwd=cwd,
        env=env,
        capture_output=True,
        text=True,
        check=False,
        shell=False,
    )


def test_runner_prefers_direct_pnpm_and_forwards_arguments(tmp_path: Path):
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    _write_fake_command(bin_dir, "pnpm", "direct")
    _write_fake_command(bin_dir, "corepack", "corepack")

    result = _run_pnpm(bin_dir, "run", "dev", "--host", "127.0.0.1")

    assert result.returncode == 0
    assert result.stdout.strip() == f"direct|{FRONTEND_DIR}|run dev --host 127.0.0.1"
    assert "via Corepack" not in result.stderr


@pytest.mark.parametrize("direct_version", ["11.25.0", "10.26.2-rc.1"])
def test_runner_uses_corepack_when_direct_pnpm_conflicts_with_package_manager(tmp_path: Path, direct_version: str):
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    direct = _write_fake_command(bin_dir, "pnpm", "direct")
    if os.name == "nt":
        direct.write_text(
            f'@echo off\r\nif "%1"=="--version" (echo {direct_version} & exit /b 0)\r\necho direct^|%CD%^|%*\r\n',
            encoding="utf-8",
        )
    else:
        direct.write_text(
            f'#!/bin/sh\nif [ "$1" = "--version" ]; then echo {direct_version}; exit 0; fi\nprintf "direct|%s|%s\\n" "$PWD" "$*"\n',
            encoding="utf-8",
        )
    _write_fake_command(bin_dir, "corepack", "corepack")

    result = _run_pnpm(bin_dir, "install", "--frozen-lockfile")

    assert result.returncode == 0
    assert result.stdout.strip() == f"corepack|{FRONTEND_DIR}|pnpm install --frozen-lockfile"
    assert "via Corepack" in result.stderr


def test_runner_rejects_mismatched_pnpm_without_corepack(tmp_path: Path):
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    direct = _write_fake_command(bin_dir, "pnpm", "direct")
    if os.name == "nt":
        direct.write_text("@echo off\r\necho 11.25.0\r\n", encoding="utf-8")
    else:
        direct.write_text("#!/bin/sh\necho 11.25.0\n", encoding="utf-8")

    result = _run_pnpm(bin_dir, "install")

    assert result.returncode == 127
    assert "pnpm 11.25.0 is on PATH" in result.stderr
    assert "pins 10.26.2" in result.stderr


@pytest.mark.parametrize("package_content", [None, "{", "{}"])
def test_runner_reports_unreadable_or_unpinned_package_manifest(tmp_path: Path, monkeypatch, capsys, package_content: str | None):
    package_json = tmp_path / "package.json"
    if package_content is not None:
        package_json.write_text(package_content, encoding="utf-8")
    run_pnpm = PNPM_MODULE["run_pnpm"]
    monkeypatch.setitem(run_pnpm.__globals__, "PACKAGE_JSON_PATH", package_json)
    monkeypatch.setitem(run_pnpm.__globals__, "find_pnpm_command", lambda: ["pnpm"])

    assert run_pnpm(["install"]) != 0
    assert str(package_json) in capsys.readouterr().err


def test_runner_warns_when_direct_pnpm_version_is_unparseable(tmp_path: Path):
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    _write_fake_command(bin_dir, "pnpm", "direct")

    result = _run_pnpm(bin_dir, "install")

    assert result.returncode == 0
    assert "could not determine pnpm version" in result.stderr.lower()


def test_version_probe_does_not_read_stdin(monkeypatch):
    run_pnpm = PNPM_MODULE["run_pnpm"]
    calls = []

    def fake_run(*args, **kwargs):
        calls.append(kwargs)
        return SimpleNamespace(returncode=0, stdout="10.26.2\n")

    monkeypatch.setitem(run_pnpm.__globals__, "find_pnpm_command", lambda: ["pnpm"])
    monkeypatch.setitem(run_pnpm.__globals__, "subprocess", SimpleNamespace(run=fake_run, DEVNULL=-3))

    assert run_pnpm(["install"]) == 0
    assert calls[0]["stdin"] == -3


def test_runner_prefers_cmd_shim_on_windows(monkeypatch):
    paths = {
        "pnpm": r"C:\tools\pnpm.exe",
        "pnpm.cmd": r"C:\tools\pnpm.cmd",
    }
    find_pnpm_command = PNPM_MODULE["find_pnpm_command"]
    monkeypatch.setitem(find_pnpm_command.__globals__, "os", SimpleNamespace(name="nt"))
    monkeypatch.setitem(find_pnpm_command.__globals__, "shutil", SimpleNamespace(which=paths.get))

    assert find_pnpm_command() == [paths["pnpm.cmd"]]


def test_runner_prefers_corepack_cmd_shim_on_windows(monkeypatch):
    paths = {
        "corepack": r"C:\tools\corepack.exe",
        "corepack.cmd": r"C:\tools\corepack.cmd",
    }
    find_pnpm_command = PNPM_MODULE["find_pnpm_command"]
    monkeypatch.setitem(find_pnpm_command.__globals__, "os", SimpleNamespace(name="nt"))
    monkeypatch.setitem(find_pnpm_command.__globals__, "shutil", SimpleNamespace(which=paths.get))

    assert find_pnpm_command() == [paths["corepack.cmd"], "pnpm"]


def test_runner_uses_corepack_pnpm_from_frontend_directory(tmp_path: Path):
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    _write_fake_command(bin_dir, "corepack", "corepack")

    result = _run_pnpm(bin_dir, "--version")

    assert result.returncode == 0
    assert result.stdout.strip() == f"corepack|{FRONTEND_DIR}|pnpm --version"
    assert result.stderr.strip() == "Using pnpm via Corepack."

    package_json = json.loads((FRONTEND_DIR / "package.json").read_text(encoding="utf-8"))
    assert package_json["packageManager"] == "pnpm@10.26.2"


def test_runner_uses_frontend_directory_when_called_from_repo_root(tmp_path: Path):
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    _write_fake_command(bin_dir, "corepack", "corepack")

    result = _run_pnpm(bin_dir, "--version", cwd=REPO_ROOT)

    assert result.returncode == 0
    assert result.stdout.strip() == f"corepack|{FRONTEND_DIR}|pnpm --version"


def test_runner_reports_actionable_error_when_pnpm_and_corepack_are_missing(tmp_path: Path):
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()

    result = _run_pnpm(bin_dir, "--version")

    assert result.returncode == 127
    assert "Neither pnpm nor Corepack is available" in result.stderr
    assert "ensure 'corepack' is on PATH" in result.stderr


def test_runner_propagates_selected_pnpm_failure(tmp_path: Path):
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    _write_fake_command(bin_dir, "pnpm", "broken-direct", exit_code=42)
    _write_fake_command(bin_dir, "corepack", "unused-corepack")

    result = _run_pnpm(bin_dir, "install", "--frozen-lockfile")

    assert result.returncode == 42
    assert result.stdout.strip() == f"broken-direct|{FRONTEND_DIR}|install --frozen-lockfile"
    assert "pnpm command failed with exit status 42" in result.stderr


def test_official_entrypoints_route_pnpm_through_shared_runner():
    root_makefile = (REPO_ROOT / "Makefile").read_text(encoding="utf-8")
    frontend_makefile = (HARTMESH_DIR / "Makefile").read_text(encoding="utf-8")
    serve_script = (REPO_ROOT / "scripts" / "serve.sh").read_text(encoding="utf-8")
    doctor_script = (REPO_ROOT / "scripts" / "doctor.py").read_text(encoding="utf-8")
    support_bundle_script = (REPO_ROOT / "scripts" / "support_bundle.py").read_text(encoding="utf-8")

    assert "cd frontend-hm && $(FRONTEND_PNPM) install" in root_makefile
    assert "PNPM = $(PYTHON) ../scripts/pnpm.py" in frontend_makefile
    assert '"$DEERFLOW_PNPM_PYTHON" "$DEERFLOW_PNPM_RUNNER" --project frontend-hm -- install --silent' in serve_script
    assert 'DEERFLOW_PNPM_RUNNER="$REPO_ROOT/scripts/pnpm.py"' in serve_script
    assert 'FRONTEND_CMD=\'env PORT=3000 "$DEERFLOW_PNPM_PYTHON" "$DEERFLOW_PNPM_RUNNER" --project frontend-hm -- run dev --hostname 127.0.0.1\'' in serve_script
    assert '"\\$DEERFLOW_PNPM_RUNNER\\" --project frontend-hm -- run preview --hostname 127.0.0.1"' in serve_script
    assert 'Path(__file__).resolve().with_name("pnpm.py")' in doctor_script
    assert 'project_root / "scripts" / "pnpm.py"' in support_bundle_script


@pytest.mark.skipif(shutil.which("make") is None, reason="GNU make is not on PATH (not bundled with Git Bash on Windows)")
def test_make_install_dry_run_does_not_invoke_bare_pnpm():
    result = subprocess.run(
        ["make", "-n", "install"],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        check=False,
        shell=False,
    )

    assert result.returncode == 0
    assert "cd frontend-hm && " in result.stdout
    assert "../scripts/pnpm.py --project frontend-hm -- install" in result.stdout
    assert "cd frontend-hm && pnpm install" not in result.stdout


def test_pin_comes_from_selected_project(tmp_path: Path, monkeypatch):
    runner = runpy.run_path(str(PNPM_SCRIPT))["run_pnpm"]
    root = tmp_path / "checkout"
    product = root / "frontend-hm"
    reference = root / "frontend"
    product.mkdir(parents=True)
    reference.mkdir()
    (product / "package.json").write_text('{"packageManager":"pnpm@10.26.2"}', encoding="utf-8")
    (reference / "package.json").write_text('{"packageManager":"pnpm@99.0.0"}', encoding="utf-8")
    calls = []

    def fake_run(command, **kwargs):
        calls.append((command, kwargs))
        return SimpleNamespace(returncode=0, stdout="10.26.2\n")

    monkeypatch.setitem(runner.__globals__, "PROJECT_ROOT", root)
    monkeypatch.setitem(runner.__globals__, "FRONTEND_DIR", reference)
    monkeypatch.setitem(runner.__globals__, "PACKAGE_JSON_PATH", reference / "package.json")
    monkeypatch.setitem(runner.__globals__, "find_pnpm_command", lambda: ["/tools/pnpm"])
    monkeypatch.setitem(runner.__globals__, "find_corepack_command", lambda: None)
    monkeypatch.setitem(runner.__globals__, "subprocess", SimpleNamespace(run=fake_run, DEVNULL=-3))
    assert runner(["--project", "frontend-hm", "--", "check"]) == 0
    assert calls[0][1]["cwd"] == product
    assert calls[-1][0] == ["/tools/pnpm", "check"]
    assert calls[-1][1]["cwd"] == product
