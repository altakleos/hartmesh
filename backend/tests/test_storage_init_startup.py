"""Startup supervision stays lightweight and reports its own memory budget."""

import json
import os
import subprocess
import sys
from pathlib import Path

import pytest


def test_supervisor_import_needs_no_site_packages():
    subprocess.run(
        [sys.executable, "-S", "-c", "import sys; import app.storage_init; assert not any(n.startswith(('app.gateway', 'deerflow', 'pydantic')) for n in sys.modules)"],
        cwd=Path(__file__).resolve().parents[1],
        check=True,
        capture_output=True,
        text=True,
    )


@pytest.mark.parametrize("code", [0, 1, 2])
def test_startup_preserves_child_document_and_process_exit(monkeypatch, capsys, code):
    from app import storage_init as start

    doc = {"command": "prepare", "returncode": code, "note": "actual child verdict"}
    monkeypatch.setattr(start, "at_start", lambda: doc)
    assert start.main(["prepare", "--at-start"]) == 0
    assert json.loads(capsys.readouterr().out) == doc


def cgroup_fixture(tmp_path, *, path="/tenant/worker", mount_root="/", peak="73400320", limit="134217728"):
    if os.name != "posix":
        pytest.skip("Linux cgroup mount paths")
    proc = tmp_path / "proc" / "self"
    proc.mkdir(parents=True)
    mount = tmp_path / "cgroup"
    mount.mkdir()
    (proc / "cgroup").write_text(f"0::{path}\n", encoding="utf-8")
    (proc / "mountinfo").write_text(f"42 23 0:28 {mount_root} {mount} rw - cgroup2 cgroup rw\n", encoding="utf-8")
    relative = path.removeprefix(mount_root).lstrip("/")
    own = mount / relative
    own.mkdir(parents=True, exist_ok=True)
    (own / "memory.peak").write_text(peak, encoding="utf-8")
    (own / "memory.max").write_text(limit, encoding="utf-8")
    # Reporting root counters instead of this process's group would look healthy.
    if own != mount:
        (mount / "memory.peak").write_text("1", encoding="utf-8")
        (mount / "memory.max").write_text("max", encoding="utf-8")
    return tmp_path / "proc", own


@pytest.mark.parametrize("path,mount_root", [("/tenant/worker", "/"), ("/tenant/worker", "/tenant"), ("/", "/")])
def test_memory_reports_own_cgroup_in_both_namespace_modes(tmp_path, path, mount_root):
    from app import storage_init as start

    proc, _ = cgroup_fixture(tmp_path, path=path, mount_root=mount_root)
    assert start.memory_status(proc_root=proc) == "memory peak 70.0 MiB of 128.0 MiB (peak_bytes=73400320 limit_bytes=134217728)"


@pytest.mark.parametrize("peak,limit", [("73400320", "max"), ("garbage", "128"), ("-1", "128"), ("10", "bad")])
def test_memory_unlimited_or_invalid_is_explicit_and_nonfatal(tmp_path, peak, limit):
    from app import storage_init as start

    proc, _ = cgroup_fixture(tmp_path, peak=peak, limit=limit)
    result = start.memory_status(proc_root=proc)
    assert ("of unlimited" in result) if limit == "max" else ("memory unavailable" in result)


def test_missing_memory_counter_is_nonfatal(tmp_path):
    from app import storage_init as start

    proc, own = cgroup_fixture(tmp_path)
    (own / "memory.peak").unlink()
    assert "memory unavailable" in start.memory_status(proc_root=proc)


@pytest.mark.parametrize("code", [0, 1, 2])
def test_completed_verdict_logs_memory_after_group_cleanup(monkeypatch, capsys, code):
    from app import storage_init as start

    events = []

    class Child:
        def communicate(self, **kwargs):
            assert kwargs == {"timeout": 600}
            return json.dumps({"command": "prepare", "returncode": code}), None

    monkeypatch.setattr(start.subprocess, "Popen", lambda *args, **kwargs: Child())
    monkeypatch.setattr(start, "end_owned_group", lambda child: events.append("contained") or True)
    monkeypatch.setattr(start, "memory_status", lambda: events.append("memory") or "memory unavailable (PermissionError)")
    assert start.main(["prepare", "--at-start"]) == 0
    output = capsys.readouterr()
    assert json.loads(output.out) == {"command": "prepare", "returncode": code}
    assert events == ["contained", "contained", "memory"]
    assert output.err.strip() == "storage-init: memory unavailable (PermissionError)"


def test_unconfirmed_child_never_imports_inventory_into_supervisor(monkeypatch, capsys):
    from app import storage_init as start

    class Child:
        def communicate(self, **kwargs):
            raise subprocess.TimeoutExpired("prepare", 600)

    monkeypatch.setattr(start.subprocess, "Popen", lambda *args, **kwargs: Child())
    monkeypatch.setattr(start, "end_owned_group", lambda child: False)
    monkeypatch.setattr(start, "_persisted_state", lambda *args: pytest.fail("unconfirmed child must retain unknown state"))
    doc = start.at_start()
    assert doc["returncode"] == 2 and doc["verdict"] == "incomplete"
    assert doc["prepared"] is None and doc["prepared_now"] is None and doc["spaces"] == []
    assert "containment could not be confirmed" in doc["note"]
    assert "storage-init: memory" in capsys.readouterr().err


def test_child_start_failure_still_reports_memory(monkeypatch, capsys):
    from app import storage_init as start

    def unavailable(*args, **kwargs):
        raise OSError("child unavailable")

    monkeypatch.setattr(start.subprocess, "Popen", unavailable)
    assert start.main(["prepare", "--at-start"]) == 0
    output = capsys.readouterr()
    assert json.loads(output.out)["returncode"] == 1
    assert "child unavailable" in json.loads(output.out)["error"]
    assert "storage-init: memory" in output.err
