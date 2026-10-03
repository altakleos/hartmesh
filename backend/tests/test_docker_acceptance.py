"""Acceptance builds use current tracked source, with isolated disposable runtime."""

from __future__ import annotations

import importlib.util
import json
import os
import subprocess
import sys
from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[2]


def load(path: Path):
    spec = importlib.util.spec_from_file_location(path.stem, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.mark.skipif(os.name == "nt", reason="POSIX Docker acceptance runner")
def test_build_snapshot_reads_only_tracked_current_bytes(tmp_path: Path) -> None:
    runner = load(ROOT / "scripts/docker_acceptance.py")
    source = tmp_path / "source"
    source.mkdir()
    subprocess.run(["git", "init", "-q", str(source)], check=True)
    tracked = source / "source.py"
    tracked.write_text("old", encoding="utf-8")
    tracked.chmod(0o755)
    (source / "linked.py").symlink_to("source.py")
    subprocess.run(["git", "-C", str(source), "add", "source.py", "linked.py"], check=True)
    tracked.write_text("current working tree", encoding="utf-8")
    (source / "panel").mkdir()
    (source / "panel/private.txt").write_text("private", encoding="utf-8")
    (source / "config.yaml").write_text("operator secret", encoding="utf-8")
    target = tmp_path / "context"
    fingerprint = runner.snapshot_tracked(source, target)
    assert len(fingerprint) == 64
    assert sorted(path.name for path in target.iterdir()) == ["linked.py", "source.py"]
    assert (target / "source.py").read_text(encoding="utf-8") == "current working tree"
    assert (target / "source.py").stat().st_mode & 0o111
    assert (target / "linked.py").is_symlink()


def test_acceptance_network_has_one_loopback_ingress_and_no_host_runtime_access() -> None:
    compose = yaml.safe_load((ROOT / "docker/acceptance/compose.yaml").read_text(encoding="utf-8"))
    assert compose["networks"]["acceptance"]["internal"] is True
    published = [(name, service["ports"]) for name, service in compose["services"].items() if "ports" in service]
    assert published == [("nginx", ["127.0.0.1::2026"])]
    for name, service in compose["services"].items():
        assert "container_name" not in service
        assert "env_file" not in service
        assert "privileged" not in service
        assert service["networks"] == (["acceptance", "ingress"] if name == "nginx" else ["acceptance"])
        assert all("docker.sock" not in mount and "~" not in mount for mount in service.get("volumes", []))
    assert compose["services"]["gateway"]["environment"]["DEER_FLOW_AUTH_DISABLED"] == "0"
    assert compose["services"]["frontend"]["environment"]["DEER_FLOW_AUTH_DISABLED"] == "0"


def test_synthetic_provider_requires_actual_upload_tool_result() -> None:
    provider = load(ROOT / "docker/acceptance/provider.py")
    body = {
        "messages": [{"role": "user", "content": "acceptance:file"}],
        "tools": [{"function": {"name": name}} for name in ("read_file", "write_file", "present_files")],
    }
    assert provider.reply(body)["tool_calls"][0]["function"]["name"] == "read_file"
    body["messages"].append({"role": "tool", "content": "file missing"})
    with pytest.raises(ValueError, match="was not read"):
        provider.reply(body)
    body["messages"][-1]["content"] = provider.UPLOAD_MARKER
    write = provider.reply(body)["tool_calls"][0]["function"]
    assert write["name"] == "write_file"
    assert json.loads(write["arguments"])["content"] == provider.ARTIFACT
    body["messages"].append({"role": "tool", "content": "written"})
    assert provider.reply(body)["tool_calls"][0]["function"]["name"] == "present_files"
    body["messages"].append({"role": "tool", "content": "presented"})
    assert provider.reply(body) == {"content": provider.FINAL}


@pytest.mark.skipif(sys.platform != "linux", reason="Linux process-group acceptance runner")
def test_runner_timeout_terminates_its_process_group(tmp_path: Path) -> None:
    runner = load(ROOT / "scripts/docker_acceptance.py")
    marker = tmp_path / "child-pid"
    program = "import pathlib,subprocess,sys,time; child=subprocess.Popen([sys.executable,'-c','import time;time.sleep(60)']); pathlib.Path(sys.argv[1]).write_text(str(child.pid)); time.sleep(60)"
    with pytest.raises(subprocess.TimeoutExpired):
        runner.run(
            [sys.executable, "-c", program, str(marker)],
            env=dict(os.environ),
            log=tmp_path / "command.log",
            timeout=1,
        )
    child = int(marker.read_text(encoding="utf-8"))
    # A terminated orphan may briefly remain a zombie awaiting init's reap;
    # it must never remain a running browser/driver process after the deadline.
    state = Path(f"/proc/{child}/stat")
    if state.exists():
        assert state.read_text(encoding="utf-8").split()[2] == "Z"


@pytest.mark.skipif(os.name == "nt", reason="POSIX Docker acceptance runner")
def test_cleanup_fault_still_records_failure_and_attempts_both_owned_images(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    runner = load(ROOT / "scripts/docker_acceptance.py")
    attempted = []

    def fail(command, **_kwargs):
        attempted.append(command)
        raise FileNotFoundError("synthetic missing Docker")

    monkeypatch.setattr(runner, "run", fail)
    monkeypatch.setattr(sys, "argv", ["docker_acceptance.py", "--artifacts", str(tmp_path)])
    assert runner.main() == 1
    result = json.loads((tmp_path / "result.json").read_text(encoding="utf-8"))
    assert result["status"] == "failed"
    assert "missing Docker" in result["error"]
    assert "missing Docker" in result["image_cleanup_error"]
    assert len([command for command in attempted if command[:3] == ["docker", "image", "inspect"]]) == 2
