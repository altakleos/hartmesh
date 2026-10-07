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
    try:
        child_state = state.read_text(encoding="utf-8").split()[2]
    except (FileNotFoundError, ProcessLookupError):
        pass  # init may reap the terminated child between lookup and read.
    else:
        assert child_state == "Z"


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


@pytest.mark.parametrize("reference", ["repo:latest", "repo@sha256:abc", "--help", "https://repo@sha256:" + "a" * 64])
def test_image_mode_refuses_mutable_or_invalid_references(reference):
    import argparse

    runner = load(ROOT / "scripts/docker_acceptance.py")
    with pytest.raises(argparse.ArgumentTypeError):
        runner.immutable_image(reference)


def test_image_mode_requires_both_components():
    runner = load(ROOT / "scripts/docker_acceptance.py")
    with pytest.raises(SystemExit) as error:
        runner.parse_args(["--backend-image", "registry.example/team/backend@sha256:" + "a" * 64])
    assert error.value.code == 2


@pytest.mark.skipif(os.name == "nt", reason="POSIX Docker acceptance runner")
@pytest.mark.parametrize("failure", [None, "startup", "browser", "interrupt", "inspect"])
@pytest.mark.parametrize("shared_source", [False, True])
def test_supplied_images_are_never_built_or_deleted_and_cleanup_is_scoped(tmp_path, monkeypatch, failure, shared_source):
    runner = load(ROOT / "scripts/docker_acceptance.py")
    backend = "registry.example/team/backend@sha256:" + "a" * 64
    frontend = "registry.example/team/frontend@sha256:" + "b" * 64
    commands = []

    def fake_run(command, **kwargs):
        commands.append(command)
        if failure == "startup" and "up" in command:
            raise RuntimeError("synthetic startup failure")
        if "test" in command and "playwright" in command:
            if failure == "browser":
                raise RuntimeError("synthetic browser failure")
            if failure == "interrupt":
                raise KeyboardInterrupt("synthetic interrupt")
        return subprocess.CompletedProcess(command, 0)

    def fake_inspect(image, **_kwargs):
        if failure == "inspect":
            raise RuntimeError("synthetic inspect failure")
        return {"requested": image, "image_id": "sha256:" + "c" * 64, "repo_digests": [image] if "@" in image else ["fixture@sha256:" + "a" * 64], "source_revision": "d" * 40}

    def fake_output(command, **_kwargs):
        if command[:2] == ["git", "rev-parse"]:
            return "e" * 40
        if "port" in command:
            return "127.0.0.1:32100"
        if "ps" in command:
            return command[-1] + "-container"
        if command[:2] == ["docker", "inspect"]:
            if command[3] == "{{.Config.Image}}":
                return {"provider": "python:3.12-alpine", "nginx": "nginx:alpine", "postgres": "postgres:16-alpine", "redis": "redis:7-alpine"}[command[-1].removesuffix("-container")]
            return "sha256:" + "c" * 64
        raise AssertionError(command)

    class Login:
        status = 200

        def read(self):
            return b'{"status":"ready"}'

        def __enter__(self):
            return self

        def __exit__(self, *_args):
            pass

    monkeypatch.setattr(runner, "run", fake_run)
    monkeypatch.setattr(runner, "inspect_image", fake_inspect)
    monkeypatch.setattr(runner.subprocess, "check_output", fake_output)
    monkeypatch.setattr(runner.urllib.request, "urlopen", lambda *_args, **_kwargs: Login())
    monkeypatch.setattr(runner, "snapshot_tracked", lambda *_args: pytest.fail("image mode copied build source"))
    for name, value in {"GITHUB_REPOSITORY": "altakleos/hartmesh", "GITHUB_RUN_ID": "1234", "GITHUB_RUN_ATTEMPT": "2", "GITHUB_EVENT_NAME": "workflow_dispatch", "GITHUB_SHA": "e" * 40}.items():
        monkeypatch.setenv(name, value)
    monkeypatch.setattr(sys, "argv", ["docker_acceptance.py", "--artifacts", str(tmp_path), "--backend-image", backend, "--frontend-image", frontend, "--stores", "postgres-redis"])
    if shared_source:
        args = runner.parse_args()
        args.backend_image = args.frontend_image = None
        code = runner.run_profile(args, source_build={"backend": backend, "frontend": frontend, "source_tree_sha256": "f" * 64})
    else:
        code = runner.main()
    assert code == (0 if failure is None else 1)
    summary = json.loads((tmp_path / "result.json").read_text(encoding="utf-8"))
    assert summary["mode"] == ("source" if shared_source else "images")
    assert summary["stores"] == "postgres-redis"
    assert summary["harness_commit"] == "e" * 40
    assert summary["source_commit"] == ("e" * 40 if shared_source else None)
    if shared_source:
        assert summary["source_tree_sha256"] == "f" * 64
    assert not any(command[:2] == ["docker", "build"] or command[:3] == ["docker", "image", "rm"] for command in commands)
    downs = [command for command in commands if "down" in command]
    assert len(downs) == (0 if failure == "inspect" else 1)
    for command in downs:
        assert command[command.index("--project-name") + 1] == summary["project"]
        assert "--volumes" in command
    if failure is None:
        assert summary["images"]["backend"]["requested"] == backend
        assert summary["images"]["frontend"]["requested"] == frontend
        assert summary["images"]["backend"]["source_revision"] == "d" * 40
        verifier = load(ROOT / "scripts/release_acceptance.py")
        if not shared_source:
            verifier.validate_result(summary, "postgres-redis", "altakleos/hartmesh", {"id": 1234, "run_attempt": 2, "head_sha": "e" * 40}, summary["harness_sha256"], {"backend": backend, "frontend": frontend})


def test_postgres_redis_fixture_is_private_and_used_by_gateway():
    overlay = yaml.safe_load((ROOT / "docker/acceptance/compose.postgres-redis.yaml").read_text(encoding="utf-8"))
    for name in ("postgres", "redis"):
        service = overlay["services"][name]
        assert service["networks"] == ["acceptance"]
        assert "ports" not in service and "container_name" not in service
        assert service["healthcheck"]
    gateway = overlay["services"]["gateway"]
    assert gateway["environment"]["ACCEPTANCE_DATABASE_BACKEND"] == "postgres"
    assert gateway["environment"]["ACCEPTANCE_STREAM_BACKEND"] == "redis"
    assert gateway["environment"]["ACCEPTANCE_LOCKOUT_STORE"] == "redis"
    assert all(gateway["depends_on"][name]["condition"] == "service_healthy" for name in ("postgres", "redis"))


def test_release_guard_and_runner_cover_the_same_harness_and_fixture_declarations():
    runner = load(ROOT / "scripts/docker_acceptance.py")
    verifier = load(ROOT / "scripts/release_acceptance.py")
    assert runner.HARNESS_PATHS == verifier.HARNESS_PATHS
    base = yaml.safe_load((ROOT / "docker/acceptance/compose.yaml").read_text(encoding="utf-8"))
    overlay = yaml.safe_load((ROOT / "docker/acceptance/compose.postgres-redis.yaml").read_text(encoding="utf-8"))
    services = {**base["services"], **overlay["services"]}
    assert {name: services[name]["image"] for name in verifier.FIXTURES} == verifier.FIXTURES


def test_candidate_workflow_artifacts_are_small_retained_and_bound_to_the_attempt():
    workflow = yaml.safe_load((ROOT / ".github/workflows/docker-acceptance.yml").read_text(encoding="utf-8"))
    steps = workflow["jobs"]["acceptance"]["steps"]
    result = next(step for step in steps if step.get("name") == "Upload candidate acceptance result")
    assert "workflow_dispatch" in result["if"] and "inputs.backend_image" in result["if"] and "inputs.frontend_image" in result["if"]
    assert result["with"]["name"] == "candidate-acceptance-${{ matrix.stores }}-${{ github.run_attempt }}"
    assert result["with"]["path"].endswith("/result.json")
    assert result["with"]["retention-days"] == 90
    assert result["with"]["if-no-files-found"] == "error"


def test_both_source_profiles_are_available_without_mutable_image_arguments():
    runner = load(ROOT / "scripts/docker_acceptance.py")
    assert runner.parse_args(["--stores", "both"]).stores == "both"
    with pytest.raises(SystemExit):
        runner.parse_args(["--stores", "both", "--backend-image", "repo/backend@sha256:" + "a" * 64, "--frontend-image", "repo/frontend@sha256:" + "b" * 64])


@pytest.mark.parametrize("failure", [None, "different-image", "profile", "cleanup", "drift", "build", "interrupt"])
def test_both_source_profiles_build_once_verify_identity_and_clean_owned_images(tmp_path, monkeypatch, failure):
    runner = load(ROOT / "scripts/docker_acceptance.py")
    args = runner.parse_args(["--stores", "both", "--artifacts", str(tmp_path)])
    builds, profiles, commands = [], [], []

    def build(backend, frontend, **kwargs):
        builds.append((backend, frontend))
        kwargs["summary"]["source_tree_sha256"] = "f" * 64
        if failure in {"build", "interrupt"}:
            raise KeyboardInterrupt("synthetic interrupt") if failure == "interrupt" else RuntimeError("synthetic build failure")

    def inspect(image, **_kwargs):
        return {"requested": image, "image_id": "sha256:" + ("c" if "backend" in image else "d") * 64}

    def profile(arguments, source_build=None):
        profiles.append((arguments.stores, source_build))
        assert not arguments.backend_image and not arguments.frontend_image
        images = {name: {**inspect(source_build[name]), "running_image_id": inspect(source_build[name])["image_id"]} for name in ("backend", "frontend")}
        if failure == "different-image" and arguments.stores == "postgres-redis":
            images["backend"]["image_id"] = images["backend"]["running_image_id"] = "sha256:" + "e" * 64
        result = {"status": "failed" if failure == "profile" else "passed", "harness_sha256": "a" * 64, "source_tree_sha256": source_build["source_tree_sha256"], "images": images}
        arguments.artifacts.mkdir(parents=True, exist_ok=True)
        (arguments.artifacts / "result.json").write_text(json.dumps(result), encoding="utf-8")
        return int(result["status"] != "passed")

    def run(command, **_kwargs):
        commands.append(command)
        if failure == "cleanup" and command[:3] == ["docker", "image", "rm"]:
            raise RuntimeError("synthetic cleanup failure")
        return subprocess.CompletedProcess(command, 0)

    monkeypatch.setattr(runner, "build_source_images", build)
    monkeypatch.setattr(runner, "inspect_image", inspect)
    monkeypatch.setattr(runner, "run_profile", profile)
    monkeypatch.setattr(runner, "run", run)
    monkeypatch.setattr(runner, "harness_fingerprint", lambda: "a" * 64)
    monkeypatch.setattr(runner, "snapshot_tracked", lambda *_args: ("e" if failure == "drift" else "f") * 64)
    assert runner.run_both_source(args) == int(failure is not None)
    result = json.loads((tmp_path / "pair-result.json").read_text(encoding="utf-8"))
    assert result["status"] == ("passed" if failure is None else "failed")
    assert len(builds) == 1
    assert len(profiles) == (0 if failure in {"build", "interrupt"} else 2)
    assert len([command for command in commands if command[:3] == ["docker", "image", "rm"]]) == 2
    if failure is None:
        assert profiles[0][1] == profiles[1][1]
