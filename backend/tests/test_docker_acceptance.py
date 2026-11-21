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
    preflight = provider.reply(body)["tool_calls"][0]["function"]
    assert preflight["name"] == "read_file"
    assert json.loads(preflight["arguments"])["path"] == "/mnt/user-data/outputs/acceptance.txt"
    body["messages"].append({"role": "tool", "content": "existing output or missing-file refusal"})
    write = provider.reply(body)["tool_calls"][0]["function"]
    assert write["name"] == "write_file"
    assert json.loads(write["arguments"])["content"] == provider.ARTIFACT
    body["messages"].append({"role": "tool", "content": "written"})
    assert provider.reply(body)["tool_calls"][0]["function"]["name"] == "present_files"
    body["messages"].append({"role": "tool", "content": "presented"})
    assert provider.reply(body) == {"content": provider.FINAL}


def test_synthetic_recipient_reads_both_attachments_before_answering():
    provider = load(ROOT / "docker/acceptance/provider.py")
    body = {"messages": [{"role": "user", "content": "acceptance:attachments"}], "tools": [{"function": {"name": "read_file"}}]}
    for name, content in (("acceptance.txt", provider.ARTIFACT), ("local.txt", "Local attachment.")):
        call = provider.reply(body)["tool_calls"][0]["function"]
        assert call["name"] == "read_file"
        assert json.loads(call["arguments"]) == {"path": "/mnt/user-data/uploads/" + name}
        body["messages"].append({"role": "tool", "content": "different bytes"})
        with pytest.raises(ValueError, match="attachment content"):
            provider.reply(body)
        body["messages"][-1]["content"] = content
    assert provider.reply(body) == {"content": "Both attachments were read."}


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
    # Waiting for the launcher does not wait for its orphaned child. SIGKILL
    # can still be pending when /proc reports the child as runnable.
    import time

    deadline = time.monotonic() + 2
    while True:
        try:
            child_state = state.read_text(encoding="utf-8").split()[2]
        except (FileNotFoundError, ProcessLookupError):
            break  # init may reap the terminated child between lookup and read.
        if child_state == "Z":
            break
        assert time.monotonic() < deadline, f"Timed-out child remains in state {child_state}"
        time.sleep(0.01)


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


@pytest.mark.parametrize(
    ("config", "revision"),
    [
        ('{"Labels":{"org.opencontainers.image.revision":"abc"},"Env":["SECRET=x"]}', "abc"),
        ('{"Labels":{"other":"label"}}', None),
        ('{"Labels":null}', None),
        ("{}", None),
        ("null", None),
    ],
)
def test_an_image_without_labels_is_read_on_every_engine(monkeypatch, config, revision):
    runner = load(ROOT / "scripts/docker_acceptance.py")
    monkeypatch.setattr(runner.subprocess, "check_output", lambda *args, **kwargs: f'"sha256:id" ["repo@sha256:d"] {config}\n')
    assert runner.inspect_image("image", env={}) == {"requested": "image", "image_id": "sha256:id", "repo_digests": ["repo@sha256:d"], "source_revision": revision}


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


def test_acceptance_gateway_runs_with_bundle_resource_limits():
    acceptance, bundle = [yaml.safe_load((ROOT / path).read_text(encoding="utf-8"))["services"]["gateway"] for path in ("docker/acceptance/compose.yaml", "deploy/compose/compose.yaml")]
    assert {name: acceptance[name] for name in ("mem_limit", "memswap_limit", "cpus", "pids_limit")} == {name: bundle[name] for name in ("mem_limit", "memswap_limit", "cpus", "pids_limit")}


def test_preview_measurement_is_digest_only_and_has_separate_evidence():
    runner = load(ROOT / "scripts/docker_acceptance.py")
    for args in (["--measure-previews"], ["--measure-previews", "--stores", "both"]):
        with pytest.raises(SystemExit):
            runner.parse_args(args)
    pinned = ["--backend-image", "example/backend@sha256:" + "a" * 64, "--frontend-image", "example/web@sha256:" + "b" * 64]
    assert runner.parse_args([*pinned, "--measure-previews"]).measure_previews
    assert not runner.parse_args(pinned).measure_previews
    assert runner.harness_fingerprint(runner.PREVIEW_PATHS) != runner.harness_fingerprint()


def test_preview_fixture_uses_real_tools_and_only_its_own_named_samples():
    provider = load(ROOT / "docker/acceptance/provider.py")
    body = {"messages": [{"role": "user", "content": "acceptance:preview"}], "tools": [{"function": {"name": name}} for name in ("bash", "present_files")]}
    copy = provider.reply(body)["tool_calls"][0]["function"]
    assert copy["name"] == "bash"
    names = ["sample.report.json", "sample.xlsx", "sample.docx", "sample.pptx"]
    assert all(f"/mnt/user-data/uploads/{name}" in json.loads(copy["arguments"])["command"] for name in names)
    body["messages"].append({"role": "tool", "content": "copied"})
    present = provider.reply(body)["tool_calls"][0]["function"]
    assert present["name"] == "present_files"
    assert set(json.loads(present["arguments"])["filepaths"]) == {f"/mnt/user-data/outputs/{name}" for name in [*names, "busy.report.json"]}
    body["messages"].append({"role": "tool", "content": "presented"})
    assert provider.reply(body)["content"] == "Preview files ready."


def test_hosted_measurement_rejects_other_image_sources_and_commits():
    runner = load(ROOT / "scripts/docker_acceptance.py")
    revision = "e" * 40
    refs = {name: f"ghcr.io/example/hartmesh-{name}@sha256:" + "b" * 64 for name in ("backend", "frontend")}
    for name, ref in refs.items():
        runner.check_preview_image(name, ref, "example/HartMesh", revision, revision)
        with pytest.raises(ValueError):
            runner.check_preview_image(name, ref.replace("example/", "elsewhere/"), "example/HartMesh", revision, revision)
        with pytest.raises(ValueError):
            runner.check_preview_image(name, ref, "example/HartMesh", "a" * 40, revision)


@pytest.mark.parametrize(("mode", "receipt_name"), [(["--measure-previews"], "preview-result.json"), (["--measure-titles", "--stores", "postgres-redis"], "title-search-result.json")])
def test_measurement_checks_each_candidate_before_starting_services(monkeypatch, tmp_path, mode, receipt_name):
    runner = load(ROOT / "scripts/docker_acceptance.py")
    revision = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT, text=True).strip()
    refs = [f"ghcr.io/example/hartmesh-{name}@sha256:" + "b" * 64 for name in ("backend", "frontend")]
    args = runner.parse_args([*mode, "--backend-image", refs[0], "--frontend-image", refs[1], "--artifacts", str(tmp_path)])
    for key, value in {"GITHUB_REPOSITORY": "example/HartMesh", "GITHUB_EVENT_NAME": "workflow_dispatch", "GITHUB_SHA": revision, "GITHUB_RUN_ID": "7", "GITHUB_RUN_ATTEMPT": "1", "GH_TOKEN": "fixture"}.items():
        monkeypatch.setenv(key, value)
    monkeypatch.setattr(runner, "inspect_image", lambda ref, **kwargs: {"requested": ref, "source_revision": revision})
    verified = []

    def record(command, *, env, **kwargs):
        if "verify-candidate" in command:
            assert env["GH_TOKEN"] == "fixture"
            verified.append(command)
            if len(verified) == 2:
                raise RuntimeError("candidate was built for another version")
        else:
            assert "GH_TOKEN" not in env
        assert "up" not in command
        return subprocess.CompletedProcess(command, 0)

    monkeypatch.setattr(runner, "run", record)
    assert runner.run_profile(args) == 1
    receipt = json.loads((tmp_path / receipt_name).read_text())
    assert receipt["error"] == "candidate was built for another version"
    for command, name, ref in zip(verified, ("backend", "frontend"), refs, strict=True):
        for option, expected in (("--component", name), ("--image", ref), ("--revision", revision), ("--version", receipt["release_version"]), ("--repository", "example/HartMesh")):
            assert command[command.index(option) + 1] == expected


def test_preview_measurement_records_missing_fixture_failure(monkeypatch, tmp_path):
    runner = load(ROOT / "scripts/docker_acceptance.py")
    args = runner.parse_args(["--measure-previews", "--backend-image", "example/backend@sha256:" + "a" * 64, "--frontend-image", "example/frontend@sha256:" + "b" * 64, "--artifacts", str(tmp_path)])
    monkeypatch.setattr(runner, "PREVIEW_PATHS", ("missing-preview-fixture",))
    assert runner.run_profile(args) == 1
    receipt = json.loads((tmp_path / "preview-result.json").read_text())
    assert receipt["status"] == "failed"
    assert "missing-preview-fixture" in receipt["error"]


def test_measurement_profile_stays_out_of_ordinary_docker_acceptance():
    fixture = ROOT / "docker/acceptance"
    base = (fixture / "acceptance-config.yaml").read_text(encoding="utf-8")
    assert not yaml.safe_load(base).get("plugins")
    merged = yaml.safe_load(base + "\n" + (fixture / "preview/plugins.yaml").read_text(encoding="utf-8"))
    assert {entry["use"] for entry in merged["plugins"]} == {"sample_projector:install", "hartmesh_legacy_report:install", "hartmesh_office_preview:install"}
    assert "sample_projector" not in (fixture / "compose.yaml").read_text(encoding="utf-8")
    gateway = yaml.safe_load((fixture / "preview/compose.yaml").read_text(encoding="utf-8"))["services"]["gateway"]
    assert all(mount.endswith(":ro") for mount in gateway["volumes"])
    assert "setpriv --reuid=1000 --regid=1000 --clear-groups" in gateway["command"][-1]
    assert "--inh-caps=-all --no-new-privs" in gateway["command"][-1]
    workflow = yaml.safe_load((ROOT / ".github/workflows/preview-measurement.yml").read_text(encoding="utf-8"))
    assert set(workflow[True]) == {"workflow_dispatch"}
    assert workflow["permissions"] == {"contents": "read", "packages": "read", "attestations": "read"}
    assert workflow["jobs"]["measure"]["runs-on"].startswith("ubuntu-")


@pytest.mark.skipif(sys.platform != "linux", reason="Linux candidate worker probe")
def test_measurement_controls_use_one_finite_wait_and_one_zero_access_reservation(monkeypatch):
    import errno
    import mmap
    import resource

    fixture = load(ROOT / "docker/acceptance/preview/sample_projector.py")
    calls = []
    monkeypatch.setattr(fixture.time, "sleep", calls.append)
    assert fixture.project(b"deadline") == b"deadline"
    assert calls == [11]
    limit = 256 << 20
    monkeypatch.setattr(resource, "getrlimit", lambda kind: (limit, limit) if kind == resource.RLIMIT_AS else pytest.fail("unexpected limit"))

    def refuse(fd, length, *, flags, prot):
        assert (fd, length, flags, prot) == (-1, limit, mmap.MAP_PRIVATE | mmap.MAP_ANONYMOUS, 0)
        calls.append(length)
        raise OSError(errno.ENOMEM, "synthetic refusal")

    monkeypatch.setattr(mmap, "mmap", refuse)
    assert json.loads(fixture.project(b"address-limit")) == {"soft": limit, "hard": limit, "errno": errno.ENOMEM}
    assert calls == [11, limit]


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


def test_title_measurement_uses_bundle_postgres_and_binds_source_or_candidate(tmp_path, monkeypatch):
    runner = load(ROOT / "scripts/docker_acceptance.py")
    args = ["--measure-titles", "--stores", "postgres-redis"]
    assert runner.parse_args(args).measure_titles
    refs = ["--backend-image", "example/backend@sha256:" + "a" * 64, "--frontend-image", "example/frontend@sha256:" + "b" * 64]
    assert runner.parse_args([*args, *refs]).measure_titles
    for invalid in (["--measure-titles"], [*args, "--measure-previews", *refs]):
        with pytest.raises(SystemExit):
            runner.parse_args(invalid)
    bundle = yaml.safe_load((ROOT / "deploy/compose/compose.yaml").read_text())["services"]
    base = yaml.safe_load((ROOT / "docker/acceptance/compose.yaml").read_text())["services"]["gateway"]
    overlay = yaml.safe_load((ROOT / "docker/acceptance/title-search/compose.yaml").read_text())["services"]
    for key in ("mem_limit", "memswap_limit", "pids_limit", "cpus"):
        assert base[key] == bundle["gateway"][key]
    for key in ("image", "command", "mem_limit", "memswap_limit", "pids_limit"):
        assert overlay["postgres"][key] == bundle["postgres"][key]
    db_config = yaml.safe_load((ROOT / "docker/acceptance/title-search/event-storage.yaml").read_text())
    assert db_config["run_events"] == yaml.safe_load((ROOT / "deploy/compose/config.yaml").read_text())["run_events"]
    monkeypatch.setattr(runner, "TITLE_SEARCH_PATHS", ("missing-title-search-fixture",))
    assert runner.run_profile(runner.parse_args([*args, *refs, "--artifacts", str(tmp_path)])) == 1
    receipt = json.loads((tmp_path / "title-search-result.json").read_text())
    assert receipt["status"] == "failed"
    assert "missing-title-search-fixture" in receipt["error"]


def test_title_source_checkout_contains_the_overlay_used_in_the_gateway_config(tmp_path, monkeypatch):
    runner = load(ROOT / "scripts/docker_acceptance.py")
    args = runner.parse_args(["--measure-titles", "--stores", "postgres-redis", "--artifacts", str(tmp_path)])

    def before_docker(command, *, env, **kwargs):
        if command[:3] == ["docker", "image", "inspect"]:
            return subprocess.CompletedProcess(command, 1)
        assert command == ["docker", "info"]
        generated = yaml.safe_load(Path(env["ACCEPTANCE_TITLE_CONFIG"]).read_text())
        assert generated["run_events"] == {"backend": "db"}
        raise RuntimeError("checked generated fixture")

    monkeypatch.setattr(runner, "run", before_docker)
    assert runner.run_profile(args) == 1
    assert json.loads((tmp_path / "title-search-result.json").read_text())["error"] == "checked generated fixture"
    overlay = "docker/acceptance/title-search/event-storage.yaml"
    assert subprocess.run(["git", "ls-files", "--error-unmatch", "--", overlay], cwd=ROOT, capture_output=True).returncode == 0


def test_file_browser_profile_requires_an_explicit_mixed_version_control():
    parser = load(ROOT / "scripts/docker_acceptance.py").parse_args
    images = ["--backend-image", "example/backend@sha256:" + "a" * 64, "--frontend-image", "example/frontend@sha256:" + "b" * 64]
    assert parser(["--measure-files", "current"]).measure_files == "current"
    assert parser([*images, "--measure-files", "previous-ui", "--file-browser-floor", "v2.2.0+hartmesh.69"]).file_browser_floor.endswith(".69")
    for invalid in (
        ["--measure-files", "previous-ui"],
        [*images, "--measure-files", "previous-gateway"],
        [*images, "--measure-files", "current", "--file-browser-floor", "v2.2.0+hartmesh.69"],
        [*images, "--measure-files", "previous-ui", "--file-browser-floor", "main"],
    ):
        with pytest.raises(SystemExit):
            parser(invalid)


@pytest.mark.parametrize("pair,component,previous", [("current", "frontend", False), ("previous-ui", "frontend", True), ("previous-ui", "backend", False), ("previous-gateway", "backend", True)])
def test_file_browser_requires_the_components_own_published_pin(tmp_path, monkeypatch, pair, component, previous):
    runner = load(ROOT / "scripts/docker_acceptance.py")
    monkeypatch.setattr(runner, "ROOT", tmp_path)
    subprocess.run(["git", "init", "--quiet", str(tmp_path)], check=True)
    pins = tmp_path / "deploy/compose/images.txt"
    pins.parent.mkdir(parents=True)
    repo = "example/hartmesh"
    tag = "v2.2.0+hartmesh.42"
    ref = f"ghcr.io/{repo}-{component}@sha256:"
    heads = []
    for digit in ["a", "b"]:
        pins.write_text(ref + digit * 64 + "\n", encoding="utf-8")
        subprocess.run(["git", "add", str(pins)], cwd=tmp_path, check=True)
        subprocess.run(["git", "-c", "user.name=Fixture", "-c", "user.email=fixture@example.org", "commit", "-qm", "fixture pins"], cwd=tmp_path, check=True)
        heads.append(subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=tmp_path, text=True).strip())
    subprocess.run(["git", "tag", tag, heads[0]], cwd=tmp_path, check=True)
    selected = ref + ("a" if previous else "b") * 64
    assert runner.file_browser_image_revision(component, selected, repo, pair, tag, heads[1], "2.2.0+hartmesh.43") == ((heads[0], "2.2.0+hartmesh.42") if previous else (heads[1], "2.2.0+hartmesh.43"))
    wrong = ref + ("b" if previous else "a") * 64
    with pytest.raises(ValueError, match="Compose pin"):
        runner.file_browser_image_revision(component, wrong, repo, pair, tag, heads[1], "2.2.0+hartmesh.43")
    with pytest.raises(ValueError, match="repository"):
        runner.file_browser_image_revision(component, selected, "another/hartmesh", pair, tag, heads[1], "2.2.0+hartmesh.43")
