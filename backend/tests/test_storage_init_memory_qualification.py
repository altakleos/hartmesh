"""Memory admission requires the real preparation verdict and external counters."""

import copy
import importlib.util
import shutil
import subprocess
from pathlib import Path

import pytest

spec = importlib.util.spec_from_file_location("storage_memory_qualification", Path(__file__).resolve().parents[2] / "scripts/qualify_storage_init_memory.py")
qualification = importlib.util.module_from_spec(spec)
spec.loader.exec_module(qualification)


def prepared_sample():
    return {
        "exit_code": 0,
        "oom_killed": False,
        "memory_events": {"oom_kill": 0},
        "limit_bytes": 128 << 20,
        "peak_bytes": 96 << 20,
        "observed_peak_bytes": 95 << 20,
        "reported_memory": {"limit_bytes": 128 << 20, "peak_bytes": 96 << 20},
        "document": {"command": "prepare", "returncode": 0, "verdict": "complete", "prepared": {"user": 10, "assistant": 10, "employee": 10, "company": 1}, "spaces": [{"mounted": True} for _ in range(31)]},
    }


def test_margin_boundary_and_external_peak():
    sample = prepared_sample()
    qualification.validate_sample(sample)
    sample["peak_bytes"] += 1
    with pytest.raises(ValueError, match="75 percent"):
        qualification.validate_sample(sample)


@pytest.mark.parametrize("mutation", ["child-refusal", "incomplete", "short-pool", "unmounted", "oom", "missing-peak", "wrong-limit", "underreported"])
def test_process_exit_zero_cannot_qualify_bad_preparation_or_memory(mutation):
    sample = prepared_sample()
    if mutation == "child-refusal":
        sample["document"]["returncode"] = 1
    elif mutation == "incomplete":
        sample["document"]["verdict"] = "incomplete"
    elif mutation == "short-pool":
        sample["document"]["spaces"].pop()
    elif mutation == "unmounted":
        sample["document"]["spaces"][0]["mounted"] = False
    elif mutation == "oom":
        sample["memory_events"]["oom_kill"] = 1
    elif mutation == "missing-peak":
        sample["peak_bytes"] = 0
    elif mutation == "wrong-limit":
        sample["reported_memory"]["limit_bytes"] *= 2
    else:
        sample["reported_memory"]["peak_bytes"] = 94 << 20
    with pytest.raises(ValueError):
        qualification.validate_sample(sample)


def test_baseline_admission_removes_only_new_telemetry_and_margin_requirements():
    sample = prepared_sample()
    sample["reported_memory"] = None
    sample["peak_bytes"] = 128 << 20
    qualification.validate_sample(sample, require_telemetry=False, require_margin=False)
    sample["document"]["returncode"] = 1
    with pytest.raises(ValueError, match="did not complete"):
        qualification.validate_sample(sample, require_telemetry=False, require_margin=False)


@pytest.mark.parametrize("limit", [128 << 20, 192 << 20])
def test_service_extraction_retains_production_command_and_containment(tmp_path, monkeypatch, limit):
    import json

    service = {
        "privileged": True,
        "pid": "host",
        "network_mode": "none",
        "pids_limit": 64,
        "restart": "no",
        "command": ["prepare", "--at-start"],
        "entrypoint": ["uv", "run", "--no-sync", "python", "-m", "app.storage_init"],
        "mem_limit": str(limit),
        "memswap_limit": str(limit),
        "image": "old-image",
        "environment": {"PYTHONDONTWRITEBYTECODE": "1"},
    }
    expected = copy.deepcopy(service)
    expected["image"] = "new-image@sha256:" + "a" * 64
    monkeypatch.setenv("HARTMESH_USERS", "2")

    def render(*args, **kwargs):
        assert kwargs["env"]["HARTMESH_USERS"] == "10"
        return json.dumps({"services": {"storage-init": service}})

    monkeypatch.setattr(qualification, "command", render)
    (tmp_path / "env").write_text("HARTMESH_USERS=10\n", encoding="utf-8")
    if limit != 128 << 20:
        with pytest.raises(ValueError, match="memory limits differ"):
            qualification.profile_service(tmp_path / "compose.yaml", expected["image"], tmp_path / "env", "test-project")
    else:
        result = qualification.profile_service(tmp_path / "compose.yaml", expected["image"], tmp_path / "env", "test-project")
        assert result == {"name": "test-project", "services": {"storage-init": expected}}


def test_qualification_refuses_an_existing_deployment(monkeypatch, tmp_path):
    monkeypatch.delenv("GITHUB_ACTIONS", raising=False)
    with pytest.raises(SystemExit):
        qualification.main(["--image", "unused", "--baseline-image", "unused", "--artifacts", str(tmp_path / "result")])


def test_command_failure_keeps_the_actual_tool_diagnostic(monkeypatch, capsys):
    failure = subprocess.CalledProcessError(1, ["docker", "compose", "create"], stderr="bind source path does not exist\n")

    def refused(*args, **kwargs):
        raise failure

    monkeypatch.setattr(qualification.subprocess, "check_output", refused)
    with pytest.raises(subprocess.CalledProcessError) as caught:
        qualification.command("docker", "compose", "create")
    assert caught.value is failure
    assert capsys.readouterr().err == "bind source path does not exist\n"


def test_recreation_arguments_are_accepted_by_actual_compose_cli(monkeypatch, tmp_path):
    if shutil.which("docker") is None or subprocess.run(["docker", "compose", "version"], capture_output=True).returncode:
        pytest.skip("Docker Compose CLI unavailable")

    class Parsed(Exception):
        pass

    def parse_without_creating(*args):
        # --help invokes the real option parser, without a daemon, container,
        # mount or storage effect. It rejects the former up-only --no-deps flag.
        result = subprocess.run([*args, "--help"], capture_output=True, text=True)
        assert result.returncode == 0, result.stderr
        raise Parsed

    monkeypatch.setattr(qualification, "command", parse_without_creating)
    with pytest.raises(Parsed):
        qualification.measure(tmp_path / "service.json", tmp_path, "fixture", recreate=True)


@pytest.mark.parametrize("mismatched", [False, True])
def test_mount_evidence_reads_protected_pool_and_checks_real_identity(monkeypatch, tmp_path, mismatched):
    import json

    slots = [{"mount_path": f"/protected/spaces/{index}.volume", "filesystem_uuid": f"fixture-uuid-{index}"} for index in range(31)]
    by_target = {row["mount_path"]: row["filesystem_uuid"] for row in slots}
    reads = []

    def protected_read(*args):
        if args[0] != "sudo":
            raise PermissionError("pool and loop metadata require host-root read authority")
        if args[1] == "cat":
            return json.dumps({"volumes": slots})
        target = args[args.index("--mountpoint") + 1]
        reads.append(target)
        return json.dumps({"filesystems": [{"target": target, "fstype": "ext4", "uuid": "another-filesystem" if mismatched else by_target[target]}]})

    monkeypatch.setattr(qualification, "command", protected_read)
    if mismatched:
        with pytest.raises(ValueError, match="does not match"):
            qualification.verify_mounts(tmp_path)
    else:
        qualification.verify_mounts(tmp_path)
        assert set(reads) == set(by_target) and len(reads) == 31
