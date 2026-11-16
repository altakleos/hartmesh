"""Memory admission requires the real preparation verdict and external counters."""

import copy
import importlib.util
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
