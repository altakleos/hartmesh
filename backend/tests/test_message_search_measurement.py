"""Contracts for the disposable PostgreSQL message-search qualification."""

import importlib.util
from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[2]
FIXTURE = ROOT / "docker/acceptance/message-search"


def load(path):
    spec = importlib.util.spec_from_file_location(path.stem, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_measurement_requires_postgres_and_preserves_the_release_gates():
    runner = load(ROOT / "scripts/docker_acceptance.py")
    with pytest.raises(SystemExit):
        runner.parse_args(["--measure-messages"])
    assert runner.parse_args(["--measure-messages", "--stores", "postgres-redis"]).measure_messages
    with pytest.raises(SystemExit):
        runner.parse_args(["--measure-messages", "--measure-titles", "--stores", "postgres-redis"])
    services = yaml.safe_load((ROOT / "deploy/compose/compose.yaml").read_text())["services"]
    profile = yaml.safe_load((ROOT / "docker/acceptance/title-search/compose.yaml").read_text())["services"]
    for key in ("image", "command", "mem_limit", "memswap_limit", "pids_limit"):
        assert services["postgres"][key] == profile["postgres"][key]
    base = yaml.safe_load((ROOT / "docker/acceptance/compose.yaml").read_text())["services"]
    for key in ("cpus", "mem_limit", "memswap_limit", "pids_limit"):
        assert services["gateway"][key] == base["gateway"][key]
    entry = yaml.safe_load((FIXTURE / "plugin.yaml").read_text())
    assert entry["use"] == "hartmesh_message_search:install"
    assert entry["enabled"] is True and entry["required"] is False


def test_corpus_size_visibility_and_distribution():
    corpus = load(FIXTURE / "seed.py")
    owners = [str(i) for i in range(20)]
    distribution = [corpus.owner_for(i) for i in range(4000)]
    assert set(distribution) == set(range(len(owners)))
    assert distribution.count(0) == 2000
    assert sum(corpus.message_count(i) for i in range(4000)) == 800_000
    probes = [corpus.message(i, seq) for i, seq in ((0, 1), (0, 2), (3999, 200))]
    assert probes[0]["type"] == "human" and probes[1]["type"] == "ai"
    assert "saffron ledger witness" in probes[0]["content"]
    assert "saffron ledger witness" not in probes[1]["content"]
    assert len({p["id"] for p in probes}) == 3


def test_missing_or_incomplete_measurements_cannot_enable_default():
    probe = load(FIXTURE / "measure.py")
    assert len(probe.LIMITS) == 8
    assert probe.decision({}) == "unverified"
    values = {k: {"status": "pass"} for k in probe.LIMITS}
    assert probe.decision(values) == "pass"
    values["footprint"] = {"status": "fail"}
    assert probe.decision(values) == "miss"
    assert probe.p95([*range(1, 21), 900]) == 20


def test_off_config_replay_and_partial_observations(tmp_path, monkeypatch):
    probe = load(FIXTURE / "measure.py")
    config = tmp_path / "gateway.yaml"
    config.write_text("log_level: info\nrun_events:\n  backend: db\n")
    inode = config.stat().st_ino
    monkeypatch.setenv("ACCEPTANCE_TITLE_CONFIG", str(config))
    monkeypatch.setenv("HARTMESH_ACCEPTANCE_URL", "http://localhost:2000")
    measurement = probe.Measurement(tmp_path, "gateway", "postgres")
    monkeypatch.setattr(measurement, "restart", lambda: None)
    measurement.enable(True)
    assert yaml.safe_load(config.read_text())["plugins"] == [yaml.safe_load((FIXTURE / "plugin.yaml").read_text())]
    measurement.enable(False)
    assert "plugins" not in yaml.safe_load(config.read_text())
    assert config.stat().st_ino == inode
    for key, threshold in probe.LIMITS.items():
        measurement.gate(key, threshold)
    import json

    receipt = tmp_path / "message-search-observations.json"
    assert json.loads(receipt.read_text())["decision"] == "pass"
    measurement.gate("lock_seconds", 2.00001)
    assert json.loads(receipt.read_text())["decision"] == "miss"
    with measurement.phase("active_browser"):
        raise RuntimeError("No result reached the browser")
    partial = json.loads(receipt.read_text())
    assert partial["decision"] == "unverified"
    assert partial["gates"]["lock_seconds"]["observed"] == 2.00001
    assert partial["errors"] == {"active_browser": "No result reached the browser"}


def test_source_and_signed_workflows_publish_the_actual_evidence_directory():
    workflow = yaml.safe_load((ROOT / ".github/workflows/message-search-measurement.yml").read_text())
    assert set(workflow[True]) == {"workflow_dispatch", "pull_request"}
    assert {
        "backend/app/gateway/routers/threads.py",
        "docker/acceptance/provider.py",
        "backend/app/gateway/search_storage.py",
        "backend/app/gateway/search_corpus.py",
        "backend/packages/harness/deerflow/runtime/events/store/db.py",
        "frontend-hm/src/core/threads/conversation-search.ts",
    } <= set(workflow[True]["pull_request"]["paths"])
    job = workflow["jobs"]["measure"]
    active_dir = '"$RUNNER_TEMP/message-search"'
    probes = [s for s in job["steps"] if "--measure-messages" in s.get("run", "")]
    assert len(probes) == 2 and all(active_dir in s["run"] for s in probes)
    upload = job["steps"][-1]
    assert upload["if"] == "always()" and upload["with"]["path"] == "${{ runner.temp }}/message-search"
    assert upload["with"]["if-no-files-found"] == "error"
