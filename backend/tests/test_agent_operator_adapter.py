"""Operator qualification uses an offline Docker double and never starts a workspace."""

from types import SimpleNamespace

import pytest
from test_storage_spaces_docker import ID, PLAN, TOKEN
from test_storage_spaces_probe import IMAGE, ProbeDocker

from deerflow.spaces.docker import DockerStorageAdapter, StorageAdapterUnsupported


def backend(docker):
    return SimpleNamespace(runtime="docker", _config_mounts=(), network_mode="open", _run=docker, _image=IMAGE, prepare_resident=lambda *args: pytest.fail("Operator must never prepare a workspace"))


def test_operator_containment_factory_never_probes_or_starts_even_if_called_accidentally(tmp_path, monkeypatch):
    monkeypatch.setattr("deerflow.spaces.docker._local_endpoint", lambda _context: None)
    docker = ProbeDocker()
    adapter = DockerStorageAdapter.for_operator(backend(docker), pool=tmp_path)
    assert not (tmp_path / ".probe").exists()
    assert not any(command[1] in {"run", "create", "start"} for command in docker.calls)
    before = list(docker.calls)
    for action in (
        lambda: adapter.prepare(PLAN),
        lambda: adapter.start(ID, PLAN),
        lambda: adapter.probe(tmp_path / ".probe", label="pool"),
        lambda: adapter.fence(ID, TOKEN),
        lambda: adapter.cleanup(TOKEN),
    ):
        with pytest.raises(StorageAdapterUnsupported, match="Operator"):
            action()
    assert docker.calls == before


@pytest.mark.parametrize("fault", [None, "mismatch", "unreadable"])
def test_operator_status_probe_removes_only_its_own_nonce(tmp_path, monkeypatch, fault):
    monkeypatch.setattr("deerflow.spaces.docker._local_endpoint", lambda _context: None)
    directory = tmp_path / ".probe"
    directory.mkdir()
    (directory / "gateway-nonce").write_bytes(b"in progress")
    (directory / "retained-directory").mkdir()
    docker = ProbeDocker(fault=fault)
    if fault:
        with pytest.raises(StorageAdapterUnsupported, match="same files"):
            DockerStorageAdapter.for_operator(backend(docker), pool=tmp_path, probe_pool=True)
    else:
        adapter = DockerStorageAdapter.for_operator(backend(docker), pool=tmp_path, probe_pool=True)
        with pytest.raises(StorageAdapterUnsupported, match="Operator"):
            adapter.prepare(PLAN)
        with pytest.raises(StorageAdapterUnsupported, match="Operator"):
            adapter.start(ID, PLAN)
    assert {path.name for path in directory.iterdir()} == {"gateway-nonce", "retained-directory"}
    assert (directory / "gateway-nonce").read_bytes() == b"in progress"
    assert len([command for command in docker.calls if command[1] == "run"]) == 1
    assert not any(command[1] in {"create", "start"} for command in docker.calls)


def test_operator_native_evidence_read_does_not_prune_acknowledgements(tmp_path, monkeypatch):
    import json
    from dataclasses import asdict, replace

    from deerflow.spaces.native_create_evidence import NativeCreateEvidence, NativeEvidenceReader

    (tmp_path / ".native-creates").mkdir(mode=0o700)
    writer = NativeCreateEvidence(tmp_path, daemon_id="daemon-1")
    attempt = writer.begin(owner_id="alice", thread_id="thread", name="old-create", slots=("a" * 32,))
    writer.close()
    path = tmp_path / ".native-creates" / (attempt.id + ".json")
    path.write_text(json.dumps({"schema_version": 1, **asdict(replace(attempt, phase="acknowledged", container_id=ID))}), encoding="utf-8")
    before = path.read_bytes(), path.stat().st_ino, path.stat().st_mtime_ns
    monkeypatch.setattr(NativeCreateEvidence, "_prune_acknowledged", lambda *args: pytest.fail("Operator must not finalize evidence"))
    reader = NativeEvidenceReader(tmp_path, daemon_id="daemon-1")
    try:
        assert reader.snapshot().creates[0].phase == "acknowledged"
        reader.assert_current()
        assert not hasattr(reader, "begin") and not hasattr(reader, "retire_resident")
    finally:
        reader.close()
    assert (path.read_bytes(), path.stat().st_ino, path.stat().st_mtime_ns) == before
