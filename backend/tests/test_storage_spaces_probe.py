"""Offline engine-double contracts; these fixtures do not qualify native storage."""

import json
import os
import stat
import sys
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace

import pytest
from test_agent_instances import instances as instances
from test_storage_spaces_docker import ID, PLAN, Docker

from deerflow.spaces.contract import Custody, MutationMode, PrincipalRef
from deerflow.spaces.docker import DockerStorageAdapter, StorageAdapterUnsupported

pytestmark = pytest.mark.skipif(sys.platform != "linux", reason="Native storage path proof requires Linux")
IMAGE = "example.invalid/sandbox@sha256:" + "1" * 64


class ProbeDocker(Docker):
    def __init__(self, *, fault=None):
        super().__init__()
        self.fault = fault
        self.sources = []

    def __call__(self, command, **kwargs):
        if command[1] == "context":
            return SimpleNamespace(returncode=0, stdout=json.dumps([{"Endpoints": {"docker": {"Host": "unix:///var/run/docker.sock"}}}]), stderr="")
        if command[1] == "run":
            self.calls.append(command)
            assert kwargs["timeout"] == 30
            source = Path(command[command.index("--mount") + 1].split(",")[1].removeprefix("src="))
            self.sources.append(source)
            assert stat.S_IMODE(source.stat().st_mode) == 0o644
            content = source.read_text(encoding="utf-8")
            return SimpleNamespace(returncode=1 if self.fault == "unreadable" else 0, stdout="other bytes" if self.fault == "mismatch" else content, stderr="")
        return super().__call__(command, **kwargs)


def provider(pool, docker, **kwargs):
    return DockerStorageAdapter(prepare=lambda _plan: ID, runner=docker, host_verifier=lambda: None, probe_image=IMAGE, pool=pool, **kwargs)


@pytest.mark.parametrize("fault", ["mismatch", "unreadable"])
def test_probe_refuses_when_the_daemon_resolves_the_path_to_other_bytes(tmp_path, fault):
    docker = ProbeDocker(fault=fault)
    with pytest.raises(StorageAdapterUnsupported, match="same files"):
        provider(tmp_path, docker)
    assert not list((tmp_path / ".probe").iterdir())
    assert not any(c[1] in ("create", "start") for c in docker.calls)


def test_probe_runs_without_runtime_on_no_network_as_uid_1000(tmp_path):
    docker = ProbeDocker()
    provider(tmp_path, docker)
    (command,) = [c for c in docker.calls if c[1] == "run"]
    assert command[:12] == ["docker", "run", "--rm", "--network", "none", "--cap-drop=ALL", "--security-opt", "no-new-privileges", "--user", "1000:1000", "--read-only", "--name"]
    assert command[-3:] == ["cat", IMAGE, "/probe"]
    assert "--runtime" not in command
    mount = command[command.index("--mount") + 1]
    assert mount.endswith(",dst=/probe,readonly")
    assert docker.sources[0].parent == tmp_path / ".probe"
    assert not docker.sources[0].exists()


def test_adapter_empties_only_its_private_probe_entries_at_construction(tmp_path):
    directory = tmp_path / ".probe"
    directory.mkdir()
    victim = tmp_path / "retained"
    victim.write_text("keep", encoding="utf-8")
    (directory / "stale").write_text("stale", encoding="utf-8")
    (directory / "link").symlink_to(victim)
    provider(tmp_path, ProbeDocker())
    assert list(directory.iterdir()) == []
    assert victim.read_text(encoding="utf-8") == "keep"
    (directory / "stale").write_text("keep", encoding="utf-8")
    provider(tmp_path, ProbeDocker(), clean_probe=False)
    assert (directory / "stale").read_text(encoding="utf-8") == "keep"


def test_probe_directory_symlink_is_never_followed(tmp_path):
    outside = tmp_path / "outside"
    outside.mkdir()
    target = outside / "retained"
    target.write_text("keep", encoding="utf-8")
    pool = tmp_path / "pool"
    pool.mkdir()
    (pool / ".probe").symlink_to(outside, target_is_directory=True)
    with pytest.raises(StorageAdapterUnsupported):
        provider(pool, ProbeDocker())
    assert target.read_text(encoding="utf-8") == "keep"


@pytest.mark.parametrize("fault", [None, "mismatch", "unreadable"])
def test_per_view_probe_removes_its_nonce_and_directory_before_callback(tmp_path, fault):
    pool = tmp_path / "pool"
    pool.mkdir()
    data = pool / ("d" * 32 + ".volume") / "data"
    control = data.parent / "control"
    data.mkdir(parents=True)
    control.mkdir()
    docker = ProbeDocker()
    adapter = provider(pool, docker)
    docker.fault = fault
    called = []
    adapter._prepare = lambda plan: called.append(plan) or ID
    plan = replace(PLAN, views=(replace(PLAN.views[0], source=str(data)),))
    if fault:
        with pytest.raises(StorageAdapterUnsupported, match="same files"):
            adapter.prepare(plan)
        assert not called
    else:
        assert adapter.prepare(plan) == ID and called == [plan]
    assert not (control / "probe").exists()
    assert list(control.iterdir()) == []


def test_cleanup_failure_prevents_probe_qualification(tmp_path, monkeypatch):
    from deerflow.spaces import docker as module

    original = os.unlink

    def unlink(path, **kwargs):
        if len(str(path)) == 32:
            raise PermissionError("fixture failure")
        return original(path, **kwargs)

    monkeypatch.setattr(module.os, "unlink", unlink)
    with pytest.raises(StorageAdapterUnsupported, match="cleanup"):
        provider(tmp_path, ProbeDocker())


def test_same_host_repeats_identity_checks_without_another_probe(tmp_path):
    docker = ProbeDocker()
    adapter = provider(tmp_path, docker)
    count = sum(c[1] == "run" for c in docker.calls)
    adapter._same_host()
    assert sum(c[1] == "run" for c in docker.calls) == count == 1


@pytest.mark.asyncio
async def test_probed_free_slot_is_still_allocatable(instances, tmp_path):
    _agents, files, _sf, catalog = instances
    controls = [catalog.verify(slot).control_path for slot in catalog.volumes]
    before = {path: list(path.iterdir()) for path in controls}
    provider(tmp_path, ProbeDocker())
    assert {path: list(path.iterdir()) for path in controls} == before
    actor = PrincipalRef("human", "alice")
    space = await files.create(actor=actor, name="Fresh files", custody=Custody.personal(actor), mode=MutationMode.NATIVE)
    assert space.id


def test_socket_path_comes_from_the_inspected_context(monkeypatch):
    from deerflow.spaces.docker import _local_endpoint

    monkeypatch.delenv("DOCKER_HOST", raising=False)

    def lstat(path):
        if str(path) == "/run/docker.sock":
            raise FileNotFoundError
        assert str(path) == "/var/run/docker.sock"
        return SimpleNamespace(st_mode=stat.S_IFSOCK, st_uid=0, st_dev=7, st_ino=8)

    monkeypatch.setattr(Path, "lstat", lstat)
    _local_endpoint([{"Endpoints": {"docker": {"Host": "unix:///var/run/docker.sock"}}}])


@pytest.mark.parametrize("fault", ["owner", "alias", "changed"])
def test_socket_owner_alias_and_replacement_are_checked(monkeypatch, fault):
    from deerflow.spaces.docker import _local_endpoint

    monkeypatch.delenv("DOCKER_HOST", raising=False)
    calls = 0

    def lstat(path):
        nonlocal calls
        calls += 1
        inode = 9 if fault == "alias" and str(path) == "/run/docker.sock" else 8
        if fault == "changed" and calls >= 4:
            inode = 10
        return SimpleNamespace(st_mode=stat.S_IFSOCK, st_uid=1 if fault == "owner" else 0, st_dev=7, st_ino=inode)

    monkeypatch.setattr(Path, "lstat", lstat)
    with pytest.raises(StorageAdapterUnsupported):
        _local_endpoint([{"Endpoints": {"docker": {"Host": "unix:///var/run/docker.sock"}}}])


def test_restricted_network_provider_cannot_bypass_preparation_or_resume(tmp_path, monkeypatch):
    from deerflow.spaces import docker as module

    docker = ProbeDocker()
    monkeypatch.setattr(module, "_local_endpoint", lambda _contexts: None)
    monkeypatch.setattr(DockerStorageAdapter, "_call", lambda self, *args: docker(["docker", *args], timeout=30))
    backend = SimpleNamespace(runtime="docker", _config_mounts=[], network_mode="open", _image=IMAGE, _start_container=lambda *_a, **_k: pytest.fail("restricted creation must not reach the callback"))
    adapter = DockerStorageAdapter.from_local_backend(backend, pool=tmp_path)
    backend.network_mode = "allowlist"
    with pytest.raises(StorageAdapterUnsupported):
        adapter.prepare(PLAN)
    with pytest.raises(StorageAdapterUnsupported):
        adapter.start(ID, PLAN)
    docker.entry["State"] = {"Status": "running", "Running": True}
    with pytest.raises(StorageAdapterUnsupported):
        adapter.verify_active(ID, PLAN)
    assert not any(c[1] in ("create", "start", "rm") for c in docker.calls)
    assert adapter.discover(PLAN.id) == [ID]
    adapter.fence(ID, PLAN.id)
    assert docker.entry is None


@pytest.mark.asyncio
async def test_runtime_always_probes_the_pool_and_cleans_only_first_construction(instances, tmp_path, monkeypatch):
    from deerflow.agent_instances import runtime
    from deerflow.community.aio_sandbox.aio_sandbox_provider import AioSandboxProvider
    from deerflow.community.aio_sandbox.local_backend import LocalContainerBackend

    _agents, files, _sf, _catalog = instances
    files.inventory_path = tmp_path / "inventory.v2.json"
    calls = []

    def build(backend, **kwargs):
        calls.append(kwargs)
        return SimpleNamespace(host_id="fixture-host")

    monkeypatch.setattr(DockerStorageAdapter, "from_local_backend", build)
    baseline = object.__new__(AioSandboxProvider)
    baseline._backend = object.__new__(LocalContainerBackend)
    _, first = await runtime.qualified_attachments(files, baseline_provider=baseline)
    _, second = await runtime.qualified_attachments(files, baseline_provider=baseline)
    assert first is second
    assert calls == [{"pool": tmp_path, "clean_probe": True}, {"pool": tmp_path, "clean_probe": False}]


@pytest.mark.parametrize("writer_runtime", [None, "runsc"])
def test_native_fixture_writer_uses_only_the_requested_runtime(monkeypatch, writer_runtime):
    from _storage_spaces_native_test_support import prepare_consumer_container

    if writer_runtime is None:
        monkeypatch.delenv("HARTMESH_NATIVE_WRITER_RUNTIME", raising=False)
    else:
        monkeypatch.setenv("HARTMESH_NATIVE_WRITER_RUNTIME", writer_runtime)
    calls = []

    def run(command, **kwargs):
        calls.append(command)
        return SimpleNamespace(stdout=ID)

    prepare_consumer_container(PLAN, IMAGE, "pass", [], {"confirmed": True}, runner=run)
    (command,) = calls
    offset = command.index("--pull=never") + 1
    assert ("--runtime" in command) == (writer_runtime is not None)
    if writer_runtime:
        assert command[offset : offset + 2] == ["--runtime", writer_runtime]
