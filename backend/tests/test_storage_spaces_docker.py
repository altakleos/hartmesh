"""Inspection must fail closed before starting or touching an environment."""

import copy
import json
import subprocess
from types import SimpleNamespace

import pytest

from deerflow.spaces.attachments import AttachmentPlan, NativeView
from deerflow.spaces.contract import PrincipalRef
from deerflow.spaces.docker import DockerStorageAdapter, StorageAdapterUnsupported

ID = "a" * 64
TOKEN = "b" * 32
PLAN = AttachmentPlan(TOKEN, "c" * 32, PrincipalRef("human", "alice"), "daemon-1", (NativeView("d" * 32, 1, "/provider/data", "/mnt/spaces/home", True),))


def inspect_entry():
    return {
        "Id": ID,
        "State": {"Status": "created", "Running": False},
        "Config": {"Labels": {"hartmesh.storage.attachment": TOKEN, "hartmesh.storage.host": "daemon-1"}},
        "HostConfig": {
            "Privileged": False,
            "CapDrop": ["ALL"],
            "CapAdd": [],
            "SecurityOpt": ["no-new-privileges"],
            "PidMode": "",
            "IpcMode": "private",
            "NetworkMode": "none",
            "Devices": [],
            "DeviceRequests": [],
            "RestartPolicy": {"Name": "no"},
        },
        "Mounts": [{"Type": "bind", "Source": "/provider/data", "Destination": "/mnt/spaces/home", "RW": True, "Propagation": "rprivate"}],
    }


class Docker:
    def __init__(self):
        self.entry = inspect_entry()
        self.calls = []
        self.unknown = False

    def __call__(self, command, **kwargs):
        self.calls.append(command)
        if self.unknown:
            raise subprocess.TimeoutExpired(command, 10)
        if command[1] == "info":
            return SimpleNamespace(returncode=0, stdout=json.dumps({"ID": "daemon-1", "OSType": "linux", "OperatingSystem": "Debian"}), stderr="")
        if command[1] == "inspect":
            return SimpleNamespace(returncode=0 if self.entry else 1, stdout=json.dumps([self.entry]) if self.entry else "", stderr="" if self.entry else "Error: No such object: " + ID)
        if command[1] == "rm":
            self.entry = None
        if command[1] == "start":
            self.entry["State"] = {"Status": "running", "Running": True}
        if command[1] == "ps":
            return SimpleNamespace(returncode=0, stdout=ID + "\n" if self.entry else "", stderr="")
        return SimpleNamespace(returncode=0, stdout=ID, stderr="")


def adapter(docker):
    return DockerStorageAdapter(prepare=lambda plan: ID, runner=docker, host_verifier=lambda: None)


def test_prepare_validates_stopped_exact_identity_before_start():
    docker = Docker()
    provider = adapter(docker)
    assert provider.prepare(PLAN) == ID
    assert not any(c[1] == "start" for c in docker.calls)
    provider.start(ID, PLAN)
    provider.fence(ID, TOKEN)
    assert docker.entry is None
    assert [c for c in docker.calls if c[1] == "rm"] == [["docker", "rm", "--force", ID]]


@pytest.mark.parametrize(
    "change",
    [
        lambda e: e["HostConfig"].update(Privileged=True),
        lambda e: e["HostConfig"].update(CapAdd=["SYS_ADMIN"]),
        lambda e: e["HostConfig"].update(CapAdd=["SYS_RESOURCE"]),
        lambda e: e["HostConfig"].update(SecurityOpt=[]),
        lambda e: e["HostConfig"].update(PidMode="host"),
        lambda e: e["HostConfig"].update(NetworkMode="host"),
        lambda e: e["HostConfig"].update(Devices=[{}]),
        lambda e: e["Mounts"].append({"Type": "bind", "Source": "/var/run/docker.sock", "Destination": "/socket", "RW": True}),
        lambda e: e["Mounts"][0].update(Source="/another/home"),
        lambda e: e["Mounts"][0].update(Propagation="rshared"),
        lambda e: e["Config"]["Labels"].update({"hartmesh.storage.attachment": "e" * 32}),
        lambda e: e["State"].update(Status="running", Running=True),
    ],
)
def test_unsafe_or_unowned_environment_never_starts(change):
    docker = Docker()
    docker.entry = copy.deepcopy(docker.entry)
    change(docker.entry)
    provider = adapter(docker)
    assert provider.prepare(PLAN) == ID
    with pytest.raises(StorageAdapterUnsupported):
        provider.start(ID, PLAN)
    assert not any(c[1] in ("start", "rm") for c in docker.calls)


def test_unknown_inspection_is_not_absence_or_retirement():
    docker = Docker()
    provider = adapter(docker)
    docker.unknown = True
    with pytest.raises(StorageAdapterUnsupported):
        provider.fence(ID, TOKEN)
    assert not any(c[1] == "rm" for c in docker.calls)


def test_foreign_label_is_never_removed():
    docker = Docker()
    docker.entry["Config"]["Labels"]["hartmesh.storage.host"] = "another-host"
    with pytest.raises(StorageAdapterUnsupported):
        adapter(docker).fence(ID, TOKEN)
    assert not any(c[1] == "rm" for c in docker.calls)


def test_daemon_namespace_must_be_qualified_before_callback_or_start():
    docker = Docker()
    called = []

    def remapped_namespace():
        raise StorageAdapterUnsupported("Different daemon mount namespace")

    with pytest.raises(StorageAdapterUnsupported):
        DockerStorageAdapter(prepare=lambda plan: called.append(plan), runner=docker, host_verifier=remapped_namespace)
    assert called == [] and docker.calls == []


def test_remote_endpoint_cannot_self_attest_local_endpoint(monkeypatch):
    from deerflow.spaces.docker import _local_endpoint

    monkeypatch.setenv("DOCKER_HOST", "tcp://another-host:2376")
    with pytest.raises(StorageAdapterUnsupported):
        _local_endpoint([{"Endpoints": {"docker": {"Host": "unix:///var/run/docker.sock"}}}])


def test_alternate_unix_endpoint_is_not_verified_by_default_daemon_pid(monkeypatch):
    from deerflow.spaces.docker import _local_endpoint

    monkeypatch.setenv("DOCKER_HOST", "unix:///tmp/other-daemon.sock")
    with pytest.raises(StorageAdapterUnsupported):
        _local_endpoint([{"Endpoints": {"docker": {"Host": "unix:///var/run/docker.sock"}}}])
