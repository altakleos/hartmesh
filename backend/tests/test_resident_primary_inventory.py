"""All-state resident census supplies facts, never cleanup/adoption authority."""

import copy
from types import SimpleNamespace

import pytest
from test_storage_spaces_docker import ID, TOKEN, Docker, adapter

from deerflow.spaces.docker import StorageHostUnqualified

SLOT = "d" * 32


class CensusDocker(Docker):
    def __call__(self, command, **kwargs):
        if command[1] == "ps":
            selector = command[command.index("--filter") + 1]
            labels = (self.entry or {}).get("Config", {}).get("Labels", {})
            if selector.startswith("label="):
                key, _, value = selector.removeprefix("label=").partition("=")
                matches = key in labels and (not value or labels[key] == value)
            else:
                matches = (self.entry or {}).get("Name", "").startswith(selector.removeprefix("name=^").rstrip("$"))
            if not matches:
                self.calls.append(command)
                return SimpleNamespace(returncode=0, stdout="", stderr="")
        if command[:3] == ["docker", "network", "ls"]:
            self.calls.append(command)
            return SimpleNamespace(returncode=0, stdout="", stderr="")
        return super().__call__(command, **kwargs)


def census():
    docker = CensusDocker()
    docker.entry.update(Name="/hartmesh-space-" + TOKEN, Created="2026-10-01T12:00:00Z")
    docker.entry["Config"]["Labels"].update({"deerflow.role": "resident", "hartmesh.storage.space." + SLOT: "1"})
    provider = adapter(docker)
    provider._backend = SimpleNamespace(_run=docker)
    return docker, provider


@pytest.mark.parametrize("state", ["created", "running", "exited"])
def test_inventory_includes_original_stopped_and_running_residents(state):
    docker, provider = census()
    docker.entry["State"] = {"Status": state, "Running": state == "running"}
    original = copy.deepcopy(docker.entry)
    actual = provider.bootstrap_residents(known_slots=(SLOT,))
    assert len(actual) == 1
    assert (actual[0].container_id, actual[0].name, actual[0].created_at, actual[0].attachment_id, actual[0].slots) == (ID, "hartmesh-space-" + TOKEN, original["Created"], TOKEN, (SLOT,))
    assert docker.entry == original and all(command[1] in {"info", "ps", "inspect", "network"} for command in docker.calls)


@pytest.mark.parametrize(
    "change",
    [
        lambda e: e.update(Name="/unrelated"),
        lambda e: e.pop("Created"),
        lambda e: e["Config"]["Labels"].update({"hartmesh.storage.host": "other"}),
        lambda e: e["Config"]["Labels"].update({"hartmesh.storage.space." + "e" * 32: "1"}),
        lambda e: e["State"].update(Running="false"),
    ],
)
def test_inventory_refuses_unknown_resident_correlation(change):
    docker, provider = census()
    change(docker.entry)
    with pytest.raises(StorageHostUnqualified):
        provider.bootstrap_residents(known_slots=(SLOT,))
    assert not any(command[1] in {"start", "rm"} for command in docker.calls)


def test_storage_attributed_open_primary_cannot_disappear_when_role_is_missing():
    docker, provider = census()
    docker.entry["Config"]["Labels"].pop("deerflow.role")
    original = docker.__call__

    def run(command, **kwargs):
        if "label=deerflow.role=resident" in command:
            docker.calls.append(command)
            return SimpleNamespace(returncode=0, stdout="", stderr="")
        return original(command, **kwargs)

    provider._backend._run = run
    with pytest.raises(StorageHostUnqualified):
        provider.bootstrap_residents(known_slots=(SLOT,))


@pytest.mark.parametrize("running", [False, True])
@pytest.mark.parametrize("foreign", [False, True])
def test_restricted_resident_requires_original_complete_graph_without_foreign_holder(running, foreign):
    import json

    from test_native_resource_set import INTERNAL, PROXY, topology

    reader, primary, _actual, objects, calls = topology(running=running)
    primary_id = primary["Id"]
    primary["Name"] = "/hartmesh-space-" + TOKEN
    labels = primary["Config"]["Labels"]
    for key in list(labels):
        if key.startswith("hartmesh.storage.space.") or key in {"deerflow.owner_user_id", "deerflow.thread_id", "hartmesh.chat.storage"}:
            del labels[key]
    labels.update({"deerflow.role": "resident", "deerflow.sandbox_id": TOKEN, "hartmesh.storage.attachment": TOKEN, "hartmesh.storage.host": "daemon-1", "hartmesh.storage.space." + SLOT: "1"})
    primary["Mounts"] = []
    if running:
        objects[("network", INTERNAL)]["Containers"][primary_id]["Name"] = primary["Name"][1:]
    for (kind, identity), entry in objects.items():
        if identity != primary_id:
            (entry["Config"]["Labels"] if kind == "container" else entry["Labels"])["deerflow.sandbox_id"] = TOKEN
    if foreign:
        objects[("network", INTERNAL)]["Containers"]["7" * 64] = {"Name": "foreign", "EndpointID": "6" * 64}
    original = reader.backend._run

    def run(command, **kwargs):
        if command[1] == "info":
            return SimpleNamespace(returncode=0, stdout=json.dumps({"ID": "daemon-1", "OSType": "linux", "OperatingSystem": "Debian"}), stderr="")
        if command[1] == "ps" or command[1:3] == ["network", "ls"]:
            calls.append(command)
            kind = "container" if command[1] == "ps" else "network"
            ids = [identity for (member_kind, identity) in objects if member_kind == kind]
            selector = command[command.index("--filter") + 1]
            if selector.startswith("label="):
                key, _, value = selector.removeprefix("label=").partition("=")
                ids = [
                    identity
                    for identity in ids
                    if key in (objects[(kind, identity)]["Config"]["Labels"] if kind == "container" else objects[(kind, identity)]["Labels"])
                    and (not value or (objects[(kind, identity)]["Config"]["Labels"] if kind == "container" else objects[(kind, identity)]["Labels"])[key] == value)
                ]
            elif selector == "name=^/hartmesh-space-":
                ids = [identity for identity in ids if objects[(kind, identity)]["Name"].startswith("/hartmesh-space-")]
            return SimpleNamespace(returncode=0, stdout="\n".join(ids), stderr="")
        return original(command, **kwargs)

    provider = adapter(run)
    provider._backend = SimpleNamespace(_run=run)
    if foreign:
        with pytest.raises(StorageHostUnqualified):
            provider.bootstrap_residents(known_slots=(SLOT,))
    else:
        actual = provider.bootstrap_residents(known_slots=(SLOT,))
        assert len(actual) == 1 and PROXY in {resource.identity for resource in actual[0].resources}
    assert not any("rm" in command or "start" in command for command in calls)
