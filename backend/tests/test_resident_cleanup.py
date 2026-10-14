"""Offline paired-resource identity checks; no native Docker invocation."""

import copy
import json
from types import SimpleNamespace

import pytest
from test_resident_topology import ATTACHMENT, backend

from deerflow.spaces.docker import DockerStorageAdapter


@pytest.fixture
def topology():
    local, _calls = backend("allowlist")
    proxy, internal = local._resource_names(ATTACHMENT)
    egress = local._egress_network_name(ATTACHMENT)
    proxy_id, internal_id, egress_id = "1" * 64, "2" * 64, "3" * 64
    resources = {
        proxy: {
            "Id": proxy_id,
            "Name": "/" + proxy,
            "Config": {"Labels": local._restricted_labels(ATTACHMENT, "network-proxy")},
            "Mounts": [],
            "NetworkSettings": {"Networks": {internal: {"NetworkID": internal_id}, egress: {"NetworkID": egress_id}}},
        },
        internal: {"Id": internal_id, "Name": internal, "Labels": local._restricted_labels(ATTACHMENT, "network"), "Containers": {proxy_id: {}}, "Driver": "bridge", "Internal": True},
        egress: {"Id": egress_id, "Name": egress, "Labels": local._restricted_labels(ATTACHMENT, "egress-network"), "Containers": {proxy_id: {}}, "Driver": "bridge", "Internal": False},
    }
    calls = []

    def engine(command, **kwargs):
        calls.append(command)
        if command[1] == "info":
            return SimpleNamespace(returncode=0, stdout=json.dumps({"ID": "daemon-1", "OSType": "linux"}), stderr="")
        if "inspect" in command:
            name = command[-1]
            resource = resources.get(name) or next((value for value in resources.values() if value["Id"] == name), None)
            if resource is None:
                error = "Error: No such object: " + name if command[1] == "inspect" else "Error: No such network: " + name
                return SimpleNamespace(returncode=1, stdout="[]", stderr=error)
            return SimpleNamespace(returncode=0, stdout=json.dumps([resource]), stderr="")
        if "rm" in command:
            identifier = command[-1]
            for name, resource in list(resources.items()):
                if resource["Id"] == identifier:
                    del resources[name]
            for resource in resources.values():
                resource.get("Containers", {}).pop(identifier, None)
            return SimpleNamespace(returncode=0, stdout=identifier, stderr="")
        raise AssertionError(command)

    adapter = DockerStorageAdapter(prepare=lambda _plan: None, runner=engine, host_verifier=lambda: None)
    adapter._backend = local
    return adapter, resources, calls, (proxy, internal, egress)


def test_cleanup_accepts_exact_docker29_lowercase_primary_and_relay_absence(topology):
    provider, resources, calls, _names = topology
    original = provider._runner

    def run(command, **kwargs):
        reply = original(command, **kwargs)
        if command[1] == "inspect" and reply.returncode == 1:
            return SimpleNamespace(returncode=1, stdout="[]", stderr="error: no such object: " + command[-1])
        return reply

    provider._runner = run
    provider._cleanup_resident(ATTACHMENT)
    assert resources == {} and any("rm" in command for command in calls)


def test_cleanup_removes_only_confirmed_immutable_relay_and_empty_networks(topology):
    adapter, resources, calls, _names = topology
    adapter._cleanup_resident(ATTACHMENT)
    assert resources == {}
    assert [command for command in calls if "rm" in command] == [
        ["docker", "rm", "--force", "1" * 64],
        ["docker", "network", "rm", "2" * 64],
        ["docker", "network", "rm", "3" * 64],
    ]


@pytest.mark.parametrize("fault", ["foreign-label", "storage-mount", "foreign-holder", "wrong-network", "unknown-primary"])
def test_cleanup_retains_uncertain_or_foreign_sets_without_mutation(topology, fault):
    adapter, resources, calls, (proxy, internal, _egress) = topology
    if fault == "foreign-label":
        resources[proxy]["Config"]["Labels"]["deerflow.sandbox_id"] = "foreign"
    elif fault == "storage-mount":
        resources[proxy]["Mounts"] = [{"Type": "bind", "Source": "/unknown/data"}]
    elif fault == "foreign-holder":
        resources[internal]["Containers"]["f" * 64] = {}
    elif fault == "wrong-network":
        resources[proxy]["NetworkSettings"]["Networks"][internal]["NetworkID"] = "f" * 64
    else:
        resources["hartmesh-space-" + ATTACHMENT] = {"Id": "f" * 64, "Name": "/hartmesh-space-" + ATTACHMENT}
    before = copy.deepcopy(resources)
    adapter._cleanup_resident(ATTACHMENT)
    assert resources == before
    assert not any("rm" in command for command in calls)


def test_cleanup_accepts_only_the_relays_declared_ephemeral_tmpfs(topology):
    adapter, resources, _calls, (proxy, _internal, _egress) = topology
    resources[proxy]["Mounts"] = [{"Type": "tmpfs", "Source": "", "Destination": "/tmp"}]
    adapter._cleanup_resident(ATTACHMENT)
    assert resources == {}


def test_failed_network_remove_is_reported_as_incomplete_best_effort(topology, caplog):
    adapter, resources, _calls, _names = topology
    engine = adapter._runner

    def failed(command, **kwargs):
        if command[1:3] == ["network", "rm"]:
            return SimpleNamespace(returncode=1, stdout="", stderr="removal refused")
        return engine(command, **kwargs)

    adapter._runner = failed
    adapter._cleanup_resident(ATTACHMENT)
    assert resources
    assert "cleanup remains incomplete" in caplog.text
