"""Actual membership must match the declared restricted network, even on reuse."""

import json
from dataclasses import replace
from types import SimpleNamespace

import pytest
from test_resident_topology import backend
from test_storage_spaces_docker import ID, PLAN, Docker, adapter

from deerflow.spaces.docker import StorageAdapterUnsupported


@pytest.fixture
def networked():
    local, _calls = backend("allowlist")
    docker = Docker()
    plan = replace(PLAN, views=(replace(PLAN.views[0], slot_id="d" * 32),))
    provider = adapter(docker)
    provider._backend = local
    name = local._resource_names(plan.id)[1]
    provider._network_of = lambda _plan: name
    _mounts, expected = provider._resident_projection(plan)
    docker.entry["Config"]["Labels"] = {**local._restricted_labels(plan.id, "resident"), **expected, "deerflow.role": "resident"}
    docker.entry["HostConfig"]["NetworkMode"] = name
    docker.entry["NetworkSettings"] = {"Networks": {name: {"NetworkID": "2" * 64}}}
    network = {
        "Id": "2" * 64,
        "Name": name,
        "Driver": "bridge",
        "Internal": True,
        "Labels": local._restricted_labels(plan.id, "network"),
        "Options": {"com.docker.network.bridge.gateway_mode_ipv4": "isolated", "com.docker.network.bridge.gateway_mode_ipv6": "isolated"},
    }

    def engine(command, **kwargs):
        if command[1:3] == ["network", "inspect"]:
            return SimpleNamespace(returncode=0, stdout=json.dumps([network]), stderr="")
        result = docker(command, **kwargs)
        if command[1] == "start":
            docker.entry["NetworkSettings"]["Networks"][name]["NetworkID"] = network["Id"]
        return result

    provider._runner = engine
    return provider, docker, plan, name, network


@pytest.mark.parametrize("active", [False, True])
@pytest.mark.parametrize("fault", ["extra-egress", "different-id", "wrong-policy", "default-mode"])
def test_validate_refuses_extra_or_unqualified_actual_networks(networked, active, fault):
    provider, docker, plan, name, network = networked
    if active:
        docker.entry["State"] = {"Status": "running", "Running": True}
    provider._validate(ID, plan, active=active)
    if fault == "extra-egress":
        docker.entry["NetworkSettings"]["Networks"]["direct-egress"] = {"NetworkID": "f" * 64}
    elif fault == "different-id":
        docker.entry["NetworkSettings"]["Networks"][name]["NetworkID"] = "f" * 64
    elif fault == "default-mode":
        docker.entry["HostConfig"]["NetworkMode"] = "default"
    else:
        network["Labels"]["deerflow.sandbox_id"] = "foreign"
    with pytest.raises(StorageAdapterUnsupported):
        provider._validate(ID, plan, active=active)
    assert not any(command[1] == "start" for command in docker.calls)


def test_created_resident_can_start_with_unconnected_endpoint_then_exact_network_id(networked):
    provider, docker, plan, name, network = networked
    docker.entry["NetworkSettings"]["Networks"][name]["NetworkID"] = ""
    provider.start(ID, plan)
    assert docker.entry["State"]["Running"] is True
    assert docker.entry["NetworkSettings"]["Networks"][name]["NetworkID"] == network["Id"]


def test_active_resident_without_connected_immutable_network_id_is_refused(networked):
    provider, docker, plan, name, _network = networked
    docker.entry["State"] = {"Status": "running", "Running": True}
    docker.entry["NetworkSettings"]["Networks"][name]["NetworkID"] = ""
    with pytest.raises(StorageAdapterUnsupported):
        provider.verify_active(ID, plan)
