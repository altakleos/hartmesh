"""Owned resident preparation persists its original graph before CID return."""

import json
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import Mock

import pytest
from test_native_create_qualification import fixture as fixture
from test_resident_topology import ATTACHMENT, CONTAINER, MOUNTS, labels
from test_resident_topology import managed as managed

from deerflow.spaces.docker import StorageHostUnqualified
from deerflow.spaces.native_resident_evidence import NativeResidentRoot
from deerflow.spaces.preparation import preparation_scope


def admitted(managed, monkeypatch):
    local, calls, provider, adapter, plan, admitted_labels = managed
    view = plan.views[0]
    root = NativeResidentRoot(view.slot_id, view.source, view.destination, True, "1" * 32, "11111111-1111-4111-8111-111111111111", 10, 11, 12, 11, view.space_id, view.generation, None)
    plan = replace(plan, views=(replace(view, native_root=root),))
    original = {}
    from deerflow.community.aio_sandbox.resident_preparation import _network_target

    target = _network_target(local, plan.id)
    shared = {"Id": "0" * 64, "Name": target, "Created": "2026-10-14T00:00:00Z", "Containers": {}}

    def engine(command, **kwargs):
        calls.append((command, kwargs))
        if command[1] == "create":
            original.update(
                Id=CONTAINER,
                Name="/hartmesh-space-" + ATTACHMENT,
                Created="2026-10-14T00:00:00.123456789Z",
                Config={"Labels": labels(command)},
                HostConfig={"NetworkMode": target, "AutoRemove": True, "PortBindings": {}},
                State={"Running": False, "Status": "created"},
                Mounts=[{"Type": "bind", "Source": source, "Destination": destination, "RW": not readonly, "Propagation": "rprivate"} for source, destination, readonly in MOUNTS],
                NetworkSettings={"Networks": {target: {"NetworkID": "", "EndpointID": ""}}, "Ports": {}},
            )
            return SimpleNamespace(returncode=0, stdout=CONTAINER, stderr="")
        if command[1] == "ps" or command[1:3] == ["network", "ls"]:
            return SimpleNamespace(returncode=0, stdout="", stderr="")
        if command[1] == "inspect":
            if command[-1] in (CONTAINER, original.get("Name", "").removeprefix("/")):
                return SimpleNamespace(returncode=0, stdout=json.dumps([original]), stderr="")
            return SimpleNamespace(returncode=1, stdout="[]", stderr="Error: No such object: " + command[-1])
        if command[1:3] == ["network", "inspect"]:
            assert command[-1] in (target, shared["Id"])
            return SimpleNamespace(returncode=0, stdout=json.dumps([shared]), stderr="")
        pytest.fail("Unexpected Docker operation: " + command[1])

    monkeypatch.setattr("deerflow.spaces.runtime_owner.subprocess.run", engine)
    return local, calls, provider, adapter, plan, admitted_labels, original


def test_stopped_preparation_durably_captures_original_graph_before_cid_return(managed, monkeypatch):
    local, calls, provider, adapter, plan, admitted_labels, original = admitted(managed, monkeypatch)
    with preparation_scope(adapter, plan):
        assert local.prepare_resident(ATTACHMENT, "hartmesh-space-" + ATTACHMENT, MOUNTS, admitted_labels) == CONTAINER
        snapshot = provider._native_creation.evidence.snapshot()
        assert len(snapshot.residents) == 1
        saved = snapshot.residents[0]
        assert (saved.attachment_id, saved.incarnation, saved.container_id, saved.created_at) == (plan.id, plan.incarnation, CONTAINER, original["Created"])
        assert saved.phase == "prepared" and saved.roots == (plan.views[0].native_root,)
        assert provider._native_creation.residents.records() == (saved,)
        assert not any(command[1] in {"start", "rm", "stop"} for command, _options in calls)


def test_original_graph_publication_failure_returns_no_cid_and_retains_capacity(managed, monkeypatch):
    local, calls, provider, adapter, plan, admitted_labels, _original = admitted(managed, monkeypatch)
    journal = provider._native_creation.evidence
    publish = journal.begin_resident

    def interrupted(record):
        publish(record)
        raise OSError("Final publication proof was lost")

    monkeypatch.setattr(journal, "begin_resident", interrupted)
    with preparation_scope(adapter, plan):
        with pytest.raises(StorageHostUnqualified):
            local.prepare_resident(ATTACHMENT, "hartmesh-space-" + ATTACHMENT, MOUNTS, admitted_labels)
    snapshot = journal.snapshot()
    assert len(snapshot.residents) == 1 and snapshot.residents[0].phase == "prepared"
    with provider._lock:
        assert provider._counted_slots_locked() == (3, 0, 0, 1)
    assert not any(command[1] in {"start", "rm", "stop"} for command, _options in calls)


def test_supported_custom_network_is_captured_as_original_target_and_identity(managed, monkeypatch):
    monkeypatch.setenv("DEER_FLOW_SANDBOX_NETWORK", "hm-custom")
    local, _calls, provider, adapter, plan, admitted_labels, _original = admitted(managed, monkeypatch)
    with preparation_scope(adapter, plan):
        assert local.prepare_resident(ATTACHMENT, "hartmesh-space-" + ATTACHMENT, MOUNTS, admitted_labels) == CONTAINER
    saved = provider._native_creation.evidence.snapshot().residents[0]
    assert saved.network_target == "hm-custom"
    assert (saved.shared_network.name, saved.shared_network.identity) == ("hm-custom", "0" * 64)


@pytest.mark.parametrize("change", ["missing", "retiring", "incarnation", "root", "during-validation"])
def test_managed_activation_refuses_changed_original_record_before_start(managed, monkeypatch, change):
    from deerflow.spaces.docker import DockerStorageAdapter

    local, _calls, provider, adapter, plan, admitted_labels, _original = admitted(managed, monkeypatch)
    with preparation_scope(adapter, plan):
        local.prepare_resident(ATTACHMENT, "hartmesh-space-" + ATTACHMENT, MOUNTS, admitted_labels)
    bridge = provider._native_creation
    saved = bridge.residents.for_attachment(ATTACHMENT)
    actual = DockerStorageAdapter.__new__(DockerStorageAdapter)
    actual._operator_only = False
    actual._backend = local
    actual._configured_mounts = ()
    actual._containment_scope = SimpleNamespace(provider=provider)
    actual._call = Mock(return_value=SimpleNamespace(returncode=0))
    actual._validate = Mock()
    if change == "missing":
        monkeypatch.setattr(bridge.residents, "for_attachment", lambda _id: None)
    elif change == "retiring":
        bridge.evidence.retire_resident(saved)
        bridge.qualify()
    elif change == "incarnation":
        plan = replace(plan, incarnation="f" * 32)
    elif change == "root":
        plan = replace(plan, views=(replace(plan.views[0], native_root=replace(plan.views[0].native_root, root_inode=99)),))
    else:

        def retire(*_args, **_kwargs):
            bridge.evidence.retire_resident(saved)
            bridge.qualify()

        actual._validate.side_effect = retire
    with pytest.raises(StorageHostUnqualified):
        actual.start(CONTAINER, plan)
    actual._call.assert_not_called()


@pytest.mark.parametrize("target", [None, "default", "bridge", "hm-custom"])
def test_managed_activation_and_resume_accept_the_original_open_target(managed, monkeypatch, target):
    from deerflow.spaces.docker import DockerStorageAdapter

    if target is None:
        monkeypatch.delenv("DEER_FLOW_SANDBOX_NETWORK", raising=False)
    else:
        monkeypatch.setenv("DEER_FLOW_SANDBOX_NETWORK", target)
    local, _calls, provider, adapter, plan, admitted_labels, _original = admitted(managed, monkeypatch)
    with preparation_scope(adapter, plan):
        local.prepare_resident(ATTACHMENT, "hartmesh-space-" + ATTACHMENT, MOUNTS, admitted_labels)
    actual = DockerStorageAdapter.__new__(DockerStorageAdapter)
    actual._operator_only = False
    actual._backend, actual._configured_mounts = local, ()
    actual._containment_scope = SimpleNamespace(provider=provider)
    actual._validate = Mock(return_value="validated")
    actual._call = Mock(return_value=SimpleNamespace(returncode=0))
    actual.start(CONTAINER, plan)
    actual._call.assert_called_once_with("start", CONTAINER)
    assert actual.verify_active(CONTAINER, plan) == "validated"
