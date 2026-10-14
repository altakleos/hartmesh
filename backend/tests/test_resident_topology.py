"""Offline builder doubles: command shape and provenance are not native proof."""

from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock

import pytest
from test_agent_instances import instances as instances
from test_native_create_qualification import fixture as fixture

from deerflow.community.aio_sandbox.local_backend import LocalContainerBackend
from deerflow.spaces.docker import StorageHostUnqualified

ATTACHMENT, SLOT, CONTAINER = "b" * 32, "d" * 32, "a" * 64
LABELS = {"hartmesh.storage.attachment": ATTACHMENT, "hartmesh.storage.host": "daemon-1", "hartmesh.storage.space." + SLOT: "1"}
MOUNTS = [("/qualified/data", "/mnt/spaces/home", False)]


def backend(mode="open"):
    result = LocalContainerBackend.__new__(LocalContainerBackend)
    result._runtime, result._image, result._container_prefix = "docker", "sandbox:test", "chat-prefix"
    result._environment, result._config_mounts = {}, []
    result._network_mode = mode
    result._network_config = {"mode": mode, "allow_domains": ["example.invalid"], "approval": "prompt", "proxy_image": "proxy:test"}
    result._required_shell_sessions, result._allow_synthetic_dns = 7, False
    result._managed_native_required, result._native_creation = False, None
    calls = []

    def run(command, **kwargs):
        calls.append((command, kwargs))
        return SimpleNamespace(returncode=0, stdout=CONTAINER, stderr="")

    result._runtime_runner = run
    return result, calls


def labels(command):
    return dict(command[index + 1].split("=", 1) for index, value in enumerate(command) if value == "--label")


def test_prepare_resident_creates_a_stopped_container_labelled_resident():
    local, calls = backend()
    assert local.prepare_resident(ATTACHMENT, "hartmesh-space-" + ATTACHMENT, MOUNTS, LABELS) == CONTAINER
    ((command, options),) = calls
    assert command[:2] == ["docker", "create"] and "--rm" in command
    assert "-p" not in command and "-d" not in command and options["timeout"] == 30
    expected = {**LABELS, "deerflow.role": "resident"}
    assert labels(command) == expected
    assert not any(key in expected for key in ("deerflow.owner_user_id", "deerflow.thread_id", "hartmesh.chat.storage"))
    assert "MAX_SHELL_SESSIONS=14" in command


def test_prepare_resident_inherits_runtime_user_seccomp_network_and_config_mounts(monkeypatch):
    local, calls = backend("allowlist")
    local._config_mounts = [SimpleNamespace(host_path=Path("/operator/tenant"), container_path="/mnt/tenant", read_only=True)]
    local._resident_resources_status = Mock(return_value="missing")
    local._restricted_resources_status = Mock(side_effect=AssertionError("Resident must not enter chat discovery"))
    local._create_internal_network, local._create_egress_network = Mock(), Mock()
    local._start_network_proxy = Mock(return_value="172.24.0.2")
    monkeypatch.setenv("DEER_FLOW_SANDBOX_RUNTIME", "runsc")
    monkeypatch.setenv("DEER_FLOW_SANDBOX_CONTAINER_USER", "1000:1000")
    monkeypatch.setenv("DEER_FLOW_SANDBOX_SECCOMP_PROFILE", "/operator/seccomp.json")
    monkeypatch.setenv("DEER_FLOW_SANDBOX_MEMORY", "1024m")
    monkeypatch.setenv("DEER_FLOW_SANDBOX_CPUS", "2")
    monkeypatch.setenv("DEER_FLOW_SANDBOX_PIDS_LIMIT", "256")
    assert local.prepare_resident(ATTACHMENT, "hartmesh-space-" + ATTACHMENT, MOUNTS, LABELS) == CONTAINER
    ((command, _options),) = calls
    proxy, network = local._resource_names(ATTACHMENT)
    assert command[command.index("--network") + 1] == network
    assert command[command.index("--runtime") + 1] == "runsc"
    assert command[command.index("--user") + 1] == "1000:1000"
    assert command[command.index("--memory") + 1] == "1024m"
    assert command[command.index("--cpus") + 1] == "2"
    assert command[command.index("--pids-limit") + 1] == "256"
    assert "seccomp=/operator/seccomp.json" in command and "no-new-privileges" in command
    assert "type=bind,src=/operator/tenant,dst=/mnt/tenant,readonly" in command
    assert "type=bind,src=/qualified/data,dst=/mnt/spaces/home" in command
    assert "-p" not in command and labels(command)["deerflow.role"] == "resident"
    assert "HTTPS_PROXY=http://" + proxy + ":3128" in command
    assert local._start_network_proxy.call_args.kwargs["publish"] is False
    local._restricted_resources_status.assert_not_called()


def test_first_restricted_resident_accepts_exact_docker29_lowercase_absence():
    local, calls = backend("allowlist")
    original = local._runtime_runner

    def run(command, **kwargs):
        if "inspect" in command:
            calls.append((command, kwargs))
            error = "error: no such object: " + command[-1] if command[1] == "inspect" else "Error: No such network: " + command[-1]
            return SimpleNamespace(returncode=1, stdout="[]", stderr=error)
        return original(command, **kwargs)

    local._runtime_runner = run
    local._create_internal_network, local._create_egress_network = Mock(), Mock()
    local._start_network_proxy = Mock(return_value="172.24.0.2")
    assert local.prepare_resident(ATTACHMENT, "hartmesh-space-" + ATTACHMENT, MOUNTS, LABELS) == CONTAINER
    assert any(command[1] == "create" for command, _kwargs in calls)


def test_resident_relay_publishes_no_port_and_carries_no_storage_label():
    local, calls = backend("allowlist")
    local._container_network_address = Mock(return_value="172.24.0.2")
    proxy, network = local._resource_names(ATTACHMENT)
    assert local._start_network_proxy(proxy, network, local._egress_network_name(ATTACHMENT), "hartmesh-space-" + ATTACHMENT, 0, ATTACHMENT, "synthetic-relay-token", publish=False) == "172.24.0.2"
    create = next(command for command, _options in calls if command[1] == "create")
    assert "-p" not in create
    assert labels(create)["deerflow.role"] == "network-proxy"
    assert not any(key.startswith("hartmesh.storage.") for key in labels(create))


@pytest.mark.parametrize("managed", [False, True])
def test_direct_resident_builder_without_capability_submits_nothing_even_without_slot_labels(managed):
    local, calls = backend()
    local._managed_native_required = managed
    with pytest.raises(StorageHostUnqualified):
        local._start_container("hartmesh-space-" + ATTACHMENT, 0, MOUNTS, start=False, publish_port=False, labels={"deerflow.role": "resident"})
    assert calls == []


@pytest.mark.parametrize("status", ["compatible", "mismatch"])
def test_resident_preparation_never_adopts_or_removes_existing_resource_sets(status):
    local, calls = backend("allowlist")
    local._resident_resources_status = Mock(return_value=status)
    local.discover = Mock(side_effect=AssertionError("Resident cannot adopt a chat"))
    local._cleanup_restricted_resources = Mock(side_effect=AssertionError("Existing set must not be guessed away"))
    with pytest.raises(StorageHostUnqualified):
        local.prepare_resident(ATTACHMENT, "hartmesh-space-" + ATTACHMENT, MOUNTS, LABELS)
    assert calls == []
    local.discover.assert_not_called()
    local._cleanup_restricted_resources.assert_not_called()


def test_uncertain_resident_creation_never_uses_name_based_rollback():
    local, calls = backend("allowlist")
    local._resident_resources_status = Mock(return_value="missing")
    local._create_internal_network = Mock(side_effect=RuntimeError("First topology effect unconfirmed"))
    local._create_egress_network = Mock()
    local._cleanup_restricted_resources = Mock(side_effect=AssertionError("Uncertain resource set cannot be guessed away"))
    with pytest.raises(RuntimeError):
        local.prepare_resident(ATTACHMENT, "hartmesh-space-" + ATTACHMENT, MOUNTS, LABELS)
    assert calls == []
    local._create_egress_network.assert_not_called()
    local._cleanup_restricted_resources.assert_not_called()


def test_resident_absence_is_bound_to_each_exact_resource():
    local, _calls = backend("allowlist")
    inspected = []

    def absent(command, **kwargs):
        inspected.append(command)
        name = command[-1]
        error = "Error: No such object: " + name if command[1] == "inspect" else "Error response from daemon: network " + name + " not found"
        return SimpleNamespace(returncode=1, stdout="[]", stderr=error)

    local._runtime_runner = absent
    assert local._resident_resources_status(ATTACHMENT, "hartmesh-space-" + ATTACHMENT) == "missing"
    proxy, network = local._resource_names(ATTACHMENT)
    assert inspected == [
        ["docker", "inspect", "hartmesh-space-" + ATTACHMENT],
        ["docker", "inspect", proxy],
        ["docker", "network", "inspect", network],
        ["docker", "network", "inspect", local._egress_network_name(ATTACHMENT)],
    ]


def test_resident_preflight_does_not_accept_an_unrelated_network_absence():
    local, _calls = backend("allowlist")

    def ambiguous(command, **kwargs):
        error = "Error: No such object: " + command[-1] if command[1] == "inspect" else "Error response from daemon: network unrelated not found"
        return SimpleNamespace(returncode=1, stdout="[]", stderr=error)

    local._runtime_runner = ambiguous
    with pytest.raises(StorageHostUnqualified):
        local._resident_resources_status(ATTACHMENT, "hartmesh-space-" + ATTACHMENT)


def test_successful_empty_container_inspection_is_not_absence():
    local, _calls = backend("allowlist")

    def malformed(command, **kwargs):
        return SimpleNamespace(returncode=0, stdout="[]", stderr="")

    local._runtime_runner = malformed
    assert local._resident_resources_status(ATTACHMENT, "hartmesh-space-" + ATTACHMENT) == "mismatch"


@pytest.mark.parametrize("change", ["mode", "policy", "mount"])
def test_resident_capability_cannot_follow_a_changed_topology(change):
    from deerflow.community.aio_sandbox.resident_preparation import resident_submission

    local, calls = backend("allowlist")
    with resident_submission(local, ATTACHMENT, "hartmesh-space-" + ATTACHMENT, MOUNTS, LABELS) as proof:
        if change == "mode":
            local._network_mode = "open"
        elif change == "policy":
            local._network_config["allow_domains"] = ["changed.invalid"]
        else:
            local._config_mounts = [SimpleNamespace(host_path=Path("/changed"), container_path="/changed", read_only=True)]
        with pytest.raises(StorageHostUnqualified):
            proof.assert_current()
    assert calls == []


@pytest.fixture
def managed(fixture, monkeypatch):
    from deerflow.spaces.attachments import AttachmentPlan, NativeView
    from deerflow.spaces.contract import PrincipalRef

    _pool, provider, owner, _files, adapter, _getter = fixture
    local, calls = backend()
    provider._backend = local
    owner.resolve_provider(lambda: provider)
    local._runtime_runner = owner.run
    adapter._backend = local
    adapter._resident_projection = lambda plan: (MOUNTS, {**LABELS, "hartmesh.storage.host": plan.host_id, "hartmesh.storage.space." + plan.views[0].slot_id: "1"})
    evidence = owner.qualify_native_evidence(provider, daemon_id=adapter.host_id)
    provider._install_native_creation(evidence, adapter)

    def engine(command, **kwargs):
        calls.append((command, kwargs))
        return SimpleNamespace(returncode=0, stdout=CONTAINER, stderr="")

    monkeypatch.setattr("deerflow.spaces.runtime_owner.subprocess.run", engine)
    plan = AttachmentPlan(ATTACHMENT, "c" * 32, PrincipalRef("nonhuman", "ai-employee"), adapter.host_id, (NativeView("e" * 32, 1, MOUNTS[0][0], MOUNTS[0][1], True, SLOT),))
    admitted_labels = {**LABELS, "hartmesh.storage.host": adapter.host_id}
    return local, calls, provider, adapter, plan, admitted_labels


def test_managed_resident_without_committed_preparation_token_has_no_effect(managed):
    local, calls, _provider, _adapter, _plan, admitted_labels = managed
    with pytest.raises(StorageHostUnqualified):
        local.prepare_resident(ATTACHMENT, "hartmesh-space-" + ATTACHMENT, MOUNTS, admitted_labels)
    assert calls == []


def test_managed_resident_uses_owned_runner_without_chat_journal_and_holds_slots_to_commit(managed):
    from deerflow.spaces.preparation import preparation_scope

    local, calls, provider, adapter, plan, admitted_labels = managed
    gate = provider._owner_admission_gate()
    with preparation_scope(adapter, plan) as token:
        assert local.prepare_resident(ATTACHMENT, "hartmesh-space-" + ATTACHMENT, MOUNTS, admitted_labels) == CONTAINER
        token.finish_worker()
        with pytest.raises(StorageHostUnqualified):
            gate.reserve("unrelated-human", SLOT)
        assert provider._native_creation.evidence.pending() == ()
    retirement = gate.reserve("unrelated-human", SLOT)
    gate.end_retirement(retirement)
    ((command, options),) = calls
    assert command[:2] == ["docker", "create"] and options["pass_fds"]
    assert labels(command)["deerflow.role"] == "resident"


@pytest.mark.parametrize("fault", ["runner", "missing-slot", "swapped-slot", "copied-expired"])
def test_managed_resident_rejects_changed_or_expired_admission_before_effects(managed, fault):
    from contextvars import copy_context
    from dataclasses import replace

    from deerflow.spaces.preparation import preparation_scope

    local, calls, _provider, adapter, plan, admitted_labels = managed
    if fault in ("missing-slot", "swapped-slot"):
        plan = replace(plan, views=(replace(plan.views[0], slot_id=None if fault == "missing-slot" else "f" * 32),))
    with preparation_scope(adapter, plan) as token:
        if fault == "runner":
            local._runtime_runner = lambda *a, **k: pytest.fail("Foreign runner must not execute")
        if fault == "copied-expired":
            context = copy_context()
            token.finish_worker()
            with pytest.raises(StorageHostUnqualified):
                context.run(local.prepare_resident, ATTACHMENT, "hartmesh-space-" + ATTACHMENT, MOUNTS, admitted_labels)
        else:
            with pytest.raises(StorageHostUnqualified):
                local.prepare_resident(ATTACHMENT, "hartmesh-space-" + ATTACHMENT, MOUNTS, admitted_labels)
    assert calls == []


def test_managed_resident_token_cannot_submit_a_second_primary(managed):
    from deerflow.spaces.preparation import preparation_scope

    local, calls, _provider, adapter, plan, admitted_labels = managed
    with preparation_scope(adapter, plan):
        local.prepare_resident(ATTACHMENT, "hartmesh-space-" + ATTACHMENT, MOUNTS, admitted_labels)
        with pytest.raises(StorageHostUnqualified):
            local.prepare_resident(ATTACHMENT, "hartmesh-space-" + ATTACHMENT, MOUNTS, admitted_labels)
    assert len(calls) == 1


@pytest.mark.asyncio
async def test_prepare_resident_binds_shared_read_only_and_labels_both_spaces(managed, instances):
    from dataclasses import replace

    from deerflow.spaces.preparation import preparation_scope
    from deerflow.spaces.readonly_roots import ReadOnlyRoot

    local, calls, provider, adapter, plan, admitted_labels = managed
    _agents, _files, _sf, catalog = instances
    volume = catalog.verify(next(iter(catalog.volumes)))
    (volume.data_path / "shared").mkdir()
    root = ReadOnlyRoot.capture(volume, "shared", "/mnt/user-data/shared")
    plan = replace(plan, readonly_roots=(root,))
    mounts = [*MOUNTS, (root.source, root.destination, True)]
    admitted_labels = {**admitted_labels, "hartmesh.storage.space." + root.slot_id: "1", root.label: root.revision}
    adapter._resident_projection = lambda _plan: (mounts, admitted_labels)
    gate = provider._owner_admission_gate()
    with preparation_scope(adapter, plan) as token:
        local.prepare_resident(ATTACHMENT, "hartmesh-space-" + ATTACHMENT, mounts, admitted_labels)
        token.finish_worker()
        for slot in (SLOT, root.slot_id):
            with pytest.raises(StorageHostUnqualified):
                gate.reserve("unrelated-human", slot)
    ((command, _options),) = calls
    assert "type=bind,src=" + root.source + ",dst=/mnt/user-data/shared,readonly" in command
    assert not any("dst=/mnt/user-data/files" in value for value in command)
    for slot in (SLOT, root.slot_id):
        assert labels(command)["hartmesh.storage.space." + slot] == "1"


def test_managed_lost_ownership_after_create_does_not_publish_or_retry(managed, monkeypatch):
    from deerflow.spaces.preparation import preparation_scope

    local, calls, _provider, adapter, plan, admitted_labels = managed

    def changed(command, **kwargs):
        calls.append((command, kwargs))
        local._native_creation.invalidate()
        return SimpleNamespace(returncode=0, stdout=CONTAINER, stderr="")

    monkeypatch.setattr("deerflow.spaces.runtime_owner.subprocess.run", changed)
    with preparation_scope(adapter, plan):
        with pytest.raises(StorageHostUnqualified):
            local.prepare_resident(ATTACHMENT, "hartmesh-space-" + ATTACHMENT, MOUNTS, admitted_labels)
        with pytest.raises(StorageHostUnqualified):
            local.prepare_resident(ATTACHMENT, "hartmesh-space-" + ATTACHMENT, MOUNTS, admitted_labels)
    assert len(calls) == 1
