"""Actual LocalContainerBackend builds requests through the managed journal."""

import json
import subprocess
import time
from types import SimpleNamespace
from unittest.mock import Mock

import pytest
from test_native_create_bridge import CONTAINER, LABELS, SLOT
from test_native_create_bridge import fixture as fixture

from deerflow.community.aio_sandbox.local_backend import LocalContainerBackend
from deerflow.spaces.docker import StorageHostUnqualified
from deerflow.spaces.native_create_evidence import ATTEMPT_LABEL


def backend(bridge):
    result = LocalContainerBackend.__new__(LocalContainerBackend)
    result._runtime, result._image, result._container_prefix = "docker", "image", "native"
    result._config_mounts, result._environment = [], {}
    result._network_mode, result._network_config = "open", {"mode": "open"}
    result._base_port, result._required_shell_sessions, result._allow_synthetic_dns = 19080, 0, False
    result._managed_native_required, result._native_creation = True, bridge
    return result


@pytest.mark.parametrize("start", [False, True])
def test_actual_builder_records_before_submission_and_retains_verified_result(fixture, start):
    journal, state, bridge, _owner = fixture
    local = backend(bridge)
    calls, seen = [], []

    def run(cmd, **kwargs):
        calls.append(cmd)
        if cmd[1] in {"run", "create"}:
            (pending,) = journal.pending()
            assert ATTEMPT_LABEL + "=" + pending.id in cmd
            seen.append(pending)
            assert ("timeout" not in kwargs) if start else kwargs["timeout"] == 30
            return SimpleNamespace(stdout=CONTAINER, stderr="", returncode=0)
        assert cmd == ["docker", "inspect", CONTAINER]
        entry = {"Id": CONTAINER, "Name": "/native-a", "Config": {"Labels": {**LABELS, ATTEMPT_LABEL: seen[0].id}}}
        return SimpleNamespace(stdout=json.dumps([entry]), stderr="", returncode=0)

    local._runtime_runner = run
    assert local._start_container("native-a", 19080, labels=LABELS, start=start) == CONTAINER
    assert len(calls) == 2 and journal.pending() == ()
    with state.lock:
        assert state.extra_slots_locked(starting={}, known_container_ids=set()) == 1


@pytest.mark.parametrize("message", ["port is already allocated", "Conflict. The container name is already in use by container"])
def test_actual_create_does_not_retry_or_adopt_past_uncertain_native_command(fixture, monkeypatch, message):
    journal, state, bridge, _owner = fixture
    local = backend(bridge)
    calls = []
    local.discover = Mock(return_value=None)
    monkeypatch.setattr("deerflow.community.aio_sandbox.local_backend.get_free_port", lambda **kwargs: 19080)

    def fail(cmd, **kwargs):
        calls.append(cmd)
        raise subprocess.CalledProcessError(125, cmd, stderr=message)

    local._runtime_runner = fail
    with pytest.raises(RuntimeError):
        local.create("chat", "a", user_id="alice", storage_labels={"hartmesh.storage.space." + SLOT: "1"})
    assert len(calls) == 1 and len(journal.pending()) == 1
    local.discover.assert_not_called()
    assert state.pending_for(owner_id="alice")


def test_managed_native_without_installed_bridge_starts_no_command(fixture, monkeypatch):
    _journal, _state, bridge, _owner = fixture
    local = backend(bridge)
    local._native_creation = None
    local._runtime_runner = Mock(side_effect=AssertionError("Unqualified managed primary submitted"))
    monkeypatch.setattr("deerflow.community.aio_sandbox.local_backend.get_free_port", lambda **kwargs: 19080)
    with pytest.raises(StorageHostUnqualified):
        local.create("chat", "a", user_id="alice", storage_labels={"hartmesh.storage.space." + SLOT: "1"})
    local._runtime_runner.assert_not_called()


def test_storage_off_keeps_ordinary_command_without_journal(fixture, monkeypatch):
    journal, _state, bridge, owner = fixture
    local = backend(bridge)
    owner.live = False
    calls = []
    monkeypatch.setattr("deerflow.community.aio_sandbox.local_backend.get_free_port", lambda **kwargs: 19080)
    local._runtime_runner = lambda cmd, **kwargs: calls.append(cmd) or SimpleNamespace(stdout=CONTAINER, stderr="", returncode=0)
    info = local.create("chat", "a", user_id="alice", storage_labels=None)
    assert info.container_id == CONTAINER and len(calls) == 1
    assert not any(value.startswith(ATTEMPT_LABEL + "=") for value in calls[0])
    assert journal.pending() == ()


def test_managed_nouser_create_retains_pending_on_uncertain_primary(fixture, monkeypatch):
    from deerflow.spaces.native_create_evidence import NativeMountlessCreateAttempt

    journal, state, bridge, _owner = fixture
    local = backend(bridge)
    local.discover = Mock(side_effect=AssertionError("Cannot adopt an uncertain request"))
    monkeypatch.setattr("deerflow.community.aio_sandbox.local_backend.get_free_port", lambda **kwargs: 19080)
    observed = []

    def run(cmd, **kwargs):
        (pending,) = journal.pending()
        assert isinstance(pending, NativeMountlessCreateAttempt) and pending.slots == ()
        assert ATTEMPT_LABEL + "=" + pending.id in cmd
        observed.append(pending)
        raise subprocess.CalledProcessError(125, cmd, stderr="uncertain result")

    local._runtime_runner = run
    with pytest.raises(StorageHostUnqualified):
        local.create("chat", "a", user_id="alice", storage_labels={})
    assert journal.pending() == tuple(observed)
    assert state.pending_for(owner_id="alice")
    local.discover.assert_not_called()


def restricted(bridge, monkeypatch, journal):
    """A backend whose first network effect fails, recording the journal at each step."""
    local = backend(bridge)
    local._network_mode, local._network_config = "isolated", {"mode": "isolated"}
    local._restricted_resources_status = Mock(return_value="missing")
    local._create_egress_network = Mock(side_effect=AssertionError("First network failure should stop provisioning"))
    monkeypatch.setattr("deerflow.community.aio_sandbox.local_backend.get_free_port", lambda **kwargs: 19080)
    local.slept = []
    monkeypatch.setattr("deerflow.community.aio_sandbox.local_backend.time.sleep", local.slept.append)
    local.seen = {"effect": [], "cleanup": []}

    def fail_first_effect(*args):
        local.seen["effect"].append(journal.pending())
        raise RuntimeError("Network outcome unconfirmed")

    def cleanup(sandbox_id, *, stop_sandbox=True):
        assert stop_sandbox is False, "An attempt that sent no primary has no primary to stop"
        local.seen["cleanup"].append(journal.pending())

    local._create_internal_network = fail_first_effect
    local._cleanup_restricted_resources = Mock(side_effect=cleanup)
    return local


def test_a_restricted_failure_before_the_primary_costs_one_attempt_not_the_owner(fixture, monkeypatch):
    journal, state, bridge, _owner = fixture
    local = restricted(bridge, monkeypatch, journal)
    local._restricted_resources_status = Mock(side_effect=["missing", "mismatch", "missing"])
    with pytest.raises(StorageHostUnqualified):
        local.create("chat", "a", user_id="alice", storage_labels={"hartmesh.storage.space." + SLOT: "1"})
    (seen,) = local.seen["effect"]
    assert len(seen) == 1, "The record precedes every sidecar and network effect"
    assert local.seen["cleanup"] == [seen], "The record holds the name while the set goes"
    assert journal.pending() == () and not state.pending_for(owner_id="alice")
    with state.lock:
        assert state.extra_slots_locked(starting={}, known_container_ids=set()) == 0
    bridge.require_admissible(name="native-a", owner_id="alice")


def with_leftover(local, *, primary=False, relay=None, removable=True):
    """The set as a crash leaves it, answered the way Docker answers ``inspect``."""
    proxy_name, network_name = local._resource_names("a")
    labels = {"deerflow.sandbox_id": "a", "deerflow.network_mode": "allowlist", "deerflow.network_policy_digest": "8" * 64}
    objects = {
        ("container", proxy_name): {
            "Id": "f" * 64,
            "Name": "/" + proxy_name,
            "Created": "2026-10-01T12:00:00Z",
            "Mounts": [],
            "HostConfig": {"Privileged": False, "CapDrop": ["ALL"], "SecurityOpt": ["no-new-privileges"], "PidMode": "", "IpcMode": "private", "RestartPolicy": {"Name": "no"}},
            "Config": {"Labels": {**labels, "deerflow.role": "network-proxy"}},
            **(relay or {}),
        },
        ("network", network_name): {"Id": "1" * 64, "Name": network_name, "Created": "2026-10-01T12:00:00Z", "Labels": {**labels, "deerflow.role": "network"}, "Containers": {}},
        ("network", local._egress_network_name("a")): {"Id": "2" * 64, "Name": local._egress_network_name("a"), "Created": "2026-10-01T12:00:00Z", "Labels": {**labels, "deerflow.role": "egress-network"}, "Containers": {}},
    }
    if primary:
        objects[("container", "native-a")] = {"Id": "c" * 64, "Name": "/native-a", "Created": "2026-10-01T12:00:00Z", "Mounts": [], "Config": {"Labels": {**labels, "deerflow.role": "sandbox"}}}
    local.leftover = objects

    def run(command, **kwargs):
        kind, reference = ("network", command[3]) if command[1] == "network" else ("container", command[2])
        found = [entry for (entry_kind, name), entry in objects.items() if entry_kind == kind and reference in (name, entry["Id"])]
        if found:
            return SimpleNamespace(returncode=0, stdout=json.dumps(found), stderr="")
        return SimpleNamespace(returncode=1, stdout="[]", stderr=("Error: No such object: " if kind == "container" else "Error: No such network: ") + reference)

    def cleanup(sandbox_id, *, stop_sandbox=True):
        local.seen["cleanup"].append(local.journal.pending())
        if removable:
            objects.clear()

    local._runtime_runner = run
    local._restricted_resources_status = Mock(side_effect=lambda *args, **kwargs: "mismatch" if objects else "missing")
    local._cleanup_restricted_resources = Mock(side_effect=cleanup)


def start_with_leftover(fixture, monkeypatch, **shape):
    journal, _state, bridge, _owner = fixture
    local = restricted(bridge, monkeypatch, journal)
    local.journal = journal
    with_leftover(local, **shape)
    return local


LABELS_OF_A_CHAT = {"hartmesh.storage.space." + SLOT: "1"}


def test_a_conversation_clears_its_leftover_and_starts_afresh(fixture, monkeypatch):
    local = start_with_leftover(fixture, monkeypatch)
    with pytest.raises(StorageHostUnqualified):
        local.create("chat", "a", user_id="alice", storage_labels=LABELS_OF_A_CHAT)
    assert local.seen["cleanup"] == [()], "Nobody's set goes with no record, and the set is gone before the attempt"
    assert len(local.seen["effect"]) == 1 and len(local.seen["effect"][0]) == 1, "The fresh start records itself and takes its first effect"


def test_a_primary_beside_the_set_is_not_a_leftover(fixture, monkeypatch):
    local = start_with_leftover(fixture, monkeypatch, primary=True)
    with pytest.raises(RuntimeError, match="ownership-fenced replacement"):
        local.create("chat", "a", user_id="alice", storage_labels=LABELS_OF_A_CHAT)
    assert not local.seen["cleanup"] and not local.seen["effect"]


@pytest.mark.parametrize(
    "relay,message",
    [
        ({"Mounts": [{"Type": "bind", "Source": "/pool/slot", "Destination": "/data", "RW": True}]}, "host storage mount"),
        ({"HostConfig": {"Privileged": True}}, "not confined"),
        ({"Config": {"Labels": {"deerflow.sandbox_id": "other", "deerflow.role": "network-proxy", "deerflow.network_mode": "allowlist", "deerflow.network_policy_digest": "8" * 64}}}, "holds a name"),
    ],
)
def test_a_leftover_that_fails_the_proof_stays_and_refuses(fixture, monkeypatch, relay, message):
    local = start_with_leftover(fixture, monkeypatch, relay=relay)
    with pytest.raises(StorageHostUnqualified, match=message):
        local.create("chat", "a", user_id="alice", storage_labels=LABELS_OF_A_CHAT)
    assert not local.seen["cleanup"] and not local.seen["effect"] and local.journal.pending() == ()


def test_a_leftover_that_will_not_go_keeps_the_conversation_refused(fixture, monkeypatch):
    local = start_with_leftover(fixture, monkeypatch, removable=False)
    with pytest.raises(RuntimeError, match="ownership-fenced replacement"):
        local.create("chat", "a", user_id="alice", storage_labels=LABELS_OF_A_CHAT)
    assert local.seen["cleanup"] and not local.seen["effect"] and local.journal.pending() == ()


def test_outside_a_managed_pool_the_set_stays_the_providers_to_replace(fixture, monkeypatch):
    local = start_with_leftover(fixture, monkeypatch)
    local._managed_native_required = False
    with pytest.raises(RuntimeError, match="ownership-fenced replacement"):
        local.create("chat", "a", user_id="alice", storage_labels=LABELS_OF_A_CHAT)
    assert not local.seen["cleanup"]


def test_a_pending_record_keeps_its_set(fixture, monkeypatch):
    journal, _state, bridge, _owner = fixture
    local = start_with_leftover(fixture, monkeypatch)
    journal.begin(owner_id="bob", thread_id="chat", name="native-a", slots=(SLOT,))
    bridge.qualify()
    with pytest.raises(StorageHostUnqualified, match="unresolved"):
        local.create("chat", "a", user_id="alice", storage_labels=LABELS_OF_A_CHAT)
    assert not local.seen["cleanup"]


def test_a_runtime_that_stops_being_current_removes_nothing(fixture, monkeypatch):
    _journal, _state, bridge, _owner = fixture
    local = start_with_leftover(fixture, monkeypatch)
    calls = iter(((),))

    def current():
        try:
            return next(calls)
        except StopIteration:
            raise StorageHostUnqualified("Runtime closed") from None

    monkeypatch.setattr(bridge, "assert_current", current)
    with pytest.raises(StorageHostUnqualified):
        local.create("chat", "a", user_id="alice", storage_labels=LABELS_OF_A_CHAT)
    assert not local.seen["cleanup"]


def test_a_set_that_never_appeared_needs_no_cleanup(fixture, monkeypatch):
    journal, state, bridge, _owner = fixture
    local = restricted(bridge, monkeypatch, journal)
    with pytest.raises(StorageHostUnqualified):
        local.create("chat", "a", user_id="alice", storage_labels={"hartmesh.storage.space." + SLOT: "1"})
    local._cleanup_restricted_resources.assert_not_called()
    assert journal.pending() == () and not state.pending_for(owner_id="alice")


def test_a_proxy_still_being_removed_is_waited_out(fixture, monkeypatch):
    journal, state, bridge, _owner = fixture
    local = restricted(bridge, monkeypatch, journal)
    local._restricted_resources_status = Mock(side_effect=["missing", "mismatch", "mismatch", "missing"])
    with pytest.raises(StorageHostUnqualified):
        local.create("chat", "a", user_id="alice", storage_labels={"hartmesh.storage.space." + SLOT: "1"})
    assert local._cleanup_restricted_resources.call_count == 2
    assert local.slept == [1, 1]
    assert journal.pending() == () and not state.pending_for(owner_id="alice")


def test_removing_the_set_has_one_deadline_of_its_own(fixture, monkeypatch):
    from deerflow.community.aio_sandbox.local_backend import _NATIVE_SLOT_INVENTORY_DEADLINE

    journal, _state, bridge, _owner = fixture
    local = restricted(bridge, monkeypatch, journal)
    local._restricted_resources_status = Mock(side_effect=["missing", "mismatch", "missing"])
    seen = []
    local._cleanup_restricted_resources = Mock(side_effect=lambda *args, **kwargs: seen.append(_NATIVE_SLOT_INVENTORY_DEADLINE.get()))
    with pytest.raises(StorageHostUnqualified):
        local.create("chat", "a", user_id="alice", storage_labels={"hartmesh.storage.space." + SLOT: "1"})
    assert len(seen) == 1 and 0 < seen[0] - time.monotonic() <= 60
    assert _NATIVE_SLOT_INVENTORY_DEADLINE.get() is None


def test_an_inherited_deadline_stays_the_shorter_one(fixture, monkeypatch):
    from deerflow.community.aio_sandbox.local_backend import _NATIVE_SLOT_INVENTORY_DEADLINE

    journal, _state, bridge, _owner = fixture
    local = restricted(bridge, monkeypatch, journal)
    inherited = time.monotonic() + 5
    seen = []
    local._restricted_resources_status = Mock(side_effect=lambda sandbox_id: (seen.append(_NATIVE_SLOT_INVENTORY_DEADLINE.get()), "missing")[1])
    token = _NATIVE_SLOT_INVENTORY_DEADLINE.set(inherited)
    try:
        assert local._remove_partial_set("a") is True
        assert _NATIVE_SLOT_INVENTORY_DEADLINE.get() == inherited
    finally:
        _NATIVE_SLOT_INVENTORY_DEADLINE.reset(token)
    assert seen == [inherited]


def test_a_spent_deadline_keeps_the_attempt_reserved(fixture, monkeypatch):
    journal, state, bridge, _owner = fixture
    local = restricted(bridge, monkeypatch, journal)
    local._restricted_resources_status = Mock(side_effect=["missing", "mismatch"])
    local._cleanup_restricted_resources = Mock(side_effect=lambda *args, **kwargs: local._run(["docker", "stop", "x"]))
    local._runtime_runner = Mock(side_effect=AssertionError("Nothing runs past the deadline"))
    monkeypatch.setattr("deerflow.community.aio_sandbox.local_backend._PARTIAL_SET_SECONDS", 0)
    with pytest.raises(StorageHostUnqualified):
        local.create("chat", "a", user_id="alice", storage_labels={"hartmesh.storage.space." + SLOT: "1"})
    local._runtime_runner.assert_not_called()
    assert len(journal.pending()) == 1 and state.pending_for(owner_id="alice")


def test_a_shutdown_during_a_cold_start_costs_one_attempt_not_the_owner(fixture, monkeypatch):
    journal, state, bridge, owner = fixture
    local = restricted(bridge, monkeypatch, journal)
    local._create_internal_network = local._create_egress_network = Mock()
    local._runtime_runner = Mock(side_effect=AssertionError("A closing runtime sends no primary"))

    def close_during_the_proxy(*args, **kwargs):
        owner.live = False
        return "172.20.0.2"

    local._start_network_proxy = close_during_the_proxy
    local._restricted_resources_status = Mock(side_effect=["missing", "mismatch", "missing"])
    with pytest.raises(StorageHostUnqualified):
        local.create("chat", "a", user_id="alice", storage_labels={"hartmesh.storage.space." + SLOT: "1"})
    local._runtime_runner.assert_not_called()
    assert local._cleanup_restricted_resources.call_count == 1
    assert journal.pending() == () and not state.pending_for(owner_id="alice")


@pytest.mark.parametrize(
    "fault",
    ["still-there", "unreadable", "cleanup-timeout"],
)
def test_a_set_not_proved_gone_keeps_the_attempt_reserved(fixture, monkeypatch, fault):
    journal, state, bridge, _owner = fixture
    local = restricted(bridge, monkeypatch, journal)
    if fault == "still-there":
        local._restricted_resources_status = Mock(side_effect=["missing", *["mismatch"] * 4])
    elif fault == "unreadable":
        local._restricted_resources_status = Mock(side_effect=["missing", *[RuntimeError("daemon unavailable")] * 4])
    else:
        local._restricted_resources_status = Mock(side_effect=["missing", "mismatch"])
        local._cleanup_restricted_resources = Mock(side_effect=subprocess.TimeoutExpired("docker", 15))
    with pytest.raises(StorageHostUnqualified):
        local.create("chat", "a", user_id="alice", storage_labels={"hartmesh.storage.space." + SLOT: "1"})
    assert len(journal.pending()) == 1 and state.pending_for(owner_id="alice")


def test_one_failed_read_does_not_stop_the_removal(fixture, monkeypatch):
    journal, state, bridge, _owner = fixture
    local = restricted(bridge, monkeypatch, journal)
    local._restricted_resources_status = Mock(side_effect=["missing", RuntimeError("daemon unavailable"), "missing"])
    with pytest.raises(StorageHostUnqualified):
        local.create("chat", "a", user_id="alice", storage_labels={"hartmesh.storage.space." + SLOT: "1"})
    assert local._cleanup_restricted_resources.call_count == 1
    assert journal.pending() == () and not state.pending_for(owner_id="alice")


def test_the_last_read_can_still_prove_the_set_gone(fixture, monkeypatch):
    journal, state, bridge, _owner = fixture
    local = restricted(bridge, monkeypatch, journal)
    local._restricted_resources_status = Mock(side_effect=["missing", *["mismatch"] * 3, "missing"])
    with pytest.raises(StorageHostUnqualified):
        local.create("chat", "a", user_id="alice", storage_labels={"hartmesh.storage.space." + SLOT: "1"})
    assert local._cleanup_restricted_resources.call_count == 3
    assert journal.pending() == () and not state.pending_for(owner_id="alice")


def test_a_restricted_failure_after_the_primary_is_sent_stays_reserved_and_removes_nothing(fixture, monkeypatch):
    journal, state, bridge, _owner = fixture
    local = restricted(bridge, monkeypatch, journal)
    local._create_internal_network = local._create_egress_network = Mock()
    local._start_network_proxy = Mock(return_value="172.20.0.2")
    # A set exists to remove, so only the guard stands between it and a live primary.
    local._restricted_resources_status = Mock(side_effect=["missing", *["mismatch"] * 4])
    local._runtime_runner = Mock(side_effect=subprocess.CalledProcessError(125, ["docker"], stderr="uncertain result"))
    with pytest.raises(StorageHostUnqualified):
        local.create("chat", "a", user_id="alice", storage_labels={"hartmesh.storage.space." + SLOT: "1"})
    local._runtime_runner.assert_called_once()
    local._cleanup_restricted_resources.assert_not_called()
    assert len(journal.pending()) == 1 and state.pending_for(owner_id="alice")


def test_an_open_mode_failure_before_the_primary_is_withdrawn_without_touching_docker(fixture, monkeypatch):
    journal, state, bridge, _owner = fixture
    local = backend(bridge)
    local._cleanup_restricted_resources = Mock(side_effect=AssertionError("Open mode has no sidecars"))
    local._runtime_runner = Mock(side_effect=AssertionError("No primary may be sent"))
    monkeypatch.setattr("deerflow.community.aio_sandbox.local_backend.get_free_port", lambda **kwargs: 19080)
    monkeypatch.setattr(state, "record_submission", Mock(side_effect=OSError("before the primary")))
    with pytest.raises(StorageHostUnqualified):
        local.create("chat", "a", user_id="alice", storage_labels={"hartmesh.storage.space." + SLOT: "1"})
    assert journal.pending() == () and not state.pending_for(owner_id="alice")


def test_a_lost_owner_keeps_the_attempt_even_with_no_primary_sent(fixture, monkeypatch):
    journal, state, bridge, owner = fixture
    local = restricted(bridge, monkeypatch, journal)

    def lose_ownership(*args):
        owner.lost = True
        raise RuntimeError("Network outcome unconfirmed")

    local._create_internal_network = lose_ownership
    with pytest.raises(StorageHostUnqualified):
        local.create("chat", "a", user_id="alice", storage_labels={"hartmesh.storage.space." + SLOT: "1"})
    assert len(journal.pending()) == 1 and state.pending_for(owner_id="alice")


def test_the_warning_says_whether_the_attempt_was_withdrawn(fixture, monkeypatch, caplog):
    journal, _state, bridge, _owner = fixture
    local = restricted(bridge, monkeypatch, journal)
    with caplog.at_level("WARNING"), pytest.raises(StorageHostUnqualified):
        local.create("chat", "a", user_id="alice", storage_labels={"hartmesh.storage.space." + SLOT: "1"})
    (message,) = [record.getMessage() for record in caplog.records if "Managed native sandbox creation unconfirmed:" in record.getMessage()]
    details = json.loads(message.split(": ", 1)[1])
    assert details["primary_submitted"] is False and details["withdrawn"] is True


def test_unconfirmed_creation_logs_origin_and_errno_without_exception_payload(fixture, monkeypatch, caplog):
    import errno

    journal, state, bridge, _owner = fixture
    local = backend(bridge)
    monkeypatch.setattr("deerflow.community.aio_sandbox.local_backend.get_free_port", lambda **kwargs: 19080)

    def fail_stage(raw):
        raise OSError(errno.ENOSPC, "private journal payload must stay private")

    monkeypatch.setattr(journal, "_write_stage", fail_stage)
    local._runtime_runner = Mock(side_effect=AssertionError("No primary submission expected"))
    with caplog.at_level("WARNING"), pytest.raises(StorageHostUnqualified):
        local.create("chat", "a", user_id="alice", storage_labels={"hartmesh.storage.space." + SLOT: "1"})
    (message,) = [record.getMessage() for record in caplog.records if "Managed native sandbox creation unconfirmed:" in record.getMessage()]
    details = json.loads(message.split(": ", 1)[1])
    assert any(cause.get("errno") == errno.ENOSPC and cause["type"] == "OSError" for cause in details["causes"])
    assert "fail_stage" in message
    assert "private journal payload" not in message
    assert details["attempt_id"] is None and details["primary_submitted"] is False
    assert len(message) < 4096
    local._runtime_runner.assert_not_called()
    assert journal.pending() == () and not state.pending_for(owner_id="alice")


def test_unconfirmed_primary_logs_return_code_and_retained_attempt_without_argv_or_stderr(fixture, monkeypatch, caplog):
    journal, state, bridge, _owner = fixture
    local = backend(bridge)
    monkeypatch.setattr("deerflow.community.aio_sandbox.local_backend.get_free_port", lambda **kwargs: 19080)

    def fail(cmd, **kwargs):
        raise subprocess.CalledProcessError(125, [*cmd, "PRIVATE_ARG", "RELAY_TOKEN=PRIVATE_TOKEN"], stderr="PRIVATE_STDERR")

    local._runtime_runner = fail
    with caplog.at_level("WARNING"), pytest.raises(StorageHostUnqualified):
        local.create("chat", "a", user_id="alice", storage_labels={"hartmesh.storage.space." + SLOT: "1"})
    (message,) = [record.getMessage() for record in caplog.records if "Managed native sandbox creation unconfirmed:" in record.getMessage()]
    details = json.loads(message.split(": ", 1)[1])
    assert any(cause.get("returncode") == 125 and cause["type"] == "CalledProcessError" for cause in details["causes"])
    assert "fail" in message and "PRIVATE_" not in message
    (pending,) = journal.pending()
    assert details["attempt_id"] == pending.id and details["primary_submitted"] is True
    assert state.pending_for(owner_id="alice")


def test_native_creation_diagnostic_bounds_deep_and_cyclic_cause_chains():
    from deerflow.community.aio_sandbox.local_backend import _native_creation_diagnostic
    from deerflow.community.aio_sandbox.native_creation import NativeCreateUnconfirmed

    error = NativeCreateUnconfirmed()
    current = error
    for _ in range(40):
        current.__cause__ = RuntimeError("PRIVATE_MESSAGE")
        current = current.__cause__
    current.__cause__ = error
    encoded = _native_creation_diagnostic(error)
    details = json.loads(encoded)
    assert len(details["causes"]) == 8 and details["truncated"] is True
    assert len(encoded) < 4096 and "PRIVATE_MESSAGE" not in encoded
