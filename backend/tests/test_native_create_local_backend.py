"""Actual LocalContainerBackend builds requests through the managed journal."""

import json
import subprocess
import time
from types import SimpleNamespace
from unittest.mock import Mock, patch

import pytest
from test_native_create_bridge import CONTAINER, LABELS, SLOT
from test_native_create_bridge import fixture as fixture

from deerflow.community.aio_sandbox.local_backend import LocalContainerBackend
from deerflow.spaces.docker import StorageHostUnqualified
from deerflow.spaces.native_create_evidence import ATTEMPT_LABEL


def backend(bridge):
    # Use the actual constructor so cleanup ownership/locks have the same
    # lifetime as production. Only runtime discovery is external to this fixture.
    with patch.object(LocalContainerBackend, "_detect_runtime", return_value="docker"):
        result = LocalContainerBackend(image="image", base_port=19080, container_prefix="native", config_mounts=[], environment={})
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
