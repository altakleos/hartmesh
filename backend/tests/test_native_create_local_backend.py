"""Actual LocalContainerBackend builds requests through the managed journal."""

import json
import subprocess
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


def test_mountless_profile_keeps_ordinary_command_without_journal(fixture, monkeypatch):
    journal, _state, bridge, owner = fixture
    local = backend(bridge)
    owner.live = False
    calls = []
    monkeypatch.setattr("deerflow.community.aio_sandbox.local_backend.get_free_port", lambda **kwargs: 19080)
    local._runtime_runner = lambda cmd, **kwargs: calls.append(cmd) or SimpleNamespace(stdout=CONTAINER, stderr="", returncode=0)
    info = local.create("chat", "a", user_id="alice", storage_labels={})
    assert info.container_id == CONTAINER and len(calls) == 1
    assert not any(value.startswith(ATTEMPT_LABEL + "=") for value in calls[0])
    assert journal.pending() == ()


def test_restricted_early_failure_retains_pending_before_sidecar_effect_and_never_guesses_cleanup(fixture, monkeypatch):
    journal, state, bridge, _owner = fixture
    local = backend(bridge)
    local._network_mode, local._network_config = "isolated", {"mode": "isolated"}
    local._restricted_resources_status = Mock(return_value="missing")
    local._cleanup_restricted_resources = Mock(side_effect=AssertionError("Unknown native set cleanup guessed names"))
    local._create_egress_network = Mock(side_effect=AssertionError("First network failure should stop provisioning"))
    monkeypatch.setattr("deerflow.community.aio_sandbox.local_backend.get_free_port", lambda **kwargs: 19080)
    observed = []

    def fail_first_effect(*args):
        (pending,) = journal.pending()
        observed.append(pending)
        raise RuntimeError("Network outcome unconfirmed")

    local._create_internal_network = fail_first_effect
    with pytest.raises(StorageHostUnqualified):
        local.create("chat", "a", user_id="alice", storage_labels={"hartmesh.storage.space." + SLOT: "1"})
    assert len(observed) == 1 and len(journal.pending()) == 1
    local._cleanup_restricted_resources.assert_not_called()
    assert state.pending_for(owner_id="alice")


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
