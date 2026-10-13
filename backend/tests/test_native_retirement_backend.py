"""Stopped native primaries are removed only by qualified immutable-ID teardown."""

from types import SimpleNamespace

import pytest
from test_native_create_provider import fixture as fixture
from test_native_slot_inventory import CID, SLOT, entry, local_fixture

from deerflow.community.aio_sandbox.backend import DestroyOutcome
from deerflow.community.aio_sandbox.sandbox_info import SandboxInfo


def prepare(fixture):
    state = {"entry": entry(running=False), "gone": False}
    local, bridge, calls = local_fixture(fixture, entries={CID: state["entry"]})
    (actual,) = local._slot_containers(SLOT)
    info = SandboxInfo("native", "", container_name=actual.container_name, container_id=CID, owner=actual.owner, chat_storage_enabled=True)
    info.native_container = actual
    original = local._runtime_runner

    def run(command, **kwargs):
        if command[1] == "ps" and state["gone"]:
            calls.append(command)
            return SimpleNamespace(stdout="", stderr="", returncode=0)
        if command[1] == "inspect" and "-f" in command:
            calls.append(command)
            return SimpleNamespace(stdout="" if state["gone"] else CID, stderr="No such object: " + command[-1] if state["gone"] else "", returncode=1 if state["gone"] else 0)
        if command[1] == "stop":
            calls.append(command)
            assert command[-1] == CID
            state["entry"]["State"]["Running"] = False
            return SimpleNamespace(stdout=CID, stderr="", returncode=0)
        if command[1] == "rm":
            calls.append(command)
            assert command == ["docker", "rm", CID] and "--force" not in command
            state["gone"] = True
            return SimpleNamespace(stdout=CID, stderr="", returncode=0)
        return original(command, **kwargs)

    local._runtime_runner = run
    return local, bridge, info, state, calls


def test_stopped_non_autoremove_primary_requires_actual_id_remove_and_absence(fixture):
    local, _bridge, info, state, calls = prepare(fixture)
    assert local.destroy(info) is DestroyOutcome.ABSENT
    assert state["gone"] and ["docker", "rm", CID] in calls


@pytest.mark.parametrize("change", ["name", "owner", "slot", "created", "id"])
def test_changed_native_incarnation_sends_no_destructive_command(fixture, change):
    local, _bridge, info, state, calls = prepare(fixture)
    if change == "name":
        state["entry"]["Name"] = "/replacement"
    elif change == "owner":
        state["entry"]["Config"]["Labels"]["deerflow.owner_user_id"] = "bob"
    elif change == "slot":
        state["entry"]["Config"]["Labels"]["hartmesh.storage.space." + "b" * 32] = "1"
    elif change == "created":
        state["entry"]["Created"] = "2026-10-12T02:00:00Z"
    else:
        info.container_id = "d" * 64
    assert local.destroy(info) is DestroyOutcome.UNKNOWN
    assert all(command[1] not in {"stop", "rm"} for command in calls)


def test_failed_remove_retains_present_container(fixture):
    local, _bridge, info, state, calls = prepare(fixture)
    original = local._runtime_runner

    def run(command, **kwargs):
        if command[1] == "rm":
            calls.append(command)
            return SimpleNamespace(stdout="", stderr="still running", returncode=1)
        return original(command, **kwargs)

    local._runtime_runner = run
    assert local.destroy(info) is not DestroyOutcome.ABSENT
    assert not state["gone"]


def test_remove_succeeded_but_lost_proof_can_retry_exact_absence(fixture):
    local, _bridge, info, state, calls = prepare(fixture)
    original = local._runtime_runner

    def lose(command, **kwargs):
        if state["gone"] and command[1] == "inspect":
            return SimpleNamespace(stdout="", stderr="daemon unavailable", returncode=1)
        return original(command, **kwargs)

    local._runtime_runner = lose
    assert local.destroy(info) is DestroyOutcome.UNKNOWN and state["gone"]
    local._runtime_runner = original
    before = len(calls)
    assert local.destroy(info) is DestroyOutcome.ABSENT
    assert all(command[1] not in {"stop", "rm"} for command in calls[before:])


def test_changed_daemon_refuses_before_mutation(fixture):
    local, bridge, info, _state, calls = prepare(fixture)

    def changed():
        raise RuntimeError("Daemon changed")

    bridge._check_daemon = changed
    assert local.destroy(info) is DestroyOutcome.UNKNOWN
    assert all(command[1] not in {"stop", "rm"} for command in calls)


def test_correlation_is_process_only_and_never_reconstructed(fixture):
    _local, _bridge, info, _state, _calls = prepare(fixture)
    restored = SandboxInfo.from_dict(info.to_dict())
    assert getattr(restored, "native_container", None) is None
