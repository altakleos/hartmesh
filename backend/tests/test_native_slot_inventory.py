"""Read-only all-prefix, all-state native inventory never grants cleanup authority."""

import json
from types import SimpleNamespace
from unittest.mock import Mock

import pytest
from test_native_create_local_backend import backend
from test_native_create_provider import fixture as fixture
from test_native_create_provider import install

from deerflow.spaces.docker import StorageHostUnqualified

SLOT, CID, OTHER = "a" * 32, "c" * 64, "d" * 64


def entry(container_id=CID, *, running=True):
    return {
        "Id": container_id,
        "Name": "/foreign-prefix-native",
        "Created": "2026-10-12T01:00:00.123456789Z",
        "Config": {
            "Labels": {
                "deerflow.owner_user_id": "alice",
                "deerflow.thread_id": "chat",
                "deerflow.sandbox_id": "native",
                "deerflow.role": "sandbox",
                "deerflow.network_mode": "open",
                "hartmesh.chat.storage": "on",
                "hartmesh.storage.space." + SLOT: "1",
            }
        },
        "HostConfig": {"AutoRemove": False},
        "State": {"Running": running},
    }


def local_fixture(fixture, *, listed=None, entries=None):
    provider, owner = fixture
    local = backend(None)
    provider._backend = local
    bridge = install(provider, owner)
    calls = []
    listed = [CID] if listed is None else listed
    entries = {CID: entry()} if entries is None else entries

    def run(command, **kwargs):
        calls.append(command)
        if command[1] == "ps":
            assert "--all" in command and "--no-trunc" in command
            assert "--filter" in command
            assert "label=hartmesh.storage.space." + SLOT + "=1" in command
            assert not any(value.startswith("name=") for value in command)
            return SimpleNamespace(stdout="\n".join(listed) + ("\n" if listed else ""), stderr="", returncode=0)
        assert command[1] == "inspect" and command[2] in entries
        return SimpleNamespace(stdout=json.dumps([entries[command[2]]]), stderr="", returncode=0)

    local._runtime_runner = run
    return local, bridge, calls


@pytest.mark.parametrize("running", [False, True])
def test_inventory_includes_foreign_prefix_and_stopped_immutable_sets(fixture, running):
    local, bridge, calls = local_fixture(fixture, entries={CID: entry(running=running)})
    (actual,) = local._slot_containers(SLOT)
    assert actual.container_id == CID and actual.container_name == "foreign-prefix-native"
    assert actual.owner == ("alice", "chat") and actual.slots == (SLOT,)
    assert actual.running is running and actual.auto_remove is False
    assert actual.daemon_id == bridge.evidence.daemon_id
    assert calls and all(command[1] in {"ps", "inspect"} for command in calls)


def test_empty_slot_inventory_is_verified_and_read_only(fixture):
    local, bridge, calls = local_fixture(fixture, listed=[], entries={})
    before = bridge._check_daemon
    bridge._check_daemon = Mock(wraps=before)
    assert local._slot_containers(SLOT) == ()
    assert bridge._check_daemon.call_count >= 2
    assert all(command[1] == "ps" for command in calls)


@pytest.mark.parametrize("listed", [[CID, CID], ["short"], [CID.upper()], [CID + " unexpected"]])
def test_bad_or_ambiguous_ps_output_is_refused(fixture, listed):
    local, _bridge, _calls = local_fixture(fixture, listed=listed)
    with pytest.raises(StorageHostUnqualified):
        local._slot_containers(SLOT)


@pytest.mark.parametrize(
    "change",
    [
        "id",
        "name",
        "owner",
        "thread",
        "sandbox",
        "role",
        "mode",
        "storage",
        "slot_value",
        "extra_slot_value",
        "running",
        "autoremove",
        "created",
    ],
)
def test_inspected_identity_or_attribution_conflicts_refuse(fixture, change):
    actual = entry()
    labels = actual["Config"]["Labels"]
    if change == "id":
        actual["Id"] = OTHER
    elif change == "name":
        actual["Name"] = ""
    elif change in {"owner", "thread", "sandbox", "role", "mode"}:
        key = {"owner": "owner_user_id", "thread": "thread_id", "sandbox": "sandbox_id", "role": "role", "mode": "network_mode"}[change]
        labels["deerflow." + key] = "" if change not in {"role", "mode"} else "unknown"
    elif change == "storage":
        labels["hartmesh.chat.storage"] = "off"
    elif change == "slot_value":
        labels["hartmesh.storage.space." + SLOT] = "0"
    elif change == "extra_slot_value":
        labels["hartmesh.storage.space." + "b" * 32] = "unknown"
    elif change == "running":
        actual["State"]["Running"] = 1
    elif change == "autoremove":
        actual["HostConfig"]["AutoRemove"] = None
    elif change == "created":
        actual["Created"] = "unknown"
    local, _bridge, _calls = local_fixture(fixture, entries={CID: actual})
    with pytest.raises(StorageHostUnqualified):
        local._slot_containers(SLOT)


@pytest.mark.parametrize("payload", ["[]", "[{},{}]", "unknown", '[{"Id":"duplicate","Id":"duplicate"}]'])
def test_ambiguous_or_malformed_inspection_refuses(fixture, payload):
    local, _bridge, _calls = local_fixture(fixture)
    original = local._runtime_runner

    def run(command, **kwargs):
        if command[1] == "inspect":
            return SimpleNamespace(stdout=payload, stderr="", returncode=0)
        return original(command, **kwargs)

    local._runtime_runner = run
    with pytest.raises(StorageHostUnqualified):
        local._slot_containers(SLOT)


def test_disappearance_after_ps_is_unknown_not_successful_absence(fixture):
    local, _bridge, _calls = local_fixture(fixture)
    original = local._runtime_runner

    def run(command, **kwargs):
        if command[1] == "inspect":
            return SimpleNamespace(stdout="", stderr="No such object: " + CID, returncode=1)
        return original(command, **kwargs)

    local._runtime_runner = run
    with pytest.raises(StorageHostUnqualified):
        local._slot_containers(SLOT)


def test_changed_daemon_after_inventory_refuses_even_empty_result(fixture):
    local, bridge, calls = local_fixture(fixture, listed=[], entries={})

    def daemon():
        if calls:
            raise StorageHostUnqualified("Daemon changed during inventory")

    bridge._check_daemon = daemon
    with pytest.raises(StorageHostUnqualified):
        local._slot_containers(SLOT)


def test_unsupported_foreign_restricted_set_is_preserved(fixture):
    actual = entry()
    actual["Config"]["Labels"]["deerflow.network_mode"] = "blocked"
    local, _bridge, calls = local_fixture(fixture, entries={CID: actual})
    with pytest.raises(StorageHostUnqualified):
        local._slot_containers(SLOT)
    assert all(command[1] in {"ps", "inspect"} for command in calls)


@pytest.mark.parametrize("slot", [None, 1, True, {}, "short"])
def test_invalid_slot_sends_no_inventory_command(fixture, slot):
    local, _bridge, calls = local_fixture(fixture)
    with pytest.raises(StorageHostUnqualified):
        local._slot_containers(slot)
    assert calls == []


def test_oversize_inventory_is_refused_before_any_inspection(fixture):
    listed = [format(index, "064x") for index in range(257)]
    local, _bridge, calls = local_fixture(fixture, listed=listed)
    with pytest.raises(StorageHostUnqualified):
        local._slot_containers(SLOT)
    assert all(command[1] == "ps" for command in calls)


def test_duplicate_names_make_inventory_inconsistent(fixture):
    first, second = entry(), entry(OTHER)
    second["Config"]["Labels"]["deerflow.sandbox_id"] = "other"
    local, _bridge, calls = local_fixture(fixture, listed=[CID, OTHER], entries={CID: first, OTHER: second})
    with pytest.raises(StorageHostUnqualified):
        local._slot_containers(SLOT)
    assert all(command[1] in {"ps", "inspect"} for command in calls)


def test_overall_inventory_deadline_stops_more_inspections(fixture, monkeypatch):
    import deerflow.community.aio_sandbox.local_backend as module

    now = {"value": 0.0}
    monkeypatch.setattr(module.time, "monotonic", lambda: now["value"])
    first, second = entry(), entry(OTHER)
    second["Name"] = "/another-native"
    second["Config"]["Labels"]["deerflow.sandbox_id"] = "other"
    local, _bridge, calls = local_fixture(fixture, listed=[CID, OTHER], entries={CID: first, OTHER: second})
    run, timeouts = local._runtime_runner, []

    def controlled(command, **kwargs):
        timeouts.append(kwargs["timeout"])
        result = run(command, **kwargs)
        now["value"] = 29.5 if command[1] == "ps" else 30.1
        return result

    local._runtime_runner = controlled
    with pytest.raises(StorageHostUnqualified):
        local._slot_containers(SLOT)
    assert len(calls) == 2 and calls[-1] == ["docker", "inspect", CID]
    assert 0 < timeouts[-1] <= 0.5


def test_late_last_inspection_cannot_publish_expired_snapshot(fixture, monkeypatch):
    import deerflow.community.aio_sandbox.local_backend as module

    now = {"value": 0.0}
    monkeypatch.setattr(module.time, "monotonic", lambda: now["value"])
    local, _bridge, _calls = local_fixture(fixture)
    original = local._runtime_runner

    def delayed(command, **kwargs):
        result = original(command, **kwargs)
        if command[1] == "inspect":
            now["value"] = 30.1
        return result

    local._runtime_runner = delayed
    with pytest.raises(StorageHostUnqualified):
        local._slot_containers(SLOT)
