"""Strict bootstrap primary census is bounded, complete and read-only."""

import copy
import json
from types import SimpleNamespace

import pytest
from test_native_create_local_backend import backend
from test_native_create_provider import fixture as fixture
from test_native_create_provider import install
from test_native_slot_inventory import CID, OTHER, SLOT, entry

from deerflow.spaces.docker import StorageHostUnqualified


def setup(fixture, *, listed=None, entries=None):
    provider, owner = fixture
    local = backend(None)
    provider._backend = local
    bridge = install(provider, owner)
    listed = [CID] if listed is None else listed
    entries = {CID: entry()} if entries is None else entries
    calls = []

    def run(command, **kwargs):
        calls.append(command)
        if command[1] == "ps":
            assert "--all" in command and "--no-trunc" in command
            assert "label=deerflow.role=sandbox" in command
            assert not any(part.startswith("name=") for part in command)
            return SimpleNamespace(returncode=0, stdout="\n".join(listed), stderr="")
        assert command[1] == "inspect"
        return SimpleNamespace(returncode=0, stdout=json.dumps([entries[command[2]]]), stderr="")

    local._runtime_runner = run
    return local, bridge, calls


@pytest.mark.parametrize("running", [False, True])
def test_census_includes_acknowledged_stopped_and_historical_prefix_primary(fixture, running):
    local, bridge, calls = setup(fixture, entries={CID: entry(running=running)})
    (actual,) = local._bootstrap_primary_inventory(known_slots=(SLOT,))
    assert actual.container_id == CID and actual.container_name == "foreign-prefix-native"
    assert actual.running is running and actual.daemon_id == bridge.evidence.daemon_id
    assert actual.slots == (SLOT,)
    assert all(command[1] in {"ps", "inspect"} for command in calls)


@pytest.mark.parametrize("listed", [[CID, CID], ["short"], [CID.upper()], [CID] * 257])
def test_malformed_duplicate_or_overflow_census_never_reports_partial_success(fixture, listed):
    local, _bridge, _calls = setup(fixture, listed=listed)
    with pytest.raises(StorageHostUnqualified):
        local._bootstrap_primary_inventory(known_slots=(SLOT,))


def test_collision_between_different_cids_and_one_sandbox_id_refuses_snapshot(fixture):
    first, second = entry(), entry(OTHER)
    second["Name"] = "/another-prefix-native"
    local, _bridge, _calls = setup(fixture, listed=[CID, OTHER], entries={CID: first, OTHER: second})
    with pytest.raises(StorageHostUnqualified):
        local._bootstrap_primary_inventory(known_slots=(SLOT,))


@pytest.mark.parametrize("change", ["missing", "foreign_slot", "old_nouser", "owner", "role"])
def test_unknown_or_foreign_candidate_never_disappears_from_snapshot(fixture, change):
    actual = copy.deepcopy(entry())
    if change == "foreign_slot":
        actual["Config"]["Labels"].pop("hartmesh.storage.space." + SLOT)
        actual["Config"]["Labels"]["hartmesh.storage.space." + "e" * 32] = "1"
    elif change == "old_nouser":
        actual["Config"]["Labels"]["hartmesh.chat.storage"] = "off"
        actual["Config"]["Labels"].pop("hartmesh.storage.space." + SLOT)
    elif change == "owner":
        actual["Config"]["Labels"].pop("deerflow.owner_user_id")
    elif change == "role":
        actual["Config"]["Labels"]["deerflow.role"] = "network-proxy"
    local, _bridge, _calls = setup(fixture, entries={} if change == "missing" else {CID: actual})
    with pytest.raises(StorageHostUnqualified):
        local._bootstrap_primary_inventory(known_slots=(SLOT,))


def test_native_revision_change_during_census_refuses_complete_result(fixture, monkeypatch):
    local, bridge, _calls = setup(fixture)
    original = local._run

    def run(*args, **kwargs):
        result = original(*args, **kwargs)
        if args[0][1] == "inspect":
            bridge.invalidate()
        return result

    monkeypatch.setattr(local, "_run", run)
    with pytest.raises(StorageHostUnqualified):
        local._bootstrap_primary_inventory(known_slots=(SLOT,))


@pytest.mark.parametrize("running", [False, True])
def test_actual_local_inspection_then_parking_remains_counted_without_sdk(fixture, running):
    from deerflow.community.aio_sandbox.aio_sandbox_provider import SandboxInfo
    from deerflow.sandbox.admission import ProviderAdmission
    from deerflow.sandbox.exceptions import SandboxRuntimeAdmissionError

    local, _bridge, calls = setup(fixture, entries={CID: entry(running=running)})
    provider, _owner = fixture
    admission = ProviderAdmission()
    provider._bind_runtime_admission(admission)
    token = provider._begin_bootstrap_accounting(admission)
    (actual,) = local._bootstrap_primary_inventory(known_slots=(SLOT,))
    info = SandboxInfo(
        sandbox_id=actual.sandbox_id,
        sandbox_url="",
        container_name=actual.container_name,
        container_id=actual.container_id,
        owner=actual.owner,
        native_slots=actual.slots,
        chat_storage_enabled=True,
    )
    provider._park_bootstrap_accounting(token, info)
    assert provider._warm_pool[actual.sandbox_id][0] is info
    assert info._bootstrap_accounting_only and not provider._sandboxes
    with provider._lock:
        assert provider._counted_slots_locked() == (3, 0, 1, 0)
    with pytest.raises(SandboxRuntimeAdmissionError):
        provider.get(actual.sandbox_id)
    assert len([command for command in calls if command[1] == "inspect"]) >= 3
    assert all(command[1] in {"ps", "inspect"} for command in calls)
