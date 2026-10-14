"""Open NoUser originals use immutable cleanup and no invented Space slot."""

from types import SimpleNamespace

import pytest
from test_native_create_provider import fixture as fixture
from test_native_retirement_backend import prepare
from test_native_slot_inventory import CID, SLOT

from deerflow.sandbox.lease import discard_sandbox_lease_manager


@pytest.mark.parametrize("created", [False, True])
@pytest.mark.parametrize("lost_proof", [False, True])
def test_open_nouser_original_or_restored_primary_uses_exact_cleanup(fixture, tmp_path, created, lost_proof):
    from deerflow.community.aio_sandbox.local_backend import _native_inspection
    from deerflow.community.aio_sandbox.sandbox_info import PROVENANCE_CREATED
    from deerflow.config.paths import Paths, paths_scope
    from deerflow.sandbox.admission import ProviderAdmission
    from deerflow.spaces.chat_paths import ChatStoragePaths
    from deerflow.spaces.native_create_evidence import NativeContainerIdentity

    local, bridge, info, state, calls = prepare(fixture)
    provider, _owner = fixture
    primary = state["entry"]
    primary["Config"]["Labels"].pop("hartmesh.storage.space." + SLOT)
    primary["Mounts"] = []
    local._native_mountless_binds = lambda _owner, _thread: ()
    info.native_container, info.native_slots = None, None
    original = local._runtime_runner
    missing_proof = [lost_proof]

    def run(command, **kwargs):
        if state["gone"] and command[1] == "inspect":
            calls.append(command)
            return SimpleNamespace(returncode=1, stdout="" if "-f" in command else "[]", stderr="unavailable" if missing_proof[0] else "Error: No such object: " + command[-1])
        return original(command, **kwargs)

    local._runtime_runner = run
    if created:
        handoff = bridge.begin(name=info.container_name, labels=primary["Config"]["Labels"])
        returned = NativeContainerIdentity(handoff.attempt.id, "daemon-A", "alice", "chat", info.container_name, (), CID)
        bridge.handoffs.record_verified(handoff, returned)
        bridge.evidence.acknowledge(handoff.attempt, returned)
        bridge.handoffs.record_acknowledged(handoff)
        info.provenance = PROVENANCE_CREATED
        with paths_scope(ChatStoragePaths(Paths(base_dir=tmp_path), user_id="alice")):
            provider._own_before_readiness("native", info)
        assert info.mountless_provider is provider and info.native_container is None
        provider._warm_pool["native"] = (info, 0)
        provider._warm_pool_identity["native"] = info.owner
        with provider._lock:
            bridge.handoffs.transferred_locked(handoff)
    else:
        admission = ProviderAdmission()
        provider._bind_runtime_admission(admission)
        permit = provider._begin_bootstrap_accounting(admission)
        actual = _native_inspection(primary, CID, "daemon-A", allow_mountless=True)
        info.native_container = local._qualify_native_mountless(primary, actual, ())
        info.native_slots = ()
        provider._park_bootstrap_accounting(permit, info)
        provider._end_bootstrap_accounting(permit)
    try:
        result = provider._destroy_warm_entry("native", info, reason="idle_timeout", still_reapable=lambda: True)
        assert result is (not lost_proof)
        assert state["gone"] and ["docker", "rm", CID] in calls
        if lost_proof:
            assert provider._warm_pool["native"][0] is info
            missing_proof[0] = False
            before = len(calls)
            assert provider._destroy_warm_entry("native", info, reason="cleanup_retry", still_reapable=lambda: True)
            assert all(command[1] not in {"stop", "rm"} for command in calls[before:])
        assert not provider._warm_pool and not provider._sandboxes
        assert not any(command[1] == "ps" for command in calls[1:])
    finally:
        discard_sandbox_lease_manager(provider)
