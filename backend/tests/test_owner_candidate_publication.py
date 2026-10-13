"""Fresh candidate slots and pure final currency close owner/slot publication races."""

from unittest.mock import Mock

import pytest
from test_native_create_provider import fixture as fixture
from test_native_slot_inventory import CID, SLOT, local_fixture

from deerflow.community.aio_sandbox.sandbox_info import SandboxInfo
from deerflow.spaces.docker import StorageHostUnqualified


def native(fixture):
    provider, _owner = fixture
    provider._unowned_since = {}
    local, _bridge, _calls = local_fixture(fixture)
    assert provider._backend is local
    info = SandboxInfo("native", "http://native", container_name="foreign-prefix-native", container_id=CID, owner=("alice", "chat"), chat_storage_enabled=True)
    return provider, local, info


def test_inspected_candidate_registers_slots_before_publication(fixture):
    provider, _local, info = native(fixture)
    with provider._owner_admission_scope("alice"):
        provider._assert_managed_container(info)
        assert info.native_slots == (SLOT,)
        with pytest.raises(StorageHostUnqualified):
            provider._owner_admission_gate().reserve("bob", SLOT)


def test_reserved_foreign_slot_refuses_candidate_before_ownership_publish(fixture):
    provider, _local, info = native(fixture)
    gate = provider._owner_admission_gate()
    reservation = gate.reserve("bob", SLOT)
    provider._publish_ownership = Mock(side_effect=AssertionError("Slot conflict published ownership"))
    try:
        with provider._owner_admission_scope("alice"), pytest.raises(StorageHostUnqualified):
            provider._register_discovered_sandbox("chat", info, user_id="alice")
        provider._publish_ownership.assert_not_called()
        assert not provider._sandboxes
    finally:
        gate.end_retirement(reservation)


@pytest.mark.parametrize("kind", ["owner", "slot"])
def test_final_publication_checks_memory_reservation_without_io(fixture, kind):
    provider, local, info = native(fixture)
    with provider._owner_admission_scope("alice"):
        provider._assert_managed_container(info)
    gate = provider._owner_admission_gate()
    reservation = gate.reserve("alice" if kind == "owner" else "bob", "d" * 32 if kind == "owner" else SLOT)
    local._runtime_runner = Mock(side_effect=AssertionError("Final metadata check performed IO"))
    try:
        with provider._lock, pytest.raises(StorageHostUnqualified):
            provider._assert_managed_publication(info)
        local._runtime_runner.assert_not_called()
    finally:
        gate.end_retirement(reservation)


def test_candidate_owner_conflict_never_registers_requested_identity(fixture):
    provider, _local, info = native(fixture)
    info.owner = ("bob", "chat")
    with provider._owner_admission_scope("bob"), pytest.raises(StorageHostUnqualified):
        provider._assert_managed_container(info)


def test_expired_candidate_context_cannot_reenter_from_detached_worker(fixture):
    import contextvars

    provider, _local, info = native(fixture)
    with provider._owner_admission_scope("alice"):
        saved = contextvars.copy_context()
    with pytest.raises(StorageHostUnqualified):
        saved.run(provider._assert_managed_container, info)


@pytest.mark.asyncio
@pytest.mark.parametrize("path", ["orphan", "discovered", "warm", "replacement"])
@pytest.mark.parametrize("retiring_owner", ["alice", "bob"])
async def test_candidate_keeps_owner_and_actual_slots_through_ownership_roundtrip(fixture, monkeypatch, path, retiring_owner):
    import asyncio
    from threading import Event

    provider, local, info = native(fixture)
    entered, release = Event(), Event()
    if path == "replacement":
        info.requires_replacement = True

        def grace(*args):
            entered.set()
            assert release.wait(5)
            return False

        provider._adoptable_after_grace = grace

        def call():
            return provider._replace_incompatible_sandbox(info, 0)
    elif path == "orphan":
        local.list_running = Mock(return_value=[info])
        provider._adoptable_after_grace = Mock(return_value=True)

        def claim(*args, **kwargs):
            entered.set()
            assert release.wait(5)
            return True

        monkeypatch.setattr(provider, "_claim_ownership", claim)
        call = provider._reconcile_orphans
    else:

        def publish(*args, **kwargs):
            entered.set()
            assert release.wait(5)

        monkeypatch.setattr(provider, "_publish_ownership", publish)
        if path == "warm":
            provider._warm_pool["native"] = (info, 0)
            provider._warm_pool_identity["native"] = info.owner
            provider._check_tracked_sandbox_alive = Mock(return_value=True)

            def call():
                return provider._reclaim_warm_pool_sandbox("chat", "native", user_id="alice")
        else:

            def call():
                return provider._register_discovered_sandbox("chat", info, user_id="alice")

    task = asyncio.create_task(asyncio.to_thread(call))
    reserved = None
    try:
        assert await asyncio.to_thread(entered.wait, 5)
        with pytest.raises(StorageHostUnqualified):
            reserved = provider._owner_admission_gate().reserve(retiring_owner, SLOT)
    finally:
        if reserved is not None:
            provider._owner_admission_gate().end_retirement(reserved)
        release.set()
        await asyncio.wait_for(task, 5)
