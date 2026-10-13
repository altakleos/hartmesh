"""Generic retained containment spans callers, workers and exact holder fences."""

import asyncio
from threading import Event
from unittest.mock import Mock

import pytest
from test_native_create_local_backend import backend
from test_native_create_provider import fixture as fixture
from test_native_create_provider import install, record, verified

from deerflow.community.aio_sandbox.backend import DestroyOutcome
from deerflow.community.aio_sandbox.local_backend import NativeSlotContainer
from deerflow.community.aio_sandbox.sandbox_info import SandboxInfo
from deerflow.sandbox.lease import discard_sandbox_lease_manager, get_sandbox_lease_manager
from deerflow.spaces.docker import StorageHostUnqualified

SLOT, SHARED, CID = "a" * 32, "b" * 32, "c" * 64


def qualify(fixture, *, containers=()):
    provider, owner = fixture
    local = backend(None)
    provider._backend = local
    bridge = install(provider, owner)
    local._slot_containers = Mock(return_value=containers)
    local.destroy = Mock(return_value=DestroyOutcome.ABSENT)
    return provider, bridge, local


def actual(owner="alice"):
    return NativeSlotContainer(CID, "foreign-native", "native", (owner, "chat"), (SLOT,), "daemon-A", False, False, "2026-10-12T01:00:00Z")


@pytest.mark.asyncio
async def test_scope_retains_owner_and_slot_until_caller_exit(fixture):
    provider, _bridge, local = qualify(fixture)
    saved = None
    async with provider._native_retirement_scope("alice", SLOT) as scope:
        saved = scope
        gate = provider._owner_admission_gate()
        with pytest.raises(StorageHostUnqualified):
            with provider._owner_admission_scope("alice"):
                pytest.fail("Owner reserved by active containment scope")
        with provider._owner_admission_scope("bob") as token:
            gate.register_slots(token, ("d" * 32, SHARED))
            with pytest.raises(StorageHostUnqualified):
                gate.register_slots(token, (SLOT,))
        await scope.prove_absent()
        assert local._slot_containers.call_count >= 2
    with pytest.raises(StorageHostUnqualified):
        await saved.prove_absent()
    with provider._owner_admission_scope("alice"):
        pass


@pytest.mark.asyncio
@pytest.mark.parametrize("pending_owner,pending_slot", [("alice", "e" * 32), ("bob", SLOT)])
async def test_pending_owner_or_slot_refuses_even_with_empty_inventory(fixture, pending_owner, pending_slot):
    provider, bridge, local = qualify(fixture)
    bridge.begin(
        name="pending-native",
        labels={
            "deerflow.owner_user_id": pending_owner,
            "deerflow.thread_id": "chat",
            "deerflow.sandbox_id": "pending",
            "deerflow.role": "sandbox",
            "hartmesh.chat.storage": "on",
            "hartmesh.storage.space." + pending_slot: "1",
        },
    )
    with pytest.raises(StorageHostUnqualified):
        async with provider._native_retirement_scope("alice", SLOT):
            pytest.fail("Unresolved owner or slot request entered retirement")
    local.destroy.assert_not_called()
    with provider._owner_admission_scope("alice"):
        pass
    assert len(bridge.evidence.pending()) == 1


@pytest.mark.asyncio
async def test_active_admission_refuses_before_any_inventory(fixture):
    provider, _bridge, local = qualify(fixture)
    with provider._owner_admission_scope("alice"):
        with pytest.raises(StorageHostUnqualified):
            async with provider._native_retirement_scope("alice", SLOT):
                pytest.fail("Live admission permitted retirement")
    local._slot_containers.assert_not_called()


@pytest.mark.asyncio
async def test_positively_unrelated_starting_work_does_not_block_retirement(fixture):
    provider, _bridge, _local = qualify(fixture)
    with provider._owner_admission_scope("bob") as reference:
        provider._owner_admission_gate().register_slots(reference, ("d" * 32, SHARED))
        provider._mark_starting("bob-native")
        try:
            async with provider._native_retirement_scope("alice", SLOT) as scope:
                await scope.prove_absent()
        finally:
            provider._unmark_starting("bob-native")


@pytest.mark.asyncio
async def test_confirmed_process_local_mountless_client_is_not_a_slot_writer(fixture):
    provider, _bridge, local = qualify(fixture)
    info = SandboxInfo("text-only", "http://text", owner=("alice", "chat"), chat_storage_enabled=False, mountless_provider=provider)
    with provider._lock:
        provider._sandbox_infos["text-only"] = info
        provider._sandboxes["text-only"] = Mock()
        provider._active_sandbox_identity["text-only"] = info.owner
    async with provider._native_retirement_scope("alice", SLOT) as scope:
        await scope.prove_absent()
    local.destroy.assert_not_called()


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ["client_without_info", "foreign_tracked_slot", "duplicate_tracking", "untransferred_ack"])
async def test_unresolved_actual_or_memory_attribution_refuses_even_empty_inventory(fixture, kind):
    provider, bridge, local = qualify(fixture)
    if kind == "untransferred_ack":
        pending = record()
        handoff = bridge.handoffs.record_pending(pending, sandbox_id="native")
        bridge.handoffs.record_verified(handoff, verified(pending))
        bridge.handoffs.record_acknowledged(handoff)
    else:
        info = SandboxInfo("native", "http://native", container_name="foreign-native", container_id=CID, owner=("bob", "chat"), chat_storage_enabled=True, native_slots=(SLOT,))
        if kind == "client_without_info":
            provider._sandboxes["native"] = Mock()
            provider._active_sandbox_identity["native"] = ("alice", "chat")
        elif kind == "foreign_tracked_slot":
            provider._warm_pool["native"] = (info, 0)
            provider._warm_pool_identity["native"] = info.owner
        else:
            provider._sandbox_infos["native"] = info
            provider._sandboxes["native"] = Mock()
            provider._active_sandbox_identity["native"] = info.owner
            provider._warm_pool["native"] = (SandboxInfo("native", "http://replacement", owner=("alice", "chat")), 0)
            provider._warm_pool_identity["native"] = ("alice", "chat")
    with pytest.raises(StorageHostUnqualified):
        async with provider._native_retirement_scope("alice", SLOT):
            pytest.fail("Unresolved containment state qualified backing release")
    local.destroy.assert_not_called()


@pytest.mark.asyncio
async def test_tracked_native_cleanup_removes_the_qualified_process_only_set(fixture):
    provider, _bridge, local = qualify(fixture)
    observed = actual()
    info = SandboxInfo("native", "http://native", container_name=observed.container_name, container_id=CID, owner=observed.owner, chat_storage_enabled=True)
    with provider._lock:
        provider._warm_pool["native"] = (info, 0)
        provider._warm_pool_identity["native"] = observed.owner
    local._slot_containers.side_effect = [(observed,), ()]
    try:
        async with provider._native_retirement_scope("alice", SLOT):
            assert local.destroy.call_args.args[0] is info
            assert info.native_container == observed and info.native_slots == (SLOT,)
    finally:
        discard_sandbox_lease_manager(provider)


@pytest.mark.asyncio
async def test_quarantined_inspected_set_can_retry_canonical_absence_after_lost_reply(fixture):
    provider, _bridge, local = qualify(fixture)
    observed = actual()
    info = SandboxInfo("native", "http://native", container_name=observed.container_name, container_id=CID, owner=observed.owner, chat_storage_enabled=True, native_container=observed, native_slots=observed.slots)
    with provider._lock:
        provider._warm_pool["native"] = (info, 0)
        provider._warm_pool_identity["native"] = info.owner
        provider._mark_cleanup_pending_locked("native", DestroyOutcome.UNKNOWN)
    try:
        async with provider._native_retirement_scope("alice", SLOT) as scope:
            await scope.prove_absent()
        assert local.destroy.call_args.args[0] is info
        assert "native" not in provider._warm_pool
    finally:
        discard_sandbox_lease_manager(provider)


@pytest.mark.asyncio
async def test_foreign_owner_target_slot_is_preserved_without_cleanup(fixture):
    provider, _bridge, local = qualify(fixture, containers=(actual("bob"),))
    with pytest.raises(StorageHostUnqualified):
        async with provider._native_retirement_scope("alice", SLOT):
            pytest.fail("Foreign slot container adopted for cleanup")
    local.destroy.assert_not_called()


@pytest.mark.asyncio
async def test_holder_fence_precedes_cleanup_for_actual_slot_container(fixture):
    provider, _bridge, local = qualify(fixture, containers=(actual(),))
    manager = get_sandbox_lease_manager(provider)
    with manager._metadata_lock:
        manager._owners_by_sandbox["native"] = {"held-execution"}
    try:
        with pytest.raises(StorageHostUnqualified):
            async with provider._native_retirement_scope("alice", SLOT):
                pytest.fail("Held execution container retired")
        local.destroy.assert_not_called()
    finally:
        with manager._metadata_lock:
            manager._owners_by_sandbox.pop("native")
        discard_sandbox_lease_manager(provider)


@pytest.mark.asyncio
@pytest.mark.parametrize("fence", ["local_teardown", "ownership_refused"])
async def test_canonical_local_and_store_refusal_preserve_native_backing(fixture, fence, monkeypatch):
    provider, _bridge, local = qualify(fixture, containers=(actual(),))
    if fence == "local_teardown":
        provider._local_teardown.add("native")
    else:
        monkeypatch.setattr(provider, "_claim_ownership", lambda *_args, **_kwargs: False)
    try:
        with pytest.raises(StorageHostUnqualified):
            async with provider._native_retirement_scope("alice", SLOT):
                pytest.fail("Refused canonical fence qualified backing release")
        local.destroy.assert_not_called()
    finally:
        provider._local_teardown.discard("native")
        discard_sandbox_lease_manager(provider)


@pytest.mark.asyncio
async def test_unknown_cleanup_retains_exact_quarantine_and_scope_refuses(fixture):
    provider, _bridge, local = qualify(fixture, containers=(actual(),))
    local.destroy.return_value = DestroyOutcome.UNKNOWN
    try:
        with pytest.raises(StorageHostUnqualified):
            async with provider._native_retirement_scope("alice", SLOT):
                pytest.fail("Unknown cleanup qualified slot absence")
        local.destroy.assert_called_once()
        assert provider._warm_pool["native"][0].container_id == CID
        assert provider._cleanup_pending["native"] is DestroyOutcome.UNKNOWN
    finally:
        discard_sandbox_lease_manager(provider)


@pytest.mark.asyncio
async def test_canonical_cleanup_and_fresh_inventory_must_both_confirm_absence(fixture):
    provider, _bridge, local = qualify(fixture)
    local._slot_containers.side_effect = [(actual(),), (), ()]
    try:
        async with provider._native_retirement_scope("alice", SLOT) as scope:
            await scope.prove_absent()
            local.destroy.assert_called_once()
            info = local.destroy.call_args.args[0]
            assert info.container_id == CID and info.container_name == "foreign-native"
            assert not provider._sandboxes and not provider._warm_pool
    finally:
        discard_sandbox_lease_manager(provider)


@pytest.mark.asyncio
async def test_new_container_at_final_proof_is_not_ignored(fixture):
    provider, _bridge, local = qualify(fixture)
    local._slot_containers.side_effect = [(), (), (actual(),)]
    async with provider._native_retirement_scope("alice", SLOT) as scope:
        with pytest.raises(StorageHostUnqualified):
            await scope.prove_absent()
    local.destroy.assert_not_called()


@pytest.mark.asyncio
async def test_proof_cancellation_drains_worker_before_owner_slot_release(fixture):
    provider, _bridge, local = qualify(fixture)
    entered, release = Event(), Event()
    calls = []

    def inventory(*args):
        calls.append(args)
        if len(calls) >= 3:
            entered.set()
            assert release.wait(5)
        return ()

    local._slot_containers.side_effect = inventory

    async def call():
        async with provider._native_retirement_scope("alice", SLOT) as scope:
            await scope.prove_absent()

    task = asyncio.create_task(call())
    try:
        assert await asyncio.to_thread(entered.wait, 5)
        task.cancel()
        await asyncio.sleep(0)
        assert not task.done()
        task.cancel()
        with pytest.raises(StorageHostUnqualified):
            with provider._owner_admission_scope("alice"):
                pytest.fail("Cancelled proof released owner before worker finished")
        release.set()
        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(task, 5)
        with provider._owner_admission_scope("alice"):
            pass
    finally:
        release.set()
        if not task.done():
            task.cancel()
        await asyncio.gather(task, return_exceptions=True)


@pytest.mark.asyncio
async def test_initial_containment_cancellation_drains_before_reservation_exit(fixture):
    provider, _bridge, local = qualify(fixture)
    entered, release = Event(), Event()

    def inventory(*_args):
        entered.set()
        assert release.wait(5)
        return ()

    local._slot_containers.side_effect = inventory

    async def call():
        async with provider._native_retirement_scope("alice", SLOT):
            pytest.fail("Cancelled containment entered filesystem caller")

    task = asyncio.create_task(call())
    try:
        assert await asyncio.to_thread(entered.wait, 5)
        task.cancel()
        await asyncio.sleep(0)
        task.cancel()
        assert not task.done()
        with pytest.raises(StorageHostUnqualified):
            with provider._owner_admission_scope("alice"):
                pytest.fail("Initial worker outlived its reservation")
        release.set()
        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(task, 5)
        with provider._owner_admission_scope("alice"):
            pass
    finally:
        release.set()
        if not task.done():
            task.cancel()
        await asyncio.gather(task, return_exceptions=True)
