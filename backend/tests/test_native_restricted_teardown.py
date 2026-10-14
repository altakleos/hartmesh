"""Canonical cleanup and restart use original durable IDs, with mocked Docker."""

import json
import os
from contextvars import copy_context
from copy import deepcopy
from dataclasses import replace
from threading import Lock
from types import SimpleNamespace
from unittest.mock import Mock

import pytest
from test_aio_sandbox_provider import _make_provider
from test_native_create_local_backend import backend
from test_native_create_provider import fixture as fixture
from test_native_create_provider import install
from test_native_resource_set import CID, EGRESS, INTERNAL, PROXY, SLOT, capture, topology

from deerflow.community.aio_sandbox.local_backend import _native_inspection
from deerflow.community.aio_sandbox.native_resource_set import NativeResourceSet
from deerflow.community.aio_sandbox.sandbox_info import SandboxInfo
from deerflow.sandbox.lease import discard_sandbox_lease_manager
from deerflow.spaces.docker import StorageHostUnqualified
from deerflow.spaces.runtime_owner import PoolRuntimeOwner


class Engine:
    def __init__(self, fixture):
        provider, owner = fixture
        reader, _primary, actual, self.objects, _calls = topology()
        self.inspect = reader.backend._run
        self.calls = []
        self.fail_proxy = False
        self.fail_proxy_id = PROXY
        self.allowed_containers = {CID, PROXY}
        self.allowed_networks = {INTERNAL, EGRESS}
        self.expected_sets = {CID: {PROXY, INTERNAL, EGRESS}}
        self.bridge = None
        self.local = backend(None)
        self.local._runtime_runner = self.run
        provider._backend = self.local
        self.bridge = install(provider, owner)
        self.provider, self.owner = provider, owner
        self.info = SandboxInfo(actual.sandbox_id, "", container_id=actual.container_id, container_name=actual.container_name, owner=actual.owner, native_slots=actual.slots, chat_storage_enabled=True)

    def run(self, command, **kwargs):
        self.calls.append(command)
        if "inspect" in command:
            return self.inspect(command, **kwargs)
        if command[1] == "ps":
            assert "--all" in command and "--no-trunc" in command
            ids = [identity for (kind, identity), value in self.objects.items() if kind == "container" and value["Config"]["Labels"].get("hartmesh.storage.space." + SLOT) == "1"]
            return SimpleNamespace(returncode=0, stdout="\n".join(ids), stderr="")
        # A real lifecycle record must precede every actual mutation.
        records = self.bridge.evidence.snapshot().teardowns
        (saved,) = [record for record in records if command[-1] == record.container_id or any(command[-1] == item.identity for item in record.resources)]
        assert {item.identity for item in saved.resources} == self.expected_sets[saved.container_id]
        if command[1] in ("stop", "rm"):
            identity = command[-1]
            assert identity in self.allowed_containers and "--force" not in command
            if identity == self.fail_proxy_id and self.fail_proxy:
                raise OSError("Injected relay cleanup failure")
            value = self.objects[("container", identity)]
            if command[1] == "stop":
                value["State"].update(Running=False, Status="exited")
                for network in self.allowed_networks:
                    if ("network", network) in self.objects:
                        self.objects[("network", network)]["Containers"].pop(identity, None)
                for link in value["NetworkSettings"]["Networks"].values():
                    link.update(NetworkID="", EndpointID="")
                if value["HostConfig"]["AutoRemove"]:
                    self.objects.pop(("container", identity))
            else:
                assert not value["State"]["Running"]
                self.objects.pop(("container", identity))
            return SimpleNamespace(returncode=0, stdout=identity, stderr="")
        assert command[:3] == ["docker", "network", "rm"]
        identity = command[-1]
        assert identity in self.allowed_networks
        assert not self.objects[("network", identity)]["Containers"]
        self.objects.pop(("network", identity))
        return SimpleNamespace(returncode=0, stdout=identity, stderr="")


def test_ordinary_canonical_cleanup_persists_complete_set_before_effects(fixture):
    engine = Engine(fixture)
    provider = engine.provider
    provider._warm_pool["native"] = (engine.info, 0)
    provider._warm_pool_identity["native"] = engine.info.owner
    try:
        assert provider._destroy_warm_entry("native", engine.info, reason="idle_timeout", still_reapable=lambda: True)
        assert not engine.objects and not provider._warm_pool
        assert engine.bridge.evidence.snapshot().teardowns == () and engine.bridge.teardowns.records() == ()
        assert ["docker", "rm", CID] in engine.calls
        assert ["docker", "network", "rm", INTERNAL] in engine.calls
    finally:
        discard_sandbox_lease_manager(provider)


@pytest.mark.asyncio
async def test_primary_gone_relay_failure_restart_keeps_scope_closed_until_original_set_absent(fixture, tmp_path):
    engine = Engine(fixture)
    engine.fail_proxy = True
    provider, owner = engine.provider, engine.owner
    try:
        with pytest.raises(StorageHostUnqualified):
            async with provider._native_retirement_scope("alice", SLOT):
                pytest.fail("Unconfirmed paired cleanup entered backing release")
        assert ("container", CID) not in engine.objects and ("container", PROXY) in engine.objects
        (saved,) = engine.bridge.evidence.snapshot().teardowns
        assert saved.target_slot == SLOT and engine.bridge.teardowns.records() == (saved,)
        owner.close()
        restarted = _make_provider(tmp_path / "restart")
        restarted._lock = Lock()
        restarted._managed_runtime_required = True
        restarted._backend = backend(None)
        restarted._backend._runtime_runner = engine.run
        replacement_owner = PoolRuntimeOwner(owner.pool)
        replacement_owner.resolve_provider(lambda: restarted)
        try:
            engine.bridge = install(restarted, replacement_owner)
            assert engine.bridge.teardowns.records() == (saved,)
            with restarted._lock:
                assert restarted._counted_slots_locked()[3] == 1
            with pytest.raises(StorageHostUnqualified):
                async with restarted._native_retirement_scope("alice", SLOT):
                    pytest.fail("Restart forgot relay and networks after primary removal")
            assert engine.bridge.evidence.snapshot().teardowns == (saved,)
            engine.fail_proxy = False
            async with restarted._native_retirement_scope("alice", SLOT) as scope:
                await scope.prove_absent()
            assert not engine.objects and not engine.bridge.teardowns.records()
            assert engine.bridge.evidence.snapshot().teardowns == ()
        finally:
            restarted._acquire_serializer.close()
            restarted._acquire_worker_executor.shutdown(wait=True)
            replacement_owner.close()
            discard_sandbox_lease_manager(restarted)
    finally:
        discard_sandbox_lease_manager(provider)


def test_closing_owner_can_finish_canonical_cleanup_without_reopening_admission(fixture):
    engine = Engine(fixture)
    provider = engine.provider
    provider._warm_pool["native"] = (engine.info, 0)
    provider._warm_pool_identity["native"] = engine.info.owner
    engine.owner.mark_closing()
    try:
        assert not engine.bridge.ready
        assert provider._destroy_warm_entry("native", engine.info, reason="shutdown", still_reapable=lambda: True)
        assert not engine.objects and not engine.bridge.teardowns.records() and not provider._warm_pool
        assert not engine.bridge.ready and engine.bridge.evidence.snapshot().teardowns == ()
        with pytest.raises(StorageHostUnqualified):
            engine.bridge.require_admissible(owner_id="alice", name="new-chat")
    finally:
        discard_sandbox_lease_manager(provider)


@pytest.mark.parametrize("failure", ["publication", "terminal-sync"])
def test_closing_cleanup_failure_restores_accounting_and_can_retry(fixture, monkeypatch, failure):
    engine = Engine(fixture)
    provider = engine.provider
    provider._warm_pool["native"] = (engine.info, 0)
    provider._warm_pool_identity["native"] = engine.info.owner
    engine.owner.mark_closing()
    failed = []
    publish, fsync = engine.bridge.evidence.begin_teardown, os.fsync

    def publication(record):
        result = publish(record)
        if not failed:
            failed.append(True)
            raise OSError("Injected closing publication reply loss")
        return result

    def sync(fd):
        if fd == engine.bridge.evidence._directory_fd and not engine.objects and not failed:
            failed.append(True)
            raise OSError("Injected closing terminal directory sync loss")
        return fsync(fd)

    if failure == "publication":
        monkeypatch.setattr(engine.bridge.evidence, "begin_teardown", publication)
    else:
        monkeypatch.setattr(os, "fsync", sync)
    try:
        assert not provider._destroy_warm_entry("native", engine.info, reason="shutdown", still_reapable=lambda: True)
        (saved,) = engine.bridge.evidence.snapshot().teardowns
        assert failed and engine.bridge.teardowns.records() == (saved,)
        assert not engine.bridge.ready
        engine.bridge.assert_current(teardown=True)
        before = len(engine.calls)
        assert provider._destroy_warm_entry("native", engine.info, reason="shutdown", still_reapable=lambda: True)
        if failure == "terminal-sync":
            assert all("inspect" in command for command in engine.calls[before:])
        assert not engine.objects and not engine.bridge.teardowns.records() and not provider._warm_pool
        assert not engine.bridge.ready and not engine.bridge.evidence.snapshot().teardowns
    finally:
        discard_sandbox_lease_manager(provider)


@pytest.mark.parametrize("lost", ["never-installed", "owner", "daemon"])
def test_teardown_restoration_never_installs_or_bypasses_owning_proof(fixture, monkeypatch, lost):
    from deerflow.community.aio_sandbox.native_creation import NativeCreateBridge, NativeCreateHandoffs

    engine = Engine(fixture)
    engine.owner.mark_closing()
    bridge = engine.bridge
    if lost == "never-installed":
        bridge = NativeCreateBridge(
            evidence=bridge.evidence,
            handoffs=NativeCreateHandoffs(daemon_id="daemon-A", lock=engine.provider._lock),
            check_owner=bridge._check_owner,
            owner_accepts=bridge._owner_accepts,
            check_daemon=bridge._check_daemon,
        )
    elif lost == "owner":
        monkeypatch.setattr(engine.owner, "_provider", object())
    else:
        monkeypatch.setattr(bridge, "_check_daemon", Mock(side_effect=StorageHostUnqualified("Changed actual daemon")))
    with pytest.raises(StorageHostUnqualified):
        bridge.qualify(teardown=True)
    assert not bridge.ready and not engine.calls
    assert not bridge.evidence.snapshot().teardowns
    with pytest.raises(StorageHostUnqualified):
        bridge.qualify()


def test_warm_eviction_during_another_owners_acquire_retains_independent_admission(fixture, monkeypatch):
    from deerflow.community.aio_sandbox.owner_admission import OWNER_ADMISSION

    engine = Engine(fixture)
    provider = engine.provider
    provider._warm_pool["native"] = (engine.info, 0)
    provider._warm_pool_identity["native"] = engine.info.owner
    original = engine.local._runtime_runner
    borrower = []

    def observed(command, **kwargs):
        if "inspect" not in command:
            gate = provider._owner_admission_gate()
            current = OWNER_ADMISSION.get()
            assert current.owner_id == "alice" and current is not borrower[0]
            with provider._lock:
                assert gate.admission_current_locked(current) and gate.admission_current_locked(borrower[0])
                assert current._group is not borrower[0]._group
                assert current._group.slots == {SLOT} and borrower[0]._group.slots == {"b" * 32}
            with pytest.raises(StorageHostUnqualified):
                gate.reserve("alice", SLOT)
            with pytest.raises(StorageHostUnqualified):
                gate.reserve("bob", "b" * 32)
        return original(command, **kwargs)

    monkeypatch.setattr(engine.local, "_runtime_runner", observed)
    try:
        with provider._native_admission("bob", ("b" * 32,)):
            reference = OWNER_ADMISSION.get()
            borrower.append(reference)
            with pytest.raises(StorageHostUnqualified):
                with provider._native_admission("alice", (SLOT,)):
                    pytest.fail("Ordinary admission borrowed another owner's reference")
            assert provider._destroy_warm_entry("native", engine.info, reason="warm_eviction", still_reapable=lambda: True)
            assert OWNER_ADMISSION.get() is reference
            gate = provider._owner_admission_gate()
            with provider._lock:
                assert gate.admission_current_locked(reference)
        assert not engine.objects and not engine.bridge.teardowns.records()
    finally:
        discard_sandbox_lease_manager(provider)


@pytest.mark.parametrize("inherited", ["expired-other", "expired-same", "foreign-gate"])
def test_cleanup_refuses_expired_or_foreign_inherited_admission_without_effects(fixture, inherited):
    from deerflow.community.aio_sandbox.owner_admission import OWNER_ADMISSION, OwnerAdmissions

    engine = Engine(fixture)
    provider = engine.provider
    provider._warm_pool["native"] = (engine.info, 0)
    provider._warm_pool_identity["native"] = engine.info.owner
    gate = OwnerAdmissions(lock=Lock()) if inherited == "foreign-gate" else provider._owner_admission_gate()
    reference = gate.enter("alice" if inherited == "expired-same" else "bob")
    if inherited != "foreign-gate":
        gate.end(reference)
    context = OWNER_ADMISSION.set(reference)
    original = deepcopy(engine.objects)
    try:
        assert not provider._destroy_warm_entry("native", engine.info, reason="warm_eviction", still_reapable=lambda: True)
        assert OWNER_ADMISSION.get() is reference
        assert engine.objects == original and all("inspect" in command for command in engine.calls)
        assert engine.bridge.evidence.snapshot().teardowns == () and provider._warm_pool
    finally:
        OWNER_ADMISSION.reset(context)
        gate.end(reference)
        discard_sandbox_lease_manager(provider)


def test_cleanup_slot_refusal_restores_the_other_owners_live_admission(fixture):
    from deerflow.community.aio_sandbox.owner_admission import OWNER_ADMISSION

    engine = Engine(fixture)
    provider = engine.provider
    provider._warm_pool["native"] = (engine.info, 0)
    provider._warm_pool_identity["native"] = engine.info.owner
    gate = provider._owner_admission_gate()
    reservation = gate.reserve("charlie", SLOT)
    original = deepcopy(engine.objects)
    try:
        with provider._native_admission("bob", ("b" * 32,)):
            reference = OWNER_ADMISSION.get()
            assert not provider._destroy_warm_entry("native", engine.info, reason="warm_eviction", still_reapable=lambda: True)
            assert OWNER_ADMISSION.get() is reference
            with provider._lock:
                assert gate.admission_current_locked(reference)
            assert engine.objects == original and all("inspect" in command for command in engine.calls)
            assert engine.bridge.evidence.snapshot().teardowns == ()
    finally:
        gate.end_retirement(reservation)
        discard_sandbox_lease_manager(provider)


def test_active_owner_ordinary_partial_cleanup_recovers_after_restart_without_primary(fixture, tmp_path, monkeypatch):
    engine = Engine(fixture)
    provider, owner = engine.provider, engine.owner
    provider._warm_pool["native"] = (engine.info, 0)
    provider._warm_pool_identity["native"] = engine.info.owner
    engine.fail_proxy = True
    try:
        assert not provider._destroy_warm_entry("native", engine.info, reason="idle_timeout", still_reapable=lambda: True)
        (saved,) = engine.bridge.evidence.snapshot().teardowns
        assert saved.target_slot is None and ("container", CID) not in engine.objects
        owner.close()
        restarted = _make_provider(tmp_path / "ordinary-restart")
        restarted._lock = Lock()
        restarted._unowned_since = {}
        restarted._managed_runtime_required = True
        restarted._backend = backend(None)
        restarted._backend._runtime_runner = engine.run
        monkeypatch.setattr(restarted._backend, "list_running", lambda: [])
        replacement_owner = PoolRuntimeOwner(owner.pool)
        replacement_owner.resolve_provider(lambda: restarted)
        try:
            engine.bridge = install(restarted, replacement_owner)
            assert not getattr(restarted, "_cleanup_pending", {}) and not restarted._warm_pool and not restarted._sandboxes
            with pytest.raises(StorageHostUnqualified):
                engine.bridge.require_admissible(owner_id="alice", name="new-chat")
            engine.fail_proxy = False
            restarted._reconcile_orphans()
            assert not engine.objects and not engine.bridge.teardowns.records()
            assert engine.bridge.evidence.snapshot().teardowns == ()
            assert not getattr(restarted, "_cleanup_pending", {}) and not restarted._warm_pool and not restarted._sandboxes
            engine.bridge.require_admissible(owner_id="alice", name="new-chat")
        finally:
            restarted._acquire_serializer.close()
            restarted._acquire_worker_executor.shutdown(wait=True)
            replacement_owner.close()
            discard_sandbox_lease_manager(restarted)
    finally:
        discard_sandbox_lease_manager(provider)


@pytest.mark.parametrize("failed", [False, True])
def test_port_reservation_is_released_only_after_complete_confirmed_cleanup(fixture, monkeypatch, failed):
    from deerflow.community.aio_sandbox import local_backend

    engine = Engine(fixture)
    engine.info.sandbox_url = "http://127.0.0.1:17932"
    engine.fail_proxy = failed
    release = Mock()
    monkeypatch.setattr(local_backend, "release_port", release)
    provider = engine.provider
    provider._warm_pool["native"] = (engine.info, 0)
    provider._warm_pool_identity["native"] = engine.info.owner
    try:
        assert provider._destroy_warm_entry("native", engine.info, reason="idle_timeout", still_reapable=lambda: True) is (not failed)
        if failed:
            release.assert_not_called()
        else:
            release.assert_called_once_with(17932)
    finally:
        discard_sandbox_lease_manager(provider)


def test_transient_failure_after_durable_completion_can_retry_exact_absence(fixture, monkeypatch):
    engine = Engine(fixture)
    provider = engine.provider
    provider._warm_pool["native"] = (engine.info, 0)
    provider._warm_pool_identity["native"] = engine.info.owner
    finish, qualify = engine.bridge.evidence.finish_teardown, engine.bridge.qualify
    completed, failed = [], []

    def done(record):
        finish(record)
        completed.append(record)

    def fault(**kwargs):
        if completed and not failed:
            failed.append(True)
            raise StorageHostUnqualified("Injected final publication failure")
        return qualify(**kwargs)

    monkeypatch.setattr(engine.bridge.evidence, "finish_teardown", done)
    monkeypatch.setattr(engine.bridge, "qualify", fault)
    try:
        assert not provider._destroy_warm_entry("native", engine.info, reason="idle_timeout", still_reapable=lambda: True)
        assert completed and failed and not engine.objects
        assert engine.bridge.evidence.snapshot().teardowns == ()
        before = len(engine.calls)
        assert provider._destroy_warm_entry("native", engine.info, reason="idle_timeout", still_reapable=lambda: True)
        assert all("inspect" in command for command in engine.calls[before:])
        assert not provider._warm_pool
    finally:
        discard_sandbox_lease_manager(provider)


def test_completed_cleanup_refuses_reappearing_members_without_publication_or_effects(fixture, monkeypatch):
    engine = Engine(fixture)
    original = deepcopy(engine.objects)
    provider = engine.provider
    provider._warm_pool["native"] = (engine.info, 0)
    provider._warm_pool_identity["native"] = engine.info.owner
    try:
        assert provider._destroy_warm_entry("native", engine.info, reason="idle_timeout", still_reapable=lambda: True)
        assert engine.info.native_completion is not None and not engine.objects
        engine.objects.update(original)
        provider._warm_pool["native"] = (engine.info, 0)
        provider._warm_pool_identity["native"] = engine.info.owner
        publish = Mock(wraps=engine.bridge.evidence.begin_teardown)
        monkeypatch.setattr(engine.bridge.evidence, "begin_teardown", publish)
        before = len(engine.calls)
        assert not provider._destroy_warm_entry("native", engine.info, reason="idle_timeout", still_reapable=lambda: True)
        publish.assert_not_called()
        assert all("inspect" in command for command in engine.calls[before:])
        assert engine.objects == original and provider._warm_pool
        assert engine.bridge.evidence.snapshot().teardowns == () and engine.bridge.teardowns.records() == ()
    finally:
        discard_sandbox_lease_manager(provider)


def test_previously_qualified_open_primary_can_cleanup_after_auto_removal(fixture):
    engine = Engine(fixture)
    primary = engine.objects[("container", CID)]
    primary["Config"]["Labels"]["deerflow.network_mode"] = "open"
    primary["Config"]["Labels"].pop("deerflow.network_policy_digest")
    engine.provider._assert_managed_container(engine.info)
    assert engine.info.native_container is not None and engine.info.native_container.network_mode == "open"
    engine.objects.clear()
    provider = engine.provider
    provider._warm_pool["native"] = (engine.info, 0)
    provider._warm_pool_identity["native"] = engine.info.owner
    try:
        assert provider._destroy_warm_entry("native", engine.info, reason="idle_timeout", still_reapable=lambda: True)
        assert not provider._warm_pool and engine.bridge.evidence.snapshot().teardowns == ()
    finally:
        discard_sandbox_lease_manager(provider)


@pytest.mark.asyncio
@pytest.mark.parametrize("shares_target", [False, True])
async def test_other_owner_pending_shared_companion_does_not_become_a_retirement_target(fixture, shares_target):
    engine = Engine(fixture)
    company, bob_slot = "b" * 32, "a" * 32 if shares_target else "3" * 32
    engine.objects[("container", CID)]["Config"]["Labels"]["hartmesh.storage.space." + company] = "1"
    engine.info.native_slots = (SLOT, company)
    engine.bridge.begin(
        name="bob-pending",
        labels={
            "deerflow.owner_user_id": "bob",
            "deerflow.thread_id": "bob-thread",
            "deerflow.sandbox_id": "bob-native",
            "deerflow.role": "sandbox",
            "hartmesh.chat.storage": "on",
            "hartmesh.storage.space." + company: "1",
            "hartmesh.storage.space." + bob_slot: "1",
        },
    )
    try:
        if shares_target:
            with pytest.raises(StorageHostUnqualified):
                async with engine.provider._native_retirement_scope("alice", SLOT):
                    pytest.fail("Pending foreign writer on target slot was ignored")
            assert ("container", CID) in engine.objects and not engine.bridge.evidence.snapshot().teardowns
        else:
            async with engine.provider._native_retirement_scope("alice", SLOT) as scope:
                await scope.prove_absent()
            assert not engine.objects
        assert engine.bridge.handoffs.pending_for(owner_id="bob")
    finally:
        discard_sandbox_lease_manager(engine.provider)


def test_record_resource_order_does_not_change_safe_cleanup_order(fixture):
    engine = Engine(fixture)
    reader, primary, actual, _objects, _calls = topology()
    original = capture(reader, primary, actual)
    saved = replace(original, target_slot=None, resources=(original.resources[2], original.resources[1], original.resources[0]))
    engine.bridge.evidence.begin_teardown(saved)
    engine.bridge.qualify()
    provider = engine.provider
    provider._warm_pool["native"] = (engine.info, 0)
    provider._warm_pool_identity["native"] = engine.info.owner
    try:
        assert provider._destroy_warm_entry("native", engine.info, reason="idle_timeout", still_reapable=lambda: True)
        assert not engine.objects
        relay_stop = next(index for index, command in enumerate(engine.calls) if command[:2] == ["docker", "stop"] and command[-1] == PROXY)
        assert relay_stop < engine.calls.index(["docker", "network", "rm", EGRESS])
    finally:
        discard_sandbox_lease_manager(provider)


def test_failed_journal_publication_sends_zero_destructive_commands(fixture, monkeypatch):
    engine = Engine(fixture)
    provider = engine.provider
    provider._warm_pool["native"] = (engine.info, 0)
    provider._warm_pool_identity["native"] = engine.info.owner

    def fail(_raw):
        raise OSError("Injected durable publication failure")

    monkeypatch.setattr(engine.bridge.evidence, "_write_stage", fail)
    try:
        assert not provider._destroy_warm_entry("native", engine.info, reason="idle_timeout", still_reapable=lambda: True)
        assert all("inspect" in command for command in engine.calls)
        assert len(engine.objects) == 4 and provider._warm_pool["native"][0] is engine.info
    finally:
        discard_sandbox_lease_manager(provider)


@pytest.mark.asyncio
async def test_failed_terminal_sync_retains_accounting_and_retries_without_republication(fixture, monkeypatch):
    engine = Engine(fixture)
    fsync = os.fsync
    failed = []

    def fault(fd):
        if fd == engine.bridge.evidence._directory_fd and not engine.objects and not failed:
            failed.append(True)
            raise OSError("Injected terminal directory sync failure")
        return fsync(fd)

    monkeypatch.setattr(os, "fsync", fault)
    try:
        with pytest.raises(StorageHostUnqualified):
            async with engine.provider._native_retirement_scope("alice", SLOT):
                pytest.fail("Unsynced terminal evidence released its backing")
        assert failed and not engine.objects
        (saved,) = engine.bridge.evidence.snapshot().teardowns
        assert engine.bridge.teardowns.records() == (saved,)
        before = len(engine.calls)
        async with engine.provider._native_retirement_scope("alice", SLOT) as scope:
            await scope.prove_absent()
        assert not engine.bridge.evidence.snapshot().teardowns and not engine.bridge.teardowns.records()
        assert all("inspect" in command or command[1] == "ps" for command in engine.calls[before:])
    finally:
        discard_sandbox_lease_manager(engine.provider)


def test_copied_destructive_context_expires_before_a_later_backend_call(fixture, monkeypatch):
    from deerflow.community.aio_sandbox.native_teardown_execution import current_cleanup

    engine = Engine(fixture)
    provider = engine.provider
    provider._warm_pool["native"] = (engine.info, 0)
    provider._warm_pool_identity["native"] = engine.info.owner
    destroy = engine.local.destroy
    copied = []

    def capture_context(info):
        assert current_cleanup(engine.local, info) is not None
        copied.append(copy_context())
        return destroy(info)

    monkeypatch.setattr(engine.local, "destroy", capture_context)
    try:
        assert provider._destroy_warm_entry("native", engine.info, reason="idle_timeout", still_reapable=lambda: True)
        before = len(engine.calls)
        assert copied[0].run(current_cleanup, engine.local, engine.info) is None
        outcome, _error = copied[0].run(provider._backend_destroy, engine.info)
        assert outcome.value == "unknown" and len(engine.calls) == before
    finally:
        discard_sandbox_lease_manager(provider)


@pytest.mark.asyncio
async def test_two_interrupted_sets_complete_individually_and_restart_never_resurrects_first(fixture, tmp_path):
    engine = Engine(fixture)
    second_ids = {CID: "7" * 64, PROXY: "6" * 64, INTERNAL: "3" * 64, EGRESS: "4" * 64}
    for (kind, _identity), entry in list(engine.objects.items()):
        raw = json.dumps(entry)
        for original, changed in second_ids.items():
            raw = raw.replace(original, changed)
        for original, changed in (
            ("foreign-prefix-native", "second-native"),
            ("retained-proxy", "second-proxy"),
            ("retained-internal", "second-internal"),
            ("retained-egress", "second-egress"),
            ('"native"', '"native2"'),
            ('"chat"', '"chat2"'),
        ):
            raw = raw.replace(original, changed)
        second = json.loads(raw)
        engine.objects[(kind, second["Id"])] = second
    engine.allowed_containers.update((second_ids[CID], second_ids[PROXY]))
    engine.allowed_networks.update((second_ids[INTERNAL], second_ids[EGRESS]))
    engine.expected_sets[second_ids[CID]] = {second_ids[PROXY], second_ids[INTERNAL], second_ids[EGRESS]}
    records = []
    for index, identity in enumerate((CID, second_ids[CID])):
        primary = engine.objects[("container", identity)]
        actual = _native_inspection(primary, identity, "daemon-A", slot_id=SLOT)
        saved = capture(NativeResourceSet(SimpleNamespace(_run=engine.inspect)), primary, actual)
        saved = replace(saved, id=("e" if index == 0 else "b") * 32)
        records.append(engine.bridge.evidence.begin_teardown(saved))
    engine.bridge.qualify()
    engine.fail_proxy_id, engine.fail_proxy = second_ids[PROXY], True
    replacement_owner = restarted = None
    try:
        with pytest.raises(StorageHostUnqualified):
            async with engine.provider._native_retirement_scope("alice", SLOT):
                pytest.fail("Second unresolved set released the backing after first completed")
        assert ("container", CID) not in engine.objects and ("container", PROXY) not in engine.objects
        assert engine.bridge.evidence.snapshot().teardowns == (records[1],)
        assert engine.bridge.teardowns.records() == (records[1],)
        engine.owner.close()
        restarted = _make_provider(tmp_path / "second-restart")
        restarted._lock = Lock()
        restarted._managed_runtime_required = True
        restarted._backend = backend(None)
        restarted._backend._runtime_runner = engine.run
        replacement_owner = PoolRuntimeOwner(engine.owner.pool)
        replacement_owner.resolve_provider(lambda: restarted)
        engine.bridge = install(restarted, replacement_owner)
        assert engine.bridge.teardowns.records() == (records[1],)
        engine.fail_proxy = False
        async with restarted._native_retirement_scope("alice", SLOT) as scope:
            await scope.prove_absent()
        assert not engine.objects and not engine.bridge.evidence.snapshot().teardowns
        assert not engine.bridge.teardowns.records()
    finally:
        if restarted is not None:
            restarted._acquire_serializer.close()
            restarted._acquire_worker_executor.shutdown(wait=True)
            discard_sandbox_lease_manager(restarted)
        if replacement_owner is not None:
            replacement_owner.close()
        discard_sandbox_lease_manager(engine.provider)
