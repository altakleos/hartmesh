"""Ordinary durable cleanup is scheduled without adopting usable clients."""

import json
import os
import time
from contextlib import contextmanager
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import Mock

import pytest
from test_native_create_provider import fixture as fixture
from test_native_resource_set import CID, EGRESS, INTERNAL, PROXY, SLOT, capture
from test_native_restricted_teardown import Engine

from deerflow.community.aio_sandbox.local_backend import _NATIVE_SLOT_INVENTORY_DEADLINE, _native_inspection
from deerflow.community.aio_sandbox.native_resource_set import NativeResourceSet
from deerflow.community.aio_sandbox.native_teardown_execution import recover_teardowns
from deerflow.sandbox.lease import discard_sandbox_lease_manager, get_sandbox_lease_manager
from deerflow.spaces.docker import DockerStorageAdapter


def saved_record(engine, *, identity=CID, record_id="a" * 32):
    primary = engine.objects[("container", identity)]
    actual = _native_inspection(primary, identity, "daemon-A", slot_id=SLOT)
    return replace(capture(NativeResourceSet(SimpleNamespace(_run=engine.inspect)), primary, actual), id=record_id, target_slot=None)


def second_set(engine):
    changed_ids = {CID: "7" * 64, PROXY: "6" * 64, INTERNAL: "3" * 64, EGRESS: "4" * 64}
    changes = (
        *changed_ids.items(),
        ("foreign-prefix-native", "second-native"),
        ("retained-proxy", "second-proxy"),
        ("retained-internal", "second-internal"),
        ("retained-egress", "second-egress"),
        ('"native"', '"native2"'),
        ('"chat"', '"chat2"'),
    )
    for (kind, _identity), entry in list(engine.objects.items()):
        raw = json.dumps(entry)
        for original, changed in changes:
            raw = raw.replace(original, changed)
        copied = json.loads(raw)
        engine.objects[(kind, copied["Id"])] = copied
    engine.allowed_containers.update((changed_ids[CID], changed_ids[PROXY]))
    engine.allowed_networks.update((changed_ids[INTERNAL], changed_ids[EGRESS]))
    engine.expected_sets[changed_ids[CID]] = {changed_ids[PROXY], changed_ids[INTERNAL], changed_ids[EGRESS]}
    return changed_ids


def test_recovery_during_another_owners_admission_restores_the_original_context(fixture):
    from deerflow.community.aio_sandbox.owner_admission import OWNER_ADMISSION

    engine = Engine(fixture)
    engine.bridge.evidence.begin_teardown(saved_record(engine))
    engine.bridge.qualify()
    try:
        with engine.provider._native_admission("bob", ("b" * 32,)):
            reference = OWNER_ADMISSION.get()
            recover_teardowns(engine.provider)
            assert OWNER_ADMISSION.get() is reference
            assert not engine.objects and not engine.bridge.teardowns.records()
            assert not engine.provider._warm_pool and not engine.provider._sandboxes
    finally:
        discard_sandbox_lease_manager(engine.provider)


def test_held_first_record_does_not_starve_later_cleanup(fixture):
    engine = Engine(fixture)
    second_ids = second_set(engine)
    first = engine.bridge.evidence.begin_teardown(saved_record(engine))
    second = engine.bridge.evidence.begin_teardown(saved_record(engine, identity=second_ids[CID], record_id="b" * 32))
    engine.bridge.qualify()
    manager = get_sandbox_lease_manager(engine.provider)
    with manager._metadata_lock:
        manager._owners_by_sandbox["native"] = {"live-execution"}
    try:
        recover_teardowns(engine.provider)
        assert engine.bridge.evidence.snapshot().teardowns == (first,)
        assert ("container", CID) in engine.objects and ("container", PROXY) in engine.objects
        assert not any(identity == item.identity for kind, identity in engine.objects for item in second.resources)
        assert not engine.provider._sandboxes and not engine.provider._warm_pool
        with manager._metadata_lock:
            manager._owners_by_sandbox.pop("native")
        recover_teardowns(engine.provider)
        assert not engine.objects and not engine.bridge.evidence.snapshot().teardowns
    finally:
        with manager._metadata_lock:
            manager._owners_by_sandbox.pop("native", None)
        discard_sandbox_lease_manager(engine.provider)


def test_completion_before_canonical_reservation_does_not_create_phantom_quarantine(fixture, monkeypatch):
    from deerflow.community.aio_sandbox import aio_sandbox_provider

    engine = Engine(fixture)
    engine.bridge.evidence.begin_teardown(saved_record(engine))
    engine.bridge.qualify()
    engine.provider._warm_pool["native"] = (engine.info, 0)
    engine.provider._warm_pool_identity["native"] = engine.info.owner
    reserve = aio_sandbox_provider.cleanup_unheld_sandbox
    completed = []

    def finish_before_reserving(provider, sandbox_id, callback, **kwargs):
        assert not completed
        assert provider._destroy_warm_entry("native", engine.info, reason="cleanup_retry", still_reapable=lambda: True)
        assert not engine.objects and not engine.bridge.teardowns.records()
        completed.append(True)
        return reserve(provider, sandbox_id, callback, **kwargs)

    monkeypatch.setattr(aio_sandbox_provider, "cleanup_unheld_sandbox", finish_before_reserving)
    try:
        recover_teardowns(engine.provider)
        assert completed and not engine.objects
        assert not engine.provider._warm_pool and not getattr(engine.provider, "_cleanup_pending", {})
        assert not engine.bridge.teardowns.records() and not engine.bridge.evidence.snapshot().teardowns
        engine.bridge.require_admissible(owner_id="alice", name="new-chat")
    finally:
        discard_sandbox_lease_manager(engine.provider)


def test_completed_snapshot_record_is_not_resurrected(fixture, monkeypatch):
    engine = Engine(fixture)
    saved = engine.bridge.evidence.begin_teardown(saved_record(engine))
    engine.bridge.qualify()
    admit = engine.provider._native_admission

    @contextmanager
    def completed_before_attempt(*args):
        with admit(*args):
            # Another owning cleanup completed between capture and admission.
            engine.objects.clear()
            engine.bridge.evidence.finish_teardown(saved)
            engine.bridge.teardowns.remove(saved)
            engine.bridge.qualify()
            yield

    retire = Mock()
    publish = Mock(wraps=engine.bridge.evidence.begin_teardown)
    monkeypatch.setattr(engine.provider, "_native_admission", completed_before_attempt)
    monkeypatch.setattr(engine.provider, "_retire_unheld_container", retire)
    monkeypatch.setattr(engine.bridge.evidence, "begin_teardown", publish)
    before = len(engine.calls)
    recover_teardowns(engine.provider)
    retire.assert_not_called()
    publish.assert_not_called()
    assert len(engine.calls) == before
    assert not engine.bridge.teardowns.records() and not engine.provider._warm_pool


@pytest.mark.parametrize("foreign", [False, True])
def test_targeted_or_foreign_records_are_retained_without_ordinary_work(fixture, monkeypatch, foreign):
    engine = Engine(fixture)
    saved = replace(saved_record(engine), daemon_id="daemon-B" if foreign else "daemon-A", target_slot=None if foreign else SLOT)
    if foreign:
        path = engine.owner.pool / ".native-creates" / (saved.id + ".json")
        descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        with os.fdopen(descriptor, "w", encoding="utf-8") as stream:
            json.dump(saved.document(), stream)
    else:
        engine.bridge.evidence.begin_teardown(saved)
    engine.bridge.qualify()
    retire = Mock()
    monkeypatch.setattr(engine.provider, "_retire_unheld_container", retire)
    before = len(engine.calls)
    recover_teardowns(engine.provider)
    retire.assert_not_called()
    assert len(engine.calls) == before and engine.bridge.teardowns.records() == (saved,)
    with engine.provider._lock:
        assert engine.provider._counted_slots_locked()[3] == 1


def test_pass_is_bounded_rotates_after_refusal_and_preserves_shorter_deadline(fixture, monkeypatch):
    engine = Engine(fixture)
    original = saved_record(engine)
    records = []
    for index in range(5):
        resources = tuple(replace(member, identity=f"{100 + index * 4 + offset:064x}", name=f"member-{index}-{offset}") for offset, member in enumerate(original.resources))
        saved = replace(original, id=f"{index + 1:032x}", container_id=f"{100 + index * 4 + 3:064x}", sandbox_id=f"native-{index}", name=f"primary-{index}", resources=resources)
        records.append(engine.bridge.evidence.begin_teardown(saved))
    engine.bridge.qualify()
    attempted = []
    limit = time.monotonic() + 10

    def refuse(sandbox_id, _info, _owner, *, _expected_teardown):
        assert _NATIVE_SLOT_INVENTORY_DEADLINE.get() == limit
        assert _expected_teardown.sandbox_id == sandbox_id
        attempted.append(sandbox_id)
        raise RuntimeError("Injected holder refusal")

    monkeypatch.setattr(engine.provider, "_retire_unheld_container", refuse)
    token = _NATIVE_SLOT_INVENTORY_DEADLINE.set(limit)
    try:
        recover_teardowns(engine.provider)
        assert attempted == [record.sandbox_id for record in records[:4]]
        recover_teardowns(engine.provider)
        assert attempted[4:] == [records[4].sandbox_id, *[record.sandbox_id for record in records[:3]]]
        assert _NATIVE_SLOT_INVENTORY_DEADLINE.get() == limit
        assert len(engine.bridge.teardowns.records()) == 5 and not engine.provider._warm_pool
    finally:
        _NATIVE_SLOT_INVENTORY_DEADLINE.reset(token)


def test_initial_and_candidate_daemon_proof_inherit_the_pass_deadline(fixture, monkeypatch):
    engine = Engine(fixture)
    engine.bridge.evidence.begin_teardown(saved_record(engine))
    engine.bridge.qualify()
    calls = []

    def command(arguments, **kwargs):
        assert arguments == ["docker", "info", "--format", "{{json .}}"]
        calls.append(kwargs["timeout"])
        return SimpleNamespace(returncode=0, stdout=json.dumps({"ID": "daemon-A", "OSType": "linux"}), stderr="")

    adapter = DockerStorageAdapter.__new__(DockerStorageAdapter)
    adapter._operator_only = False
    adapter._host_verifier = lambda: None
    adapter._runner, adapter.host_id = engine.local._run, "daemon-A"
    monkeypatch.setattr(engine.local, "_runtime_runner", command)
    monkeypatch.setattr(engine.bridge, "_check_daemon", adapter._same_host)
    monkeypatch.setattr(engine.provider, "_retire_unheld_container", Mock())
    token = _NATIVE_SLOT_INVENTORY_DEADLINE.set(time.monotonic() + 10)
    try:
        recover_teardowns(engine.provider)
        assert len(calls) >= 3 and all(0 < timeout <= 10 for timeout in calls)
        assert len(engine.bridge.teardowns.records()) == 1
    finally:
        _NATIVE_SLOT_INVENTORY_DEADLINE.reset(token)
