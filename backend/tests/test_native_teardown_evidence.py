"""Private lifecycle metadata on disk; no Docker, native or bundle invocation."""

import json
import os
from dataclasses import replace

import pytest
from test_native_create_bridge import fixture as fixture
from test_native_create_evidence import COMPANY, CONTAINER, OTHER, SLOT, begin, evidence, identity
from test_native_create_evidence import pool as pool

from deerflow.spaces.docker import StorageHostUnqualified
from deerflow.spaces.native_teardown_evidence import NativeTeardownRecord, NativeTeardownResource

STAMP = "2026-10-14T00:00:00.123456789Z"


def record(**changes):
    resources = tuple(
        NativeTeardownResource(kind, role, value * 64, name, STAMP)
        for kind, role, value, name in (
            ("container", "network-proxy", "f", "retained-relay"),
            ("network", "network", "1", "retained-internal"),
            ("network", "egress-network", "2", "retained-egress"),
        )
    )
    value = NativeTeardownRecord("e" * 32, "daemon-A", "alice", "chat-a", "sandbox-a", (SLOT, COMPANY), SLOT, CONTAINER, "hartmesh-native-a", STAMP, "allowlist", "9" * 64, resources)
    return replace(value, **changes)


def test_teardown_roundtrip_keeps_exact_timestamps_and_survives_restart(pool):
    store = evidence(pool)
    saved = record()
    try:
        assert store.begin_teardown(saved) == saved
        assert store.snapshot().teardowns == (saved,) and store.pending() == ()
        path = pool / ".native-creates" / (saved.id + ".json")
        doc = json.loads(path.read_text(encoding="utf-8"))
        assert doc["schema_version"] == 2 and doc["record_kind"] == "teardown"
        assert doc["created_at"] == STAMP and all(item["created_at"] == STAMP for item in doc["resources"])
        assert path.stat().st_mode & 0o777 == 0o600
    finally:
        store.close()
    reopened = evidence(pool)
    try:
        assert reopened.snapshot().teardowns == (saved,)
        assert path.exists()
    finally:
        reopened.close()


def test_mixed_namespace_never_prunes_teardown_as_an_acknowledged_create(pool, monkeypatch):
    store = evidence(pool)
    attempt = begin(store)
    saved = record()
    store.begin_teardown(saved)
    unlink = os.unlink

    def interrupted(path, **kwargs):
        if str(path) == attempt.id + ".json":
            raise OSError("Acknowledged create unlink interrupted")
        return unlink(path, **kwargs)

    monkeypatch.setattr(os, "unlink", interrupted)
    try:
        with pytest.raises(OSError):
            store.acknowledge(attempt, identity(attempt))
    finally:
        store.close()
    monkeypatch.setattr(os, "unlink", unlink)
    reopened = evidence(pool)
    try:
        snapshot = reopened.snapshot()
        assert snapshot.creates == () and snapshot.teardowns == (saved,)
        assert (pool / ".native-creates" / (saved.id + ".json")).exists()
    finally:
        reopened.close()


@pytest.mark.parametrize("change", ["target", "proxy", "policy"])
def test_primary_correlation_is_unique_and_cannot_change_on_retry(pool, change):
    store = evidence(pool)
    saved = record()
    try:
        store.begin_teardown(saved)
        assert store.begin_teardown(replace(saved, id="3" * 32)) == saved
        changed = replace(saved, id="4" * 32)
        if change == "target":
            changed = replace(changed, target_slot=None)
        elif change == "proxy":
            changed = replace(changed, resources=(replace(changed.resources[0], identity="5" * 64), *changed.resources[1:]))
        else:
            changed = replace(changed, policy_digest="6" * 64)
        with pytest.raises(StorageHostUnqualified):
            store.begin_teardown(changed)
        assert store.snapshot().teardowns == (saved,)
    finally:
        store.close()


def test_teardown_and_create_share_capacity_without_eviction(pool, monkeypatch):
    from deerflow.spaces import native_create_evidence as module

    monkeypatch.setattr(module, "MAX_RECORDS", 2)
    store = evidence(pool)
    try:
        saved = store.begin_teardown(record())
        attempt = begin(store, owner="bob", name="bob-chat", slots=(OTHER, COMPANY))
        with pytest.raises(StorageHostUnqualified):
            begin(store, owner="carol", name="carol-chat", slots=(OTHER,))
        assert store.snapshot().teardowns == (saved,) and store.pending() == (attempt,)
    finally:
        store.close()


def test_teardown_target_blocks_reassignment_but_incidental_readonly_slot_does_not(pool):
    store = evidence(pool)
    try:
        store.begin_teardown(record())
        with pytest.raises(StorageHostUnqualified):
            begin(store, owner="bob", name="bob-chat", slots=(SLOT,))
        allowed = begin(store, owner="bob", name="bob-chat", slots=(OTHER, COMPANY))
        assert store.pending() == (allowed,)
    finally:
        store.close()


def test_foreign_daemon_records_are_retained_but_cannot_be_published_or_finished(pool):
    first = evidence(pool)
    saved = first.begin_teardown(record())
    first.close()
    other = evidence(pool, daemon="daemon-B")
    try:
        assert other.snapshot().teardowns == (saved,)
        for operation in (other.begin_teardown, other.finish_teardown):
            with pytest.raises(StorageHostUnqualified):
                operation(saved)
        assert other.snapshot().teardowns == (saved,)
    finally:
        other.close()


@pytest.mark.parametrize("closing", [False, True])
def test_new_teardown_evidence_closes_bridge_until_exact_accounting_restoration(fixture, closing):
    journal, _state, bridge, owner = fixture
    journal.begin_teardown(record())
    if closing:
        owner.live = False
    with pytest.raises(StorageHostUnqualified):
        bridge.assert_current(teardown=closing)
    assert not bridge.ready
    if closing:
        with pytest.raises(StorageHostUnqualified):
            bridge.qualify()
    else:
        bridge.qualify()
        assert bridge.ready and bridge.teardowns.records() == (record(),)
    assert journal.snapshot().teardowns == (record(),)


def test_unlink_directory_fsync_failure_remains_visible_until_durable_retry(pool, monkeypatch):
    store = evidence(pool)
    saved = store.begin_teardown(record())
    fsync = os.fsync

    def fail(fd):
        if fd == store._directory_fd:
            raise OSError("Removal directory sync failed")
        return fsync(fd)

    monkeypatch.setattr(os, "fsync", fail)
    try:
        with pytest.raises(OSError):
            store.finish_teardown(saved)
        assert not (pool / ".native-creates" / (saved.id + ".json")).exists()
        assert store.snapshot().teardowns == (saved,)
        with pytest.raises(StorageHostUnqualified):
            begin(store)
        monkeypatch.setattr(os, "fsync", fsync)
        store.finish_teardown(saved)
        assert store.snapshot().teardowns == ()
    finally:
        store.close()


@pytest.mark.parametrize("fault", ["duplicate-role", "duplicate-id", "naive-time", "unknown-slot", "bad-digest", "incomplete-set"])
def test_malformed_teardown_record_never_publishes(pool, fault):
    saved = record()
    if fault == "duplicate-role":
        saved = replace(saved, resources=(saved.resources[0], saved.resources[0], saved.resources[2]))
    elif fault == "duplicate-id":
        saved = replace(saved, resources=(replace(saved.resources[0], identity=saved.container_id), *saved.resources[1:]))
    elif fault == "naive-time":
        saved = replace(saved, created_at="2026-10-14T00:00:00")
    elif fault == "unknown-slot":
        saved = replace(saved, target_slot=OTHER)
    elif fault == "bad-digest":
        saved = replace(saved, policy_digest="bad")
    else:
        saved = replace(saved, resources=saved.resources[:1])
    store = evidence(pool)
    try:
        with pytest.raises(StorageHostUnqualified):
            store.begin_teardown(saved)
        assert list((pool / ".native-creates").iterdir()) == []
    finally:
        store.close()


def test_existing_publication_retries_directory_durability_before_returning(pool, monkeypatch):
    store = evidence(pool)
    saved = record()
    fsync = os.fsync

    def fail(fd):
        if fd == store._directory_fd:
            raise OSError("Publication directory sync failed")
        return fsync(fd)

    monkeypatch.setattr(os, "fsync", fail)
    try:
        with pytest.raises(OSError):
            store.begin_teardown(saved)
        assert (pool / ".native-creates" / (saved.id + ".json")).exists()
        with pytest.raises(OSError):
            store.begin_teardown(replace(saved, id="3" * 32))
        monkeypatch.setattr(os, "fsync", fsync)
        assert store.begin_teardown(replace(saved, id="4" * 32)) == saved
    finally:
        store.close()


def test_failed_completion_cannot_be_republished_as_a_durable_begin(pool, monkeypatch):
    store = evidence(pool)
    saved = store.begin_teardown(record())
    fsync = os.fsync

    def fail(fd):
        if fd == store._directory_fd:
            raise OSError("Removal directory sync failed")
        return fsync(fd)

    monkeypatch.setattr(os, "fsync", fail)
    try:
        with pytest.raises(OSError):
            store.finish_teardown(saved)
        with pytest.raises(StorageHostUnqualified):
            store.begin_teardown(replace(saved, id="3" * 32))
    finally:
        store.close()


@pytest.mark.parametrize("operation", ["begin", "finish"])
def test_namespace_replacement_during_sync_never_completes_on_a_new_directory(pool, monkeypatch, operation):
    store = evidence(pool)
    saved = record()
    if operation == "finish":
        store.begin_teardown(saved)
    fsync = os.fsync
    changed = []

    def replace_namespace(fd):
        fsync(fd)
        if fd == store._directory_fd and not changed:
            (pool / ".native-creates").rename(pool / "retained-evidence")
            (pool / ".native-creates").mkdir(mode=0o700)
            changed.append(True)

    monkeypatch.setattr(os, "fsync", replace_namespace)
    try:
        with pytest.raises(StorageHostUnqualified):
            (store.begin_teardown if operation == "begin" else store.finish_teardown)(saved)
        assert changed and list((pool / ".native-creates").iterdir()) == []
        assert not store.creation_ready
    finally:
        store.close()


def test_completion_does_not_unlink_a_same_content_replacement_inode(pool, monkeypatch):
    store = evidence(pool)
    saved = store.begin_teardown(record())
    path = pool / ".native-creates" / (saved.id + ".json")
    raw, fsync, replaced = path.read_bytes(), os.fsync, []

    def replace_file_after_sync(fd):
        fsync(fd)
        if fd != store._directory_fd and not replaced:
            path.rename(pool / "retained-teardown")
            path.write_bytes(raw)
            path.chmod(0o600)
            replaced.append(path.stat().st_ino)

    monkeypatch.setattr(os, "fsync", replace_file_after_sync)
    try:
        with pytest.raises(StorageHostUnqualified):
            store.finish_teardown(saved)
        assert path.read_bytes() == raw and path.stat().st_ino == replaced[0]
        assert (pool / "retained-teardown").exists() and not store.creation_ready
    finally:
        store.close()


def test_unknown_teardown_fields_remain_on_disk_after_restart_refusal(pool):
    store = evidence(pool)
    saved = store.begin_teardown(record())
    store.close()
    path = pool / ".native-creates" / (saved.id + ".json")
    doc = json.loads(path.read_text(encoding="utf-8"))
    doc["unrecognized_authority"] = True
    path.write_text(json.dumps(doc), encoding="utf-8")
    with pytest.raises(StorageHostUnqualified):
        evidence(pool)
    assert json.loads(path.read_text(encoding="utf-8"))["unrecognized_authority"] is True


def test_unconfirmed_removal_still_counts_when_the_directory_looks_empty(pool, monkeypatch):
    from deerflow.spaces import native_create_evidence as module

    store = evidence(pool)
    saved = store.begin_teardown(record())
    fsync = os.fsync

    def fail(fd):
        if fd == store._directory_fd:
            raise OSError("Removal sync failed")
        return fsync(fd)

    monkeypatch.setattr(os, "fsync", fail)
    monkeypatch.setattr(module, "MAX_RECORDS", 1)
    try:
        with pytest.raises(OSError):
            store.finish_teardown(saved)
        assert list((pool / ".native-creates").iterdir()) == []
        with pytest.raises(StorageHostUnqualified):
            begin(store, owner="bob", name="bob-chat", slots=(OTHER, COMPANY))
        assert store.snapshot().teardowns == (saved,)
    finally:
        store.close()


def test_bridge_uses_one_mixed_snapshot_and_memory_gate_closes_before_fresh_read(fixture, monkeypatch):
    journal, _state, bridge, _owner = fixture
    snapshot, calls = journal.snapshot, []

    def observed():
        calls.append(True)
        return snapshot()

    monkeypatch.setattr(journal, "snapshot", observed)
    assert bridge.assert_current() == () and calls == [True]
    journal.begin_teardown(record())
    assert not bridge.ready
    with pytest.raises(StorageHostUnqualified):
        bridge.assert_current()
    assert calls == [True, True]
