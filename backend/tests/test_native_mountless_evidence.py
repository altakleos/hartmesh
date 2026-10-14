"""Truthful no-slot records, with no native/Docker or bundle invocation."""

import json
from dataclasses import replace

import pytest
from test_native_create_evidence import begin, evidence, identity
from test_native_create_evidence import pool as pool
from test_native_teardown_evidence import record

from deerflow.spaces.docker import StorageHostUnqualified


def test_mountless_pending_survives_restart_without_fabricated_slots(pool):
    store = evidence(pool)
    try:
        attempt = store.begin_mountless(owner_id="alice", thread_id="chat-a", name="hartmesh-mountless-a")
        assert attempt.slots == () and store.pending(owner_id="alice") == (attempt,)
        document = json.loads((pool / ".native-creates" / (attempt.id + ".json")).read_text(encoding="utf-8"))
        assert document["schema_version"] == 3 and document["record_kind"] == "mountless-create" and document["slots"] == []
        store.close()
        reopened = evidence(pool)
        try:
            assert reopened.pending() == (attempt,)
            with pytest.raises(StorageHostUnqualified):
                reopened.begin_mountless(owner_id="alice", thread_id="another", name="different-name")
        finally:
            reopened.close()
    finally:
        store.close()


def test_mountless_ack_requires_exact_identity_and_preserves_existing_slot_schema(pool):
    store = evidence(pool)
    try:
        slot = begin(store, owner="bob", name="hartmesh-slots")
        attempt = store.begin_mountless(owner_id="alice", thread_id="chat-a", name="hartmesh-mountless-a")
        with pytest.raises(StorageHostUnqualified):
            store.acknowledge(attempt, identity(attempt, slots=slot.slots))
        assert attempt in store.pending()
        store.acknowledge(attempt, identity(attempt))
        assert store.pending() == (slot,)
        document = json.loads((pool / ".native-creates" / (slot.id + ".json")).read_text(encoding="utf-8"))
        assert document["schema_version"] == 1 and "record_kind" not in document
        with pytest.raises(StorageHostUnqualified):
            store.begin(owner_id="another", thread_id="chat", name="ordinary-empty", slots=())
    finally:
        store.close()


def test_mountless_teardown_roundtrip_uses_explicit_additive_format(pool):
    from deerflow.spaces.native_teardown_evidence import NativeMountlessTeardownRecord

    original = record(slots=(), target_slot=None)
    saved = NativeMountlessTeardownRecord(**original.__dict__)
    store = evidence(pool)
    try:
        assert store.begin_teardown(saved) == saved
        document = json.loads((pool / ".native-creates" / (saved.id + ".json")).read_text(encoding="utf-8"))
        assert document["schema_version"] == 4 and document["record_kind"] == "mountless-teardown"
        assert document["slots"] == [] and document["target_slot"] is None
    finally:
        store.close()
    reopened = evidence(pool)
    try:
        assert reopened.snapshot().teardowns == (saved,)
        with pytest.raises(StorageHostUnqualified):
            reopened.begin_teardown(replace(saved, target_slot="a" * 32))
    finally:
        reopened.close()


@pytest.mark.parametrize("kind", ["create", "teardown"])
@pytest.mark.parametrize("change", ["slots", "kind", "version", "boolean_version"])
def test_mountless_codec_refuses_nonempty_slots_and_mismatched_schema(pool, kind, change):
    from deerflow.spaces.native_create_evidence import NativeCreateEvidence
    from deerflow.spaces.native_teardown_evidence import NativeMountlessTeardownRecord

    store = evidence(pool)
    try:
        if kind == "create":
            saved = store.begin_mountless(owner_id="alice", thread_id="chat-a", name="hartmesh-mountless-a")
        else:
            saved = NativeMountlessTeardownRecord(**record(slots=(), target_slot=None).__dict__)
            store.begin_teardown(saved)
        document = json.loads((pool / ".native-creates" / (saved.id + ".json")).read_text(encoding="utf-8"))
        if change == "slots":
            document["slots"] = ["a" * 32]
        elif change == "kind":
            document["record_kind"] = "teardown" if kind == "teardown" else "mountless-teardown"
        elif change == "version":
            document["schema_version"] = 2 if kind == "teardown" else 1
        else:
            document["schema_version"] = True
        with pytest.raises(StorageHostUnqualified):
            NativeCreateEvidence._validate(document, saved.id)
    finally:
        store.close()
