"""Actorless original resident graph durability, using disk files and no Docker."""

import json
import os
from dataclasses import replace

import pytest
from test_native_create_bridge import fixture as fixture
from test_native_create_evidence import COMPANY, CONTAINER, OTHER, SLOT, begin, evidence
from test_native_create_evidence import pool as pool
from test_native_teardown_evidence import STAMP
from test_native_teardown_evidence import record as chat_record

from deerflow.spaces.docker import StorageHostUnqualified
from deerflow.spaces.native_resident_evidence import NativeResidentRecord, NativeResidentRoot

ATTACHMENT = "6" * 32
INCARNATION = "7" * 32
HOME = "8" * 32


def record(**changes):
    roots = (
        NativeResidentRoot(SLOT, "/pool/a/data", "/mnt/spaces/home", True, "1" * 32, "11111111-1111-4111-8111-111111111111", 10, 11, 12, 11, HOME, 1, None),
        NativeResidentRoot(COMPANY, "/pool/b/data/shared", "/mnt/user-data/shared", False, "2" * 32, "22222222-2222-4222-8222-222222222222", 20, 21, 22, 23, None, None, "9" * 64),
    )
    value = NativeResidentRecord(
        "5" * 32,
        "daemon-A",
        ATTACHMENT,
        INCARNATION,
        CONTAINER,
        "hartmesh-space-" + ATTACHMENT,
        STAMP,
        "prepared",
        "allowlist",
        "9" * 64,
        roots,
        (("/config/skills", "/mnt/skills", False),),
        chat_record().resources,
        network_target="retained-internal",
    )
    return replace(value, **changes)


def test_prepared_record_is_actorless_private_and_survives_restart(pool):
    store = evidence(pool)
    saved = record()
    try:
        assert store.begin_resident(saved) == saved
        snapshot = store.snapshot()
        assert snapshot.residents == (saved,) and snapshot.creates == snapshot.teardowns == ()
        path = pool / ".native-creates" / (saved.id + ".json")
        document = json.loads(path.read_text(encoding="utf-8"))
        assert document["schema_version"] == 5 and document["record_kind"] == "resident-resources"
        assert not {"actor", "owner_id", "thread_id", "sandbox_id"}.intersection(document)
        assert path.stat().st_mode & 0o777 == 0o600
        assert NativeResidentRecord.parse(document, saved.id) == saved
    finally:
        store.close()
    reopened = evidence(pool)
    try:
        assert reopened.snapshot().residents == (saved,)
    finally:
        reopened.close()


def test_prepared_resident_does_not_block_unrelated_chat_or_readonly_company_slot(pool):
    store = evidence(pool)
    try:
        saved = store.begin_resident(record())
        assert store.creation_ready
        attempt = begin(store, name="other-chat", slots=(OTHER, COMPANY))
        assert store.snapshot().residents == (saved,) and store.pending() == (attempt,)
    finally:
        store.close()


def test_retirement_is_monotonic_and_preserves_the_original_complete_graph(pool):
    store = evidence(pool)
    try:
        saved = store.begin_resident(record())
        initial = store.resident_revision
        retiring = store.retire_resident(saved)
        assert retiring == replace(saved, phase="retiring")
        assert store.resident_revision > initial
        assert store.retire_resident(retiring) == retiring
        with pytest.raises(StorageHostUnqualified):
            store.begin_resident(saved)
        assert store.snapshot().residents == (retiring,)
    finally:
        store.close()


@pytest.mark.parametrize("change", ["container", "created", "root", "member", "incarnation"])
def test_resident_publication_cannot_replace_original_correlation(pool, change):
    store = evidence(pool)
    saved = store.begin_resident(record())
    changes = {
        "container": {"container_id": "4" * 64},
        "created": {"created_at": "2026-10-14T01:00:00Z"},
        "root": {"roots": (replace(saved.roots[0], root_inode=99), saved.roots[1])},
        "member": {"resources": (replace(saved.resources[0], identity="4" * 64), *saved.resources[1:])},
        "incarnation": {"incarnation": "4" * 32},
    }
    try:
        with pytest.raises(StorageHostUnqualified):
            store.begin_resident(replace(saved, id="3" * 32, **changes[change]))
        assert store.snapshot().residents == (saved,)
    finally:
        store.close()


def test_resident_unlink_fsync_failure_retains_charge_until_durable_retry(pool, monkeypatch):
    store = evidence(pool)
    saved = store.retire_resident(store.begin_resident(record()))
    fsync = os.fsync

    def fail(fd):
        if fd == store._directory_fd:
            raise OSError("Resident removal sync interrupted")
        return fsync(fd)

    monkeypatch.setattr(os, "fsync", fail)
    try:
        with pytest.raises(OSError):
            store.finish_resident(saved)
        assert not (pool / ".native-creates" / (saved.id + ".json")).exists()
        assert store.snapshot().residents == (saved,)
        monkeypatch.setattr(os, "fsync", fsync)
        store.finish_resident(saved)
        assert store.snapshot().residents == ()
    finally:
        store.close()


def test_prepared_resident_cannot_finish_without_retirement(pool):
    store = evidence(pool)
    saved = store.begin_resident(record())
    try:
        with pytest.raises(StorageHostUnqualified):
            store.finish_resident(saved)
        assert store.snapshot().residents == (saved,)
    finally:
        store.close()


def test_mixed_namespace_preserves_chat_records_and_resident_facts(pool):
    store = evidence(pool)
    try:
        resources = tuple(replace(member, identity=value * 64, name="resident-" + member.name) for member, value in zip(record().resources, ("6", "7", "8"), strict=True))
        resident = store.begin_resident(record(container_id="4" * 64, resources=resources, network_target="resident-retained-internal"))
        chat = store.begin_teardown(chat_record())
        snapshot = store.snapshot()
        assert snapshot.residents == (resident,) and snapshot.teardowns == (chat,)
    finally:
        store.close()


def test_restart_refuses_chat_and_resident_graphs_with_the_same_companion(pool):
    store = evidence(pool)
    resident = record(container_id="4" * 64)
    try:
        store.begin_teardown(chat_record())
    finally:
        store.close()
    path = pool / ".native-creates" / (resident.id + ".json")
    path.write_text(json.dumps(resident.document()), encoding="utf-8")
    path.chmod(0o600)
    with pytest.raises(StorageHostUnqualified):
        evidence(pool)


def test_new_resident_record_requires_exact_bridge_accounting_restoration(fixture):
    journal, _state, bridge, _owner = fixture
    saved = journal.begin_resident(record())
    with pytest.raises(StorageHostUnqualified):
        bridge.assert_current()
    assert not bridge.ready
    bridge.qualify()
    assert bridge.ready and bridge.residents.records() == (saved,)
    bridge.require_admissible(name="unrelated-chat", owner_id="alice")


@pytest.mark.parametrize("fault", ["actor-field", "chat-version", "phase", "writable-config", "root-uuid", "root-generation", "overlap", "duplicate-role", "incomplete"])
def test_malformed_actorless_graph_never_publishes(pool, fault):
    document = record().document()
    if fault == "actor-field":
        document["owner_id"] = "pretend-human"
    elif fault == "chat-version":
        document["schema_version"] = 2
    elif fault == "phase":
        document["phase"] = "active"
    elif fault == "writable-config":
        document["configured_mounts"][0][2] = True
    elif fault == "root-uuid":
        document["roots"][0]["filesystem_uuid"] = "unqualified"
    elif fault == "root-generation":
        document["roots"][0]["generation"] = True
    elif fault == "overlap":
        document["configured_mounts"][0][1] = "/mnt/spaces/home/subdir"
    elif fault == "duplicate-role":
        document["resources"][1] = document["resources"][0]
    else:
        document["resources"].pop()
    with pytest.raises(StorageHostUnqualified):
        NativeResidentRecord.parse(document, document["id"])
    assert not any((pool / ".native-creates").iterdir())
