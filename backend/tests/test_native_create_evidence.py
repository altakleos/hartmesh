"""Durable host evidence precedes native Docker requests and survives uncertainty."""

import json
import os

import pytest

from deerflow.spaces.docker import StorageHostUnqualified

SLOT = "a" * 32
COMPANY = "b" * 32
OTHER = "c" * 32
CONTAINER = "d" * 64


@pytest.fixture
def pool(tmp_path):
    root = tmp_path / "pool"
    root.mkdir(mode=0o750)
    (root / ".native-creates").mkdir(mode=0o700)
    return root


def evidence(pool, *, daemon="daemon-A"):
    from deerflow.spaces.native_create_evidence import NativeCreateEvidence

    return NativeCreateEvidence(pool, daemon_id=daemon)


def begin(store, *, owner="alice", name="hartmesh-native-a", slots=(SLOT, COMPANY)):
    return store.begin(owner_id=owner, thread_id="chat-a", name=name, slots=slots)


def identity(attempt, **overrides):
    from deerflow.spaces.native_create_evidence import NativeContainerIdentity

    values = dict(attempt_id=attempt.id, daemon_id=attempt.daemon_id, name=attempt.name, owner_id=attempt.owner_id, thread_id=attempt.thread_id, slots=attempt.slots, container_id=CONTAINER)
    return NativeContainerIdentity(**{**values, **overrides})


def test_pending_is_private_durable_and_reopened_before_any_inventory(pool):
    store = evidence(pool)
    try:
        attempt = begin(store)
        assert store.pending(slots=(SLOT,)) == (attempt,)
        path = pool / ".native-creates" / (attempt.id + ".json")
        metadata = path.stat()
        assert metadata.st_mode & 0o777 == 0o600 and metadata.st_nlink == 1
        doc = json.loads(path.read_text(encoding="utf-8"))
        assert doc["phase"] == "pending" and doc["container_id"] is None
        store.close()
        reopened = evidence(pool)
        try:
            assert reopened.pending(slots=(SLOT,)) == (attempt,)
            with pytest.raises(StorageHostUnqualified):
                begin(reopened)
        finally:
            reopened.close()
    finally:
        store.close()


def test_pending_blocks_matching_owner_or_name_but_not_other_users(pool):
    store = evidence(pool)
    try:
        first = begin(store)
        with pytest.raises(StorageHostUnqualified):
            begin(store, name="another-thread")
        with pytest.raises(StorageHostUnqualified):
            begin(store, owner="bob")
        second = begin(store, owner="bob", name="hartmesh-native-b", slots=(OTHER, COMPANY))
        assert store.pending(slots=(SLOT,)) == (first,)
        assert store.pending(slots=(OTHER,)) == (second,)
        assert set(store.pending(slots=(COMPANY,))) == {first, second}
    finally:
        store.close()


@pytest.mark.parametrize("mismatch", ["attempt_id", "daemon_id", "name", "owner_id", "thread_id", "slots", "container_id"])
def test_acknowledgment_requires_the_exact_verified_create_identity(pool, mismatch):
    store = evidence(pool)
    try:
        attempt = begin(store)
        changed = (OTHER,) if mismatch == "slots" else "unexpected"
        with pytest.raises(StorageHostUnqualified):
            store.acknowledge(attempt, identity(attempt, **{mismatch: changed}))
        assert store.pending(slots=(SLOT,)) == (attempt,)
    finally:
        store.close()


def test_verified_ack_is_persisted_before_record_is_removed(pool, monkeypatch):
    store = evidence(pool)
    attempt = begin(store)
    unlink = os.unlink
    observed = []

    def inspect_then_unlink(path, **kwargs):
        if str(path) == attempt.id + ".json":
            with open(pool / ".native-creates" / str(path), encoding="utf-8") as source:
                doc = json.load(source)
            assert doc["phase"] == "acknowledged" and doc["container_id"] == CONTAINER
            observed.append(True)
        return unlink(path, **kwargs)

    monkeypatch.setattr(os, "unlink", inspect_then_unlink)
    try:
        store.acknowledge(attempt, identity(attempt))
        assert observed == [True] and store.pending() == ()
    finally:
        store.close()


def test_failed_ack_publication_keeps_pending_across_restart(pool, monkeypatch):
    store = evidence(pool)
    attempt = begin(store)
    replace = os.replace

    def fail(*args, **kwargs):
        raise OSError("Acknowledgment publication failed")

    monkeypatch.setattr(os, "replace", fail)
    try:
        with pytest.raises((OSError, StorageHostUnqualified)):
            store.acknowledge(attempt, identity(attempt))
    finally:
        store.close()
    monkeypatch.setattr(os, "replace", replace)
    assert json.loads((pool / ".native-creates" / (attempt.id + ".json")).read_text(encoding="utf-8"))["phase"] == "pending"
    try:
        reopened = evidence(pool)
    except StorageHostUnqualified:
        return
    try:
        # Unknown staging from the interrupted write may refuse all inspection;
        # it must never turn the pending attempt into absent evidence.
        try:
            pending = reopened.pending()
        except StorageHostUnqualified:
            return
        assert any(item.id == attempt.id for item in pending)
    finally:
        reopened.close()


@pytest.mark.parametrize("fault", ["symlink", "hardlink", "fifo", "directory", "mode", "oversized", "invalid_json", "unknown_schema", "wrong_name"])
def test_unknown_record_evidence_is_never_adopted_or_removed(pool, fault):
    directory = pool / ".native-creates"
    path = directory / ("e" * 32 + ".json")
    retained = pool / "retained"
    retained.write_bytes(b"retain")
    if fault == "symlink":
        path.symlink_to(retained)
    elif fault == "fifo":
        os.mkfifo(path)
    elif fault == "directory":
        path.mkdir()
    else:
        path.write_bytes(b"x" * 65537 if fault == "oversized" else b"not a known record")
        path.chmod(0o644 if fault == "mode" else 0o600)
        if fault == "hardlink":
            os.link(path, directory / "another")
        elif fault in ("unknown_schema", "wrong_name"):
            path.write_text(json.dumps({"schema_version": 2 if fault == "unknown_schema" else 1, "id": "f" * 32}), encoding="utf-8")
    with pytest.raises(StorageHostUnqualified):
        store = evidence(pool)
        try:
            store.pending()
        finally:
            store.close()
    assert path.exists() and retained.read_bytes() == b"retain"


@pytest.mark.parametrize("fault", ["symlink", "mode", "changed"])
def test_private_directory_identity_cannot_be_repaired_or_switched_by_runtime(pool, fault):
    directory = pool / ".native-creates"
    if fault == "symlink":
        directory.rmdir()
        outside = pool / "outside"
        outside.mkdir(mode=0o700)
        directory.symlink_to(outside, target_is_directory=True)
    elif fault == "mode":
        directory.chmod(0o770)
    if fault != "changed":
        with pytest.raises(StorageHostUnqualified):
            evidence(pool)
        return
    store = evidence(pool)
    directory.rename(pool / ".retained-creates")
    directory.mkdir(mode=0o700)
    try:
        with pytest.raises(StorageHostUnqualified):
            begin(store)
        assert list(directory.iterdir()) == []
    finally:
        store.close()


def test_pending_records_are_not_evicted_when_record_limit_is_reached(pool, monkeypatch):
    from deerflow.spaces import native_create_evidence as module

    monkeypatch.setattr(module, "MAX_RECORDS", 2)
    store = evidence(pool)
    try:
        first = begin(store)
        second = begin(store, owner="bob", name="hartmesh-native-b", slots=(OTHER, COMPANY))
        with pytest.raises(StorageHostUnqualified):
            begin(store, owner="carol", name="hartmesh-native-c", slots=(OTHER,))
        assert set(store.pending()) == {first, second}
    finally:
        store.close()


def test_gateway_umask_cannot_publish_an_unreadable_evidence_record(pool):
    store = evidence(pool)
    mask = os.umask(0o600)
    try:
        attempt = begin(store)
        assert store.pending() == (attempt,)
        assert (pool / ".native-creates" / (attempt.id + ".json")).stat().st_mode & 0o777 == 0o600
    finally:
        os.umask(mask)
        store.close()


def test_staging_replacement_cannot_publish_other_evidence(pool, monkeypatch):
    from deerflow.spaces.filesystem import ConfinedFilesystem

    store = evidence(pool)
    rename = ConfinedFilesystem._rename_no_replace
    original_write = store._write_stage

    def write_then_replace(raw):
        stage = original_write(raw)
        (pool / ".native-creates" / stage).rename(pool / "retained-stage")
        (pool / ".native-creates" / stage).write_bytes(b"other evidence")
        return stage

    monkeypatch.setattr(store, "_write_stage", write_then_replace)
    try:
        with pytest.raises(StorageHostUnqualified):
            begin(store)
        assert not any(path.name.endswith(".json") for path in (pool / ".native-creates").iterdir())
        assert (pool / "retained-stage").exists()
    finally:
        monkeypatch.setattr(ConfinedFilesystem, "_rename_no_replace", rename)
        store.close()


def test_restart_can_prune_only_valid_durable_acknowledgment_for_same_daemon(pool, monkeypatch):
    store = evidence(pool)
    attempt = begin(store)
    unlink = os.unlink

    def fail_unlink(path, **kwargs):
        if str(path) == attempt.id + ".json":
            raise OSError("Interrupted acknowledgment cleanup")
        return unlink(path, **kwargs)

    monkeypatch.setattr(os, "unlink", fail_unlink)
    try:
        with pytest.raises((OSError, StorageHostUnqualified)):
            store.acknowledge(attempt, identity(attempt))
    finally:
        store.close()
    monkeypatch.setattr(os, "unlink", unlink)
    foreign = evidence(pool, daemon="daemon-B")
    try:
        assert len(foreign.pending(slots=(SLOT,))) == 1
        assert (pool / ".native-creates" / (attempt.id + ".json")).exists()
    finally:
        foreign.close()
    same = evidence(pool)
    try:
        assert same.pending() == ()
        assert list((pool / ".native-creates").iterdir()) == []
    finally:
        same.close()


def test_pending_attempt_from_another_daemon_cannot_be_acknowledged(pool):
    first = evidence(pool)
    attempt = begin(first)
    first.close()
    second = evidence(pool, daemon="daemon-B")
    try:
        with pytest.raises(StorageHostUnqualified):
            second.acknowledge(attempt, identity(attempt, daemon_id="daemon-B"))
        assert second.pending() == (attempt,)
    finally:
        second.close()


def test_directory_replaced_during_begin_fsync_refuses_submission_permission(pool, monkeypatch):
    store = evidence(pool)
    fsync = os.fsync
    replaced = []

    def replace_directory_after_publication(fd):
        fsync(fd)
        if fd == store._directory_fd and not replaced:
            (pool / ".native-creates").rename(pool / "retained-evidence")
            (pool / ".native-creates").mkdir(mode=0o700)
            replaced.append(True)

    monkeypatch.setattr(os, "fsync", replace_directory_after_publication)
    try:
        with pytest.raises(StorageHostUnqualified):
            begin(store)
        assert replaced and list((pool / ".native-creates").iterdir()) == []
        assert len(list((pool / "retained-evidence").iterdir())) == 1
    finally:
        store.close()


@pytest.mark.parametrize("acknowledgment", [False, True])
def test_directory_fsync_failure_preserves_uncertainty_across_restart(pool, monkeypatch, acknowledgment):
    store = evidence(pool)
    attempt = begin(store) if acknowledgment else None
    fsync = os.fsync

    def fail_directory_fsync(fd):
        if fd == store._directory_fd:
            raise OSError("Durable directory publication failed")
        return fsync(fd)

    monkeypatch.setattr(os, "fsync", fail_directory_fsync)
    try:
        with pytest.raises((OSError, StorageHostUnqualified)):
            if acknowledgment:
                store.acknowledge(attempt, identity(attempt))
            else:
                begin(store)
        documents = [json.loads(path.read_text(encoding="utf-8")) for path in (pool / ".native-creates").glob("*.json")]
        assert len(documents) == 1
        assert documents[0]["phase"] == ("acknowledged" if acknowledgment else "pending")
    finally:
        store.close()
    monkeypatch.setattr(os, "fsync", fsync)
    reopened = evidence(pool)
    try:
        # Same-daemon restart fsyncs the complete immutable acknowledgment
        # again before pruning; an unreturned create remains pending.
        assert len(reopened.pending()) == (0 if acknowledgment else 1)
    finally:
        reopened.close()


def test_duplicate_json_fields_are_unknown_evidence(pool):
    store = evidence(pool)
    attempt = begin(store)
    store.close()
    path = pool / ".native-creates" / (attempt.id + ".json")
    raw = path.read_text(encoding="utf-8")
    path.write_text(raw[:-1] + ',"phase":"pending"}', encoding="utf-8")
    with pytest.raises(StorageHostUnqualified):
        evidence(pool)
    assert path.read_text(encoding="utf-8").endswith(',"phase":"pending"}')


def test_total_byte_bound_refuses_without_evicting_pending(pool, monkeypatch):
    from deerflow.spaces import native_create_evidence as module

    store = evidence(pool)
    try:
        first = begin(store)
        size = (pool / ".native-creates" / (first.id + ".json")).stat().st_size
        monkeypatch.setattr(module, "MAX_TOTAL_BYTES", size)
        with pytest.raises(StorageHostUnqualified):
            begin(store, owner="bob", name="hartmesh-native-b", slots=(OTHER,))
        assert store.pending() == (first,)
    finally:
        store.close()
