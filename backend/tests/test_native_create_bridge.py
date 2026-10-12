"""Offline primary command doubles exercise the actual durable bridge protocol."""

import json
from threading import Lock
from types import SimpleNamespace

import pytest

from deerflow.spaces.docker import StorageHostUnqualified
from deerflow.spaces.native_create_evidence import ATTEMPT_LABEL, NativeCreateEvidence

SLOT, CONTAINER = "a" * 32, "c" * 64
LABELS = {
    "deerflow.owner_user_id": "alice",
    "deerflow.thread_id": "chat",
    "deerflow.sandbox_id": "sandbox-a",
    "deerflow.role": "sandbox",
    "deerflow.network_mode": "open",
    "hartmesh.chat.storage": "on",
    "hartmesh.storage.space." + SLOT: "1",
}


@pytest.fixture
def fixture(tmp_path):
    from deerflow.community.aio_sandbox.native_creation import NativeCreateBridge, NativeCreateHandoffs

    pool = tmp_path / "pool"
    pool.mkdir(mode=0o750)
    (pool / ".native-creates").mkdir(mode=0o700)
    journal = NativeCreateEvidence(pool, daemon_id="daemon-A")
    state = NativeCreateHandoffs(daemon_id="daemon-A", lock=Lock())
    owner = SimpleNamespace(live=True, daemon="daemon-A")

    def check_owner(*, teardown=False):
        if not owner.live and not teardown:
            raise StorageHostUnqualified("Runtime closing")

    def check_daemon():
        if owner.daemon != "daemon-A":
            raise StorageHostUnqualified("Daemon changed")

    bridge = NativeCreateBridge(evidence=journal, handoffs=state, check_owner=check_owner, owner_accepts=lambda: owner.live, check_daemon=check_daemon)
    bridge.qualify()
    assert bridge.ready
    try:
        yield journal, state, bridge, owner
    finally:
        journal.close()


def command():
    return ["docker", "run", "--name", "native-a", "image"]


def engine(journal, *, error=None, mismatch=None, after_run=None):
    calls, observed = [], []

    def run(cmd, **kwargs):
        calls.append(cmd)
        if cmd[1] == "run":
            (pending,) = journal.pending()
            assert pending.name == "native-a" and pending.slots == (SLOT,)
            assert ATTEMPT_LABEL + "=" + pending.id in cmd
            observed.append(pending)
            if error:
                raise error
            if after_run:
                after_run()
            return SimpleNamespace(stdout=CONTAINER, returncode=0, stderr="")
        assert cmd == ["docker", "inspect", CONTAINER]
        record = observed[0]
        entry = {"Id": CONTAINER, "Name": "/native-a", "Config": {"Labels": {**LABELS, ATTEMPT_LABEL: record.id}}}
        if mismatch == "id":
            entry["Id"] = "d" * 64
        elif mismatch == "name":
            entry["Name"] = "/other"
        elif mismatch == "extra_slot":
            entry["Config"]["Labels"]["hartmesh.storage.space." + "e" * 32] = "1"
        elif mismatch:
            entry["Config"]["Labels"][mismatch] = "wrong"
        return SimpleNamespace(stdout=json.dumps([entry]), returncode=0, stderr="")

    return run, calls, observed


def test_pending_is_durable_and_verified_identity_retained_before_ack(fixture):
    journal, state, bridge, _owner = fixture
    runner, calls, observed = engine(journal)
    assert bridge.submit(name="native-a", labels=LABELS, command=command(), runner=runner) == CONTAINER
    assert len(calls) == 2 and journal.pending() == ()
    assert not state.pending_for(owner_id="alice")
    with state.lock:
        assert state.extra_slots_locked(starting={}, known_container_ids=set()) == 1
    assert observed[0].name == "native-a"


@pytest.mark.parametrize("mismatch", ["id", "name", "extra_slot", ATTEMPT_LABEL, "deerflow.owner_user_id", "deerflow.thread_id", "deerflow.sandbox_id", "deerflow.role"])
def test_wrong_actual_inspection_keeps_unreturned_request_pending(fixture, mismatch):
    journal, state, bridge, _owner = fixture
    runner, calls, _observed = engine(journal, mismatch=mismatch)
    with pytest.raises(StorageHostUnqualified):
        bridge.submit(name="native-a", labels=LABELS, command=command(), runner=runner)
    assert len(calls) == 2 and len(journal.pending()) == 1
    assert state.pending_for(owner_id="alice")
    with state.lock:
        assert state.extra_slots_locked(starting={}, known_container_ids={CONTAINER}) == 1


def test_failed_primary_command_remains_pending_even_with_no_inventory(fixture):
    journal, state, bridge, _owner = fixture
    runner, calls, _observed = engine(journal, error=RuntimeError("port is already allocated"))
    with pytest.raises(StorageHostUnqualified):
        bridge.submit(name="native-a", labels=LABELS, command=command(), runner=runner)
    assert len(calls) == 1 and len(journal.pending()) == 1
    with pytest.raises(StorageHostUnqualified):
        bridge.require_admissible(name="other-chat", owner_id="alice")
    bridge.require_admissible(name="bob-chat", owner_id="bob")
    assert state.pending_for(owner_id="alice")


def test_journal_fsync_failure_submits_no_primary_command(fixture, monkeypatch):
    journal, state, bridge, _owner = fixture
    runner, calls, _observed = engine(journal)

    def fail(raw):
        raise OSError("No durable journal write")

    monkeypatch.setattr(journal, "_write_stage", fail)
    with pytest.raises(StorageHostUnqualified):
        bridge.submit(name="native-a", labels=LABELS, command=command(), runner=runner)
    assert calls == [] and journal.pending() == ()


def test_different_daemon_after_submission_keeps_identity_unknown(fixture):
    journal, state, bridge, owner = fixture
    runner, calls, _observed = engine(journal, after_run=lambda: setattr(owner, "daemon", "daemon-B"))
    with pytest.raises(StorageHostUnqualified):
        bridge.submit(name="native-a", labels=LABELS, command=command(), runner=runner)
    assert len(calls) == 1 and len(journal.pending()) == 1
    with state.lock:
        assert state.extra_slots_locked(starting={}, known_container_ids={CONTAINER}) == 1


def test_ack_failure_keeps_positively_verified_identity_counted(fixture, monkeypatch):
    journal, state, bridge, _owner = fixture
    runner, _calls, _observed = engine(journal)

    def fail(*args):
        raise OSError("Acknowledgment fsync interrupted")

    monkeypatch.setattr(journal, "acknowledge", fail)
    with pytest.raises(StorageHostUnqualified) as caught:
        bridge.submit(name="native-a", labels=LABELS, command=command(), runner=runner)
    assert caught.value.handoff.verified.container_id == CONTAINER
    assert len(journal.pending()) == 1
    with state.lock:
        assert state.extra_slots_locked(starting={}, known_container_ids=set()) == 1
        assert state.extra_slots_locked(starting={}, known_container_ids={CONTAINER}) == 0


def test_callback_failure_after_pending_publication_submits_nothing(fixture, monkeypatch):
    journal, state, bridge, _owner = fixture
    runner, calls, _observed = engine(journal)
    original = state.record_pending

    def fail(record, **kwargs):
        original(record, **kwargs)
        raise RuntimeError("Callback interrupted")

    monkeypatch.setattr(state, "record_pending", fail)
    with pytest.raises(StorageHostUnqualified):
        bridge.submit(name="native-a", labels=LABELS, command=command(), runner=runner)
    assert calls == [] and len(journal.pending()) == 1
    assert state.pending_for(owner_id="alice")


def test_same_handoff_cannot_resubmit_an_unreturned_primary_command(fixture):
    journal, _state, bridge, _owner = fixture
    handoff = bridge.begin(name="native-a", labels=LABELS)
    runner, calls, _observed = engine(journal, error=RuntimeError("Command failed"))
    with pytest.raises(StorageHostUnqualified):
        bridge.submit(name="native-a", labels=LABELS, command=command(), runner=runner, handoff=handoff)
    with pytest.raises(StorageHostUnqualified):
        bridge.submit(name="native-a", labels=LABELS, command=command(), runner=runner, handoff=handoff)
    assert len(calls) == 1 and len(journal.pending()) == 1


def test_known_namespace_failure_disables_memory_publication(fixture):
    journal, _state, bridge, _owner = fixture
    (journal.pool / ".native-creates/unknown").write_bytes(b"retained uncertainty")
    with pytest.raises(StorageHostUnqualified):
        bridge.assert_current()
    assert not bridge.ready and journal.ready


def test_interrupted_stage_write_disables_publication_and_submits_nothing(fixture, monkeypatch):
    journal, _state, bridge, _owner = fixture
    runner, calls, _observed = engine(journal)

    def fail(raw):
        (journal.pool / ".native-creates/.write-unknown").write_bytes(b"interrupted")
        raise OSError("Stage write failed")

    monkeypatch.setattr(journal, "_write_stage", fail)
    with pytest.raises(StorageHostUnqualified):
        bridge.submit(name="native-a", labels=LABELS, command=command(), runner=runner)
    assert not bridge.ready and calls == []


def test_closing_qualification_failure_retains_exact_known_teardown_binding(fixture):
    _journal, _state, bridge, owner = fixture
    owner.live = False
    with pytest.raises(StorageHostUnqualified):
        bridge.qualify()
    assert not bridge.ready
    bridge.assert_current(teardown=True)


def test_postpublication_fsync_failure_restores_pending_capacity_before_readiness(fixture, monkeypatch):
    import os

    journal, state, bridge, _owner = fixture
    runner, calls, _observed = engine(journal)
    original = os.fsync
    failed = []

    def fail_once(fd):
        if fd == journal._directory_fd and list((journal.pool / ".native-creates").glob("*.json")) and not failed:
            failed.append(True)
            raise OSError("Published pending directory fsync interrupted")
        return original(fd)

    monkeypatch.setattr(os, "fsync", fail_once)
    with pytest.raises(StorageHostUnqualified):
        bridge.submit(name="native-a", labels=LABELS, command=command(), runner=runner)
    assert failed and calls == [] and len(journal.pending()) == 1
    assert bridge.ready and state.pending_for(owner_id="alice")
    with state.lock:
        assert state.extra_slots_locked(starting={}, known_container_ids=set()) == 1
