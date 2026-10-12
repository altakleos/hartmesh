"""Native request identity and capacity survive worker/caller handoff barriers."""

from threading import Lock

import pytest

from deerflow.spaces.docker import StorageHostUnqualified
from deerflow.spaces.native_create_evidence import NativeContainerIdentity, NativeCreateAttempt


def state():
    from deerflow.community.aio_sandbox.native_creation import NativeCreateHandoffs

    result = NativeCreateHandoffs(daemon_id="daemon-A", lock=Lock())
    token = result.begin_restore()
    assert not result.ready
    result.finish_restore(token, ())
    assert result.ready
    return result


def attempt(identifier="a", owner="alice", name="native-a"):
    return NativeCreateAttempt(identifier * 32, "daemon-A", owner, "chat", name, ("b" * 32,))


def verified(record, **changed):
    values = dict(attempt_id=record.id, daemon_id=record.daemon_id, owner_id=record.owner_id, thread_id=record.thread_id, name=record.name, slots=record.slots, container_id="c" * 64)
    return NativeContainerIdentity(**{**values, **changed})


def count(result, *, starting=None, known=()):
    with result.lock:
        return result.extra_slots_locked(starting=starting or {}, known_container_ids=set(known))


def test_identityless_pending_counts_without_fabricated_client():
    result = state()
    record = attempt()
    reservation = object()
    handoff = result.record_pending(record, sandbox_id="sandbox-a", reservation=reservation)
    assert handoff.verified is None and count(result) == 1
    assert count(result, starting={"sandbox-a": reservation}) == 0
    assert count(result, known=("c" * 64,)) == 1
    assert result.pending_for(owner_id="alice") and result.pending_for(name="native-a")
    assert result.pending_for(slot_id="b" * 32)
    assert not result.pending_for(owner_id="bob")


def test_verified_ack_is_retained_and_counted_before_backend_return():
    result = state()
    record = attempt()
    reservation = object()
    handoff = result.record_pending(record, sandbox_id="sandbox-a", reservation=reservation)
    identity = verified(record)
    result.record_verified(handoff, identity)
    result.record_acknowledged(handoff)
    assert not result.pending_for(owner_id="alice")
    assert count(result) == 1 and handoff.verified is identity
    assert count(result, starting={"sandbox-a": reservation}) == 0
    assert count(result, known=(identity.container_id,)) == 0
    with result.lock:
        result.transferred_locked(handoff)
    assert count(result) == 0


@pytest.mark.parametrize("field", ["attempt_id", "daemon_id", "owner_id", "thread_id", "name", "slots", "container_id"])
def test_mismatched_verified_identity_cannot_authorize_handoff(field):
    result = state()
    record = attempt()
    handoff = result.record_pending(record, sandbox_id="sandbox-a")
    changed = ("d" * 32,) if field == "slots" else "wrong"
    with pytest.raises(StorageHostUnqualified):
        result.record_verified(handoff, verified(record, **{field: changed}))
    assert handoff.verified is None and count(result) == 1


def test_unverified_handoff_cannot_disappear_in_transfer_or_ack():
    result = state()
    handoff = result.record_pending(attempt(), sandbox_id="sandbox-a")
    with pytest.raises(StorageHostUnqualified):
        result.record_acknowledged(handoff)
    with result.lock, pytest.raises(StorageHostUnqualified):
        result.transferred_locked(handoff)
    assert count(result) == 1


def test_snapshot_restore_cannot_drop_new_pending_callback():
    result = state()
    token = result.begin_restore()
    new = result.record_pending(attempt("d", "bob", "native-b"), sandbox_id="sandbox-b")
    result.finish_restore(token, (attempt(),))
    assert count(result) == 2 and new.verified is None
    assert result.pending_for(owner_id="alice") and result.pending_for(owner_id="bob")


def test_old_snapshot_cannot_resurrect_completed_handoff():
    result = state()
    record = attempt()
    handoff = result.record_pending(record, sandbox_id="sandbox-a")
    token = result.begin_restore()
    result.record_verified(handoff, verified(record))
    result.record_acknowledged(handoff)
    with result.lock:
        result.transferred_locked(handoff)
    result.finish_restore(token, (record,))
    assert count(result) == 0 and not result.pending_for(owner_id="alice")


def test_snapshot_cannot_rewrite_live_attempt_correlation():
    from dataclasses import replace

    result = state()
    record = attempt()
    handoff = result.record_pending(record, sandbox_id="sandbox-a")
    token = result.begin_restore()
    result.record_verified(handoff, verified(record))
    with pytest.raises(StorageHostUnqualified):
        result.finish_restore(token, (replace(record, owner_id="bob"),))
    result.abort_restore(token)
    assert not result.ready and count(result) == 1
    assert handoff.attempt is record and handoff.verified == verified(record)


def test_restored_foreign_daemon_ack_is_uncertain_and_cannot_supply_teardown_identity():
    from dataclasses import replace

    result = state()
    foreign = replace(attempt(), daemon_id="daemon-B", phase="acknowledged", container_id="c" * 64)
    token = result.begin_restore()
    result.finish_restore(token, (foreign,))
    assert result.pending_for(owner_id="alice") and count(result, known=("c" * 64,)) == 1


def test_restore_update_limit_refuses_snapshot_without_forgetting_handoffs(monkeypatch):
    from deerflow.community.aio_sandbox import native_creation as module

    result = state()
    monkeypatch.setattr(module, "MAX_RESTORE_UPDATES", 1)
    token = result.begin_restore()
    first = result.record_pending(attempt(), sandbox_id="sandbox-a")
    second = result.record_pending(attempt("d", "bob", "native-b"), sandbox_id="sandbox-b")
    with pytest.raises(StorageHostUnqualified):
        result.finish_restore(token, ())
    result.abort_restore(token)
    assert not result.ready and count(result) == 2
    assert first.verified is second.verified is None
    retry = result.begin_restore()
    result.finish_restore(retry, (first.attempt, second.attempt))
    assert result.ready and count(result) == 2


def test_failed_ack_transfer_keeps_pending_and_capacity_when_known_container_disappears():
    result = state()
    record = attempt()
    handoff = result.record_pending(record, sandbox_id="sandbox-a")
    identity = verified(record)
    result.record_verified(handoff, identity)
    with result.lock:
        result.transferred_locked(handoff)
    assert result.pending_for(owner_id="alice") and result.pending_for(name="native-a")
    assert result.pending_for(slot_id="b" * 32)
    assert count(result, known=(identity.container_id,)) == 0
    assert count(result) == 1


def test_new_worker_same_sandbox_id_cannot_hide_old_unresolved_attempt():
    result = state()
    old_reservation, new_reservation = object(), object()
    result.record_pending(attempt(), sandbox_id="sandbox-a", reservation=old_reservation)
    with result.lock:
        assert result.extra_slots_locked(starting={"sandbox-a": new_reservation}, known_container_ids=set()) == 1
        assert result.extra_slots_locked(starting={"sandbox-a": old_reservation}, known_container_ids=set()) == 0
