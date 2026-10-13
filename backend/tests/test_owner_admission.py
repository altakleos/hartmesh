"""Pure live owner/slot reservations, including borrowed and detached work."""

from threading import Lock

import pytest

from deerflow.spaces.docker import StorageHostUnqualified


def state():
    from deerflow.community.aio_sandbox.owner_admission import OwnerAdmissions

    return OwnerAdmissions(lock=Lock())


def test_idle_owner_and_slot_reservation_blocks_owner_and_foreign_slot_only():
    gate = state()
    retirement = gate.reserve("alice", "a" * 32)
    with pytest.raises(StorageHostUnqualified):
        gate.enter("alice")
    bob = gate.enter("bob")
    with pytest.raises(StorageHostUnqualified):
        gate.register_slots(bob, ("a" * 32,))
    gate.register_slots(bob, ("b" * 32, "c" * 32))
    gate.end(bob)
    assert gate.current(retirement)
    gate.end_retirement(retirement)
    assert not gate.current(retirement)
    token = gate.enter("alice")
    gate.end(token)


@pytest.mark.parametrize("phase", ["before_prepare", "after_slots"])
def test_owner_admission_before_preparation_blocks_retirement(phase):
    gate = state()
    admitted = gate.enter("alice")
    if phase == "after_slots":
        gate.register_slots(admitted, ("a" * 32, "c" * 32))
    with pytest.raises(StorageHostUnqualified):
        gate.reserve("alice", "a" * 32)
    gate.end(admitted)
    retirement = gate.reserve("alice", "a" * 32)
    gate.end_retirement(retirement)


def test_other_owner_native_slot_admission_blocks_target_slot_retirement():
    gate = state()
    bob = gate.enter("bob")
    gate.register_slots(bob, ("a" * 32,))
    with pytest.raises(StorageHostUnqualified):
        gate.reserve("alice", "a" * 32)
    gate.end(bob)


def test_company_intersection_does_not_serialize_unrelated_user_slot():
    gate = state()
    bob = gate.enter("bob")
    gate.register_slots(bob, ("b" * 32, "c" * 32))
    retirement = gate.reserve("alice", "a" * 32)
    charlie = gate.enter("charlie")
    gate.register_slots(charlie, ("d" * 32, "c" * 32))
    gate.end(charlie)
    gate.end(bob)
    gate.end_retirement(retirement)


def test_retained_prewarm_reference_lives_after_caller_exit():
    gate = state()
    caller = gate.enter("alice")
    future = gate.enter("alice", borrow=caller)
    gate.end(caller)
    with pytest.raises(StorageHostUnqualified):
        gate.reserve("alice", "a" * 32)
    worker = gate.enter("alice", borrow=future)
    gate.register_slots(worker, ("a" * 32,))
    gate.end(worker)
    with pytest.raises(StorageHostUnqualified):
        gate.reserve("bob", "a" * 32)
    gate.end(future)
    retirement = gate.reserve("bob", "a" * 32)
    gate.end_retirement(retirement)


def test_detached_copied_context_cannot_borrow_expired_token():
    gate = state()
    parent = gate.enter("alice")
    retained = gate.enter("alice", borrow=parent)
    gate.end(parent)
    with pytest.raises(StorageHostUnqualified):
        gate.enter("alice", borrow=parent)
    gate.end(retained)


def test_repeated_old_reference_exit_cannot_release_future_reference():
    gate = state()
    old = gate.enter("alice")
    future = gate.enter("alice", borrow=old)
    assert old is not future
    gate.end(old)
    gate.end(old)
    with pytest.raises(StorageHostUnqualified):
        gate.reserve("alice", "a" * 32)
    gate.end(future)


def test_stale_retirement_exit_cannot_clear_new_owner_or_slot_reservation():
    gate = state()
    old = gate.reserve("alice", "a" * 32)
    gate.end_retirement(old)
    new = gate.reserve("alice", "a" * 32)
    gate.end_retirement(old)
    assert gate.current(new) and not gate.current(old)
    with pytest.raises(StorageHostUnqualified):
        gate.enter("alice")
    gate.end_retirement(new)


def test_borrow_cannot_cross_owner_or_provider():
    gate, peer = state(), state()
    token = gate.enter("alice")
    with pytest.raises(StorageHostUnqualified):
        gate.enter("bob", borrow=token)
    with pytest.raises(StorageHostUnqualified):
        peer.enter("alice", borrow=token)
    gate.end(token)


def test_final_publication_checks_both_owner_and_slot_under_existing_lock():
    gate = state()
    retirement = gate.reserve("alice", "a" * 32)
    with gate.lock:
        assert not gate.allowed_locked("alice", ())
        assert not gate.allowed_locked("bob", ("a" * 32,))
        assert gate.allowed_locked("bob", ("b" * 32, "c" * 32))
        assert gate.allowed_locked("alice", ("a" * 32,), retirement=retirement)
    gate.end_retirement(retirement)


def test_new_slots_are_visible_to_every_live_borrowed_reference():
    gate = state()
    outer = gate.enter("alice")
    inner = gate.enter("alice", borrow=outer)
    gate.register_slots(inner, ("a" * 32,))
    gate.end(inner)
    with pytest.raises(StorageHostUnqualified):
        gate.reserve("bob", "a" * 32)
    gate.end(outer)


def test_expired_token_cannot_register_slots():
    gate = state()
    token = gate.enter("alice")
    gate.end(token)
    with pytest.raises(StorageHostUnqualified):
        gate.register_slots(token, ("a" * 32,))


def test_conflicting_slot_registration_has_no_partial_publication():
    gate = state()
    token = gate.enter("bob")
    retiring = gate.reserve("alice", "a" * 32)
    with pytest.raises(StorageHostUnqualified):
        gate.register_slots(token, ("b" * 32, "a" * 32))
    other = gate.reserve("charlie", "b" * 32)
    gate.end_retirement(other)
    gate.end_retirement(retiring)
    gate.end(token)


def test_retirement_exemption_cannot_cross_owner():
    gate = state()
    token = gate.reserve("alice", "a" * 32)
    with gate.lock:
        assert not gate.allowed_locked("bob", ("a" * 32,), retirement=token)
    gate.end_retirement(token)


def test_expired_retirement_exemption_never_authorizes_publication():
    gate = state()
    token = gate.reserve("alice", "a" * 32)
    gate.end_retirement(token)
    with gate.lock:
        assert not gate.allowed_locked("alice", ("a" * 32,), retirement=token)
