"""Batch resident exclusion is atomic; read-only companions protect release."""

from threading import Lock

import pytest

from deerflow.community.aio_sandbox.owner_admission import NativeRetirementBusy, OwnerAdmissions
from deerflow.spaces.docker import StorageHostUnqualified

HOME, OTHER, SHARED = "a" * 32, "b" * 32, "c" * 32


def test_batch_reserves_every_original_owner_and_writer_before_publication():
    gate = OwnerAdmissions(lock=Lock())
    token = gate.reserve_resident_batch((("resident:first", (HOME,), (SHARED,)), ("resident:second", (OTHER,), (SHARED,))))
    try:
        assert gate.current_resident_batch(token)
        for owner in ("resident:first", "resident:second"):
            with pytest.raises(StorageHostUnqualified):
                gate.enter(owner)
        for slot in (HOME, OTHER, SHARED):
            with pytest.raises(NativeRetirementBusy):
                gate.reserve("release", slot)
    finally:
        gate.end_resident_batch(token)
    assert not gate.current_resident_batch(token)
    assert not gate._owners and not gate._slots and not gate._active


def test_shared_readonly_reference_allows_unrelated_work_but_prevents_release():
    gate = OwnerAdmissions(lock=Lock())
    chat = gate.enter("chat-owner")
    gate.register_slots(chat, (SHARED,))
    token = gate.reserve_resident_batch((("resident:first", (HOME,), (SHARED,)),))
    try:
        second = gate.enter("other-chat-owner")
        gate.register_slots(second, (SHARED,))
        gate.end(second)
        gate.end(chat)
        with pytest.raises(NativeRetirementBusy):
            gate.reserve("release", SHARED)
        assert gate.current_resident_batch(token)
    finally:
        gate.end(chat)
        gate.end_resident_batch(token)
    release = gate.reserve("release", SHARED)
    gate.end_retirement(release)


def test_busy_later_member_cannot_publish_any_partial_batch():
    gate = OwnerAdmissions(lock=Lock())
    live = gate.enter("resident:second")
    gate.register_slots(live, (OTHER, SHARED))
    try:
        with pytest.raises(NativeRetirementBusy):
            gate.reserve_resident_batch((("resident:first", (HOME,), (SHARED,)), ("resident:second", (OTHER,), (SHARED,))))
        assert not gate._owners and not gate._slots and gate._active == {live: live._group}
    finally:
        gate.end(live)


def test_existing_shared_slot_retirement_refuses_the_whole_resident_scope():
    gate = OwnerAdmissions(lock=Lock())
    release = gate.reserve("release", SHARED)
    try:
        with pytest.raises(NativeRetirementBusy):
            gate.reserve_resident_batch((("resident:first", (HOME,), (SHARED,)),))
        assert set(gate._owners) == {"release"} and set(gate._slots) == {SHARED} and not gate._active
    finally:
        gate.end_retirement(release)
