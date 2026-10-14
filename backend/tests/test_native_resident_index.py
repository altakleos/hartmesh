"""Original actorless resident charge restoration is memory-only and exact."""

from dataclasses import replace
from threading import Lock

import pytest
from test_native_resident_evidence import record

from deerflow.community.aio_sandbox.native_resident_index import NativeResidentIndex
from deerflow.spaces.docker import StorageHostUnqualified


def state(records=()):
    index = NativeResidentIndex(daemon_id="daemon-A", lock=Lock())
    token = index.begin_restore()
    index.finish_restore(token, records)
    return index


def test_resident_charge_requires_exact_attachment_cid_host_incarnation_and_scope():
    saved = record()
    index = state((saved,))
    with index.lock:
        assert index.extra_slots_locked(known_claims=frozenset()) == 1
        assert index.extra_slots_locked(known_claims={saved.correlation}) == 0
        for field in ("attachment_id", "container_id", "daemon_id", "incarnation", "roots"):
            changes = {field: "4" * (64 if field == "container_id" else 32)}
            if field == "roots":
                changes[field] = (replace(saved.roots[0], generation=2), saved.roots[1])
            assert index.extra_slots_locked(known_claims={replace(saved, **changes).correlation}) == 1


def test_retirement_state_never_regresses_under_a_delayed_snapshot():
    saved = record()
    index = state((saved,))
    token = index.begin_restore()
    retiring = replace(saved, phase="retiring")
    index.add(retiring)
    index.finish_restore(token, (saved,))
    assert index.records() == (retiring,) and index.ready


def test_durable_completion_cannot_be_resurrected_by_a_stale_snapshot():
    saved = record(phase="retiring")
    index = state((saved,))
    token = index.begin_restore()
    index.remove(saved)
    index.finish_restore(token, (saved,))
    assert index.records() == () and index.ready


def test_newly_published_resident_cannot_be_omitted_by_a_stale_snapshot():
    index = state()
    token = index.begin_restore()
    saved = record()
    index.add(saved)
    index.finish_restore(token, ())
    assert index.records() == (saved,) and index.ready


@pytest.mark.parametrize("change", ["primary", "created", "member", "root"])
def test_restore_conflict_retains_original_charge_and_closes_readiness(change):
    saved = record()
    index = state((saved,))
    token = index.begin_restore()
    changes = {
        "primary": {"container_id": "4" * 64},
        "created": {"created_at": "2026-10-14T01:00:00Z"},
        "member": {"resources": (replace(saved.resources[0], identity="4" * 64), *saved.resources[1:])},
        "root": {"roots": (replace(saved.roots[0], source_inode=99), saved.roots[1])},
    }
    with pytest.raises(StorageHostUnqualified):
        index.finish_restore(token, (replace(saved, **changes[change]),))
    index.abort_restore(token)
    assert not index.ready and index.records() == (saved,)


def test_foreign_resident_is_counted_but_cannot_be_mutated():
    saved = record(daemon_id="foreign")
    index = state((saved,))
    with index.lock:
        assert index.extra_slots_locked(known_claims=frozenset()) == 1
    for operation in (index.add, index.remove):
        with pytest.raises(StorageHostUnqualified):
            operation(saved)
    assert index.records() == (saved,)


def test_duplicate_original_graph_cannot_replace_or_duplicate_charge():
    saved = record()
    index = state((saved,))
    with pytest.raises(StorageHostUnqualified):
        index.add(replace(saved, id="3" * 32))
    assert index.records() == (saved,)
