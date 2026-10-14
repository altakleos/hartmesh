"""Durable teardown memory accounting; no native or bundle invocation."""

from dataclasses import replace
from threading import Lock

import pytest
from test_native_teardown_evidence import COMPANY, CONTAINER, SLOT, record

from deerflow.community.aio_sandbox.native_teardown import NativeTeardownIndex
from deerflow.spaces.docker import StorageHostUnqualified


def state(records=()):
    value = NativeTeardownIndex(daemon_id="daemon-A", lock=Lock())
    token = value.begin_restore()
    value.finish_restore(token, records)
    return value


def count(value, known=()):
    with value.lock:
        return value.extra_slots_locked(known_container_ids=set(known))


def test_restored_teardown_counts_without_client_and_exact_cid_deduplicates():
    saved = record()
    value = state((saved,))
    assert value.ready and count(value) == 1
    assert count(value, (CONTAINER,)) == 0
    assert count(value, ("7" * 64,)) == 1
    assert value.records() == (saved,)


def test_teardown_blocks_owner_name_cid_and_explicit_target_not_companion():
    value = state((record(),))
    for arguments in ({"owner_id": "alice"}, {"name": "hartmesh-native-a"}, {"container_id": CONTAINER}, {"slots": (SLOT,)}):
        assert value.blocks(**arguments)
    assert not value.blocks(owner_id="bob", name="other", container_id="7" * 64, slots=(COMPANY,))


def test_stale_snapshot_cannot_resurrect_durably_completed_record():
    saved = record()
    value = state((saved,))
    token = value.begin_restore()
    value.remove(saved)
    value.finish_restore(token, (saved,))
    assert value.ready and value.records() == () and count(value) == 0


def test_stale_snapshot_cannot_omit_newly_published_record():
    value = state()
    token = value.begin_restore()
    saved = record()
    value.add(saved)
    value.finish_restore(token, ())
    assert value.ready and value.records() == (saved,) and count(value) == 1


def test_restore_conflict_retains_original_accounting_and_closes_readiness():
    saved = record()
    value = state((saved,))
    token = value.begin_restore()
    with pytest.raises(StorageHostUnqualified):
        value.finish_restore(token, (replace(saved, owner_id="bob"),))
    value.abort_restore(token)
    assert not value.ready and value.records() == (saved,) and count(value) == 1


def test_remove_requires_exact_immutable_record_and_never_owner_wide():
    saved = record()
    value = state((saved,))
    with pytest.raises(StorageHostUnqualified):
        value.remove(replace(saved, target_slot=None))
    assert value.records() == (saved,)
    value.remove(saved)
    assert count(value) == 0


def test_conflicting_primary_cid_cannot_duplicate_or_replace_accounting():
    saved = record()
    value = state((saved,))
    with pytest.raises(StorageHostUnqualified):
        value.add(replace(saved, id="3" * 32))
    assert value.records() == (saved,) and count(value) == 1


def test_foreign_daemon_record_is_counted_and_fences_matching_admission():
    foreign = replace(record(), daemon_id="daemon-B")
    value = state((foreign,))
    assert count(value) == 1 and value.blocks(owner_id="alice")
    with pytest.raises(StorageHostUnqualified):
        value.add(foreign)
    with pytest.raises(StorageHostUnqualified):
        value.remove(foreign)
    assert value.records() == (foreign,)


@pytest.mark.parametrize("collision", ["paired-id", "paired-name"])
def test_overlapping_actual_sets_refuse_accounting_before_any_cleanup(collision):
    first = record()
    resources = tuple(replace(item, identity=str(index + 4) * 64, name="other-" + item.name) for index, item in enumerate(first.resources))
    if collision == "paired-id":
        resources = (replace(resources[0], identity=first.resources[0].identity), *resources[1:])
    else:
        resources = (replace(resources[0], name=first.resources[0].name), *resources[1:])
    second = replace(first, id="3" * 32, container_id="7" * 64, name="other-primary", resources=resources)
    value = state((first,))
    token = value.begin_restore()
    with pytest.raises(StorageHostUnqualified):
        value.finish_restore(token, (first, second))
    value.abort_restore(token)
    assert not value.ready and value.records() == (first,)


def test_independent_same_owner_sets_remain_separately_counted_after_one_completes():
    first = record()
    resources = tuple(replace(item, identity=str(index + 4) * 64, name="other-" + item.name) for index, item in enumerate(first.resources))
    second = replace(first, id="3" * 32, container_id="7" * 64, name="other-primary", resources=resources)
    value = state((first, second))
    assert count(value) == 2
    token = value.begin_restore()
    value.remove(first)
    value.finish_restore(token, (first, second))
    assert value.records() == (second,) and count(value) == 1 and value.blocks(owner_id="alice")
    assert state(value.records()).records() == (second,)
