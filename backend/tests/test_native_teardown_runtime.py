"""Restore exact mixed lifecycle evidence before runtime publication."""

from types import SimpleNamespace

import pytest
from test_native_create_bridge import fixture as fixture
from test_native_create_provider import fixture as _provider_fixture
from test_native_create_provider import install
from test_native_teardown_evidence import COMPANY, CONTAINER, SLOT, record

from deerflow.community.aio_sandbox.sandbox_info import SandboxInfo
from deerflow.spaces.docker import StorageHostUnqualified


@pytest.fixture
def provider_fixture(tmp_path):
    yield from _provider_fixture.__wrapped__(tmp_path)


def test_qualification_restores_teardown_accounting_before_readiness(fixture):
    journal, _creates, bridge, _owner = fixture
    saved = journal.begin_teardown(record())
    assert not bridge.ready
    bridge.qualify()
    assert bridge.ready and bridge.teardowns.records() == (saved,)
    with bridge.teardowns.lock:
        assert bridge.teardowns.extra_slots_locked(known_container_ids=set()) == 1
        assert bridge.teardowns.extra_slots_locked(known_container_ids={CONTAINER}) == 0
    with pytest.raises(StorageHostUnqualified):
        bridge.require_admissible(name="another-thread", owner_id="alice")
    bridge.require_admissible(name="bob-thread", owner_id="bob")


def test_restored_record_refuses_cached_candidate_even_if_known_live(fixture):
    journal, _creates, bridge, _owner = fixture
    saved = journal.begin_teardown(record())
    bridge.qualify()
    info = SimpleNamespace(container_id=saved.container_id, container_name=saved.name, owner=(saved.owner_id, saved.thread_id), native_slots=saved.slots, sandbox_id=saved.sandbox_id)
    for teardown in (False, True):
        with pytest.raises(StorageHostUnqualified):
            bridge.assert_candidate_current(info, teardown=teardown, known_live=True)


def test_unrelated_candidate_sharing_companion_slot_remains_admissible(fixture):
    journal, _creates, bridge, _owner = fixture
    journal.begin_teardown(record())
    bridge.qualify()
    info = SimpleNamespace(container_id="7" * 64, container_name="bob-thread", owner=("bob", "thread"), native_slots=(COMPANY,), sandbox_id="bob-sandbox")
    bridge.assert_candidate_current(info, known_live=True)
    info.native_slots = (SLOT, COMPANY)
    with pytest.raises(StorageHostUnqualified):
        bridge.assert_candidate_current(info, known_live=True)


def test_journal_publication_closes_restored_memory_gate_immediately(fixture):
    journal, _creates, bridge, _owner = fixture
    assert bridge.ready
    saved = journal.begin_teardown(record())
    assert not bridge.ready
    bridge.qualify()
    assert bridge.ready
    journal.finish_teardown(saved)
    assert not bridge.ready
    bridge.teardowns.remove(saved)
    bridge.qualify()
    assert bridge.ready and not bridge.teardowns.records()


def test_cached_publication_rechecks_restored_teardown_under_metadata_lock(provider_fixture):
    provider, owner = provider_fixture
    bridge = install(provider, owner)
    saved = bridge.evidence.begin_teardown(record())
    bridge.qualify()
    info = SandboxInfo(saved.sandbox_id, "", container_id=saved.container_id, container_name=saved.name, owner=(saved.owner_id, saved.thread_id), native_slots=saved.slots)
    with provider._lock, pytest.raises(StorageHostUnqualified):
        provider._assert_managed_publication(info)


def test_provider_capacity_counts_record_once_against_exact_tracked_cid(provider_fixture):
    provider, owner = provider_fixture
    bridge = install(provider, owner)
    saved = bridge.evidence.begin_teardown(record())
    bridge.qualify()
    with provider._lock:
        assert provider._counted_slots_locked()[1:] == (0, 0, 1)
        info = SandboxInfo(saved.sandbox_id, "", container_id=saved.container_id, container_name=saved.name, owner=(saved.owner_id, saved.thread_id), native_slots=saved.slots)
        provider._warm_pool[saved.sandbox_id] = (info, 0)
        assert provider._counted_slots_locked()[1:] == (0, 1, 0)


def test_missing_journal_record_cannot_publish_readiness_with_retained_accounting(fixture):
    journal, _creates, bridge, _owner = fixture
    saved = journal.begin_teardown(record())
    bridge.qualify()
    (journal.pool / ".native-creates" / (saved.id + ".json")).unlink()
    with pytest.raises(StorageHostUnqualified):
        bridge.qualify()
    assert not bridge.ready and bridge.teardowns.records() == (saved,)
