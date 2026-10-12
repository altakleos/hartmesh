"""Provider publication and budgets retain exact native creation handoffs."""

from threading import Lock
from types import SimpleNamespace
from unittest.mock import Mock

import pytest
from test_aio_sandbox_provider import _make_provider

from deerflow.community.aio_sandbox.backend import DestroyOutcome
from deerflow.spaces.docker import StorageHostUnqualified
from deerflow.spaces.native_create_evidence import NativeContainerIdentity, NativeCreateAttempt
from deerflow.spaces.runtime_owner import PoolRuntimeOwner


@pytest.fixture
def fixture(tmp_path):
    pool = tmp_path / "pool"
    pool.mkdir(mode=0o750)
    (pool / ".gateway-owner.lock").touch(mode=0o600)
    (pool / ".native-creates").mkdir(mode=0o700)
    provider = _make_provider(tmp_path)
    provider._lock = Lock()
    provider._managed_runtime_required = True
    provider._backend = SimpleNamespace(list_running=Mock(return_value=[]), destroy=Mock(return_value=DestroyOutcome.ABSENT))
    owner = PoolRuntimeOwner(pool)
    owner.resolve_provider(lambda: provider)
    try:
        yield provider, owner
    finally:
        provider._acquire_serializer.close()
        provider._acquire_worker_executor.shutdown(wait=True)
        owner.close()


def install(provider, owner):
    evidence = owner.qualify_native_evidence(provider, daemon_id="daemon-A")
    adapter = SimpleNamespace(host_id="daemon-A", _same_host=Mock())
    provider._install_native_creation(evidence, adapter)
    assert provider._pool_runtime_current()
    return provider._native_creation


def record():
    return NativeCreateAttempt("a" * 32, "daemon-A", "alice", "chat", "native-a", ("b" * 32,))


def verified(attempt):
    return NativeContainerIdentity(attempt.id, attempt.daemon_id, attempt.owner_id, attempt.thread_id, attempt.name, attempt.slots, "c" * 64)


def test_owner_binding_alone_cannot_reconcile_before_journal_installation(fixture):
    provider, owner = fixture
    assert owner.accepts(provider)
    assert not provider._pool_runtime_current()
    provider._reconcile_orphans()
    provider._backend.list_running.assert_not_called()


def test_native_installation_reuses_exact_helper_bridge_and_backend_binding(fixture):
    provider, owner = fixture
    first = install(provider, owner)
    second = install(provider, owner)
    assert first is second and first.evidence is owner.native_create_evidence
    assert provider._backend._native_creation is first
    assert provider._backend._managed_native_required


def test_identityless_attempt_keeps_budget_after_starting_clears(fixture):
    provider, owner = fixture
    bridge = install(provider, owner)
    provider._starting.add("native")
    reservation = provider._begin_native_creation_call("native")
    handoff = bridge.handoffs.record_pending(record(), sandbox_id="native", reservation=reservation)
    with provider._lock:
        assert provider._counted_slots_locked() == (3, 0, 0, 1)
    provider._finish_native_creation_call("native", reservation)
    provider._unmark_starting("native", native_reservation=reservation)
    assert handoff.verified is None and "native" not in provider._starting
    with provider._lock:
        assert provider._counted_slots_locked() == (3, 0, 0, 1)
    assert not provider._warm_pool and not provider._sandboxes


def test_verified_return_without_caller_result_quarantines_before_starting_clears(fixture):
    provider, owner = fixture
    bridge = install(provider, owner)
    provider._starting.add("native")
    reservation = provider._begin_native_creation_call("native")
    attempt = record()
    handoff = bridge.handoffs.record_pending(attempt, sandbox_id="native", reservation=reservation)
    bridge.handoffs.record_verified(handoff, verified(attempt))
    bridge.handoffs.record_acknowledged(handoff)
    provider._finish_native_creation_call("native", reservation)
    assert provider._warm_pool["native"][0].container_id == "c" * 64
    assert provider._warm_pool["native"][0].owner == ("alice", "chat")
    assert provider._cleanup_pending["native"] is DestroyOutcome.UNKNOWN
    provider._unmark_starting("native", native_reservation=reservation)
    with provider._lock:
        assert provider._counted_slots_locked() == (3, 0, 1, 0)
    assert not provider._sandboxes


def test_quarantine_failure_preserves_verified_handoff_and_budget(fixture, monkeypatch):
    provider, owner = fixture
    bridge = install(provider, owner)
    provider._starting.add("native")
    reservation = provider._begin_native_creation_call("native")
    attempt = record()
    handoff = bridge.handoffs.record_pending(attempt, sandbox_id="native", reservation=reservation)
    bridge.handoffs.record_verified(handoff, verified(attempt))
    bridge.handoffs.record_acknowledged(handoff)

    def fail(*args, **kwargs):
        raise RuntimeError("Quarantine insertion interrupted")

    monkeypatch.setattr(provider, "_quarantine_after_failed_destroy", fail)
    provider._finish_native_creation_call("native", reservation)
    provider._unmark_starting("native", native_reservation=reservation)
    with provider._lock:
        assert provider._counted_slots_locked() == (3, 0, 0, 1)
    assert handoff.verified.container_id == "c" * 64


def test_old_finalizer_cannot_clear_new_worker_starting_reservation(fixture):
    provider, owner = fixture
    install(provider, owner)
    provider._starting.add("native")
    current = provider._begin_native_creation_call("native")
    provider._unmark_starting("native", native_reservation=object())
    assert "native" in provider._starting
    provider._unmark_starting("native", native_reservation=current)
    assert "native" not in provider._starting


def pending_candidate(provider, owner):
    from deerflow.community.aio_sandbox.sandbox_info import SandboxInfo

    bridge = install(provider, owner)
    labels = {"deerflow.owner_user_id": "alice", "deerflow.thread_id": "chat", "deerflow.sandbox_id": "native", "deerflow.role": "sandbox", "hartmesh.chat.storage": "on", "hartmesh.storage.space." + "b" * 32: "1"}
    handoff = bridge.begin(name="native-a", labels=labels)
    info = SandboxInfo("native", "http://native", container_name="native-a", container_id="c" * 64, owner=("alice", "chat"), chat_storage_enabled=True)
    return bridge, handoff, info


def test_orphan_reconciliation_does_not_claim_or_adopt_pending_candidate(fixture, monkeypatch):
    provider, owner = fixture
    _bridge, _handoff, info = pending_candidate(provider, owner)
    provider._unowned_since = {}
    provider._backend.list_running.return_value = [info]
    provider._claim_ownership = Mock(return_value=True)
    monkeypatch.setattr(provider, "_adoptable_after_grace", lambda *_args: True)
    provider._reconcile_orphans()
    provider._claim_ownership.assert_not_called()
    assert not provider._warm_pool


def test_discovery_cannot_claim_pending_candidate(fixture):
    provider, owner = fixture
    _bridge, _handoff, info = pending_candidate(provider, owner)
    provider._publish_ownership = Mock()
    with pytest.raises(StorageHostUnqualified):
        provider._register_discovered_sandbox("chat", info, user_id="alice")
    provider._publish_ownership.assert_not_called()
    assert not provider._sandboxes


def test_unknown_pending_candidate_cannot_be_removed_by_guessed_name(fixture):
    provider, owner = fixture
    _bridge, _handoff, info = pending_candidate(provider, owner)
    outcome, _error = provider._backend_destroy(info)
    assert outcome is DestroyOutcome.UNKNOWN
    provider._backend.destroy.assert_not_called()


def test_actual_verified_pending_result_can_use_canonical_exact_cleanup(fixture):
    provider, owner = fixture
    bridge, handoff, info = pending_candidate(provider, owner)
    bridge.handoffs.record_verified(handoff, verified(handoff.attempt))
    outcome, _error = provider._backend_destroy(info)
    assert outcome is DestroyOutcome.ABSENT
    provider._backend.destroy.assert_called_once_with(info)
    assert len(bridge.evidence.pending()) == 1 and bridge.handoffs.pending_for(owner_id="alice")
