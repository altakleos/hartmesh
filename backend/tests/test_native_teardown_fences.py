"""Actual holder fences precede client closure and native cleanup effects."""

from contextvars import copy_context
from threading import Event
from types import SimpleNamespace
from unittest.mock import Mock

import pytest
from test_native_create_provider import fixture as fixture
from test_native_retirement_scope import qualify

from deerflow.community.aio_sandbox.native_teardown import current_native_lease
from deerflow.community.aio_sandbox.sandbox_info import SandboxInfo
from deerflow.sandbox.lease import cleanup_unheld_sandbox, current_cleanup_reservation, discard_sandbox_lease_manager, get_sandbox_lease_manager


def test_holder_proof_is_exact_and_expires_in_copied_context(fixture):
    provider, _owner = fixture
    captured = []

    def callback():
        proof = current_cleanup_reservation(provider, "native")
        assert proof is not None
        assert current_cleanup_reservation(provider, "other") is None
        assert current_cleanup_reservation(object(), "native") is None
        captured.append(copy_context())

    try:
        assert current_cleanup_reservation(provider, "native") is None
        assert cleanup_unheld_sandbox(provider, "native", callback)
        assert captured[0].run(current_cleanup_reservation, provider, "native") is None
    finally:
        discard_sandbox_lease_manager(provider)


@pytest.mark.parametrize("path", ["tracked", "unhealthy", "warm"])
def test_held_native_client_is_preserved_before_untracking_or_closure(fixture, path):
    provider, bridge, local = qualify(fixture)
    client = Mock()
    info = SandboxInfo("native", "http://native", container_name="foreign-native", container_id="c" * 64, owner=("alice", "chat"), chat_storage_enabled=True, native_slots=("a" * 32,))
    if path == "warm":
        provider._warm_pool["native"] = (info, 0)
        provider._warm_pool_identity["native"] = info.owner
    else:
        provider._sandboxes["native"] = client
        provider._sandbox_infos["native"] = info
        provider._active_sandbox_identity["native"] = info.owner
    manager = get_sandbox_lease_manager(provider)
    with manager._metadata_lock:
        manager._owners_by_sandbox["native"] = {"held-execution"}
    try:
        if path == "warm":
            assert provider._destroy_warm_entry("native", info, reason="idle_timeout", still_reapable=lambda: True) is False
            assert provider._warm_pool["native"][0] is info
        elif path == "unhealthy":
            provider._drop_unhealthy_sandbox("native", "unhealthy", expected_info=info)
            assert provider._sandboxes["native"] is client and provider._sandbox_infos["native"] is info
        else:
            provider._destroy_tracked("native", still_reapable=lambda: True)
            assert provider._sandboxes["native"] is client and provider._sandbox_infos["native"] is info
        client.close.assert_not_called()
        local.destroy.assert_not_called()
        assert bridge.evidence.snapshot().teardowns == () and not provider._local_teardown
    finally:
        with manager._metadata_lock:
            manager._owners_by_sandbox.pop("native")
        discard_sandbox_lease_manager(provider)


def test_held_store_proof_expires_when_lease_ends_even_in_copied_context(fixture):
    provider, _owner = fixture
    captured = []

    def callback():
        assert provider._reserve_local_teardown("native", lambda: True)
        try:
            assert provider._claim_ownership("native", for_destroy=True)
            with provider._held_teardown_lease("native"):
                assert current_native_lease(provider, "native") is not None
                assert current_native_lease(provider, "other") is None
                captured.append(copy_context())
            assert captured[0].run(current_native_lease, provider, "native") is None
        finally:
            provider._finish_local_teardown("native")

    try:
        assert cleanup_unheld_sandbox(provider, "native", callback)
    finally:
        discard_sandbox_lease_manager(provider)


def test_observed_store_ownership_loss_closes_held_destructive_proof(fixture, monkeypatch):
    provider, _owner = fixture
    provider._ownership_config = SimpleNamespace(renewal_interval_seconds=0.001)
    lost = Event()

    def claim(*_args, **_kwargs):
        lost.set()
        return False

    def callback():
        assert provider._reserve_local_teardown("native", lambda: True)
        try:
            monkeypatch.setattr(provider._ownership, "claim", claim)
            with provider._held_teardown_lease("native") as lease:
                assert lost.wait(5)
                assert lease.lost.wait(5)
                assert current_native_lease(provider, "native") is None
        finally:
            provider._finish_local_teardown("native")

    try:
        assert cleanup_unheld_sandbox(provider, "native", callback)
    finally:
        discard_sandbox_lease_manager(provider)
