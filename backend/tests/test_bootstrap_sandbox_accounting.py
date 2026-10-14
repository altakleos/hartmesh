"""Bootstrap parking is accounting, never an admitted SDK client."""

from copy import copy
from dataclasses import replace
from unittest.mock import Mock

import pytest
from test_native_create_provider import fixture as fixture
from test_native_create_provider import install

from deerflow.community.aio_sandbox.aio_sandbox_provider import SandboxInfo
from deerflow.sandbox.admission import ProviderAdmission
from deerflow.sandbox.exceptions import SandboxRuntimeAdmissionError
from deerflow.spaces.docker import StorageHostUnqualified


def closed(provider):
    admission = ProviderAdmission()
    provider._bind_runtime_admission(admission)
    return admission


def primary(sandbox_id="native", cid="a"):
    return SandboxInfo(
        sandbox_id=sandbox_id,
        sandbox_url="",
        container_name="native-" + sandbox_id,
        container_id=cid * 64,
        owner=("alice", "thread"),
        chat_storage_enabled=True,
        native_slots=("b" * 32,),
    )


def test_restoration_requires_exact_bound_gate_and_qualified_owner(fixture):
    provider, owner = fixture
    admission = closed(provider)
    with pytest.raises(StorageHostUnqualified):
        provider._begin_bootstrap_accounting(admission)
    install(provider, owner)
    with pytest.raises(SandboxRuntimeAdmissionError):
        provider._begin_bootstrap_accounting(ProviderAdmission())
    token = provider._begin_bootstrap_accounting(admission)
    assert token is not None
    with pytest.raises(SandboxRuntimeAdmissionError):
        provider.get("native")


def test_parking_original_identity_counts_once_without_client_or_readiness(fixture):
    provider, owner = fixture
    install(provider, owner)
    admission = closed(provider)
    token = provider._begin_bootstrap_accounting(admission)
    info = primary()
    provider._park_bootstrap_accounting(token, info)
    provider._park_bootstrap_accounting(token, info)
    assert provider._warm_pool["native"][0] is info
    assert provider._warm_pool_identity["native"] == info.owner
    assert not provider._sandboxes and not provider._thread_sandboxes
    with provider._lock:
        assert provider._counted_slots_locked() == (3, 0, 1, 0)
    with pytest.raises(SandboxRuntimeAdmissionError):
        provider.get("native")
    provider._backend.destroy.assert_not_called()


def test_changed_incarnation_is_refused_without_replacing_existing_charge(fixture):
    provider, owner = fixture
    install(provider, owner)
    token = provider._begin_bootstrap_accounting(closed(provider))
    original = primary()
    provider._park_bootstrap_accounting(token, original)
    changed = primary(cid="c")
    with pytest.raises(StorageHostUnqualified):
        provider._park_bootstrap_accounting(token, changed)
    assert provider._warm_pool["native"][0] is original
    assert not provider._sandboxes


@pytest.mark.parametrize("change", ["seal", "open", "owner_closing", "end"])
def test_stale_restoration_token_cannot_publish_late_parking(fixture, change):
    provider, owner = fixture
    install(provider, owner)
    admission = closed(provider)
    token = provider._begin_bootstrap_accounting(admission)
    if change == "seal":
        provider._close_runtime_admission(seal=True)
    elif change == "open":
        provider.external_changed(lambda: admission.open(provider))
    elif change == "owner_closing":
        owner.mark_closing()
    else:
        provider._end_bootstrap_accounting(token)
    with pytest.raises((SandboxRuntimeAdmissionError, StorageHostUnqualified)):
        provider._park_bootstrap_accounting(token, primary())
    assert not provider._warm_pool and not provider._sandboxes


def test_unknown_identity_or_local_teardown_never_becomes_bootstrap_charge(fixture):
    provider, owner = fixture
    install(provider, owner)
    token = provider._begin_bootstrap_accounting(closed(provider))
    for info in (primary(cid=""), replace(primary(), container_id=None), replace(primary(), owner=None)):
        with pytest.raises(StorageHostUnqualified):
            provider._park_bootstrap_accounting(token, info)
    provider._local_teardown.add("native")
    with pytest.raises(StorageHostUnqualified):
        provider._park_bootstrap_accounting(token, primary())
    assert not provider._warm_pool


def test_foreign_ownership_is_refused_without_effects(fixture, monkeypatch):
    provider, owner = fixture
    install(provider, owner)
    token = provider._begin_bootstrap_accounting(closed(provider))
    monkeypatch.setattr(provider, "_claim_ownership", Mock(return_value=False))
    with pytest.raises(StorageHostUnqualified):
        provider._park_bootstrap_accounting(token, primary())
    assert not provider._warm_pool
    provider._backend.destroy.assert_not_called()


def test_facts_only_parking_never_constructs_sdk_after_gate_opens(fixture, monkeypatch):
    import importlib

    module = importlib.import_module("deerflow.community.aio_sandbox.aio_sandbox_provider")
    provider, owner = fixture
    install(provider, owner)
    admission = closed(provider)
    token = provider._begin_bootstrap_accounting(admission)
    original = primary()
    provider._park_bootstrap_accounting(token, original)
    provider._end_bootstrap_accounting(token)
    provider.external_changed(lambda: admission.open(provider))
    sdk = Mock()
    monkeypatch.setattr(module, "AioSandbox", sdk)
    monkeypatch.setattr(provider, "_check_tracked_sandbox_alive", lambda *_args: True)
    assert provider._reclaim_warm_pool_sandbox("thread", "native", user_id="alice") is None
    sdk.assert_not_called()
    assert provider._warm_pool["native"][0] is original


@pytest.mark.parametrize("path", ["created", "discovered"])
def test_later_registration_never_replaces_accounted_original_with_different_cid(fixture, path):
    provider, owner = fixture
    install(provider, owner)
    admission = closed(provider)
    token = provider._begin_bootstrap_accounting(admission)
    original = primary()
    provider._park_bootstrap_accounting(token, original)
    provider._end_bootstrap_accounting(token)
    provider.external_changed(lambda: admission.open(provider))
    replacement = primary(cid="c")
    with pytest.raises(StorageHostUnqualified):
        if path == "created":
            provider._register_created_sandbox("thread", "native", replacement, user_id="alice")
        else:
            provider._register_discovered_sandbox("thread", replacement, user_id="alice")
    assert provider._warm_pool["native"][0] is original and not provider._sandboxes


def test_open_then_close_never_revives_prior_restoration_permit(fixture):
    provider, owner = fixture
    install(provider, owner)
    admission = closed(provider)
    token = provider._begin_bootstrap_accounting(admission)
    provider.external_changed(lambda: admission.open(provider))
    provider._close_runtime_admission()
    with pytest.raises(SandboxRuntimeAdmissionError):
        provider._park_bootstrap_accounting(token, primary())
    assert not provider._warm_pool


def test_permit_pins_original_native_bridge_even_when_replacement_is_qualified(fixture):
    provider, owner = fixture
    original = install(provider, owner)
    token = provider._begin_bootstrap_accounting(closed(provider))
    replacement = copy(original)
    provider._native_creation = provider._backend._native_creation = replacement
    assert provider._pool_runtime_current()
    with pytest.raises(StorageHostUnqualified):
        provider._park_bootstrap_accounting(token, primary())
    assert not provider._warm_pool


def test_absent_admission_never_creates_restoration_permit(fixture):
    provider, owner = fixture
    install(provider, owner)
    with pytest.raises(SandboxRuntimeAdmissionError):
        provider._begin_bootstrap_accounting(None)
    assert getattr(provider, "_bootstrap_accounting", None) is None
