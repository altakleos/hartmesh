"""Generic exact-provider bootstrap admission, without business policy."""

import asyncio
import threading
from types import SimpleNamespace
from unittest.mock import Mock

import pytest
from _sandbox_provider_fakes import _make_provider


def admission():
    from deerflow.sandbox.admission import ProviderAdmission

    return ProviderAdmission()


def denied():
    from deerflow.sandbox.exceptions import SandboxRuntimeAdmissionError

    return SandboxRuntimeAdmissionError


def test_gate_starts_closed_and_opens_only_for_exact_bound_provider():
    gate = admission()
    provider, alternate = object(), object()
    with pytest.raises(denied()):
        gate.require(provider)
    gate.bind(provider)
    with pytest.raises(denied()):
        gate.require(provider)
    gate.open(provider)
    gate.require(provider)
    with pytest.raises(denied()):
        gate.require(alternate)
    with pytest.raises(denied()):
        gate.open(alternate)


def test_provider_identity_never_uses_equality_or_hash():
    class EqualProvider:
        __hash__ = None

        def __eq__(self, other):
            return True

    gate = admission()
    provider = EqualProvider()
    gate.bind(provider)
    gate.open(provider)
    gate.require(provider)
    with pytest.raises(denied()):
        gate.require(EqualProvider())


def test_final_seal_cannot_be_reopened_by_late_initializer():
    gate = admission()
    provider = object()
    gate.bind(provider)
    gate.open(provider)
    gate.seal()
    with pytest.raises(denied()):
        gate.open(provider)
    with pytest.raises(denied()):
        gate.require(provider)


def test_temporary_close_retains_provider_binding_and_permits_owned_retry():
    gate = admission()
    provider = object()
    gate.bind(provider)
    gate.open(provider)
    gate.close()
    with pytest.raises(denied()):
        gate.require(provider)
    with pytest.raises(denied()):
        gate.bind(object())
    gate.open(provider)
    gate.require(provider)


@pytest.mark.anyio
async def test_chat_host_optional_capability_remains_generic_and_absent_means_unrestricted():
    from deerflow.spaces.chat_bridge import ChatStorageHost

    provider = object()
    legacy = ChatStorageHost(asyncio.get_running_loop(), lambda *_args: None)
    legacy.require_admission(provider)
    gate = admission()
    host = ChatStorageHost(asyncio.get_running_loop(), lambda *_args: None, admission=gate)
    with pytest.raises(denied()):
        host.require_admission(provider)
    gate.bind(provider)
    gate.open(provider)
    host.require_admission(provider)


def test_blocked_reservation_refuses_before_same_starting_shortcut_or_eviction(tmp_path, monkeypatch):
    provider, backend = _make_provider(tmp_path, monkeypatch)
    gate = admission()
    gate.bind(provider)
    provider._runtime_admission = gate
    provider._starting.add("already-starting")
    with pytest.raises(denied()):
        with provider._lock:
            provider._try_reserve_slot_locked("already-starting")
    with pytest.raises(denied()):
        provider._admit_create("new", allow_eviction=True)
    assert not backend.destroyed and provider._starting == {"already-starting"}


@pytest.mark.anyio
async def test_blocked_external_reservation_shortcut_never_publishes_or_evicts(tmp_path, monkeypatch):
    provider, backend = _make_provider(tmp_path, monkeypatch)
    gate = admission()
    gate.bind(provider)
    provider._runtime_admission = gate
    published = []
    with pytest.raises(denied()):
        await provider.admit_external_async("home", lambda: published.append("slot"), wait=False, is_reserved=lambda: True)
    assert not published and not backend.destroyed


def test_constructor_binding_captures_host_capability_before_runtime_reconciliation(tmp_path, monkeypatch):
    from deerflow.spaces import chat_bridge

    gate = admission()
    host = SimpleNamespace(pool_runtime_owner=None, require_admission=gate.require, admission=gate)
    monkeypatch.setattr(chat_bridge, "get_chat_storage_host", lambda: host)
    provider, _backend = _make_provider(tmp_path, monkeypatch)
    provider._managed_pool_required()
    assert provider._runtime_admission is gate


def installed_gate(provider, *, ready=False):
    gate = admission()
    gate.bind(provider)
    provider._runtime_admission = gate
    if ready:
        gate.open(provider)
    return gate


@pytest.mark.parametrize("operation", ["acquire", "prewarm", "get", "get_scoped"])
def test_closed_gate_refuses_cached_and_speculative_paths_without_effects(tmp_path, monkeypatch, operation):
    provider, backend = _make_provider(tmp_path, monkeypatch)
    sandbox_id = provider.acquire("thread", user_id="user")
    installed_gate(provider)
    before = dict(provider._sandboxes)
    with pytest.raises(denied()):
        if operation in {"acquire", "prewarm"}:
            getattr(provider, operation)("thread", user_id="user")
        elif operation == "get":
            provider.get(sandbox_id)
        else:
            provider.get_scoped(sandbox_id, thread_id="thread", user_id="user")
    assert provider._sandboxes == before
    assert backend.created == [sandbox_id] and not backend.destroyed
    assert getattr(provider, "_prewarm_future", None) is None


@pytest.mark.anyio
@pytest.mark.parametrize("operation", ["acquire_async", "prewarm_async"])
async def test_closed_gate_refuses_async_acquisition_before_submission(tmp_path, monkeypatch, operation):
    provider, backend = _make_provider(tmp_path, monkeypatch)
    installed_gate(provider)
    with pytest.raises(denied()):
        await getattr(provider, operation)("thread", user_id="user")
    assert not backend.created and not backend.destroyed
    assert not provider._starting
    assert getattr(provider, "_prewarm_future", None) is None


@pytest.mark.parametrize("source", ["cached", "warm", "discovered", "created"])
def test_gate_closing_during_ownership_io_refuses_final_publication(tmp_path, monkeypatch, source):
    provider, backend = _make_provider(tmp_path, monkeypatch)
    if source == "created":
        sandbox_id = provider._sandbox_id_for_thread("thread", "user")
        provider._starting.add(sandbox_id)
        info = backend.create("thread", sandbox_id)
    else:
        sandbox_id = provider.acquire("thread", user_id="user")
    if source == "warm":
        provider.release(sandbox_id)
    gate = installed_gate(provider, ready=True)
    original = provider._publish_ownership

    def publish(identity):
        original(identity)
        gate.close()

    monkeypatch.setattr(provider, "_publish_ownership", publish)
    with pytest.raises(denied()):
        if source == "cached":
            provider._reuse_in_process_sandbox("thread", user_id="user")
        elif source == "warm":
            provider._reclaim_warm_pool_sandbox("thread", sandbox_id, user_id="user")
        elif source == "created":
            provider._register_created_sandbox("thread", sandbox_id, info, user_id="user")
        else:
            provider.release(sandbox_id)
            info = provider._warm_pool[sandbox_id][0]
            provider._register_discovered_sandbox("thread", info, user_id="user")
    if source in {"warm", "discovered"}:
        assert sandbox_id in provider._warm_pool and sandbox_id not in provider._sandboxes
        assert not backend.destroyed
    if source == "created":
        assert sandbox_id not in provider._sandboxes
        assert backend.destroyed == [sandbox_id]
    assert provider._starting == ({sandbox_id} if source == "created" else set())


@pytest.mark.anyio
async def test_external_retry_observes_closure_before_reserved_shortcut_or_more_eviction(tmp_path, monkeypatch):
    provider, backend = _make_provider(tmp_path, monkeypatch, replicas=1)
    provider._starting.add("busy")
    gate = installed_gate(provider, ready=True)
    reserved = []
    external = Mock(return_value=False)
    provider.evict_external = external

    def evict(*, exclude):
        gate.close()
        return None

    monkeypatch.setattr(provider, "_evict_oldest_warm", evict)
    with pytest.raises(denied()):
        await provider.admit_external_async("home", lambda: reserved.append("home"), wait=False, is_reserved=lambda: False)
    assert not reserved and not backend.destroyed
    external.assert_not_called()
    assert provider._starting == {"busy"}


def test_binding_preexisting_provider_preserves_accounting_and_rejects_replacement(tmp_path, monkeypatch):
    provider, backend = _make_provider(tmp_path, monkeypatch)
    sandbox_id = provider.acquire("thread", user_id="user")
    gate = admission()
    provider._bind_runtime_admission(gate)
    provider._bind_runtime_admission(gate)
    with pytest.raises(denied()):
        provider.acquire("thread", user_id="user")
    assert sandbox_id in provider._sandboxes and backend.created == [sandbox_id]
    with pytest.raises(denied()):
        provider._bind_runtime_admission(admission())
    gate.open(provider)
    assert provider.acquire("thread", user_id="user") == sandbox_id


def test_shared_capability_cannot_admit_alternate_constructor(tmp_path, monkeypatch):
    winner, _ = _make_provider(tmp_path, monkeypatch)
    loser, backend = _make_provider(tmp_path, monkeypatch)
    gate = admission()
    winner._bind_runtime_admission(gate)
    loser._runtime_admission = gate
    gate.open(winner)
    with pytest.raises(denied()):
        loser.acquire("thread", user_id="user")
    assert not backend.created


@pytest.mark.parametrize("seal", [False, True])
def test_real_lease_release_closes_command_scope_after_admission_closure(tmp_path, monkeypatch, seal):
    from deerflow.sandbox.lease import SandboxLeaseManager

    provider, backend = _make_provider(tmp_path, monkeypatch)
    manager = SandboxLeaseManager(provider)
    sandbox_id = manager.acquire("run", "thread", user_id="user")
    sandbox = provider.get(sandbox_id)
    sandbox.release_command_scope = Mock()
    installed_gate(provider, ready=True)
    provider._close_runtime_admission(seal=seal)
    try:
        manager.release("run")
        sandbox.release_command_scope.assert_called_once_with("run")
        assert manager.binding_for("run") is None
        assert sandbox_id in provider._warm_pool and not backend.destroyed
        with pytest.raises(denied()):
            provider.get(sandbox_id)
    finally:
        manager.close()


def test_closed_constructor_reconciliation_defers_adoption_and_keeps_original_identity(tmp_path, monkeypatch):
    provider, backend = _make_provider(tmp_path, monkeypatch)
    info = backend.create("thread", "orphan")
    info.owner = ("user", "thread")
    monkeypatch.setattr(backend, "list_running", lambda: [info])
    gate = installed_gate(provider)
    provider._reconcile_orphans()
    assert "orphan" not in provider._warm_pool and not backend.destroyed
    gate.open(provider)
    provider._reconcile_orphans()
    assert provider._warm_pool["orphan"][0] is info
    assert provider._warm_pool_identity["orphan"] == ("user", "thread")


@pytest.mark.anyio
async def test_prewarm_submitted_before_closure_drains_and_does_not_publish(tmp_path, monkeypatch):
    from _sandbox_provider_fakes import _aio_mod

    provider, backend = _make_provider(tmp_path, monkeypatch)
    entered, resume = threading.Event(), threading.Event()
    installed_gate(provider, ready=True)

    def ready(*_args, **_kwargs):
        entered.set()
        assert resume.wait(5)
        return True

    monkeypatch.setattr(_aio_mod(), "wait_for_sandbox_ready", ready)
    task = asyncio.create_task(provider.prewarm_async("thread", user_id="user"))
    try:
        assert await asyncio.to_thread(entered.wait, 5)
        provider._close_runtime_admission()
        resume.set()
        with pytest.raises(denied()):
            await asyncio.wait_for(task, 5)
        assert not provider._sandboxes and not provider._warm_pool and not provider._starting
        assert backend.destroyed == backend.created and len(backend.created) == 1
        assert provider._prewarm_future.done()
    finally:
        resume.set()
        await asyncio.gather(task, return_exceptions=True)
        await asyncio.to_thread(provider._stop_prewarm_worker)


@pytest.mark.anyio
async def test_closing_admission_wakes_capacity_waiters_without_forgiving_accounting(tmp_path, monkeypatch):
    provider, backend = _make_provider(tmp_path, monkeypatch, replicas=1)
    provider._config["capacity_wait_timeout"] = 30
    provider._starting.add("busy")
    installed_gate(provider, ready=True)
    sleeping = asyncio.Event()
    original = provider._await_capacity_slot

    def waiter():
        future = original()
        sleeping.set()
        return future

    monkeypatch.setattr(provider, "_await_capacity_slot", waiter)
    task = asyncio.create_task(provider.admit_external_async("home", Mock(), wait=True))
    try:
        await asyncio.wait_for(sleeping.wait(), 5)
        provider._close_runtime_admission()
        with pytest.raises(denied()):
            await asyncio.wait_for(task, 5)
        assert provider._starting == {"busy"} and not backend.destroyed
        assert not provider._capacity_async_waiters
    finally:
        if not task.done():
            task.cancel()
        await asyncio.gather(task, return_exceptions=True)


@pytest.mark.anyio
@pytest.mark.parametrize("asynchronous", [False, True])
async def test_prewarm_closed_after_parking_retains_accounting_but_refuses_return(tmp_path, monkeypatch, asynchronous):
    provider, backend = _make_provider(tmp_path, monkeypatch)
    installed_gate(provider, ready=True)
    original = provider.release

    def park(sandbox_id):
        original(sandbox_id)
        provider._close_runtime_admission()

    monkeypatch.setattr(provider, "release", park)
    try:
        with pytest.raises(denied()):
            if asynchronous:
                await provider.prewarm_async("thread", user_id="user")
            else:
                await asyncio.to_thread(provider.prewarm, "thread", user_id="user")
        assert len(provider._warm_pool) == 1 and not provider._sandboxes and not provider._starting
        assert len(backend.created) == 1 and not backend.destroyed
    finally:
        await asyncio.to_thread(provider._stop_prewarm_worker)


def test_real_constructor_captures_gate_before_reconciliation_and_background_threads(tmp_path, monkeypatch):
    from _sandbox_provider_fakes import _aio_mod

    from deerflow.spaces import chat_bridge

    _fixture, backend = _make_provider(tmp_path, monkeypatch)
    cls = _aio_mod().AioSandboxProvider
    gate = admission()
    host = SimpleNamespace(pool_runtime_owner=None, admission=gate)
    monkeypatch.setattr(chat_bridge, "get_chat_storage_host", lambda: host)
    monkeypatch.setattr(cls, "_load_config", lambda _self: {"idle_timeout": 0, "replicas": 2})
    monkeypatch.setattr(cls, "_create_backend", lambda _self: backend)
    monkeypatch.setattr(_aio_mod().atexit, "register", lambda *_args: None)
    monkeypatch.setattr(cls, "_register_signal_handlers", lambda _self: None)
    observed = []

    def observe(self):
        assert self._runtime_admission is gate
        with pytest.raises(denied()):
            self._require_runtime_admission()
        observed.append(self)

    for name in ("_reconcile_orphans", "_start_lease_renewal", "_start_prewarm_reaper"):
        monkeypatch.setattr(cls, name, observe)
    provider = cls()
    assert observed == [provider, provider, provider]


@pytest.mark.anyio
@pytest.mark.parametrize("asynchronous", [False, True])
async def test_generic_retention_refuses_closed_admission_without_publishing_holder(tmp_path, monkeypatch, asynchronous):
    from deerflow.sandbox.lease import SandboxLeaseManager

    provider, _backend = _make_provider(tmp_path, monkeypatch)
    sandbox_id = provider.acquire("thread", user_id="user")
    manager = SandboxLeaseManager(provider)
    installed_gate(provider)
    try:
        with pytest.raises(denied()):
            if asynchronous:
                await manager.retain_async("run", sandbox_id, thread_id="thread", user_id="user")
            else:
                manager.retain("run", sandbox_id, thread_id="thread", user_id="user")
        assert manager.binding_for("run") is None
        assert not manager._owners_by_sandbox
    finally:
        manager.close()


@pytest.mark.anyio
@pytest.mark.parametrize("asynchronous", [False, True])
async def test_closure_between_acquisition_and_lease_binding_refuses_new_holder(tmp_path, monkeypatch, asynchronous):
    from deerflow.sandbox.lease import SandboxLeaseManager

    provider, _backend = _make_provider(tmp_path, monkeypatch)
    installed_gate(provider, ready=True)
    manager = SandboxLeaseManager(provider)
    original = provider.acquire

    def acquire(*args, **kwargs):
        result = original(*args, **kwargs)
        provider._close_runtime_admission()
        return result

    monkeypatch.setattr(provider, "acquire", acquire)

    async def acquire_async(*args, **kwargs):
        return acquire(*args, **kwargs)

    monkeypatch.setattr(provider, "acquire_async", acquire_async)
    try:
        with pytest.raises(denied()):
            if asynchronous:
                await manager.acquire_async("run", "thread", user_id="user")
            else:
                manager.acquire("run", "thread", user_id="user")
        assert manager.binding_for("run") is None and not manager._owners_by_sandbox
        assert not provider._sandboxes and len(provider._warm_pool) == 1
    finally:
        manager.close()


@pytest.mark.anyio
@pytest.mark.parametrize("asynchronous", [False, True])
async def test_existing_holder_return_rechecks_closure_after_cached_lookup(tmp_path, monkeypatch, asynchronous):
    from deerflow.sandbox.lease import SandboxLeaseManager

    provider, _backend = _make_provider(tmp_path, monkeypatch)
    installed_gate(provider, ready=True)
    manager = SandboxLeaseManager(provider)
    sandbox_id = manager.acquire("run", "thread", user_id="user")
    original = provider.get

    def lookup(identity):
        result = original(identity)
        provider._close_runtime_admission()
        return result

    monkeypatch.setattr(provider, "get", lookup)
    try:
        with pytest.raises(denied()):
            if asynchronous:
                await manager.acquire_async("run", "thread", user_id="user")
            else:
                manager.acquire("run", "thread", user_id="user")
        assert manager.binding_for("run") == sandbox_id
        assert sandbox_id in provider._sandboxes
    finally:
        manager.release("run")
        manager.close()
