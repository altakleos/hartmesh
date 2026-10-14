"""Host-owned environments share the existing budget; Docker is always doubled."""

import asyncio
import threading
from unittest.mock import Mock

import pytest
from _sandbox_provider_fakes import _make_provider

from deerflow.sandbox.exceptions import SandboxCapacityExceededError


@pytest.fixture
def native_provider(tmp_path):
    from test_native_create_provider import fixture

    yield from fixture.__wrapped__(tmp_path)


def test_external_claims_refuse_chat_creation_without_losing_native_reservations(tmp_path, monkeypatch):
    provider, backend = _make_provider(tmp_path, monkeypatch, replicas=2)
    provider._config["capacity_wait_timeout"] = 0
    provider._starting.add("native-pending")
    provider.external_slots = lambda: 1
    try:
        with pytest.raises(SandboxCapacityExceededError) as refused:
            provider._admit_create("new-chat", allow_eviction=True)
        assert refused.value.reserved == 2
        assert provider._starting == {"native-pending"}
        assert not backend.alive
    finally:
        provider.reset()


def test_prewarm_never_evicts_external_environments(tmp_path, monkeypatch):
    from deerflow.community.aio_sandbox.aio_sandbox_provider import SandboxSlotsBusyError

    provider, _backend = _make_provider(tmp_path, monkeypatch, replicas=1)
    provider.external_slots = lambda: 1
    provider.evict_external = Mock(return_value=True)
    try:
        with pytest.raises(SandboxSlotsBusyError):
            provider._admit_create("prewarm", allow_eviction=False)
        provider.evict_external.assert_not_called()
    finally:
        provider.reset()


@pytest.mark.anyio
async def test_same_home_concurrent_admissions_remain_idempotent_at_full_capacity(tmp_path, monkeypatch):
    provider, _backend = _make_provider(tmp_path, monkeypatch, replicas=1)
    slots, calls = set(), []
    provider.external_slots = lambda: len(slots)

    def reserve():
        assert provider._lock.locked()
        calls.append("home")
        slots.add("home")

    def reserved():
        assert provider._lock.locked()
        return "home" in slots

    try:
        await asyncio.gather(*(provider.admit_external_async("home", reserve, wait=False, is_reserved=reserved) for _ in range(2)))
        assert slots == {"home"} and calls == ["home"]
        assert provider._counted_slots_locked() == (1, 0, 0, 1)
    finally:
        provider.reset()


@pytest.mark.anyio
async def test_different_homes_contend_for_the_same_last_slot(tmp_path, monkeypatch):
    provider, _backend = _make_provider(tmp_path, monkeypatch, replicas=1)
    slots = set()
    provider.external_slots = lambda: len(slots)
    try:
        outcomes = await asyncio.gather(*(provider.admit_external_async(home, lambda home=home: slots.add(home), wait=False) for home in ("first", "second")), return_exceptions=True)
        assert outcomes[0] is None and isinstance(outcomes[1], SandboxCapacityExceededError)
        assert slots == {"first"}
    finally:
        provider.reset()


@pytest.mark.anyio
async def test_predicate_is_rechecked_after_eviction_await_without_waiting(tmp_path, monkeypatch):
    provider, _backend = _make_provider(tmp_path, monkeypatch, replicas=1)
    provider._starting.add("busy")
    slots = set()
    provider.external_slots = lambda: len(slots)
    reserve = Mock()
    provider.evict_external = Mock(return_value=False)

    def another_admission(**_kwargs):
        def changed():
            provider._starting.remove("busy")
            slots.add("home")

        provider.external_changed(changed)
        return None

    monkeypatch.setattr(provider, "_evict_oldest_warm", another_admission)
    try:
        await provider.admit_external_async("home", reserve, wait=False, is_reserved=lambda: "home" in slots)
        reserve.assert_not_called()
        assert slots == {"home"}
        provider.evict_external.assert_not_called()
    finally:
        provider.reset()


@pytest.mark.anyio
async def test_cancellation_during_eviction_never_reserves_after_late_completion(tmp_path, monkeypatch):
    provider, _backend = _make_provider(tmp_path, monkeypatch, replicas=1)
    provider._starting.add("busy")
    entered, release, drained = threading.Event(), threading.Event(), threading.Event()
    reserve = Mock()

    def evict(**_kwargs):
        entered.set()
        assert release.wait(5)
        try:
            with provider._lock:
                provider._starting.remove("busy")
            return "busy"
        finally:
            drained.set()

    monkeypatch.setattr(provider, "_evict_oldest_warm", evict)
    task = asyncio.create_task(provider.admit_external_async("home", reserve, wait=True))
    try:
        assert await asyncio.to_thread(entered.wait, 2)
        task.cancel()
        await asyncio.sleep(0)
        await asyncio.sleep(0)
        assert not task.done(), "the preparation fence must cover the eviction worker"
        task.cancel()
        release.set()
        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(task, 2)
        assert not provider._capacity_async_waiters
        reserve.assert_not_called()
    finally:
        release.set()
        assert await asyncio.to_thread(drained.wait, 2)
        provider.reset()


def test_external_claims_are_added_once_to_pending_native_and_teardown_claims(native_provider):
    from test_native_create_provider import install, record
    from test_native_teardown_evidence import record as teardown_record

    provider, owner = native_provider
    bridge = install(provider, owner)
    bridge.handoffs.record_pending(record(), sandbox_id="native", reservation=object())
    bridge.evidence.begin_teardown(teardown_record())
    bridge.qualify()
    provider.external_slots = lambda: 1
    with provider._lock:
        assert provider._counted_slots_locked() == (3, 0, 0, 3)


@pytest.mark.parametrize("asynchronous", [False, True])
@pytest.mark.anyio
async def test_chat_admission_tries_warm_before_external_eviction(tmp_path, monkeypatch, asynchronous):
    provider, _backend = _make_provider(tmp_path, monkeypatch, replicas=1)
    slots, calls = {"home"}, []
    provider.external_slots = lambda: len(slots)
    monkeypatch.setattr(provider, "_evict_oldest_warm", lambda **_kwargs: calls.append("warm"))

    def evict():
        assert not provider._lock.locked()
        calls.append("external")
        provider.external_changed(slots.clear)
        return True

    provider.evict_external = evict
    try:
        if asynchronous:
            await provider._admit_create_async("chat")
        else:
            await asyncio.to_thread(provider._admit_create, "chat", allow_eviction=True)
        assert calls == ["warm", "external"] and provider._starting == {"chat"}
    finally:
        provider.reset()


@pytest.mark.anyio
async def test_external_change_wakes_waiting_admission(tmp_path, monkeypatch):
    provider, _backend = _make_provider(tmp_path, monkeypatch, replicas=1)
    provider._config["capacity_wait_timeout"] = 5
    slots = {"busy"}
    provider.external_slots = lambda: len(slots)
    checked = asyncio.Event()
    monkeypatch.setattr(provider, "_open_capacity_wait_span", lambda *_args: checked.set() or True)
    task = asyncio.create_task(provider.admit_external_async("home", lambda: slots.add("home"), wait=True))
    try:
        await asyncio.wait_for(checked.wait(), 2)
        provider.external_changed(lambda: slots.remove("busy"))
        await asyncio.wait_for(task, 2)
        assert slots == {"home"} and not provider._capacity_async_waiters
    finally:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        provider.reset()


@pytest.mark.parametrize("already_reserved", [False, True])
@pytest.mark.anyio
async def test_shutdown_refuses_even_an_existing_external_claim(tmp_path, monkeypatch, already_reserved):
    provider, _backend = _make_provider(tmp_path, monkeypatch, replicas=1)
    provider._shutdown_called = True
    reserve = Mock()
    try:
        with pytest.raises(RuntimeError, match="shut down"):
            await provider.admit_external_async("home", reserve, wait=False, is_reserved=lambda: already_reserved)
        reserve.assert_not_called()
    finally:
        provider.reset()


@pytest.mark.anyio
async def test_shutdown_during_warm_eviction_prevents_external_eviction(tmp_path, monkeypatch):
    provider, _backend = _make_provider(tmp_path, monkeypatch, replicas=1)
    provider._starting.add("busy")
    provider.evict_external = Mock(return_value=False)
    reserve = Mock()

    def evict(**_kwargs):
        provider.external_changed(lambda: setattr(provider, "_shutdown_called", True))

    monkeypatch.setattr(provider, "_evict_oldest_warm", evict)
    try:
        with pytest.raises(RuntimeError, match="shut down"):
            await provider.admit_external_async("home", reserve, wait=False)
        reserve.assert_not_called()
        provider.evict_external.assert_not_called()
    finally:
        provider.reset()


def test_sync_shutdown_during_external_eviction_prevents_late_reservation(tmp_path, monkeypatch):
    provider, _backend = _make_provider(tmp_path, monkeypatch, replicas=1)
    slots = {"home"}
    provider.external_slots = lambda: len(slots)

    def evict():
        def changed():
            slots.clear()
            provider._shutdown_called = True

        provider.external_changed(changed)
        return True

    provider.evict_external = evict
    try:
        with pytest.raises(RuntimeError, match="shut down"):
            provider._admit_create("chat", allow_eviction=True)
        assert not provider._starting
    finally:
        provider.reset()
