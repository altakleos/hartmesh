"""Prewarm: a conversation's sandbox built and parked before its first turn asks.

On a cold first turn the wait before the model is mostly the sandbox: the
container's create plus its readiness wait. None of it depends on what the
person is about to type -- the container a turn acquires is named by
``(user, thread)`` -- so it can be built when the conversation is opened and
parked; the first turn's ordinary warm reclaim then finds it.

Nothing about acquisition changes, and that is the point: these tests prove
the prewarmed container is the one the turn would have built, that it is
claimed through the existing path, and that a speculative build can never make
a real turn slower -- it never evicts, never stands in for an active sandbox,
and does not outlive its welcome.

Every backend here is a fake; the tests establish identity and lifecycle, not
timings.
"""

from __future__ import annotations

import asyncio
import threading
import time
from concurrent.futures import ThreadPoolExecutor

import pytest
from _sandbox_provider_fakes import _aio_mod, _make_provider

from deerflow.runtime.turn_phases import AcquisitionSource, turn_phases

USER = "user-7"


def _prewarm(provider, thread_id: str) -> str | None:
    return provider.prewarm(thread_id, user_id=USER)


def _acquire(provider, thread_id: str) -> str:
    return provider.acquire(thread_id, user_id=USER)


def _frozen_clock(monkeypatch) -> dict[str, float]:
    clock = {"now": time.time()}
    monkeypatch.setattr(_aio_mod().time, "time", lambda: clock["now"])
    return clock


# ── The container the turn would have built ───────────────────────────────


def test_the_first_turn_reclaims_the_prewarmed_container_instead_of_creating(tmp_path, monkeypatch):
    provider, backend = _make_provider(tmp_path, monkeypatch)

    parked = _prewarm(provider, "thread-open")

    assert parked is not None
    assert backend.created == [parked], "prewarm builds exactly one container"
    assert parked in provider._warm_pool, "prewarm parks; it never holds the container active"
    assert provider._thread_sandboxes == {}

    with turn_phases(correlation_id="first-turn") as journal:
        first = _acquire(provider, "thread-open")

    assert first == parked
    assert backend.created == [parked], "the first turn must not create"
    assert backend.destroyed == [], "the first turn must not tear anything down"
    snapshot = journal.snapshot()
    assert snapshot.acquisition_source is AcquisitionSource.WARM_RECLAIM
    assert snapshot.resource_creates == 0
    assert provider._thread_sandboxes[(USER, "thread-open")] == parked


def test_prewarm_builds_for_the_identity_it_was_given(tmp_path, monkeypatch):
    """Another person's turn on the same thread id is another container."""
    provider, backend = _make_provider(tmp_path, monkeypatch)

    parked = _prewarm(provider, "thread-shared-id")
    other = provider.acquire("thread-shared-id", user_id="user-8")

    assert other != parked
    assert backend.created == [parked, other]


def test_a_second_prewarm_builds_nothing(tmp_path, monkeypatch):
    provider, backend = _make_provider(tmp_path, monkeypatch)

    first = _prewarm(provider, "thread-twice")

    assert _prewarm(provider, "thread-twice") is None
    assert backend.created == [first]


def test_prewarm_leaves_a_container_another_worker_started_to_the_turn(tmp_path, monkeypatch):
    provider, backend = _make_provider(tmp_path, monkeypatch, discoverable=True)
    sandbox_id = provider._sandbox_id_for_thread("thread-elsewhere", USER)
    backend.alive[sandbox_id] = True
    backend.infos[sandbox_id] = backend._unused_info(sandbox_id)

    assert _prewarm(provider, "thread-elsewhere") is None
    assert backend.created == []
    assert provider._warm_pool == {}


# ── A speculative build never makes a real turn slower ────────────────────


def test_prewarm_never_evicts_to_make_room(tmp_path, monkeypatch):
    provider, backend = _make_provider(tmp_path, monkeypatch, replicas=2)

    one = _acquire(provider, "thread-1")
    provider.release(one)
    two = _acquire(provider, "thread-2")
    provider.release(two)

    assert _prewarm(provider, "thread-3") is None
    assert backend.created == [one, two]
    assert backend.destroyed == [], "a prewarm takes a free slot or nothing"
    assert set(provider._warm_pool) == {one, two}


def test_prewarm_does_not_wait_for_a_slot(tmp_path, monkeypatch):
    provider, backend = _make_provider(tmp_path, monkeypatch, replicas=1)
    provider._config["capacity_wait_timeout"] = 30
    active = _acquire(provider, "thread-busy-elsewhere")

    started = time.monotonic()
    assert _prewarm(provider, "thread-waiting") is None
    assert time.monotonic() - started < 5, "a prewarm waited for capacity"
    assert backend.created == [active]


def test_prewarm_is_a_noop_while_the_thread_holds_a_sandbox(tmp_path, monkeypatch):
    provider, backend = _make_provider(tmp_path, monkeypatch)

    active = _acquire(provider, "thread-busy")

    assert _prewarm(provider, "thread-busy") is None
    assert backend.created == [active]
    assert provider._thread_sandboxes[(USER, "thread-busy")] == active


def test_prewarm_failure_leaves_nothing_behind(tmp_path, monkeypatch):
    provider, backend = _make_provider(tmp_path, monkeypatch)

    def _refuse(*_a, **_k):
        raise RuntimeError("daemon refused")

    backend.create = _refuse

    with pytest.raises(RuntimeError):
        _prewarm(provider, "thread-fail")

    assert provider._warm_pool == {}
    assert provider._thread_sandboxes == {}
    assert provider._starting == set(), "a failed prewarm must give its slot back"


def test_concurrent_prewarms_count_containers_still_starting(tmp_path, monkeypatch):
    """Two colleagues opening a new chat in the same second must not both build past the budget."""
    provider, backend = _make_provider(tmp_path, monkeypatch, replicas=2)

    provider._starting.update({"someone-elses-start", "and-another"})
    assert _prewarm(provider, "thread-third") is None
    assert backend.created == []

    provider._starting.clear()
    provider._starting.add("someone-elses-start")
    assert _prewarm(provider, "thread-second") is not None
    assert len(backend.created) == 1


# ── It does not outlive its welcome ───────────────────────────────────────


def test_an_unclaimed_prewarm_is_reaped_after_the_claim_timeout(tmp_path, monkeypatch):
    provider, backend = _make_provider(tmp_path, monkeypatch)
    provider._config["prewarm_claim_timeout"] = 10
    clock = _frozen_clock(monkeypatch)

    abandoned = _prewarm(provider, "thread-abandoned")
    clock["now"] += 11

    provider._reap_unclaimed_prewarms()

    assert backend.destroyed == [abandoned]
    assert abandoned not in provider._warm_pool


def test_a_prewarm_within_the_claim_timeout_is_kept(tmp_path, monkeypatch):
    provider, backend = _make_provider(tmp_path, monkeypatch)
    provider._config["prewarm_claim_timeout"] = 10
    clock = _frozen_clock(monkeypatch)

    fresh = _prewarm(provider, "thread-fresh")
    clock["now"] += 9

    provider._reap_unclaimed_prewarms()

    assert backend.destroyed == []
    assert fresh in provider._warm_pool


def test_a_claimed_prewarm_is_governed_by_the_idle_timeout_not_the_claim_timeout(tmp_path, monkeypatch):
    """Once a turn used it, the container is an ordinary parked sandbox again."""
    provider, backend = _make_provider(tmp_path, monkeypatch)
    provider._config["prewarm_claim_timeout"] = 10
    clock = _frozen_clock(monkeypatch)

    used = _prewarm(provider, "thread-used")
    assert _acquire(provider, "thread-used") == used
    provider.release(used)
    clock["now"] += 60

    provider._reap_unclaimed_prewarms()

    assert backend.destroyed == [], "a claimed container is not a prewarm any more"
    assert used in provider._warm_pool


def test_a_container_rebuilt_under_an_evicted_prewarms_id_is_not_mistaken_for_it(tmp_path, monkeypatch):
    """Ids are deterministic and reused; the mark must follow the container."""
    provider, backend = _make_provider(tmp_path, monkeypatch, replicas=1)
    provider._config["prewarm_claim_timeout"] = 10
    clock = _frozen_clock(monkeypatch)

    parked = _prewarm(provider, "thread-evicted")
    other = _acquire(provider, "thread-other")  # at capacity: evicts the prewarm
    assert backend.destroyed == [parked]
    provider.release(other)
    clock["now"] += 5
    rebuilt = _acquire(provider, "thread-evicted")  # evicts `other`, rebuilds under the old id
    assert rebuilt == parked
    provider.release(rebuilt)
    clock["now"] += 55

    provider._reap_unclaimed_prewarms()

    assert backend.destroyed == [parked, other], "the rebuilt container survives the stale mark"
    assert rebuilt in provider._warm_pool


def test_the_reaper_runs_even_when_idle_cleanup_is_disabled(tmp_path, monkeypatch):
    """``idle_timeout: 0`` is a supported config that never starts the idle checker.

    The claim timeout must not ride on that switch -- the same reason lease
    renewal has its own thread. This exercises the real wiring: the reaper
    thread, not the private method.
    """
    provider, backend = _make_provider(tmp_path, monkeypatch)
    provider._config.update({"idle_timeout": 0, "prewarm_claim_timeout": 1})
    provider._prewarm_reaper_stop = threading.Event()
    provider._prewarm_reaper_thread = None
    monkeypatch.setattr(_aio_mod().AioSandboxProvider, "PREWARM_CHECK_INTERVAL", 0.02)
    clock = _frozen_clock(monkeypatch)

    abandoned = _prewarm(provider, "thread-idle-off")
    clock["now"] += 2
    provider._start_prewarm_reaper()
    try:
        deadline = time.monotonic() + 5
        while backend.destroyed != [abandoned] and time.monotonic() < deadline:
            time.sleep(0.01)
    finally:
        provider._stop_prewarm_reaper()

    assert backend.destroyed == [abandoned]
    assert abandoned not in provider._warm_pool


def test_a_zero_claim_timeout_starts_no_reaper(tmp_path, monkeypatch):
    provider, _backend = _make_provider(tmp_path, monkeypatch)
    provider._config["prewarm_claim_timeout"] = 0
    provider._prewarm_reaper_stop = threading.Event()
    provider._prewarm_reaper_thread = None

    provider._start_prewarm_reaper()

    assert provider._prewarm_reaper_thread is None


def test_the_claim_timeout_defaults_and_can_be_turned_off(tmp_path, monkeypatch):
    provider, _backend = _make_provider(tmp_path, monkeypatch)

    assert provider._prewarm_claim_timeout() == _aio_mod().DEFAULT_PREWARM_CLAIM_TIMEOUT
    provider._config["prewarm_claim_timeout"] = 0
    assert provider._prewarm_claim_timeout() == 0, "zero disables the reaper, as idle_timeout: 0 does"


# ── Its async face ────────────────────────────────────────────────────────


@pytest.mark.anyio
async def test_prewarm_async_parks_without_blocking_the_loop(tmp_path, monkeypatch):
    provider, backend = _make_provider(tmp_path, monkeypatch)

    parked = await provider.prewarm_async("thread-async", user_id=USER)

    assert parked is not None
    assert backend.created == [parked]
    assert parked in provider._warm_pool


@pytest.mark.asyncio
async def test_busy_prewarms_do_not_queue_or_starve_real_work(tmp_path, monkeypatch):
    provider, backend = _make_provider(tmp_path, monkeypatch)
    existing = _acquire(provider, "existing")
    loop = asyncio.get_running_loop()
    entered = asyncio.Event()
    ready = threading.Event()
    loop.set_default_executor(ThreadPoolExecutor(max_workers=2))

    def readiness(*_args, **_kwargs):
        loop.call_soon_threadsafe(entered.set)
        assert ready.wait(5)
        return True

    monkeypatch.setattr(_aio_mod(), "wait_for_sandbox_ready", readiness)
    first = asyncio.create_task(provider.prewarm_async("cold", user_id=USER))
    duplicates = []
    try:
        await asyncio.wait_for(entered.wait(), 2)
        duplicates = [asyncio.create_task(provider.prewarm_async(name, user_id=USER)) for name in ["cold"] * 8 + ["other"] * 8]
        assert await asyncio.wait_for(asyncio.gather(*duplicates), 1) == [None] * 16
        assert await asyncio.wait_for(asyncio.to_thread(lambda: "config read"), 1) == "config read"
        assert await asyncio.wait_for(provider.acquire_async("existing", user_id=USER), 1) == existing
        assert len(backend.created) == 2
    finally:
        ready.set()
        await asyncio.gather(first, *duplicates, return_exceptions=True)
        provider.shutdown()


@pytest.mark.asyncio
async def test_prewarm_cancellation_drains_creation_before_returning(tmp_path, monkeypatch):
    provider, backend = _make_provider(tmp_path, monkeypatch)
    loop = asyncio.get_running_loop()
    entered = asyncio.Event()

    def readiness(*_args, cancelled=None, **_kwargs):
        loop.call_soon_threadsafe(entered.set)
        if cancelled is None:
            return True
        assert cancelled.wait(3)
        return False

    monkeypatch.setattr(_aio_mod(), "wait_for_sandbox_ready", readiness)
    task = asyncio.create_task(provider.prewarm_async("cancelled", user_id=USER))
    try:
        await asyncio.wait_for(entered.wait(), 2)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(task, 2)
        assert backend.destroyed == backend.created
        assert provider._starting == set()
        assert provider._warm_pool == {}
        assert provider._acquire_serializer._table == {}
    finally:
        provider.shutdown()


@pytest.mark.asyncio
async def test_shutdown_drains_prewarm_without_needing_event_loop_callbacks(tmp_path, monkeypatch):
    provider, backend = _make_provider(tmp_path, monkeypatch)
    loop = asyncio.get_running_loop()
    entered = asyncio.Event()

    def readiness(*_args, cancelled=None, **_kwargs):
        loop.call_soon_threadsafe(entered.set)
        if cancelled is None:
            return True
        assert cancelled.wait(3)
        return False

    monkeypatch.setattr(_aio_mod(), "wait_for_sandbox_ready", readiness)
    task = asyncio.create_task(provider.prewarm_async("shutdown", user_id=USER))
    await asyncio.wait_for(entered.wait(), 2)
    provider.shutdown()
    await asyncio.gather(task, return_exceptions=True)
    assert backend.destroyed == backend.created
    assert provider._starting == set()
    assert provider._warm_pool == {}
    assert await provider.prewarm_async("after-shutdown", user_id=USER) is None


def test_prewarm_skips_cross_process_lock_contention(tmp_path, monkeypatch):
    provider, backend = _make_provider(tmp_path, monkeypatch)
    monkeypatch.setattr(_aio_mod(), "_try_lock_file_exclusive", lambda _file: False)
    try:
        assert _prewarm(provider, "busy") is None
        assert backend.created == []
    finally:
        provider.shutdown()


@pytest.mark.asyncio
@pytest.mark.parametrize("rediscovered", [False, True])
async def test_cancel_after_readiness_rolls_back_only_newly_created_prewarm(tmp_path, monkeypatch, rediscovered):
    provider, backend = _make_provider(tmp_path, monkeypatch, adopt_on_conflict=rediscovered)
    sandbox_id = provider._sandbox_id_for_thread("late-cancel", USER)
    if rediscovered:
        backend.alive[sandbox_id] = True
        backend.infos[sandbox_id] = backend._unused_info(sandbox_id)
    loop = asyncio.get_running_loop()
    entered = asyncio.Event()
    resume = threading.Event()
    register = provider._register_created_sandbox

    def paused_register(*args, **kwargs):
        loop.call_soon_threadsafe(entered.set)
        assert resume.wait(3)
        return register(*args, **kwargs)

    monkeypatch.setattr(provider, "_register_created_sandbox", paused_register)
    task = asyncio.create_task(provider.prewarm_async("late-cancel", user_id=USER))
    try:
        await asyncio.wait_for(entered.wait(), 2)
        task.cancel()
        await asyncio.sleep(0)
        resume.set()
        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(task, 2)
        assert provider._starting == set()
        if rediscovered:
            assert backend.destroyed == []
            assert sandbox_id in provider._warm_pool
            assert sandbox_id not in provider._prewarmed_unclaimed
        else:
            assert backend.destroyed == [sandbox_id]
            assert provider._warm_pool == {}
    finally:
        resume.set()
        await asyncio.gather(task, return_exceptions=True)
        provider.shutdown()
