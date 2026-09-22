"""``replicas`` is a budget the provider holds, not a number it logs about.

The defect this file pins: the AIO provider treated ``sandbox.replicas`` as a
soft cap. When every slot was in *active* use and there was nothing warm to
evict, it wrote ``All 2 replica slots are in active use; creating sandbox ...
beyond the soft limit`` and created the container anyway, and nothing refused
the one after that. On the released Compose profile the memory arithmetic is
exact at two -- ``2880 + 2 x (1024 + 96) = 5120`` MiB against a 5.0 GiB line --
so the third container is not slower, it is an OOM kill of whatever the kernel
scores highest. The async create path was worse: it had no in-flight count at
all, so two concurrent creates could each read the count, each find the last
slot, and each take it.

Two things about the shape of these tests, both deliberate:

**They count containers, not map entries.** ``_live(backend)`` is the fake
backend's own record of what it started and has not destroyed. A test that
asserts on ``provider._sandboxes`` passes on the defect, because the defect
tracks its overshoot perfectly well.

**Saturation is constructed, because it does not occur on its own.** Six
saturation events across four releases of estate qualification every one found
a warm entry to evict: ``Evicted warm-pool sandbox ... to stay within
replicas=2`` six times and the soft-limit branch zero times. Both slots merely
*occupied* takes the path that already worked. The breach needs both slots
**mid-execution** when a third thread asks, which is why the tests below hold
their sandboxes rather than releasing between turns -- a saturation case built
the obvious way exercises the branch that was never broken, which is how this
survived nineteen releases of being read.

Every backend here is a fake with exact call records. These are admission and
accounting invariants; they say nothing about restricted-runsc timings, and
nothing here demonstrates a deployment-wide budget across Gateway processes
(see ``test_two_gateways_over_one_backend_each_hold_their_own_budget``).
"""

from __future__ import annotations

import asyncio
import threading
import time

import pytest
from test_sandbox_warm_reuse_latency import ACCEPTED_USER, _aio_mod, _binding, _FakeBackend, _make_provider

from deerflow.runtime.turn_phases import TurnPhase, turn_phases
from deerflow.sandbox.exceptions import SandboxCapacityExceededError

USER = "capacity-owner"


def _live(backend: _FakeBackend) -> list[str]:
    """The containers the backend has started and not destroyed.

    The only honest count: the provider's maps are what the defect kept
    correct while it overshot.
    """
    return sorted(sandbox_id for sandbox_id, alive in backend.alive.items() if alive)


def _no_wait(provider) -> None:
    """Refuse immediately instead of waiting, where the wait is not the subject."""
    provider._config["capacity_wait_timeout"] = 0


class _BlockingBackend(_FakeBackend):
    """A backend whose ``create`` parks until every admitted create has arrived.

    This is what makes the concurrency tests test concurrency: without it the
    first create returns before the second is admitted, and three sequential
    creates never overlap in the window the budget has to hold.
    """

    def __init__(self, *, hold: int) -> None:
        super().__init__()
        self._arrived = threading.Semaphore(0)
        self._release = threading.Event()
        self._hold = hold
        self.concurrent_peak = 0
        self._in_flight = 0
        self._count_lock = threading.Lock()

    def create(self, thread_id, sandbox_id, **kwargs):
        with self._count_lock:
            self._in_flight += 1
            self.concurrent_peak = max(self.concurrent_peak, self._in_flight)
        self._arrived.release()
        assert self._release.wait(timeout=10), "the test never released the parked creates"
        try:
            return super().create(thread_id, sandbox_id, **kwargs)
        finally:
            with self._count_lock:
                self._in_flight -= 1

    def wait_until_parked(self) -> None:
        for _ in range(self._hold):
            assert self._arrived.acquire(timeout=10), "a create never reached the backend"

    def let_them_finish(self) -> None:
        self._release.set()


# ── The breach itself ────────────────────────────────────────────────────


def test_a_third_thread_is_refused_while_two_sandboxes_are_actively_in_use(tmp_path, monkeypatch):
    """The case the soft cap created through: two slots busy, nothing to evict.

    On the defect this test does not raise at all -- the third ``acquire``
    returns a sandbox id and the backend holds three containers.
    """
    provider, backend = _make_provider(tmp_path, monkeypatch, replicas=2)
    _no_wait(provider)
    first = provider.acquire("thread-a", user_id=USER)
    second = provider.acquire("thread-b", user_id=USER)

    with pytest.raises(SandboxCapacityExceededError) as refusal:
        provider.acquire("thread-c", user_id=USER)

    assert _live(backend) == sorted([first, second]), "no third container was started"
    assert backend.destroyed == [], "and no live turn was evicted to make room"
    assert refusal.value.details["retryable"] is True
    assert refusal.value.replicas == 2


@pytest.mark.anyio
async def test_the_async_path_refuses_the_same_third_thread(tmp_path, monkeypatch):
    """The async create path had neither the count nor the refusal.

    On the defect ``_create_sandbox_async`` read ``_replica_count()``, evicted
    if it could, and proceeded -- so this returned a third container.
    """
    provider, backend = _make_provider(tmp_path, monkeypatch, replicas=2)
    _no_wait(provider)
    first = await provider.acquire_async("thread-a", user_id=USER)
    second = await provider.acquire_async("thread-b", user_id=USER)

    with pytest.raises(SandboxCapacityExceededError):
        await provider.acquire_async("thread-c", user_id=USER)

    assert _live(backend) == sorted([first, second])


def test_the_accepted_path_is_refused_by_the_same_decision(tmp_path, monkeypatch):
    """Accepted projections create through the same door and pay the same budget.

    Driven through ``provision_accepted_skills``, the projection API the
    worker calls, rather than the internal acquisition underneath it: the
    binding step runs too, so this is the path the released profile takes on
    every turn.
    """
    provider, backend = _make_provider(tmp_path, monkeypatch, replicas=2)
    _no_wait(provider)

    def _provision(thread_id: str) -> str:
        return provider.provision_accepted_skills(thread_id, user_id=ACCEPTED_USER, binding=_binding())

    first = _provision("thread-a")
    second = _provision("thread-b")

    with pytest.raises(SandboxCapacityExceededError):
        _provision("thread-c")

    assert _live(backend) == sorted([first, second])


# ── Concurrency: the two creates that each found the last slot ───────────


def test_three_concurrent_sync_acquisitions_start_only_two_containers(tmp_path, monkeypatch):
    """Three threads ask at once; the budget admits two and refuses one.

    The creates are held open at the backend, so all three admissions happen
    inside the window where a container is started but not yet registered --
    neither active nor warm, and invisible to a count that reads only those
    two maps.
    """
    provider, _unused = _make_provider(tmp_path, monkeypatch, replicas=2)
    backend = _BlockingBackend(hold=2)
    provider._backend = backend
    _no_wait(provider)

    outcomes: dict[str, object] = {}
    barrier = threading.Barrier(3)

    def _ask(name: str) -> None:
        barrier.wait(timeout=10)
        try:
            outcomes[name] = provider.acquire(f"thread-{name}", user_id=USER)
        except BaseException as error:  # noqa: BLE001 - recorded, then asserted on
            outcomes[name] = error

    askers = [threading.Thread(target=_ask, args=(name,), name=f"ask-{name}") for name in ("a", "b", "c")]
    for asker in askers:
        asker.start()
    backend.wait_until_parked()
    backend.let_them_finish()
    for asker in askers:
        asker.join(timeout=15)
        assert not asker.is_alive()

    refused = [value for value in outcomes.values() if isinstance(value, SandboxCapacityExceededError)]
    granted = [value for value in outcomes.values() if isinstance(value, str)]
    assert len(granted) == 2 and len(refused) == 1, outcomes
    assert _live(backend) == sorted(granted)
    assert backend.concurrent_peak == 2, "the third never reached the backend"


@pytest.mark.anyio
async def test_three_concurrent_async_acquisitions_start_only_two_containers(tmp_path, monkeypatch):
    """The async race the defect made reachable, on the real entry point.

    ``_create_sandbox_async`` counted nothing in flight, so two coroutines
    could each read ``total < replicas`` and each create. Here all three are
    admitted while the first two are parked at the backend.
    """
    provider, _unused = _make_provider(tmp_path, monkeypatch, replicas=2)
    backend = _BlockingBackend(hold=2)
    provider._backend = backend
    _no_wait(provider)

    async def _ask(name: str):
        return await provider.acquire_async(f"thread-{name}", user_id=USER)

    tasks = [asyncio.create_task(_ask(name)) for name in ("a", "b", "c")]
    await asyncio.to_thread(backend.wait_until_parked)
    backend.let_them_finish()
    outcomes = await asyncio.gather(*tasks, return_exceptions=True)

    refused = [value for value in outcomes if isinstance(value, SandboxCapacityExceededError)]
    granted = [value for value in outcomes if isinstance(value, str)]
    assert len(granted) == 2 and len(refused) == 1, outcomes
    assert _live(backend) == sorted(granted)
    assert backend.concurrent_peak == 2


def test_simultaneous_distinct_identities_do_not_each_get_the_last_slot(tmp_path, monkeypatch):
    """Two users, one free slot: exactly one of them gets it.

    Distinct identities take distinct ids, distinct thread keys and distinct
    file locks, so nothing between the caller and the backend serializes them
    -- the reservation is the only thing that can.
    """
    provider, first_backend = _make_provider(tmp_path, monkeypatch, replicas=2)
    _no_wait(provider)
    held = provider.acquire("thread-held", user_id="tenant-owner")
    # Swapped in only now: the held sandbox must be built before creates park.
    backend = _BlockingBackend(hold=1)
    provider._backend = backend

    outcomes: dict[str, object] = {}
    barrier = threading.Barrier(2)

    def _ask(user: str) -> None:
        barrier.wait(timeout=10)
        try:
            outcomes[user] = provider.acquire("shared-thread-name", user_id=user)
        except BaseException as error:  # noqa: BLE001
            outcomes[user] = error

    askers = [threading.Thread(target=_ask, args=(user,)) for user in ("alice", "bob")]
    for asker in askers:
        asker.start()
    backend.wait_until_parked()
    backend.let_them_finish()
    for asker in askers:
        asker.join(timeout=15)

    granted = [value for value in outcomes.values() if isinstance(value, str)]
    refused = [value for value in outcomes.values() if isinstance(value, SandboxCapacityExceededError)]
    assert len(granted) == 1 and len(refused) == 1, outcomes
    assert _live(first_backend) == [held]
    assert _live(backend) == granted, "one identity started a container; the other was refused"


# ── Reuse never spends a slot ────────────────────────────────────────────


def test_a_compatible_warm_follow_up_at_capacity_takes_no_slot(tmp_path, monkeypatch):
    """The follow-up turn reclaims its own parked container at a full budget.

    Its container is already counted, so reclaiming it must neither reserve
    again nor be refused. A budget that refused here would have made every
    second turn on a busy deployment a failure.
    """
    provider, backend = _make_provider(tmp_path, monkeypatch, replicas=2)
    _no_wait(provider)
    parked = provider.acquire("thread-a", user_id=USER)
    provider.release(parked)
    other = provider.acquire("thread-b", user_id=USER)
    assert len(_live(backend)) == 2, "the budget is full: one parked, one active"

    again = provider.acquire("thread-a", user_id=USER)

    assert again == parked
    assert backend.created == [parked, other], "no container was started for the follow-up"
    assert backend.destroyed == [], "and nothing was evicted for it"


def test_an_in_process_reuse_at_capacity_takes_no_slot(tmp_path, monkeypatch):
    """The same thread asking twice within one turn is one slot, not two."""
    provider, backend = _make_provider(tmp_path, monkeypatch, replicas=2)
    _no_wait(provider)
    first = provider.acquire("thread-a", user_id=USER)
    provider.acquire("thread-b", user_id=USER)

    assert provider.acquire("thread-a", user_id=USER) == first
    assert len(_live(backend)) == 2


def test_eviction_is_preferred_to_waiting_when_something_is_parked(tmp_path, monkeypatch):
    """A parked container is the cheap slot and is taken without a wait.

    This is the path the estate's six saturation events actually took, and it
    must keep working unchanged: the budget is what is new, not the eviction.
    """
    provider, backend = _make_provider(tmp_path, monkeypatch, replicas=2)
    parked = provider.acquire("thread-a", user_id=USER)
    provider.release(parked)
    provider.acquire("thread-b", user_id=USER)

    with turn_phases(correlation_id="evict-not-wait") as journal:
        started = time.monotonic()
        third = provider.acquire("thread-c", user_id=USER)
    elapsed = time.monotonic() - started

    assert backend.destroyed == [parked]
    assert third in _live(backend)
    assert len(_live(backend)) == 2
    assert elapsed < 1.0, "the wait budget was not spent"
    snapshot = journal.snapshot()
    assert snapshot.capacity_waits == 0
    assert snapshot.capacity_refusals == 0
    assert snapshot.evictions == 1


def test_a_live_turn_is_never_evicted_for_capacity(tmp_path, monkeypatch):
    """Refusing is the answer when the only containers are in use."""
    provider, backend = _make_provider(tmp_path, monkeypatch, replicas=2)
    _no_wait(provider)
    first = provider.acquire("thread-a", user_id=USER)
    second = provider.acquire("thread-b", user_id=USER)

    with pytest.raises(SandboxCapacityExceededError):
        provider.acquire("thread-c", user_id=USER)

    assert backend.destroyed == []
    assert _live(backend) == sorted([first, second])
    assert provider.get(first) is not None and provider.get(second) is not None


# ── A reservation released only when the set is gone ─────────────────────


def test_a_create_that_never_reached_the_backend_frees_its_reservation(tmp_path, monkeypatch):
    """The crash between reserving a slot and starting a container.

    The reservation must not outlive the attempt, or one failed create costs
    the deployment a slot for the life of the process.
    """
    provider, backend = _make_provider(tmp_path, monkeypatch, replicas=2)
    _no_wait(provider)
    held = provider.acquire("thread-a", user_id=USER)

    working_create = backend.create

    def _explode(*_args, **_kwargs):
        raise RuntimeError("docker daemon went away")

    backend.create = _explode
    with pytest.raises(RuntimeError, match="docker daemon"):
        provider.acquire("thread-b", user_id=USER)
    assert provider._starting == set(), "the reservation was dropped with the attempt"

    backend.create = working_create
    later = provider.acquire("thread-b", user_id=USER)
    assert _live(backend) == sorted([held, later])


def test_a_container_started_but_never_registered_frees_its_slot_only_once_gone(tmp_path, monkeypatch):
    """The crash between ``create`` and registration.

    The container exists, so the slot stays spent until the ownership-fenced
    teardown confirms it absent; only then does the next acquisition get it.
    Releasing the reservation at the raise instead would hand the slot out
    while the container it paid for was still running -- two live containers
    for one slot, which is the breach wearing a different hat.

    The ordering is what this test is for, so the teardown is held open and a
    second thread asks *during* it. Run sequentially there is no instant where
    the unready container is alive and a slot is wanted, and the assertion
    passes whether the reservation is freed before the teardown or after.
    """
    aio = _aio_mod()
    provider, backend = _make_provider(tmp_path, monkeypatch, replicas=2)
    provider._config["capacity_wait_timeout"] = 10
    held = provider.acquire("thread-a", user_id=USER)
    monkeypatch.setattr(aio, "wait_for_sandbox_ready", lambda _url, timeout=60, **_kw: False)

    tearing_down = threading.Event()
    finish_teardown = threading.Event()
    real_destroy = backend.destroy

    def _slow_destroy(info):
        tearing_down.set()
        assert finish_teardown.wait(timeout=10)
        return real_destroy(info)

    backend.destroy = _slow_destroy
    failed: list[BaseException] = []

    def _fail_to_start() -> None:
        try:
            provider.acquire("thread-b", user_id=USER)
        except BaseException as error:  # noqa: BLE001
            failed.append(error)

    starter = threading.Thread(target=_fail_to_start, name="unready")
    starter.start()
    assert tearing_down.wait(timeout=10)
    backend.destroy = real_destroy
    monkeypatch.setattr(aio, "wait_for_sandbox_ready", lambda _url, timeout=60, **_kw: True)

    acquired: list[object] = []
    asker = threading.Thread(target=lambda: acquired.append(provider.acquire("thread-c", user_id=USER)), name="asker")
    asker.start()
    time.sleep(0.3)
    assert acquired == [], "the slot is still spent: its container has not been confirmed gone"
    assert len(_live(backend)) == 2, "and no third container was started in the meantime"

    finish_teardown.set()
    starter.join(timeout=10)
    asker.join(timeout=10)

    assert not starter.is_alive() and not asker.is_alive()
    assert failed and isinstance(failed[0], RuntimeError) and "failed to become ready" in str(failed[0])
    assert isinstance(acquired[0], str)
    assert _live(backend) == sorted([held, acquired[0]]), "the unready container was torn down, not leaked"
    assert provider._starting == set()


def test_an_ownership_store_failure_does_not_leave_the_slot_spent(tmp_path, monkeypatch):
    """A store that cannot answer fails the acquisition closed and gives the slot back."""
    aio = _aio_mod()
    provider, backend = _make_provider(tmp_path, monkeypatch, replicas=2)
    _no_wait(provider)
    held = provider.acquire("thread-a", user_id=USER)

    working_publish = aio.AioSandboxProvider._publish_ownership

    def _refuse(_self, _sandbox_id):
        raise aio.OwnershipBackendError("ownership store is unreachable")

    aio.AioSandboxProvider._publish_ownership = _refuse
    try:
        with pytest.raises(aio.OwnershipBackendError):
            provider.acquire("thread-b", user_id=USER)
    finally:
        aio.AioSandboxProvider._publish_ownership = working_publish
    assert provider._starting == set()

    later = provider.acquire("thread-b", user_id=USER)
    assert _live(backend) == sorted([held, later])


def test_a_set_whose_teardown_did_not_confirm_keeps_holding_its_slot(tmp_path, monkeypatch):
    """Quarantined sets count, so an unconfirmed stop cannot free a slot.

    This half of the contract predates this change (``#75``); it is pinned
    here because the budget is only as honest as what it counts.
    """
    provider, backend = _make_provider(tmp_path, monkeypatch, replicas=2)
    _no_wait(provider)
    parked = provider.acquire("thread-a", user_id=USER)
    provider.release(parked)
    provider.acquire("thread-b", user_id=USER)

    def _fails(_info):
        raise RuntimeError("stop timed out")

    monkeypatch.setattr(backend, "destroy", _fails)
    with pytest.raises(SandboxCapacityExceededError):
        provider.acquire("thread-c", user_id=USER)

    assert parked in provider._cleanup_pending
    assert parked in provider._warm_pool, "retained, never handed out"


def test_a_set_whose_teardown_did_not_confirm_occupies_a_slot_in_the_count(tmp_path, monkeypatch):
    """Retaining the entry is not the same as counting it, and only one is the budget.

    The refusal in the test above comes from a failed *eviction*; it would
    still refuse if quarantined sets were left out of the count entirely.
    This one has nothing active and nothing to evict, so the only thing that
    can refuse the second acquisition is the quarantined set occupying a slot.

    The case the estate actually produces: a ``docker stop`` that times out
    under runsc leaves a container running that nobody can confirm gone.
    """
    provider, backend = _make_provider(tmp_path, monkeypatch, replicas=2)
    provider._config["capacity_wait_timeout"] = 1
    stranded = provider.acquire("thread-a", user_id=USER)
    provider.release(stranded)

    def _times_out(_info):
        raise RuntimeError("docker stop timed out")

    backend.destroy = _times_out
    info, _parked_at = provider._warm_pool[stranded]
    provider._warm_pool[stranded] = (info, time.time() - 3600)
    provider._reap_expired_warm(60.0)
    assert stranded in provider._cleanup_pending, "not confirmed absent"
    assert provider._sandboxes == {}, "nothing is active; only the stranded set is counted"

    # One slot left of the two, so this one is granted...
    granted = provider.acquire("thread-b", user_id=USER)
    assert granted in _live(backend)
    # ...and the next is refused by the stranded set, not by a failed eviction.
    with pytest.raises(SandboxCapacityExceededError) as refusal:
        provider.acquire("thread-c", user_id=USER)
    assert refusal.value.warm == 1, "the stranded set is reported as the slot it is holding"
    assert len(_live(backend)) == 2


def test_teardown_racing_an_acquisition_hands_over_exactly_one_slot(tmp_path, monkeypatch):
    """An idle reap and an acquisition want the same slot; one of them gets it.

    The entry stays visible in the warm pool for the whole stop, so both see
    it. The container must be started only after the stop confirms absence.
    """
    provider, backend = _make_provider(tmp_path, monkeypatch, replicas=2)
    provider._config["capacity_wait_timeout"] = 5
    parked = provider.acquire("thread-a", user_id=USER)
    provider.release(parked)
    provider.acquire("thread-b", user_id=USER)

    stopping = threading.Event()
    finish_stop = threading.Event()
    real_destroy = backend.destroy

    def _slow_destroy(info):
        stopping.set()
        assert finish_stop.wait(timeout=10)
        return real_destroy(info)

    backend.destroy = _slow_destroy
    # Aged rather than reaped with a zero timeout: zero *disables* the reaper.
    info, _parked_at = provider._warm_pool[parked]
    provider._warm_pool[parked] = (info, time.time() - 3600)
    reaper = threading.Thread(target=lambda: provider._reap_expired_warm(60.0), name="reap")
    reaper.start()
    assert stopping.wait(timeout=10)

    acquired: list[object] = []
    asker = threading.Thread(target=lambda: acquired.append(provider.acquire("thread-c", user_id=USER)), name="ask")
    asker.start()
    time.sleep(0.2)
    assert backend.created == [parked, provider._thread_sandboxes[(USER, "thread-b")]], "no container started while the stop is unresolved"
    finish_stop.set()
    reaper.join(timeout=10)
    asker.join(timeout=10)

    assert not asker.is_alive()
    assert len(acquired) == 1 and isinstance(acquired[0], str)
    assert len(_live(backend)) == 2


# ── The wait ─────────────────────────────────────────────────────────────


def test_a_slot_freed_during_the_wait_is_taken_rather_than_refused(tmp_path, monkeypatch):
    """The wait exists for the overlap at the edges of two turns.

    Nothing is parked here, so eviction cannot be what rescues this: the
    waiter is woken by the teardown that freed the slot.
    """
    provider, backend = _make_provider(tmp_path, monkeypatch, replicas=2)
    provider._config["capacity_wait_timeout"] = 10
    first = provider.acquire("thread-a", user_id=USER)
    provider.acquire("thread-b", user_id=USER)

    acquired: list[object] = []

    def _ask() -> None:
        try:
            acquired.append(provider.acquire("thread-c", user_id=USER))
        except BaseException as error:  # noqa: BLE001
            acquired.append(error)

    asker = threading.Thread(target=_ask, name="waiting-asker")
    asker.start()
    # Long enough that the waiter is asleep on the gate rather than mid-loop.
    time.sleep(0.3)
    assert acquired == [], "still waiting, not refused"
    provider.destroy(first)
    asker.join(timeout=10)

    assert not asker.is_alive()
    assert isinstance(acquired[0], str)
    assert len(_live(backend)) == 2


def test_a_turn_that_parks_its_container_ends_the_wait_instead_of_running_it_out(tmp_path, monkeypatch):
    """Parking frees no slot, but it is exactly what the wait is waiting for.

    Saturation is two turns overlapping at their edges, and the one that is
    about to finish *parks* its container rather than destroying it — so the
    counted total does not move and a waiter that only wakes on a freed slot
    sleeps out its whole budget with an evictable container sitting there,
    then pays a cold start anyway. The wake-up has to cover "a set became
    evictable", not only "a set went away".
    """
    provider, backend = _make_provider(tmp_path, monkeypatch, replicas=2)
    provider._config["capacity_wait_timeout"] = 30
    first = provider.acquire("thread-a", user_id=USER)
    provider.acquire("thread-b", user_id=USER)

    acquired: list[object] = []

    def _ask() -> None:
        started = time.monotonic()
        try:
            acquired.append((provider.acquire("thread-c", user_id=USER), time.monotonic() - started))
        except BaseException as error:  # noqa: BLE001
            acquired.append(error)

    asker = threading.Thread(target=_ask, name="waiting-for-a-park")
    asker.start()
    time.sleep(0.3)
    assert acquired == [], "asleep on the gate, with nothing parked yet"
    provider.release(first)
    asker.join(timeout=10)

    assert not asker.is_alive()
    granted, waited = acquired[0]
    assert isinstance(granted, str)
    assert waited < 5.0, f"the wait ended when the container parked, not at the budget ({waited:.1f}s)"
    assert backend.destroyed == [first], "the parked container paid for the slot"
    assert len(_live(backend)) == 2


def test_the_wait_is_measured_and_counted_once_however_many_passes_it_takes(tmp_path, monkeypatch):
    """One span and one count per turn, per the phase vocabulary's own rule.

    A name opened twice in a turn has its second span measured and dropped, so
    a retried wait would hide the very figure the phase exists to explain.
    """
    provider, _backend = _make_provider(tmp_path, monkeypatch, replicas=2)
    provider._config["capacity_wait_timeout"] = 0.4
    provider.acquire("thread-a", user_id=USER)
    provider.acquire("thread-b", user_id=USER)

    # Woken twice without a slot appearing, so the admission loop really does
    # go round three times. A spurious wake is documented as harmless; this is
    # what makes "one span however many passes" a claim about more than one.
    def _wake_twice() -> None:
        for _ in range(2):
            time.sleep(0.08)
            with provider._lock:
                provider._wake_capacity_waiters_locked()

    waker = threading.Thread(target=_wake_twice, name="spurious-wakes")
    with turn_phases(correlation_id="waited-then-refused") as journal:
        waker.start()
        with pytest.raises(SandboxCapacityExceededError):
            provider.acquire("thread-c", user_id=USER)
    waker.join(timeout=5)

    snapshot = journal.snapshot()
    assert snapshot.capacity_waits == 1
    assert snapshot.capacity_refusals == 1
    waits = [record for record in snapshot.phases if record.phase == TurnPhase.SANDBOX_CAPACITY_WAIT]
    assert len(waits) == 1, "the wait is one span"
    assert waits[0].duration_ms is not None and waits[0].duration_ms >= 300
    line = snapshot.to_log_line()
    assert "capacity_waits=1" in line and "capacity_refusals=1" in line


def test_a_turn_that_is_never_refused_counts_no_capacity_at_all(tmp_path, monkeypatch):
    """The counters name a deployment at its limit; an ordinary turn is silent."""
    provider, _backend = _make_provider(tmp_path, monkeypatch, replicas=2)
    with turn_phases(correlation_id="ordinary") as journal:
        provider.acquire("thread-a", user_id=USER)

    snapshot = journal.snapshot()
    assert (snapshot.capacity_waits, snapshot.capacity_refusals) == (0, 0)
    assert "capacity_waits" not in snapshot.to_log_line()


@pytest.mark.anyio
async def test_repeated_cancellation_of_a_waiting_acquisition_leaves_nothing_behind(tmp_path, monkeypatch):
    """Stop during the wait is answered at once, and costs no container.

    Cancelled three times over: each attempt must take no slot, start no
    container and leave no waiter registered, or a person pressing Stop while
    the deployment is busy would slowly consume its budget.
    """
    provider, backend = _make_provider(tmp_path, monkeypatch, replicas=2)
    provider._config["capacity_wait_timeout"] = 30
    first = await provider.acquire_async("thread-a", user_id=USER)
    await provider.acquire_async("thread-b", user_id=USER)

    for attempt in range(3):
        task = asyncio.create_task(provider.acquire_async(f"thread-c{attempt}", user_id=USER))
        # Let it reach the wait rather than cancelling it before admission.
        for _ in range(200):
            await asyncio.sleep(0.005)
            if provider._capacity_async_waiters:
                break
        assert provider._capacity_async_waiters, "the acquisition never reached the wait"
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert provider._capacity_async_waiters == [], "a cancelled waiter deregisters"
        assert provider._starting == set(), "and holds no reservation"

    assert len(_live(backend)) == 2
    await asyncio.to_thread(provider.destroy, first)
    assert await provider.acquire_async("thread-d", user_id=USER)
    assert len(_live(backend)) == 2


def test_a_prewarm_never_waits_and_never_evicts_for_its_slot(tmp_path, monkeypatch):
    """A prewarm is speculation; it may not spend a turn's container or a turn's time.

    It gets the fast refusal its caller already handles, and the parked
    container a real follow-up would reclaim is left alone.
    """
    provider, backend = _make_provider(tmp_path, monkeypatch, replicas=2)
    provider._config["capacity_wait_timeout"] = 30
    parked = provider.acquire("thread-a", user_id=USER)
    provider.release(parked)
    provider.acquire("thread-b", user_id=USER)

    started = time.monotonic()
    assert provider._prewarm_accepted_skills("thread-c", user_id=ACCEPTED_USER) is None
    assert time.monotonic() - started < 1.0, "a prewarm does not wait for a slot"
    assert backend.destroyed == [], "and does not evict a container a turn would reclaim"
    assert parked in provider._warm_pool


# ── Boundaries this change does not claim to cross ───────────────────────


def test_two_gateways_over_one_backend_each_hold_their_own_budget(tmp_path, monkeypatch):
    """Accounting is per Gateway process, and this test says so rather than implying more.

    Each provider refuses its own third container. Together they can still
    reach four, because nothing here coordinates across processes -- the
    released profile runs one Gateway replica, and a deployment-wide budget
    would need the reservation in a shared store (the E2B provider's
    ``RedisE2BCapacityStore`` is the shape that would take). Pinned so the
    limit of this change is written down rather than assumed.
    """
    first, backend = _make_provider(tmp_path / "a", monkeypatch, replicas=2)
    second, _other = _make_provider(tmp_path / "b", monkeypatch, replicas=2)
    second._backend = backend
    _no_wait(first)
    _no_wait(second)

    first.acquire("thread-a", user_id=USER)
    first.acquire("thread-b", user_id=USER)
    with pytest.raises(SandboxCapacityExceededError):
        first.acquire("thread-c", user_id=USER)

    second.acquire("thread-x", user_id="other-owner")
    second.acquire("thread-y", user_id="other-owner")
    with pytest.raises(SandboxCapacityExceededError):
        second.acquire("thread-z", user_id="other-owner")

    assert len(_live(backend)) == 4, "two processes, two budgets; not one deployment-wide budget"


def test_inventory_rebuilt_at_restart_counts_against_the_budget(tmp_path, monkeypatch):
    """Containers adopted at startup are real, so they occupy real slots.

    A budget that counted only what this process started would treat a
    restarted Gateway as an empty deployment and rebuild straight past the
    memory line the old containers are still using.
    """
    provider, backend = _make_provider(tmp_path, monkeypatch, replicas=2)
    survivor_a = provider.acquire("thread-a", user_id=USER)
    survivor_b = provider.acquire("thread-b", user_id=USER)

    restarted, _unused = _make_provider(tmp_path, monkeypatch, replicas=2)
    restarted._backend = backend
    restarted._unowned_since = {}
    monkeypatch.setattr(backend, "list_running", lambda: [backend.infos[survivor_a], backend.infos[survivor_b]])
    restarted._reconcile_orphans()
    assert set(restarted._warm_pool) == {survivor_a, survivor_b}, "adopted, and counted"
    _no_wait(restarted)

    rebuilt = restarted.acquire("thread-c", user_id=USER)
    assert backend.destroyed == [survivor_a], "the adopted inventory paid for the slot"
    assert len(_live(backend)) == 2
    restarted.acquire("thread-d", user_id=USER)
    with pytest.raises(SandboxCapacityExceededError):
        restarted.acquire("thread-e", user_id=USER)
    assert len(_live(backend)) == 2
    assert rebuilt in _live(backend)


# ── What the refusal becomes upstream ────────────────────────────────────


def _refusal_result(exc: BaseException):
    """Put *exc* through the real tool-error middleware and read what it made."""
    from types import SimpleNamespace

    from deerflow.agents.middlewares.tool_error_handling_middleware import ToolErrorHandlingMiddleware
    from deerflow.agents.middlewares.tool_result_meta import TOOL_META_KEY

    request = SimpleNamespace(tool_call={"name": "execute_command", "id": "call-1"})

    def _raise(_request):
        raise exc

    message = ToolErrorHandlingMiddleware().wrap_tool_call(request, _raise)
    return message, message.additional_kwargs[TOOL_META_KEY]


def test_a_capacity_refusal_is_not_something_the_model_retries(tmp_path, monkeypatch):
    """The refusal must reach the model as an operational state, not a tool fault.

    Classified by keywords it lands on the unknown-error rule -- recoverable,
    "try_alternative" -- which is an invitation to call the tool again against
    a budget that is still full. The category comes off the exception's own
    type instead, so no wording can drift it.
    """
    provider, _backend = _make_provider(tmp_path, monkeypatch, replicas=2)
    _no_wait(provider)
    provider.acquire("thread-a", user_id=USER)
    provider.acquire("thread-b", user_id=USER)
    with pytest.raises(SandboxCapacityExceededError) as refusal:
        provider.acquire("thread-c", user_id=USER)

    message, meta = _refusal_result(refusal.value)

    assert meta["error_type"] == "capacity"
    assert meta["recoverable_by_model"] is False
    assert meta["recommended_next_action"] == "summarize"
    assert message.status == "error"
    assert "room for" in message.content, "the model is told what is true, not 'the tool failed'"


def test_an_ordinary_sandbox_failure_keeps_its_old_classification(tmp_path, monkeypatch):
    """Only a declaring exception skips the text rules; nothing else changes."""
    from deerflow.sandbox.exceptions import SandboxCommandError

    _message, meta = _refusal_result(SandboxCommandError("no such file or directory"))

    assert meta["error_type"] == "not_found"
    assert meta["recoverable_by_model"] is True


def test_a_sandbox_tool_does_not_flatten_the_refusal_into_its_output():
    """The tool boundary catches ``SandboxError``; this one has to pass through.

    Returned as ``f"Error: {e}"`` the type is gone before any classifier sees
    it, which is how a typed refusal turns back into an unclassifiable tool
    error at the last step. Asserted on the tool's own behaviour rather than
    on the tuple it is named in: a tuple is not evidence that the tool body
    reaches it.
    """
    from deerflow.sandbox import tools as sandbox_tools

    assert issubclass(SandboxCapacityExceededError, sandbox_tools.SandboxError), "which is why it needs naming explicitly"

    from types import SimpleNamespace

    def _refuse(_runtime=None):
        raise SandboxCapacityExceededError(active=2, replicas=2)

    original = sandbox_tools.ensure_sandbox_initialized
    sandbox_tools.ensure_sandbox_initialized = _refuse
    try:
        # A `SandboxError` returned by this body would come back as a string;
        # the refusal has to come back out.
        with pytest.raises(SandboxCapacityExceededError):
            sandbox_tools.ls_tool.func(runtime=SimpleNamespace(context={}, state={}), path="/mnt/user-data/outputs")
    finally:
        sandbox_tools.ensure_sandbox_initialized = original


def test_a_write_at_saturation_is_a_result_the_run_survives_not_an_escape():
    """Through the real middleware order, not one layer in isolation.

    ``ReadBeforeWriteMiddleware`` composes *outer* of
    ``ToolErrorHandlingMiddleware`` -- the only layer that turns an exception
    into a ToolMessage -- so a refusal raised from the write gate escapes
    every handler and kills the run, taking the receipt the outermost layer
    opened with it. Reachable with stock configuration: the gate is on by
    default and inspects the file before every ``write_file``.
    """
    from types import SimpleNamespace

    from deerflow.agents.middlewares.read_before_write_middleware import ReadBeforeWriteMiddleware
    from deerflow.agents.middlewares.tool_error_handling_middleware import ToolErrorHandlingMiddleware
    from deerflow.agents.middlewares.tool_result_meta import TOOL_META_KEY

    def _refusing_reader(_runtime, _path):
        raise SandboxCapacityExceededError(active=2, replicas=2)

    gate = ReadBeforeWriteMiddleware(content_reader=_refusing_reader)
    errors = ToolErrorHandlingMiddleware()
    request = SimpleNamespace(
        tool_call={"name": "write_file", "id": "call-1", "args": {"path": "/mnt/user-data/outputs/report.md", "content": "x"}},
        runtime=SimpleNamespace(context={}, state={"messages": []}),
        state={"messages": []},
    )

    def _tool_body(_request):
        # The tool asks for the same sandbox the gate could not get.
        raise SandboxCapacityExceededError(active=2, replicas=2)

    # Composed outermost-first, exactly as `_build_runtime_middlewares` orders them.
    result = gate.wrap_tool_call(request, lambda req: errors.wrap_tool_call(req, _tool_body))

    assert result.status == "error", "a result, not an exception that ends the run"
    meta = result.additional_kwargs[TOOL_META_KEY]
    assert meta["error_type"] == "capacity"
    assert meta["recoverable_by_model"] is False
    assert "replicas" not in result.content, "the raw refusal's counts are not model-facing"


# ── The budget's own configuration ───────────────────────────────────────


def test_the_wait_budget_is_configurable_and_never_unbounded():
    aio = _aio_mod()
    resolve = aio.resolve_capacity_wait_timeout

    assert resolve(None) == aio.DEFAULT_CAPACITY_WAIT_TIMEOUT
    assert resolve(0) == 0, "refusing immediately is a legitimate setting"
    assert resolve(12) == 12
    assert resolve(10_000) == aio.CAPACITY_WAIT_TIMEOUT_MAX, "bounded, not honoured"
    assert resolve(-1) == aio.DEFAULT_CAPACITY_WAIT_TIMEOUT
    assert resolve(float("inf")) == aio.CAPACITY_WAIT_TIMEOUT_MAX
    assert resolve(float("nan")) == aio.DEFAULT_CAPACITY_WAIT_TIMEOUT
    assert resolve(True) == aio.DEFAULT_CAPACITY_WAIT_TIMEOUT, "a boolean is a mistake, not a budget"
    assert resolve("soon") == aio.DEFAULT_CAPACITY_WAIT_TIMEOUT


def test_the_config_schema_refuses_a_budget_that_is_not_a_number():
    from pydantic import ValidationError

    from deerflow.config.sandbox_config import SandboxConfig

    base = {"use": "deerflow.sandbox.local:LocalSandboxProvider"}
    assert SandboxConfig(**base).capacity_wait_timeout is None
    assert SandboxConfig(**base, capacity_wait_timeout=0).capacity_wait_timeout == 0
    for refused in (True, -1, 301):
        with pytest.raises(ValidationError):
            SandboxConfig(**base, capacity_wait_timeout=refused)


# ── What a person sees when there is no room ─────────────────────────────


@pytest.mark.anyio
async def test_a_refusal_before_the_model_ends_the_turn_legibly(tmp_path, monkeypatch):
    """On the released profile the sandbox is acquired before the model runs.

    A refusal there never passes a tool boundary, so the typed tool-result
    contract cannot help: without its own branch the worker's generic handler
    gives the person ``Runtime operation failed (reference: <hex>)``, which is
    indistinguishable from a crash and tells neither them nor an operator that
    the deployment is simply full.
    """
    from types import SimpleNamespace
    from unittest.mock import AsyncMock

    from deerflow.runtime.runs.manager import RunManager
    from deerflow.runtime.runs.worker import SANDBOX_CAPACITY_MESSAGE, SANDBOX_CAPACITY_STOP_REASON, RunContext, run_agent

    run_manager = RunManager()
    record = await run_manager.create("thread-no-room")
    bridge = SimpleNamespace(publish=AsyncMock(), publish_end=AsyncMock(), cleanup=AsyncMock())

    class _RefusingAgent:
        async def astream(self, graph_input, config=None, stream_mode=None, subgraphs=False):
            del graph_input, config, stream_mode, subgraphs
            raise SandboxCapacityExceededError(
                "This deployment is already running as many sandboxes as it has room for",
                active=2,
                replicas=2,
            )
            yield  # pragma: no cover - makes this an async generator

    await run_agent(
        bridge,
        run_manager,
        record,
        ctx=RunContext(checkpointer=None),
        agent_factory=lambda **_kwargs: _RefusingAgent(),
        graph_input={},
        config={},
        stream_modes=["messages-tuple"],
    )

    errors = [call.args[2] for call in bridge.publish.await_args_list if call.args[1] == "error"]
    assert len(errors) == 1
    assert errors[0]["message"] == SANDBOX_CAPACITY_MESSAGE
    assert "room for" in errors[0]["message"], "the person is told what is true"
    assert "Runtime operation failed" not in errors[0]["message"], "the generic terminal is the defect"
    assert errors[0]["stop_reason"] == SANDBOX_CAPACITY_STOP_REASON
    assert errors[0]["name"] == "SandboxCapacityExceededError"
    fetched = await run_manager.get(record.run_id)
    assert fetched.error == SANDBOX_CAPACITY_MESSAGE
