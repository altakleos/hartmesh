"""Prewarm: the accepted-skills projection parked before the first turn asks.

The cold turn's pre-model block is the sandbox: on the released profile,
container create plus readiness measured 10 to 29 seconds of a block that is
otherwise about two seconds. Every byte of that is paid while the person
waits, and none of it depends on what they are about to type -- an accepted
projection container is shaped by ``(user, thread)`` and nothing else on a
local backend, which is exactly what its reuse fingerprint says.

So the container is built when the thread is opened and parked; the first
turn's own reclaim path then finds it. Nothing about acquisition changes, and
that is the point: these tests prove the prewarmed container is the one the
turn would have built, that it is claimed through the existing fences, and
that a speculative build can never make a real turn slower -- it never evicts,
never stands in for an active sandbox, and does not outlive its welcome.

Every backend here is a fake; the tests establish identity and lifecycle, not
restricted-runsc timings.
"""

from __future__ import annotations

import threading
import time

import pytest
from test_sandbox_warm_reuse_latency import ACCEPTED_USER, _acquire_accepted, _aio_mod, _make_provider

from deerflow.runtime.turn_phases import AcquisitionSource, turn_phases
from deerflow.sandbox.capabilities import WorkspacePrewarm, sandbox_capability


def _prewarm(provider, thread_id: str) -> str | None:
    return provider._prewarm_accepted_skills(thread_id, user_id=ACCEPTED_USER)


# ── The container the turn would have built ───────────────────────────────


def test_the_first_turn_reclaims_the_prewarmed_container_instead_of_creating(tmp_path, monkeypatch):
    provider, backend = _make_provider(tmp_path, monkeypatch)

    parked = _prewarm(provider, "thread-open")

    assert parked is not None
    assert backend.created == [parked], "prewarm builds exactly one container"
    assert parked in provider._warm_pool, "prewarm parks; it never holds the container active"
    assert parked in provider._accepted_only_sandbox_ids
    assert parked in provider._accepted_reuse_fingerprints

    with turn_phases(correlation_id="first-turn") as journal:
        first = _acquire_accepted(provider, "thread-open")

    assert first == parked
    assert backend.created == [parked], "the first turn must not create"
    assert backend.destroyed == [], "the first turn must not tear anything down"
    snapshot = journal.snapshot()
    assert snapshot.acquisition_source is AcquisitionSource.ACCEPTED_WARM_RECLAIM
    assert snapshot.resource_creates == 0
    assert provider._thread_sandboxes[(ACCEPTED_USER, "thread-open")] == parked


def test_prewarm_is_idempotent(tmp_path, monkeypatch):
    provider, backend = _make_provider(tmp_path, monkeypatch)

    first = _prewarm(provider, "thread-twice")
    second = _prewarm(provider, "thread-twice")

    assert first == second
    assert backend.created == [first]


def test_a_changed_create_input_after_prewarm_is_not_reclaimed(tmp_path, monkeypatch):
    """The fingerprint gates reuse; a prewarm built from stale inputs is replaced.

    This is the mutation guard for the identity claim: if the prewarm recorded
    something other than the acquisition's own fingerprint, the reclaim would
    either always succeed (unsafe) or never succeed (useless).
    """
    provider, backend = _make_provider(tmp_path, monkeypatch)
    aio = _aio_mod()

    parked = _prewarm(provider, "thread-stale")
    monkeypatch.setattr(aio.AioSandboxProvider, "_lark_integration_active", lambda *_a, **_k: True)

    first = _acquire_accepted(provider, "thread-stale")

    assert first == parked, "the deterministic name is the same; the container is not"
    assert backend.destroyed == [parked], "the stale prewarm is replaced under the ordinary fences"
    assert backend.created == [parked, parked]


# ── A speculative build never makes a real turn slower ────────────────────


def test_prewarm_never_evicts_to_make_room(tmp_path, monkeypatch):
    provider, backend = _make_provider(tmp_path, monkeypatch, replicas=2)

    one = _acquire_accepted(provider, "thread-1")
    provider.release(one)
    two = _acquire_accepted(provider, "thread-2")
    provider.release(two)

    assert _prewarm(provider, "thread-3") is None
    assert backend.created == [one, two]
    assert backend.destroyed == [], "a prewarm takes a free slot or nothing"
    assert set(provider._warm_pool) == {one, two}


def test_prewarm_is_a_noop_while_the_thread_holds_a_sandbox(tmp_path, monkeypatch):
    provider, backend = _make_provider(tmp_path, monkeypatch)

    active = _acquire_accepted(provider, "thread-busy")

    assert _prewarm(provider, "thread-busy") is None
    assert backend.created == [active]
    assert provider._thread_sandboxes[(ACCEPTED_USER, "thread-busy")] == active


def test_prewarm_refuses_a_backend_whose_container_the_binding_shapes(tmp_path, monkeypatch):
    """On the remote backend the binding is baked into the Pod at creation.

    A prewarm has no binding, so the Pod it would build is not the one the
    turn needs; the fingerprint already says so, and the prewarm defers to it.
    """
    provider, backend = _make_provider(tmp_path, monkeypatch)
    aio = _aio_mod()
    provider._backend = aio.RemoteSandboxBackend.__new__(aio.RemoteSandboxBackend)

    assert _prewarm(provider, "thread-remote") is None
    assert backend.created == []


def test_prewarm_failure_leaves_nothing_behind(tmp_path, monkeypatch):
    provider, backend = _make_provider(tmp_path, monkeypatch)

    def _refuse(*_a, **_k):
        raise RuntimeError("daemon refused")

    backend.create = _refuse

    with pytest.raises(RuntimeError):
        _prewarm(provider, "thread-fail")

    assert provider._warm_pool == {}
    assert provider._thread_sandboxes == {}
    assert "thread-fail" not in str(provider._accepted_only_sandbox_ids)


# ── It does not outlive its welcome ───────────────────────────────────────


def test_an_unclaimed_prewarm_is_reaped_after_the_claim_timeout(tmp_path, monkeypatch):
    provider, backend = _make_provider(tmp_path, monkeypatch)
    provider._config["prewarm_claim_timeout"] = 10
    aio = _aio_mod()
    now = time.time()
    clock = {"now": now}
    monkeypatch.setattr(aio.time, "time", lambda: clock["now"])

    abandoned = _prewarm(provider, "thread-abandoned")
    clock["now"] = now + 11

    provider._reap_unclaimed_prewarms()

    assert backend.destroyed == [abandoned]
    assert abandoned not in provider._warm_pool


def test_a_prewarm_within_the_claim_timeout_is_kept(tmp_path, monkeypatch):
    provider, backend = _make_provider(tmp_path, monkeypatch)
    provider._config["prewarm_claim_timeout"] = 10
    aio = _aio_mod()
    now = time.time()
    clock = {"now": now}
    monkeypatch.setattr(aio.time, "time", lambda: clock["now"])

    fresh = _prewarm(provider, "thread-fresh")
    clock["now"] = now + 9

    provider._reap_unclaimed_prewarms()

    assert backend.destroyed == []
    assert fresh in provider._warm_pool


def test_a_claimed_prewarm_is_governed_by_the_idle_timeout_not_the_claim_timeout(tmp_path, monkeypatch):
    """Once a turn used it, the container is an ordinary parked sandbox again."""
    provider, backend = _make_provider(tmp_path, monkeypatch)
    provider._config["prewarm_claim_timeout"] = 10
    aio = _aio_mod()
    now = time.time()
    clock = {"now": now}
    monkeypatch.setattr(aio.time, "time", lambda: clock["now"])

    used = _prewarm(provider, "thread-used")
    assert _acquire_accepted(provider, "thread-used") == used
    provider.release(used)
    clock["now"] = now + 60

    provider._reap_unclaimed_prewarms()

    assert backend.destroyed == [], "a claimed container is not a prewarm any more"
    assert used in provider._warm_pool


def test_a_container_rebuilt_under_a_replaced_prewarms_id_is_not_mistaken_for_it(tmp_path, monkeypatch):
    """Ids are deterministic and reused; the mark must follow the container.

    Prewarm, then a turn whose inputs changed replaces the container under the
    same id, uses it, and parks it. That parked container is a real turn's --
    the stale prewarm mark must not have it stopped as "unclaimed".
    """
    provider, backend = _make_provider(tmp_path, monkeypatch)
    provider._config["prewarm_claim_timeout"] = 10
    aio = _aio_mod()
    now = time.time()
    clock = {"now": now}
    monkeypatch.setattr(aio.time, "time", lambda: clock["now"])

    parked = _prewarm(provider, "thread-rebuilt")
    monkeypatch.setattr(aio.AioSandboxProvider, "_lark_integration_active", lambda *_a, **_k: True)
    rebuilt = _acquire_accepted(provider, "thread-rebuilt")
    assert rebuilt == parked and backend.destroyed == [parked]
    provider.release(rebuilt)
    clock["now"] = now + 60

    provider._reap_unclaimed_prewarms()

    assert backend.destroyed == [parked], "the rebuilt container is a turn's own parked sandbox"
    assert rebuilt in provider._warm_pool


def test_a_container_rebuilt_under_an_evicted_prewarms_id_is_not_mistaken_for_it(tmp_path, monkeypatch):
    provider, backend = _make_provider(tmp_path, monkeypatch, replicas=1)
    provider._config["prewarm_claim_timeout"] = 10
    aio = _aio_mod()
    now = time.time()
    clock = {"now": now}
    monkeypatch.setattr(aio.time, "time", lambda: clock["now"])

    parked = _prewarm(provider, "thread-evicted")
    other = _acquire_accepted(provider, "thread-other")  # at capacity: evicts the prewarm
    assert backend.destroyed == [parked]
    provider.release(other)
    clock["now"] = now + 5
    rebuilt = _acquire_accepted(provider, "thread-evicted")  # evicts `other`, rebuilds under the old id
    assert rebuilt == parked
    provider.release(rebuilt)
    clock["now"] = now + 60

    provider._reap_unclaimed_prewarms()

    assert backend.destroyed == [parked, other], "the rebuilt container survives the stale mark"
    assert rebuilt in provider._warm_pool


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
    aio = _aio_mod()
    monkeypatch.setattr(aio.AioSandboxProvider, "PREWARM_CHECK_INTERVAL", 0.02)
    now = time.time()
    clock = {"now": now}
    monkeypatch.setattr(aio.time, "time", lambda: clock["now"])

    abandoned = _prewarm(provider, "thread-idle-off")
    clock["now"] = now + 2
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


def test_the_claim_timeout_defaults_and_is_bounded(tmp_path, monkeypatch):
    provider, _backend = _make_provider(tmp_path, monkeypatch)
    aio = _aio_mod()

    assert provider._prewarm_claim_timeout() == aio.DEFAULT_PREWARM_CLAIM_TIMEOUT
    provider._config["prewarm_claim_timeout"] = 0
    assert provider._prewarm_claim_timeout() == 0, "zero disables the reaper, as idle_timeout: 0 does"


# ── The capability, and its async face ────────────────────────────────────


def test_the_provider_offers_the_prewarm_capability(tmp_path, monkeypatch):
    provider, _backend = _make_provider(tmp_path, monkeypatch)

    assert sandbox_capability(provider, WorkspacePrewarm) is provider


@pytest.mark.anyio
async def test_prewarm_async_parks_without_blocking_the_loop(tmp_path, monkeypatch):
    provider, backend = _make_provider(tmp_path, monkeypatch)

    parked = await provider.prewarm_accepted_skills_async("thread-async", user_id=ACCEPTED_USER)

    assert parked is not None
    assert backend.created == [parked]
    assert parked in provider._warm_pool
