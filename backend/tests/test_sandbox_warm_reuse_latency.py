"""Warm reuse on the compatible follow-up path, and the phases that prove it.

The defect these cover: the accepted-skills acquisition path checked only
*active* tracking and then fell through to create. Its terminal is park, so the
container it wanted was sitting in the warm pool -- and create's replica
enforcement evicts a warm entry before the backend can observe that the target
already exists. A compatible follow-up therefore paid for an unrelated
container teardown and a cold start it never needed.

Every backend here is a fake. These tests establish ordering, identity and
lifecycle invariants; they say nothing about restricted-runsc performance, and
the eviction they suppress is real work that a genuinely new third resource
still has to do.
"""

from __future__ import annotations

import asyncio
import contextlib
import copy
import importlib
import threading
import time
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from deerflow.community.aio_sandbox.ownership.memory import MemoryOwnershipStore
from deerflow.config.paths import Paths
from deerflow.config.sandbox_config import SandboxOwnershipConfig
from deerflow.runtime.turn_phases import AcquisitionSource, TurnPhase, turn_phases
from deerflow.sandbox.accepted_material import AcceptedSkillSandboxBindingV1
from deerflow.sandbox.acquire_serialization import AcquireSerializer

ACCEPTED_USER = "warm-owner"


def _aio_mod():
    return importlib.import_module("deerflow.community.aio_sandbox.aio_sandbox_provider")


class _FakeBackend:
    """A counted container backend: no Docker, no sleeps, exact call records.

    It answers ``create`` the way ``LocalContainerBackend`` does: a fresh start
    is reported as ``created``, and (with ``adopt_on_conflict``) a running
    container under the deterministic name is returned as ``rediscovered``.
    The provenance is set as an attribute rather than a constructor argument
    so this file still collects against a ``SandboxInfo`` that predates it, and
    the lifecycle tests below then fail on behaviour rather than on import.
    """

    def __init__(self, *, adopt_on_conflict: bool = False, discoverable: bool = False) -> None:
        self.created: list[str] = []
        self.destroyed: list[str] = []
        self.adopted: list[str] = []
        self.alive: dict[str, bool] = {}
        self.unverifiable: set[str] = set()
        self.infos: dict[str, object] = {}
        # ``LocalContainerBackend.create`` answers a Docker name conflict by
        # discovering and returning the running container; this mirrors it.
        self.adopt_on_conflict = adopt_on_conflict
        # Whether ``discover`` answers for running containers, as the local
        # backend's does; off by default so the older tests keep their shape.
        self.discoverable = discoverable

    def create(self, thread_id, sandbox_id, **_kwargs):
        if self.adopt_on_conflict and self.alive.get(sandbox_id):
            self.adopted.append(sandbox_id)
            found = copy.copy(self.infos[sandbox_id])
            found.provenance = "rediscovered"
            return found
        self.created.append(sandbox_id)
        self.alive[sandbox_id] = True
        self.infos[sandbox_id] = self._unused_info(sandbox_id)
        started = copy.copy(self.infos[sandbox_id])
        started.provenance = "created"
        return started

    def _unused_info(self, sandbox_id):
        aio = _aio_mod()
        return aio.SandboxInfo(
            sandbox_id=sandbox_id,
            sandbox_url=f"http://sandbox/{sandbox_id}",
            container_name=f"deer-flow-sandbox-{sandbox_id}",
        )

    def destroy(self, info) -> None:
        self.destroyed.append(info.sandbox_id)
        self.alive[info.sandbox_id] = False

    def is_alive(self, info) -> bool:
        if info.sandbox_id in self.unverifiable:
            raise RuntimeError("daemon did not answer")
        return self.alive.get(info.sandbox_id, True)

    def discover(self, sandbox_id):
        if self.discoverable and self.alive.get(sandbox_id):
            return copy.copy(self.infos[sandbox_id])
        return None

    def list_running(self):
        return []


def _make_provider(tmp_path, monkeypatch, *, replicas: int = 2, adopt_on_conflict: bool = False, discoverable: bool = False) -> tuple[object, _FakeBackend]:
    """A provider wired to a fake backend, with no threads and no real config.

    ``replicas=2`` matches the consumed profile's two sandbox slots, which is
    what makes the eviction in these tests the same eviction the deployment
    saw.
    """
    aio = _aio_mod()
    provider = aio.AioSandboxProvider.__new__(aio.AioSandboxProvider)
    provider._config = {"idle_timeout": 600, "replicas": replicas}
    provider._sandboxes = {}
    provider._sandbox_infos = {}
    provider._thread_sandboxes = {}
    provider._warm_pool = {}
    provider._active_sandbox_identity = {}
    provider._warm_pool_identity = {}
    provider._unowned_since = {}
    provider._last_activity = {}
    provider._local_teardown = set()
    provider._starting = set()
    provider._accepted_only_sandbox_ids = set()
    provider._accepted_reuse_fingerprints = {}
    provider._acquire_epoch = {}
    provider._acquire_epoch_counter = 0
    provider._acquire_inflight = {}
    provider._acquire_serializer = AcquireSerializer(thread_name_prefix="warm-reuse-test")
    provider._lock = threading.Lock()
    provider._idle_checker_stop = MagicMock()
    provider._idle_checker_thread = None
    provider._renewal_stop = MagicMock()
    provider._renewal_thread = None
    provider._shutdown_called = False
    provider._owner_id = "warm-reuse-worker"
    provider._ownership_config = SandboxOwnershipConfig()
    provider._ownership = MemoryOwnershipStore(owner_id="warm-reuse-worker", ttl_seconds=600)

    backend = _FakeBackend(adopt_on_conflict=adopt_on_conflict, discoverable=discoverable)
    provider._backend = backend

    paths = Paths(base_dir=tmp_path / "state")
    monkeypatch.setattr(aio, "get_paths", lambda: paths)
    monkeypatch.setattr(aio, "get_effective_user_id", lambda: None)
    monkeypatch.setattr(aio, "wait_for_sandbox_ready", lambda _url, timeout=60, **_kw: True)

    async def _ready_async(_url, timeout=60, **_kw):
        return True

    monkeypatch.setattr(aio, "wait_for_sandbox_ready_async", _ready_async)
    monkeypatch.setattr(
        aio,
        "get_app_config",
        lambda: SimpleNamespace(skills=SimpleNamespace(container_path="/mnt/skills")),
    )
    monkeypatch.setattr(aio.AioSandboxProvider, "_get_extra_mounts", lambda *_a, **_k: [])
    monkeypatch.setattr(aio.AioSandboxProvider, "_lark_integration_active", lambda *_a, **_k: False)
    monkeypatch.setattr(aio.AioSandboxProvider, "_lark_broker_active", lambda *_a, **_k: False)
    monkeypatch.setattr(aio.AioSandboxProvider, "_local_config_mount_exclusion_root", lambda *_a, **_k: None)
    monkeypatch.setattr(aio.AioSandboxProvider, "_ensure_skills_projection", staticmethod(lambda _user_id: None))
    return provider, backend


def _binding(snapshot_id: str | None = None, run_id: str = "run-1") -> AcceptedSkillSandboxBindingV1:
    return AcceptedSkillSandboxBindingV1(snapshot_id=snapshot_id, run_id=run_id, generation=0)


def _acquire_accepted(provider, thread_id: str, *, run_id: str = "run-1") -> str:
    return provider._acquire_accepted_skills_internal(
        thread_id,
        user_id=ACCEPTED_USER,
        binding=_binding(run_id=run_id),
    )


# ── The repair ───────────────────────────────────────────────────────────


def test_compatible_accepted_followup_reuses_the_parked_resource(tmp_path, monkeypatch):
    """The second turn on one thread must not create or tear anything down."""
    provider, backend = _make_provider(tmp_path, monkeypatch)

    first = _acquire_accepted(provider, "thread-follow")
    assert backend.created == [first]
    provider.release(first)
    assert first in provider._warm_pool

    second = _acquire_accepted(provider, "thread-follow", run_id="run-2")

    assert second == first
    assert backend.created == [first], "a compatible follow-up must not create a container"
    assert backend.destroyed == [], "a compatible follow-up must not tear anything down"
    assert provider._thread_sandboxes[(ACCEPTED_USER, "thread-follow")] == first
    assert first not in provider._warm_pool


def test_compatible_accepted_followup_at_capacity_spares_the_unrelated_thread(tmp_path, monkeypatch):
    """At capacity the follow-up must not evict a different thread's container.

    This is the observed twenty seconds: two slots, both parked, and the
    acquisition stopped the older *unrelated* sandbox on its way to rediscovering
    its own.
    """
    provider, backend = _make_provider(tmp_path, monkeypatch, replicas=2)

    mine = _acquire_accepted(provider, "thread-mine")
    provider.release(mine)
    other = _acquire_accepted(provider, "thread-other")
    provider.release(other)
    assert set(provider._warm_pool) == {mine, other}

    again = _acquire_accepted(provider, "thread-mine", run_id="run-2")

    assert again == mine
    assert backend.destroyed == [], "an unrelated parked container must survive a compatible follow-up"
    assert backend.created == [mine, other]
    assert other in provider._warm_pool


def test_accepted_aba_thread_switching_preserves_both_resource_identities(tmp_path, monkeypatch):
    """A, B, then A again: each thread gets its own container back, once."""
    provider, backend = _make_provider(tmp_path, monkeypatch, replicas=2)

    first_a = _acquire_accepted(provider, "thread-a")
    provider.release(first_a)
    first_b = _acquire_accepted(provider, "thread-b")
    provider.release(first_b)
    second_a = _acquire_accepted(provider, "thread-a", run_id="run-2")
    provider.release(second_a)
    second_b = _acquire_accepted(provider, "thread-b", run_id="run-2")

    assert (second_a, second_b) == (first_a, first_b)
    assert backend.created == [first_a, first_b], "A/B/A must create exactly one container per thread"
    assert backend.destroyed == []


def test_ordinary_acquisition_still_reclaims_across_thread_switching(tmp_path, monkeypatch):
    """The ordinary path's own warm reclaim is preserved, sync entry point."""
    provider, backend = _make_provider(tmp_path, monkeypatch, replicas=2)

    first = provider.acquire("thread-ordinary", user_id=ACCEPTED_USER)
    provider.release(first)
    other = provider.acquire("thread-ordinary-2", user_id=ACCEPTED_USER)
    provider.release(other)
    again = provider.acquire("thread-ordinary", user_id=ACCEPTED_USER)

    assert again == first
    assert backend.created == [first, other]
    assert backend.destroyed == []


@pytest.mark.anyio
async def test_ordinary_async_acquisition_still_reclaims(tmp_path, monkeypatch):
    """The async entry point reuses the same warm entry, without blocking."""
    provider, backend = _make_provider(tmp_path, monkeypatch, replicas=2)

    first = await provider.acquire_async("thread-async", user_id=ACCEPTED_USER)
    provider.release(first)
    again = await provider.acquire_async("thread-async", user_id=ACCEPTED_USER)

    assert again == first
    assert backend.created == [first]
    assert backend.destroyed == []


def test_a_genuinely_new_third_resource_still_pays_for_its_eviction(tmp_path, monkeypatch):
    """Capacity is still capacity: a third thread evicts, and it is recorded.

    The repair removes unnecessary teardown, not necessary teardown. Hiding
    this wait behind an overlapping unbudgeted container is the thing the
    separately tracked capacity defect already does; nothing here helps it.
    """
    provider, backend = _make_provider(tmp_path, monkeypatch, replicas=2)

    first = _acquire_accepted(provider, "thread-1")
    provider.release(first)
    second = _acquire_accepted(provider, "thread-2")
    provider.release(second)

    with turn_phases(correlation_id="third") as journal:
        third = _acquire_accepted(provider, "thread-3")

    assert third not in (first, second)
    assert backend.destroyed == [first], "the oldest unrelated parked container pays for the new one"
    snapshot = journal.snapshot()
    assert snapshot.evictions == 1
    assert snapshot.resource_creates == 1
    assert snapshot.resource_teardowns == 1
    assert snapshot.acquisition_source is AcquisitionSource.CREATED


def test_eviction_never_stops_the_container_it_is_making_room_for(tmp_path, monkeypatch):
    """Evicting the create target would buy a cold start with a teardown."""
    provider, backend = _make_provider(tmp_path, monkeypatch, replicas=1)
    aio = _aio_mod()
    provider._warm_pool["sandbox-target"] = (
        aio.SandboxInfo(sandbox_id="sandbox-target", sandbox_url="http://sandbox/target"),
        1.0,
    )

    assert provider._evict_oldest_warm(exclude="sandbox-target") is None
    assert "sandbox-target" in provider._warm_pool
    assert backend.destroyed == []


# ── Refusals stay refusals ───────────────────────────────────────────────


def test_parked_container_that_failed_its_health_check_is_not_reused(tmp_path, monkeypatch):
    provider, backend = _make_provider(tmp_path, monkeypatch)

    first = _acquire_accepted(provider, "thread-dead")
    provider.release(first)
    backend.alive[first] = False

    second = _acquire_accepted(provider, "thread-dead", run_id="run-2")

    assert backend.destroyed == [first], "a dead parked container is dropped, not handed out"
    assert backend.created == [first, second]


def test_parked_container_being_torn_down_is_not_reused(tmp_path, monkeypatch):
    provider, _backend = _make_provider(tmp_path, monkeypatch)

    first = _acquire_accepted(provider, "thread-teardown")
    provider.release(first)
    provider._local_teardown.add(first)

    reclaimed = provider._reclaim_accepted_warm_sandbox(
        "thread-teardown",
        first,
        user_id=ACCEPTED_USER,
        fingerprint=provider._accepted_reuse_fingerprints[first],
    )

    assert reclaimed is None


def test_parked_container_lost_to_a_peer_is_not_reused(tmp_path, monkeypatch):
    provider, _backend = _make_provider(tmp_path, monkeypatch)
    aio = _aio_mod()

    first = _acquire_accepted(provider, "thread-lost")
    provider.release(first)

    def _refuse(sandbox_id):
        raise aio.SandboxBeingDestroyedError(sandbox_id)

    monkeypatch.setattr(provider, "_publish_ownership", _refuse)

    reclaimed = provider._reclaim_accepted_warm_sandbox(
        "thread-lost",
        first,
        user_id=ACCEPTED_USER,
        fingerprint=provider._accepted_reuse_fingerprints[first],
    )

    assert reclaimed is None


def test_a_container_this_process_did_not_provision_as_accepted_is_never_reused(tmp_path, monkeypatch):
    """An accepted-suffixed id alone is not evidence that reuse is valid."""
    provider, _backend = _make_provider(tmp_path, monkeypatch)

    first = _acquire_accepted(provider, "thread-untracked")
    provider.release(first)
    provider._accepted_only_sandbox_ids.discard(first)

    reclaimed = provider._reclaim_accepted_warm_sandbox(
        "thread-untracked",
        first,
        user_id=ACCEPTED_USER,
        fingerprint=provider._accepted_reuse_fingerprints.get(first, ""),
    )

    assert reclaimed is None


def test_changed_create_time_inputs_refuse_reuse_without_destroying_anything(tmp_path, monkeypatch):
    provider, backend = _make_provider(tmp_path, monkeypatch)

    first = _acquire_accepted(provider, "thread-changed")
    provider.release(first)

    reclaimed = provider._reclaim_accepted_warm_sandbox(
        "thread-changed",
        first,
        user_id=ACCEPTED_USER,
        fingerprint="f" * 64,
    )

    assert reclaimed is None
    assert backend.destroyed == [], "refusing to reuse is never a reason to tear a container down"
    assert first in provider._warm_pool


def test_no_cross_user_reuse_of_a_parked_accepted_container(tmp_path, monkeypatch):
    """A parked id whose tenant changed is a collision, not a reuse."""
    provider, _backend = _make_provider(tmp_path, monkeypatch)
    aio = _aio_mod()

    first = _acquire_accepted(provider, "thread-tenant")
    provider.release(first)

    with pytest.raises(aio.SandboxIdentityCollisionError):
        provider._reclaim_accepted_warm_sandbox(
            "thread-tenant",
            first,
            user_id="a-different-user",
            fingerprint=provider._accepted_reuse_fingerprints[first],
        )


def test_an_active_non_accepted_sandbox_still_refuses_the_accepted_path(tmp_path, monkeypatch):
    """Isolation is unchanged: an ordinary active container cannot be adopted."""
    provider, _backend = _make_provider(tmp_path, monkeypatch)
    from deerflow.sandbox.accepted_material import AcceptedSkillSandboxBindingError

    ordinary = provider.acquire("thread-mixed", user_id=ACCEPTED_USER)
    assert ordinary not in provider._accepted_only_sandbox_ids

    with pytest.raises(AcceptedSkillSandboxBindingError, match="accepted_skill_snapshot_isolation_conflict"):
        _acquire_accepted(provider, "thread-mixed")


def test_the_ordinary_path_never_reclaims_an_accepted_container(tmp_path, monkeypatch):
    """Ordinary and accepted ids are disjoint, so the pools cannot cross."""
    provider, backend = _make_provider(tmp_path, monkeypatch, replicas=4)

    accepted = _acquire_accepted(provider, "thread-split")
    provider.release(accepted)

    ordinary = provider.acquire("thread-split", user_id=ACCEPTED_USER)

    assert ordinary != accepted
    assert accepted in provider._warm_pool
    assert backend.created == [accepted, ordinary]


def test_concurrent_compatible_followups_create_one_resource(tmp_path, monkeypatch):
    """Serialized acquisition: racing callers share the reclaimed container."""
    provider, backend = _make_provider(tmp_path, monkeypatch)

    first = _acquire_accepted(provider, "thread-race")
    provider.release(first)

    results: list[str] = []
    errors: list[BaseException] = []
    barrier = threading.Barrier(4)

    def _worker() -> None:
        try:
            barrier.wait(timeout=5)
            results.append(_acquire_accepted(provider, "thread-race", run_id="run-2"))
        except BaseException as exc:  # noqa: BLE001 - surfaced by the assertion below
            errors.append(exc)

    workers = [threading.Thread(target=_worker) for _ in range(4)]
    for worker in workers:
        worker.start()
    for worker in workers:
        worker.join(timeout=10)

    assert errors == []
    assert results == [first] * 4
    assert backend.created == [first]
    assert backend.destroyed == []


# ── Phase evidence along the real acquisition path ───────────────────────


def test_a_reused_resource_is_recorded_as_a_reuse_not_a_create(tmp_path, monkeypatch):
    provider, _backend = _make_provider(tmp_path, monkeypatch)

    first = _acquire_accepted(provider, "thread-phases")
    provider.release(first)

    with turn_phases(correlation_id="reuse", run_id="run-reuse") as journal:
        _acquire_accepted(provider, "thread-phases", run_id="run-2")

    snapshot = journal.snapshot()
    assert snapshot.acquisition_source is AcquisitionSource.ACCEPTED_WARM_RECLAIM
    assert snapshot.acquisition_source.reuses_existing_resource
    assert snapshot.resource_creates == 0
    assert snapshot.resource_teardowns == 0
    assert snapshot.evictions == 0
    assert snapshot.phase_ms(TurnPhase.SANDBOX_CREATE) is None
    assert snapshot.phase_ms(TurnPhase.SANDBOX_READINESS) is None


def test_a_cold_acquisition_times_creation_and_readiness_separately(tmp_path, monkeypatch):
    provider, _backend = _make_provider(tmp_path, monkeypatch)

    with turn_phases(correlation_id="cold", run_id="run-cold") as journal:
        _acquire_accepted(provider, "thread-cold")

    snapshot = journal.snapshot()
    assert snapshot.acquisition_source is AcquisitionSource.CREATED
    assert not snapshot.acquisition_source.reuses_existing_resource
    assert snapshot.phase_ms(TurnPhase.SANDBOX_CREATE) is not None
    assert snapshot.phase_ms(TurnPhase.SANDBOX_READINESS) is not None
    assert snapshot.resource_creates == 1


def test_acquisition_records_nothing_when_no_journal_is_bound(tmp_path, monkeypatch):
    """Instrumentation must be inert outside a measured turn."""
    provider, backend = _make_provider(tmp_path, monkeypatch)

    sandbox_id = _acquire_accepted(provider, "thread-unmeasured")

    assert backend.created == [sandbox_id]


def test_compatible_warm_acquire_is_cheap_against_a_fake_backend(tmp_path, monkeypatch):
    """An exploratory sample of the application cost of a compatible reuse.

    This is not a p95 qualification and must never be reported as one. The
    backend is a fake, so the number here is the provider's own bookkeeping --
    identity checks, the fingerprint, the ownership round trip against the
    in-memory store -- with none of the container work a restricted-runsc host
    actually does. Qualifying the one-second target belongs to a measurement on
    the supported profile, with its own trials and failures recorded.
    """
    provider, backend = _make_provider(tmp_path, monkeypatch)
    trials = 50

    first = _acquire_accepted(provider, "thread-sample")
    durations_ms: list[float] = []
    failures = 0
    for index in range(trials):
        provider.release(first)
        started = time.perf_counter()
        try:
            reused = _acquire_accepted(provider, "thread-sample", run_id=f"run-{index}")
        except Exception:  # noqa: BLE001 - counted, then surfaced by the assertion
            failures += 1
            continue
        durations_ms.append((time.perf_counter() - started) * 1000.0)
        assert reused == first

    assert failures == 0, f"{failures} of {trials} compatible reuses failed"
    assert len(durations_ms) == trials
    assert backend.created == [first], "every trial after the first must reuse"
    assert backend.destroyed == []
    ordered = sorted(durations_ms)
    p50_ms = ordered[int(round(0.50 * (len(ordered) - 1)))]
    p95_ms = ordered[int(round(0.95 * (len(ordered) - 1)))]
    print(f"warm-acquire fake-backend sample: trials={trials} failures={failures} p50_ms={p50_ms:.3f} p95_ms={p95_ms:.3f} max_ms={ordered[-1]:.3f}")
    # An in-process bookkeeping bound only; the one-second target is a property
    # of the supported profile and is not measured here.
    assert p95_ms < 1000.0, f"in-process bookkeeping p95 was {p95_ms:.2f}ms over {trials} trials"


# ── Panel-review additions ───────────────────────────────────────────────


@pytest.mark.anyio
async def test_cancellation_after_a_reclaim_parks_the_container_instead_of_destroying_it(tmp_path, monkeypatch):
    """A cancelled follow-up must not turn a reclaim into next turn's cold start.

    The cancellation handler used to assume the only thing the accepted path
    could hand back was a just-created container to roll back. After the
    repair it can hand back one parked before the call; the caller never
    received it, so it goes back to the warm pool under the same identity.
    """
    provider, backend = _make_provider(tmp_path, monkeypatch)
    first = _acquire_accepted(provider, "thread-cancel")
    provider.release(first)

    reclaimed = threading.Event()
    allow_bind = threading.Event()

    def _bind(*_args, **_kwargs):
        reclaimed.set()
        assert allow_bind.wait(timeout=5)

    monkeypatch.setattr(provider, "bind_accepted_skill_snapshot", _bind)

    task = asyncio.create_task(
        provider.provision_accepted_skills_async("thread-cancel", user_id=ACCEPTED_USER, binding=_binding(run_id="run-2")),
    )
    assert await asyncio.to_thread(reclaimed.wait, 5)
    task.cancel("caller went away after the reclaim")
    allow_bind.set()
    with pytest.raises(asyncio.CancelledError):
        await task

    assert backend.destroyed == [], "a reclaimed container is parked again, never destroyed"
    assert first in provider._warm_pool
    assert first in provider._accepted_reuse_fingerprints
    again = _acquire_accepted(provider, "thread-cancel", run_id="run-3")
    assert again == first
    assert backend.created == [first]


@pytest.mark.anyio
async def test_cancellation_after_a_create_still_rolls_the_container_back(tmp_path, monkeypatch):
    provider, backend = _make_provider(tmp_path, monkeypatch)
    created = threading.Event()
    allow_bind = threading.Event()

    def _bind(*_args, **_kwargs):
        created.set()
        assert allow_bind.wait(timeout=5)

    monkeypatch.setattr(provider, "bind_accepted_skill_snapshot", _bind)

    with turn_phases(correlation_id="cancel-create") as journal:
        task = asyncio.create_task(
            provider.provision_accepted_skills_async("thread-cancel-create", user_id=ACCEPTED_USER, binding=_binding()),
        )
        assert await asyncio.to_thread(created.wait, 5)
        task.cancel("caller went away after the create")
        allow_bind.set()
        with pytest.raises(asyncio.CancelledError):
            await task

    assert backend.destroyed == backend.created
    assert provider._warm_pool == {}
    assert journal.snapshot().resource_teardowns == 1


def test_local_backend_same_id_input_drift_replaces_our_own_parked_container(tmp_path, monkeypatch):
    """On the local backend a same-id mismatch is repaired by fenced replacement.

    The refusal leaves the parked container running under its deterministic
    name, and a create would collide with it and blindly adopt the old mounts.
    So our own parked entry is stopped under the ordinary teardown fences
    first, then the container is rebuilt with the requested inputs. Nothing
    is adopted, nothing unrelated is touched, and the source label is
    ``created`` because the backend really did start one.
    """
    aio = _aio_mod()
    provider, backend = _make_provider(tmp_path, monkeypatch, adopt_on_conflict=True)

    first = _acquire_accepted(provider, "thread-drift")
    provider.release(first)
    # The one create-time input that can differ for the same id locally.
    monkeypatch.setattr(aio.AioSandboxProvider, "_lark_integration_active", lambda *_a, **_k: True)

    with turn_phases(correlation_id="drift") as journal:
        again = _acquire_accepted(provider, "thread-drift", run_id="run-2")

    assert again == first
    assert backend.adopted == []
    assert backend.created == [first, first], "rebuilt with the new inputs"
    assert backend.destroyed == [first], "our own parked container, and only it"
    assert first not in provider._warm_pool
    assert provider._thread_sandboxes[(ACCEPTED_USER, "thread-drift")] == first
    assert journal.snapshot().acquisition_source is AcquisitionSource.CREATED


def test_an_unverifiable_parked_container_is_reused_under_the_existing_unknown_rule(tmp_path, monkeypatch):
    """Backend health-check failures are unknown, not dead — the warm pool's rule."""
    provider, backend = _make_provider(tmp_path, monkeypatch)

    first = _acquire_accepted(provider, "thread-unverifiable")
    provider.release(first)
    backend.unverifiable.add(first)

    again = _acquire_accepted(provider, "thread-unverifiable", run_id="run-2")

    assert again == first
    assert backend.created == [first]
    assert backend.destroyed == []


def test_an_eviction_is_a_timed_phase_and_a_lookup_is_too(tmp_path, monkeypatch):
    provider, _backend = _make_provider(tmp_path, monkeypatch, replicas=2)
    first = _acquire_accepted(provider, "thread-1")
    provider.release(first)
    second = _acquire_accepted(provider, "thread-2")
    provider.release(second)

    with turn_phases(correlation_id="evict") as evicting:
        _acquire_accepted(provider, "thread-3")
    with turn_phases(correlation_id="lookup") as looking:
        _acquire_accepted(provider, "thread-2", run_id="run-2")

    assert evicting.snapshot().phase_ms(TurnPhase.SANDBOX_EVICTION) is not None
    assert evicting.snapshot().phase_ms(TurnPhase.SANDBOX_LOOKUP) is not None
    assert looking.snapshot().phase_ms(TurnPhase.SANDBOX_LOOKUP) is not None
    assert looking.snapshot().phase_ms(TurnPhase.SANDBOX_EVICTION) is None


def test_serializer_wait_is_recorded_as_queue_time(tmp_path, monkeypatch):
    provider, _backend = _make_provider(tmp_path, monkeypatch)
    real_hold = provider._acquire_serializer.hold

    @contextlib.contextmanager
    def _slow_hold(key):
        time.sleep(0.02)
        with real_hold(key):
            yield

    monkeypatch.setattr(provider._acquire_serializer, "hold", _slow_hold)

    with turn_phases(correlation_id="queue") as journal:
        _acquire_accepted(provider, "thread-queue")

    assert journal.snapshot().queue_ms >= 15.0


def test_a_readiness_timeout_counts_as_a_failed_attempt(tmp_path, monkeypatch):
    aio = _aio_mod()
    provider, backend = _make_provider(tmp_path, monkeypatch)
    monkeypatch.setattr(aio, "wait_for_sandbox_ready", lambda _url, timeout=60, **_kw: False)

    with turn_phases(correlation_id="unready") as journal, pytest.raises(RuntimeError, match="failed to become ready"):
        _acquire_accepted(provider, "thread-unready")

    snapshot = journal.snapshot()
    assert snapshot.failed_attempts == 1
    assert snapshot.resource_creates == 1
    assert backend.destroyed == backend.created


def test_dropping_a_dead_parked_container_counts_as_a_teardown(tmp_path, monkeypatch):
    provider, backend = _make_provider(tmp_path, monkeypatch)
    first = _acquire_accepted(provider, "thread-dead-count")
    provider.release(first)
    backend.alive[first] = False

    with turn_phases(correlation_id="dead") as journal:
        _acquire_accepted(provider, "thread-dead-count", run_id="run-2")

    assert journal.snapshot().resource_teardowns == 1
    assert backend.destroyed == [first]
