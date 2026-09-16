"""Provenance must reflect what happened, not which method ran.

PR #73 made the compatible follow-up reclaim its parked container. It left one
gap: whenever the accepted path reached ``_create_sandbox`` and the backend
answered with a container that already existed under the deterministic name,
the acquisition was labelled ``created``, and that label chose destructive
cleanup on cancellation and counted a create that never happened. This file
pins the repair from the outside in:

* the backend says whether ``create`` started or found the resource
  (``SandboxInfo.provenance``), and the provider believes only that word;
* cancellation undoes this invocation's acquisition by origin: a container the
  backend confirms it started is destroyed, anything that existed before the
  call (reclaimed, rediscovered, unknown) is parked again, an active one is
  left to its holders;
* a same-id mismatch of our *own* parked container is repaired by replacing it
  under the ordinary teardown fences (identity-fenced like every promote
  path), never by an unrelated eviction and never by silently adopting the
  old mounts; a refused replacement refuses the acquisition rather than
  taking the container over from whoever holds it;
* a foreign container adopted at capacity is a third tracked set and pays the
  ordinary soft-cap eviction before create, with its wait visible.

Every backend here is a fake with exact call records: these are lifecycle and
accounting invariants, not restricted-runsc timings. The "Lifecycle" tests
below assert only on backend counters and provider state, so on PR #73's
source they fail on behaviour, not on a missing symbol; the "Diagnostics"
tests assert on the journal's new counters and are the ones that fail by
``AttributeError`` there.
"""

from __future__ import annotations

import asyncio
import threading

import pytest
from test_sandbox_warm_reuse_latency import ACCEPTED_USER, _acquire_accepted, _aio_mod, _binding, _make_provider

from deerflow.community.aio_sandbox.aio_sandbox_provider import SandboxIdentityCollisionError
from deerflow.runtime.turn_phases import AcquisitionSource, TurnPhase, turn_phases
from deerflow.sandbox.accepted_material import AcceptedSkillSandboxBindingError


def _drift_inputs(monkeypatch) -> None:
    """Change the one create-time input that can differ for the same id locally."""
    aio = _aio_mod()
    monkeypatch.setattr(aio.AioSandboxProvider, "_lark_integration_active", lambda *_a, **_k: True)


def _hold_binding(provider, monkeypatch) -> tuple[threading.Event, threading.Event]:
    """Park the worker after acquisition, inside ``bind_accepted_skill_snapshot``."""
    acquired = threading.Event()
    allow = threading.Event()

    def _bind(*_args, **_kwargs):
        acquired.set()
        assert allow.wait(timeout=5)

    monkeypatch.setattr(provider, "bind_accepted_skill_snapshot", _bind)
    return acquired, allow


def _seed_foreign_container(backend, provider, thread_id: str) -> str:
    """A running container under this thread's accepted id that this process never tracked.

    What another Gateway process leaves behind: the backend knows it, our
    maps do not.
    """
    sandbox_id = f"{provider._sandbox_id_for_thread(thread_id, ACCEPTED_USER)}-accepted"
    backend.alive[sandbox_id] = True
    backend.infos[sandbox_id] = backend._unused_info(sandbox_id)
    return sandbox_id


# ── Lifecycle ────────────────────────────────────────────────────────────


@pytest.mark.anyio
async def test_cancellation_after_a_rediscovery_parks_the_pre_existing_container(tmp_path, monkeypatch):
    """The brief's counterexample, on a backend that found rather than started.

    The caller never received the container, and this call did not create it,
    so cancellation must leave it running and parked -- exactly what happens
    after a reclaim -- rather than destroy it because ``_create_sandbox`` was
    the method that returned it.
    """
    provider, backend = _make_provider(tmp_path, monkeypatch, adopt_on_conflict=True)
    foreign = _seed_foreign_container(backend, provider, "thread-foreign")
    acquired, allow = _hold_binding(provider, monkeypatch)

    task = asyncio.create_task(
        provider.provision_accepted_skills_async("thread-foreign", user_id=ACCEPTED_USER, binding=_binding(run_id="run-2")),
    )
    assert await asyncio.to_thread(acquired.wait, 5)
    task.cancel("caller went away after the rediscovery")
    allow.set()
    with pytest.raises(asyncio.CancelledError):
        await task

    assert backend.adopted == [foreign]
    assert backend.created == [], "no new container was started"
    assert backend.destroyed == [], "a rediscovered container is parked again, never destroyed"
    assert foreign in provider._warm_pool, "parked under this process's ownership"
    assert foreign not in provider._sandboxes


def test_a_refused_replacement_refuses_the_acquisition(tmp_path, monkeypatch):
    """Our own drifted container that the fences would not let us replace.

    Replacement is refused (a peer holds the lease). The brief allows refuse
    or fenced replacement, never a third thing: create would find the
    container under its name, take the lease over from the very peer whose
    fence just refused the stop, and hand the turn a container whose inputs
    are known to differ. So the acquisition fails closed; nothing is
    destroyed, nothing adopted, and the recorded inputs stay the true ones.
    """
    provider, backend = _make_provider(tmp_path, monkeypatch, adopt_on_conflict=True)
    first = _acquire_accepted(provider, "thread-held")
    provider.release(first)
    recorded = provider._accepted_reuse_fingerprints[first]
    _drift_inputs(monkeypatch)
    # A peer owns the lease: every ownership-fenced destroy refuses.
    monkeypatch.setattr(provider, "_claim_ownership", lambda *_a, **_k: False)

    with pytest.raises(AcceptedSkillSandboxBindingError, match="accepted_sandbox_inputs_changed"):
        _acquire_accepted(provider, "thread-held", run_id="run-2")

    assert backend.destroyed == []
    assert backend.created == [first]
    assert backend.adopted == []
    assert first in provider._warm_pool
    assert provider._accepted_reuse_fingerprints[first] == recorded, "the request's inputs are not written over the container's true ones"


@pytest.mark.anyio
async def test_cancellation_after_a_confirmed_create_still_rolls_it_back(tmp_path, monkeypatch):
    """Legitimate rollback is preserved: the backend said it started this one."""
    provider, backend = _make_provider(tmp_path, monkeypatch)
    acquired, allow = _hold_binding(provider, monkeypatch)

    task = asyncio.create_task(
        provider.provision_accepted_skills_async("thread-fresh", user_id=ACCEPTED_USER, binding=_binding()),
    )
    assert await asyncio.to_thread(acquired.wait, 5)
    task.cancel("caller went away after the create")
    allow.set()
    with pytest.raises(asyncio.CancelledError):
        await task

    assert backend.destroyed == backend.created
    assert provider._warm_pool == {}
    assert provider._sandboxes == {}


@pytest.mark.anyio
async def test_cancellation_after_an_already_active_acquisition_leaves_it_to_its_holder(tmp_path, monkeypatch):
    provider, backend = _make_provider(tmp_path, monkeypatch)
    first = _acquire_accepted(provider, "thread-active")
    acquired, allow = _hold_binding(provider, monkeypatch)

    task = asyncio.create_task(
        provider.provision_accepted_skills_async("thread-active", user_id=ACCEPTED_USER, binding=_binding(run_id="run-2")),
    )
    assert await asyncio.to_thread(acquired.wait, 5)
    task.cancel("second caller went away")
    allow.set()
    with pytest.raises(asyncio.CancelledError):
        await task

    assert backend.destroyed == []
    assert first in provider._sandboxes, "still active for the holder that made it active"
    assert first not in provider._warm_pool


@pytest.mark.anyio
async def test_cleanup_is_awaited_through_repeated_cancellation(tmp_path, monkeypatch):
    """A second cancel during cleanup must not detach it or strand the container."""
    provider, backend = _make_provider(tmp_path, monkeypatch, adopt_on_conflict=True)
    foreign = _seed_foreign_container(backend, provider, "thread-twice")
    acquired, allow = _hold_binding(provider, monkeypatch)
    release_started = threading.Event()
    release_may_finish = threading.Event()
    real_release = provider.release

    def _slow_release(sandbox_id):
        release_started.set()
        assert release_may_finish.wait(timeout=5)
        real_release(sandbox_id)

    monkeypatch.setattr(provider, "release", _slow_release)

    task = asyncio.create_task(
        provider.provision_accepted_skills_async("thread-twice", user_id=ACCEPTED_USER, binding=_binding(run_id="run-2")),
    )
    assert await asyncio.to_thread(acquired.wait, 5)
    task.cancel("first")
    allow.set()
    assert await asyncio.to_thread(release_started.wait, 5)
    task.cancel("second, while cleanup is in flight")
    release_may_finish.set()
    with pytest.raises(asyncio.CancelledError):
        await task

    assert foreign in provider._warm_pool, "cleanup ran to completion despite the second cancel"
    assert backend.destroyed == []


def test_same_id_input_drift_at_capacity_replaces_our_own_and_spares_the_unrelated_thread(tmp_path, monkeypatch):
    """The brief's second counterexample: two parked, drift on the first.

    Replacing our own parked container frees its own slot, so the unrelated
    thread's container is never stopped to make room, and the rebuilt
    container is a real creation.
    """
    provider, backend = _make_provider(tmp_path, monkeypatch, replicas=2, adopt_on_conflict=True)
    a = _acquire_accepted(provider, "thread-a")
    provider.release(a)
    b = _acquire_accepted(provider, "thread-b")
    provider.release(b)
    _drift_inputs(monkeypatch)

    again = _acquire_accepted(provider, "thread-a", run_id="run-2")

    assert again == a
    assert backend.adopted == []
    assert backend.destroyed == [a], "our own drifted container, never the unrelated one"
    assert backend.created == [a, b, a]
    assert b in provider._warm_pool
    assert a in provider._sandboxes


def test_a_foreign_container_adopted_at_capacity_is_a_third_tracked_set_and_the_cap_still_holds(tmp_path, monkeypatch):
    """A foreign container adopted at capacity consumes a tracked slot like any create.

    It is not our own parked entry (that case never reaches create: it is
    replaced or the acquisition refused), so the soft cap evicts the oldest
    unrelated warm entry before create exactly as for a new container, the
    wait stays visible, and no overshoot is kept.
    """
    provider, backend = _make_provider(tmp_path, monkeypatch, replicas=2, adopt_on_conflict=True, discoverable=True)
    a = _acquire_accepted(provider, "thread-a")
    provider.release(a)
    b = _acquire_accepted(provider, "thread-b")
    provider.release(b)
    foreign = _seed_foreign_container(backend, provider, "thread-c")

    with turn_phases(correlation_id="foreign-at-capacity") as journal:
        got = _acquire_accepted(provider, "thread-c")

    assert got == foreign
    assert backend.adopted == [foreign]
    assert backend.created == [a, b]
    assert backend.destroyed == [a], "the oldest unrelated warm entry, once, before create"
    replicas, total = provider._replica_count()
    assert total <= replicas
    snapshot = journal.snapshot()
    assert snapshot.acquisition_source is AcquisitionSource.REDISCOVERED
    assert snapshot.resource_creates == 0
    assert snapshot.resource_rediscoveries == 1
    assert snapshot.evictions == 1
    assert snapshot.phase_ms(TurnPhase.SANDBOX_EVICTION) is not None


def test_a_genuinely_new_third_resource_still_evicts_and_its_wait_stays_visible(tmp_path, monkeypatch):
    provider, backend = _make_provider(tmp_path, monkeypatch, replicas=2, discoverable=True)
    a = _acquire_accepted(provider, "thread-a")
    provider.release(a)
    b = _acquire_accepted(provider, "thread-b")
    provider.release(b)

    with turn_phases(correlation_id="third") as journal:
        c = _acquire_accepted(provider, "thread-c")

    assert backend.destroyed == [a]
    assert backend.created == [a, b, c]
    assert journal.snapshot().phase_ms(TurnPhase.SANDBOX_EVICTION) is not None, "the real wait stays visible"


def test_rediscovery_does_not_record_the_request_inputs_as_the_containers_own(tmp_path, monkeypatch):
    """A rediscovered container's create-time inputs are unknown to this process.

    Recording the request's fingerprint would let the next turn reclaim a
    container whose mounts nobody verified. Without a recorded fingerprint the
    next turn treats it as changed and replaces it -- one honest cold start
    rather than an unverified reuse.
    """
    provider, backend = _make_provider(tmp_path, monkeypatch, adopt_on_conflict=True)
    foreign = _seed_foreign_container(backend, provider, "thread-foreign")

    got = _acquire_accepted(provider, "thread-foreign")
    assert got == foreign
    assert foreign not in provider._accepted_reuse_fingerprints
    provider.release(got)

    again = _acquire_accepted(provider, "thread-foreign", run_id="run-2")

    assert again == foreign
    assert backend.destroyed == [foreign], "replaced under the fences rather than reused unverified"
    assert backend.created == [foreign], "and rebuilt with inputs this process now knows"
    assert foreign in provider._accepted_reuse_fingerprints


def test_compatible_aba_switching_stays_free_of_creates_and_unrelated_teardowns(tmp_path, monkeypatch):
    provider, backend = _make_provider(tmp_path, monkeypatch, replicas=2, adopt_on_conflict=True, discoverable=True)
    a = _acquire_accepted(provider, "thread-a")
    provider.release(a)
    b = _acquire_accepted(provider, "thread-b")
    provider.release(b)

    for thread_id, expected in (("thread-a", a), ("thread-b", b), ("thread-a", a)):
        got = _acquire_accepted(provider, thread_id, run_id=f"run-{thread_id}")
        assert got == expected
        provider.release(got)

    assert backend.created == [a, b]
    assert backend.adopted == []
    assert backend.destroyed == []


def test_the_ordinary_sync_path_labels_a_backend_rediscovery_as_such(tmp_path, monkeypatch):
    """The ordinary path reaches create only after its own discovery layer missed.

    A name conflict the backend resolves by adoption is still a rediscovery,
    on the sync caller.
    """
    provider, backend = _make_provider(tmp_path, monkeypatch, adopt_on_conflict=True)
    sandbox_id = provider._sandbox_id_for_thread("thread-ordinary", "user-1")
    backend.alive[sandbox_id] = True
    backend.infos[sandbox_id] = backend._unused_info(sandbox_id)

    with turn_phases(correlation_id="ordinary-sync") as journal:
        got = provider.acquire("thread-ordinary", user_id="user-1")

    assert got == sandbox_id
    assert backend.adopted == [sandbox_id]
    assert backend.created == []
    assert journal.snapshot().acquisition_source is AcquisitionSource.REDISCOVERED


@pytest.mark.anyio
async def test_the_ordinary_async_path_labels_a_backend_rediscovery_as_such(tmp_path, monkeypatch):
    provider, backend = _make_provider(tmp_path, monkeypatch, adopt_on_conflict=True)
    sandbox_id = provider._sandbox_id_for_thread("thread-ordinary-async", "user-1")
    backend.alive[sandbox_id] = True
    backend.infos[sandbox_id] = backend._unused_info(sandbox_id)

    with turn_phases(correlation_id="ordinary-async") as journal:
        got = await provider.acquire_async("thread-ordinary-async", user_id="user-1")

    assert got == sandbox_id
    assert backend.adopted == [sandbox_id]
    assert backend.created == []
    assert journal.snapshot().acquisition_source is AcquisitionSource.REDISCOVERED


def test_concurrent_holders_of_a_rediscovered_container_get_one_adoption(tmp_path, monkeypatch):
    provider, backend = _make_provider(tmp_path, monkeypatch, adopt_on_conflict=True)
    foreign = _seed_foreign_container(backend, provider, "thread-shared")
    results: list[str] = []
    errors: list[BaseException] = []

    def _worker(run_id: str) -> None:
        try:
            results.append(_acquire_accepted(provider, "thread-shared", run_id=run_id))
        except BaseException as exc:  # noqa: BLE001
            errors.append(exc)

    workers = [threading.Thread(target=_worker, args=(f"run-{index}",)) for index in range(4)]
    for worker in workers:
        worker.start()
    for worker in workers:
        worker.join(timeout=10)

    assert errors == []
    assert results == [foreign] * 4
    assert backend.adopted == [foreign]
    assert backend.created == []
    assert backend.destroyed == []


def test_a_parked_container_reserved_for_teardown_is_neither_replaced_nor_handed_out(tmp_path, monkeypatch):
    provider, backend = _make_provider(tmp_path, monkeypatch, adopt_on_conflict=True)
    first = _acquire_accepted(provider, "thread-reserved")
    provider.release(first)
    _drift_inputs(monkeypatch)
    provider._local_teardown.add(first)

    with pytest.raises(AcceptedSkillSandboxBindingError, match="accepted_sandbox_inputs_changed"):
        _acquire_accepted(provider, "thread-reserved", run_id="run-2")

    assert backend.destroyed == [], "the replacement respected the reservation"
    assert backend.adopted == [], "and the reaper's container was not handed out under it"


def test_a_parked_container_lost_to_a_peer_is_neither_replaced_nor_taken_over(tmp_path, monkeypatch):
    provider, backend = _make_provider(tmp_path, monkeypatch, adopt_on_conflict=True)
    first = _acquire_accepted(provider, "thread-lost")
    provider.release(first)
    _drift_inputs(monkeypatch)
    monkeypatch.setattr(provider, "_claim_ownership", lambda *_a, **_k: False)

    with pytest.raises(AcceptedSkillSandboxBindingError, match="accepted_sandbox_inputs_changed"):
        _acquire_accepted(provider, "thread-lost", run_id="run-2")

    assert backend.destroyed == [], "an ownership-fenced refusal leaves the container running"
    assert backend.adopted == [], "and does not take it over from the peer"


def test_a_colliding_parked_entry_of_another_identity_is_never_replaced(tmp_path, monkeypatch):
    """The replacement step is identity-fenced like every other promote path."""
    aio = _aio_mod()
    provider, backend = _make_provider(tmp_path, monkeypatch, adopt_on_conflict=True)
    monkeypatch.setattr(aio.AioSandboxProvider, "_sandbox_id_for_thread", lambda *_a, **_k: "colliding")
    first = _acquire_accepted(provider, "thread-a")
    provider.release(first)
    _drift_inputs(monkeypatch)

    with pytest.raises(SandboxIdentityCollisionError):
        _acquire_accepted(provider, "thread-b", run_id="run-2")

    assert backend.destroyed == []
    assert first in provider._warm_pool


# ── Diagnostics ──────────────────────────────────────────────────────────


def test_a_rediscovery_is_an_attempt_and_an_adoption_but_not_a_create(tmp_path, monkeypatch):
    provider, backend = _make_provider(tmp_path, monkeypatch, adopt_on_conflict=True)
    foreign = _seed_foreign_container(backend, provider, "thread-count")

    with turn_phases(correlation_id="rediscovery") as journal:
        _acquire_accepted(provider, "thread-count")

    snapshot = journal.snapshot()
    assert snapshot.acquisition_source is AcquisitionSource.REDISCOVERED
    assert snapshot.acquisition_source.reuses_existing_resource
    assert snapshot.create_attempts == 1
    assert snapshot.resource_creates == 0
    assert snapshot.resource_rediscoveries == 1
    assert snapshot.unknown_create_results == 0
    assert snapshot.teardown_attempts == 0
    assert snapshot.evictions == 0
    # Reconciled against the backend's own record.
    assert (len(backend.created), len(backend.adopted), len(backend.destroyed)) == (0, 1, 0)
    assert backend.adopted == [foreign]


def test_a_backend_that_stays_silent_about_provenance_is_reported_as_unknown(tmp_path, monkeypatch):
    provider, backend = _make_provider(tmp_path, monkeypatch)
    real_create = backend.create

    def _silent_create(thread_id, sandbox_id, **kwargs):
        info = real_create(thread_id, sandbox_id, **kwargs)
        info.provenance = "unknown"
        return info

    monkeypatch.setattr(backend, "create", _silent_create)

    with turn_phases(correlation_id="silent") as journal:
        _acquire_accepted(provider, "thread-silent")

    snapshot = journal.snapshot()
    assert snapshot.acquisition_source is AcquisitionSource.UNKNOWN_PROVENANCE
    assert not snapshot.acquisition_source.reuses_existing_resource
    assert snapshot.create_attempts == 1
    assert snapshot.resource_creates == 0, "silence is not a creation"
    assert snapshot.resource_rediscoveries == 0
    assert snapshot.unknown_create_results == 1


@pytest.mark.anyio
async def test_cancellation_of_an_unknown_provenance_acquisition_parks_rather_than_destroys(tmp_path, monkeypatch):
    provider, backend = _make_provider(tmp_path, monkeypatch)
    real_create = backend.create

    def _silent_create(thread_id, sandbox_id, **kwargs):
        info = real_create(thread_id, sandbox_id, **kwargs)
        info.provenance = "unknown"
        return info

    monkeypatch.setattr(backend, "create", _silent_create)
    acquired, allow = _hold_binding(provider, monkeypatch)

    task = asyncio.create_task(
        provider.provision_accepted_skills_async("thread-unknown", user_id=ACCEPTED_USER, binding=_binding()),
    )
    assert await asyncio.to_thread(acquired.wait, 5)
    task.cancel("caller went away")
    allow.set()
    with pytest.raises(asyncio.CancelledError):
        await task

    assert backend.destroyed == [], "not proven ours to roll back"
    assert backend.created[0] in provider._warm_pool, "owned, tracked and reapable rather than stranded"


def test_a_replacement_is_a_confirmed_teardown_and_the_rebuild_a_confirmed_create(tmp_path, monkeypatch):
    provider, backend = _make_provider(tmp_path, monkeypatch, adopt_on_conflict=True)
    first = _acquire_accepted(provider, "thread-replace")
    provider.release(first)
    _drift_inputs(monkeypatch)

    with turn_phases(correlation_id="replace") as journal:
        _acquire_accepted(provider, "thread-replace", run_id="run-2")

    snapshot = journal.snapshot()
    assert snapshot.acquisition_source is AcquisitionSource.CREATED
    assert snapshot.teardown_attempts == 1
    assert snapshot.resource_teardowns == 1
    assert snapshot.teardown_refusals == 0
    assert snapshot.create_attempts == 1
    assert snapshot.resource_creates == 1
    assert snapshot.resource_rediscoveries == 0
    assert snapshot.evictions == 0, "replacing our own entry is not an eviction of someone else's"
    assert snapshot.phase_ms(TurnPhase.SANDBOX_EVICTION) is not None, "but its wait is timed"
    assert (len(backend.created), len(backend.destroyed)) == (2, 1)


def test_a_fenced_refusal_is_counted_as_a_refusal_and_not_as_a_disappearance(tmp_path, monkeypatch):
    provider, backend = _make_provider(tmp_path, monkeypatch, adopt_on_conflict=True)
    first = _acquire_accepted(provider, "thread-refused")
    provider.release(first)
    _drift_inputs(monkeypatch)
    monkeypatch.setattr(provider, "_claim_ownership", lambda *_a, **_k: False)

    with turn_phases(correlation_id="refused") as journal, pytest.raises(AcceptedSkillSandboxBindingError):
        _acquire_accepted(provider, "thread-refused", run_id="run-2")

    snapshot = journal.snapshot()
    assert snapshot.teardown_attempts == 1
    assert snapshot.teardown_refusals == 1
    assert snapshot.resource_teardowns == 0
    assert snapshot.failed_attempts == 1
    assert snapshot.create_attempts == 0
    assert snapshot.acquisition_source is None
    assert snapshot.phase_ms(TurnPhase.SANDBOX_EVICTION) is not None, "the refused stop's wait stays visible"
    assert backend.destroyed == []


def test_an_explicit_destroy_refused_by_ownership_is_not_a_teardown(tmp_path, monkeypatch):
    provider, backend = _make_provider(tmp_path, monkeypatch)
    first = _acquire_accepted(provider, "thread-destroy")
    monkeypatch.setattr(provider, "_claim_ownership", lambda *_a, **_k: False)

    with turn_phases(correlation_id="destroy-refused") as journal:
        provider.destroy(first)

    snapshot = journal.snapshot()
    assert snapshot.teardown_attempts == 1
    assert snapshot.teardown_refusals == 1
    assert snapshot.resource_teardowns == 0
    assert backend.destroyed == []


@pytest.mark.anyio
async def test_a_rollback_teardown_is_counted_once_and_only_when_the_backend_destroyed(tmp_path, monkeypatch):
    provider, backend = _make_provider(tmp_path, monkeypatch)
    acquired, allow = _hold_binding(provider, monkeypatch)

    with turn_phases(correlation_id="rollback") as journal:
        task = asyncio.create_task(
            provider.provision_accepted_skills_async("thread-rollback", user_id=ACCEPTED_USER, binding=_binding()),
        )
        assert await asyncio.to_thread(acquired.wait, 5)
        task.cancel("caller went away")
        allow.set()
        with pytest.raises(asyncio.CancelledError):
            await task

    snapshot = journal.snapshot()
    assert snapshot.create_attempts == 1
    assert snapshot.resource_creates == 1
    assert snapshot.teardown_attempts == 1
    assert snapshot.resource_teardowns == 1
    assert snapshot.teardown_refusals == 0
    assert (len(backend.created), len(backend.destroyed)) == (1, 1)


def test_the_wire_form_carries_every_counter_separately():
    with turn_phases(correlation_id="wire") as journal:
        journal.record_create_attempt()
        journal.record_resource_rediscovery()
        journal.record_teardown_attempt()
        journal.record_teardown_refusal()
        journal.record_teardown_failure()
    wire = journal.snapshot().to_wire()
    # 4 since the record gained the acquisition-reuse field; the version is the
    # contract's own stamp, so a consumer can tell the shapes apart.
    assert wire["version"] == 4
    assert wire["acquisition_reuse"] is None
    assert wire["teardown_failures"] == 1
    assert wire["create_attempts"] == 1
    assert wire["resource_creates"] == 0
    assert wire["resource_rediscoveries"] == 1
    assert wire["unknown_create_results"] == 0
    assert wire["teardown_attempts"] == 1
    assert wire["teardown_refusals"] == 1
    assert wire["resource_teardowns"] == 0
