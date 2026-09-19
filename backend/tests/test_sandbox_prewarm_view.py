"""Prewarm publishes the accepted skill view, so the first turn's bind verifies in place.

The prewarmed container was reclaimed in 14 ms on the released profile and
the first turn still spent 2.2 s in skill projection: the container was
parked, the *view* it mounts was not. ``bind_skill_snapshot_active_view``
stages a copy of the snapshot the first time a thread sees it -- capture,
fsync'd write, two re-captures, 1.2 to 2 s on the tenant class -- and every
later bind verifies the published tree in place in tens of milliseconds. So
the prewarm publishes the view for the snapshot the turn is most likely to
bring, under a provisional identity that any real run supersedes.

The identity never authorizes the reuse; the bytes do. A turn that brings a
different snapshot fails the in-place verification and takes today's slow
path, which is the whole cost of guessing wrong. A view that cannot be
published costs the turn nothing either: the container is parked regardless.
"""

from __future__ import annotations

from pathlib import Path

import pytest
from test_accepted_skill_snapshots import _parsed_skill, _refuse_staging, _write_skill
from test_sandbox_warm_reuse_latency import ACCEPTED_USER, _acquire_accepted, _aio_mod, _make_provider

from deerflow.runtime.skill_snapshot import cleanup_abandoned_skill_snapshots, snapshot_effective_skills
from deerflow.sandbox.accepted_material import AcceptedSkillSandboxBindingV1

THREAD = "thread-open"

# The identity the prewarm publishes under; a real run must always outrank it.
PREWARM_GENERATION = 0


@pytest.fixture
def snapshot_free_coordinator():
    from deerflow.runtime.skill_projection import SkillProjectionCoordinator

    return SkillProjectionCoordinator()


@pytest.fixture
def provider_and_paths(tmp_path, monkeypatch):
    provider, backend = _make_provider(tmp_path, monkeypatch)
    from deerflow.runtime import skill_snapshot as snapshot_module

    paths = _aio_mod().get_paths()
    monkeypatch.setattr(snapshot_module, "get_paths", lambda: paths)
    yield provider, backend, paths
    cleanup_abandoned_skill_snapshots()


def _snapshot(tmp_path: Path, *, name: str = "report", body: str = "one command"):
    skill_file = _write_skill(tmp_path / name, body=body, name=name)
    snapshot = snapshot_effective_skills((_parsed_skill(skill_file),), user_id=ACCEPTED_USER)
    assert snapshot is not None
    return snapshot


def _first_turn_binds(provider, sandbox_id: str, snapshot_id: str) -> None:
    """The bind the turn's own provisioning performs, under a real generation."""
    provider.bind_accepted_skill_snapshot(
        sandbox_id,
        thread_id=THREAD,
        user_id=ACCEPTED_USER,
        binding=AcceptedSkillSandboxBindingV1(snapshot_id=snapshot_id, run_id="run-1", generation=1),
    )


def test_prewarm_publishes_the_view_and_the_first_turn_verifies_it_in_place(provider_and_paths, tmp_path, monkeypatch):
    provider, backend, paths = provider_and_paths
    snapshot = _snapshot(tmp_path)

    parked = provider._prewarm_accepted_skills(THREAD, user_id=ACCEPTED_USER, resolve_skill_snapshot=lambda: snapshot)

    assert parked is not None and backend.created == [parked]
    view = paths.skill_snapshot_active_view_dir(ACCEPTED_USER, THREAD)
    assert [entry.name for entry in view.iterdir()] == [snapshot.snapshot_id], "the view holds exactly the guessed snapshot"

    writes = _refuse_staging(monkeypatch)
    first = _acquire_accepted(provider, THREAD)
    _first_turn_binds(provider, first, snapshot.snapshot_id)

    assert first == parked
    assert writes == [], "the first turn adopted the published tree without staging a byte"


def test_a_turn_with_a_different_snapshot_restages_and_is_never_refused(provider_and_paths, tmp_path):
    provider, _backend, paths = provider_and_paths
    guessed = _snapshot(tmp_path, name="guessed", body="the default agent's skills")
    actual = _snapshot(tmp_path, name="actual", body="what the run really brought")
    assert guessed.snapshot_id != actual.snapshot_id

    parked = provider._prewarm_accepted_skills(THREAD, user_id=ACCEPTED_USER, resolve_skill_snapshot=lambda: guessed)
    first = _acquire_accepted(provider, THREAD)
    _first_turn_binds(provider, first, actual.snapshot_id)

    assert first == parked
    view = paths.skill_snapshot_active_view_dir(ACCEPTED_USER, THREAD)
    assert [entry.name for entry in view.iterdir()] == [actual.snapshot_id], "the wrong guess is replaced, not kept beside the truth"


def test_a_view_that_cannot_be_published_still_parks_the_container(provider_and_paths, tmp_path, monkeypatch):
    provider, backend, paths = provider_and_paths
    snapshot = _snapshot(tmp_path)
    from deerflow.runtime import skill_snapshot as snapshot_module

    def refuse(**_kwargs):
        raise OSError("view volume read-only")

    monkeypatch.setattr(snapshot_module, "bind_skill_snapshot_active_view", refuse)

    parked = provider._prewarm_accepted_skills(THREAD, user_id=ACCEPTED_USER, resolve_skill_snapshot=lambda: snapshot)

    assert parked is not None and parked in provider._warm_pool, "the container is the prewarm's job; the view is a bonus"
    assert backend.destroyed == []


def test_prewarm_without_a_snapshot_publishes_nothing(provider_and_paths):
    provider, _backend, paths = provider_and_paths

    parked = provider._prewarm_accepted_skills(THREAD, user_id=ACCEPTED_USER)

    assert parked is not None
    view = paths.skill_snapshot_active_view_dir(ACCEPTED_USER, THREAD)
    assert not view.exists() or list(view.iterdir()) == []


def test_prewarm_never_overrides_a_view_a_real_run_bound(provider_and_paths, tmp_path):
    """A thread whose view a run already owns keeps it; the guess yields to a real generation."""
    provider, _backend, paths = provider_and_paths
    bound = _snapshot(tmp_path, name="bound", body="a run bound this")
    guessed = _snapshot(tmp_path, name="guessed", body="a later guess")
    from deerflow.runtime import skill_snapshot as snapshot_module
    from deerflow.runtime.skill_projection import SkillProjectionEvidence

    snapshot_module.bind_skill_snapshot_active_view(
        user_id=ACCEPTED_USER,
        thread_id=THREAD,
        snapshot_id=bound.snapshot_id,
        run_id="run-1",
        generation=1,
        evidence=SkillProjectionEvidence.from_snapshot(bound),
    )

    parked = provider._prewarm_accepted_skills(THREAD, user_id=ACCEPTED_USER, resolve_skill_snapshot=lambda: guessed)

    assert parked is not None
    view = paths.skill_snapshot_active_view_dir(ACCEPTED_USER, THREAD)
    assert [entry.name for entry in view.iterdir()] == [bound.snapshot_id]


def test_a_failed_first_bind_can_still_recover_the_thread(provider_and_paths, tmp_path):
    """A guess must never be ownership, or a failed bind wedges the thread forever.

    When a turn's own bind raises after it claimed the coordinator -- the
    tenant's disk filling during staging is the realistic cause, and it is
    the very bind this prewarm exists to make cheap -- the runtime unwinds by
    clearing the view. That unwind has two steps and a prewarm that *owned*
    the view failed both: the compare-and-clear carries the run's
    ``(run_id, generation)`` and cannot match the guess, and the fallback
    refuses outright because a recorded view is an owned one. Neither reaches
    ``finalize_release``, so the coordinator keeps ``clearing`` set and every
    later turn on that thread is refused -- permanently, since
    ``fence_committed_owner`` also refuses a clearing state.

    So the prewarm publishes bytes and keeps nothing: the tree stays for the
    turn to verify, and the view is unowned, which is the state the unwind
    knows how to finish.
    """
    from deerflow.runtime.skill_snapshot import release_unowned_skill_snapshot_active_view

    provider, _backend, _paths = provider_and_paths
    snapshot = _snapshot(tmp_path)

    assert provider._prewarm_accepted_skills(THREAD, user_id=ACCEPTED_USER, resolve_skill_snapshot=lambda: snapshot) is not None

    assert release_unowned_skill_snapshot_active_view(user_id=ACCEPTED_USER, thread_id=THREAD) is True


def test_a_prewarm_that_builds_nothing_never_resolves_a_guess(provider_and_paths, tmp_path):
    """Reading and digesting the skill tree is only worth it once a container exists.

    Every "built nothing" answer -- the thread already holds a sandbox, one
    is already parked, no slot is free -- happens before any container does,
    and on a two-slot tenant that is the ordinary state once two chats are
    live. Resolving there would spend two full tree passes, under the lease
    lock live turns also take, on a guess nothing can use.
    """
    provider, _backend, _paths = provider_and_paths
    calls: list[int] = []

    def resolve():
        calls.append(1)
        return _snapshot(tmp_path)

    first = provider._prewarm_accepted_skills(THREAD, user_id=ACCEPTED_USER, resolve_skill_snapshot=resolve)
    assert first is not None and calls == [1]

    # Already parked: the second prewarm answers from the pool and asks nothing.
    assert provider._prewarm_accepted_skills(THREAD, user_id=ACCEPTED_USER, resolve_skill_snapshot=resolve) == first
    assert calls == [1]


def test_the_prewarm_releases_the_lease_it_took(provider_and_paths, tmp_path):
    """Whoever takes the lease releases it: the tree stays published, the claim does not.

    A lease left behind pins its snapshot tree against
    ``_prune_retained_snapshots`` for the life of the process, so a thread
    opened and never used would hold a retained digest forever.
    """
    from deerflow.runtime import skill_snapshot as snapshot_module

    provider, _backend, _paths = provider_and_paths
    snapshot = _snapshot(tmp_path)
    assert snapshot_module._lease_counts.get(snapshot.root, 0) == 1

    assert provider._prewarm_accepted_skills(THREAD, user_id=ACCEPTED_USER, resolve_skill_snapshot=lambda: snapshot) is not None

    assert snapshot_module._lease_counts.get(snapshot.root, 0) == 0, "the prewarm released what it took"
    assert (snapshot.root).exists(), "the published bytes outlive the lease"


def test_a_coordinator_generation_always_outranks_the_prewarms(snapshot_free_coordinator):
    """The whole supersession argument rests on real generations starting above 0.

    The prewarm publishes at generation 0 precisely because
    ``bind_skill_snapshot_active_view`` refuses an equal-or-lower generation:
    if the coordinator ever issued 0, every prewarmed thread's first turn
    would raise ``skill_snapshot_binding_conflict`` instead of superseding
    the guess. Derived from the coordinator rather than hard-coded, so a
    re-based counter fails here and not on a tenant.
    """
    reservation = snapshot_free_coordinator.reserve_admission(
        user_id=ACCEPTED_USER,
        thread_id=THREAD,
        reservation_id="admission:generation-probe",
        snapshot_id=None,
        evidence=None,
    )

    assert reservation.generation > PREWARM_GENERATION
