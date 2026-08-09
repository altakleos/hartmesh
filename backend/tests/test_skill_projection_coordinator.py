from __future__ import annotations

from dataclasses import replace

import pytest


def _coordinator():
    from deerflow.runtime.skill_projection import SkillProjectionCoordinator

    return SkillProjectionCoordinator()


def _activate(
    coordinator,
    *,
    user_id: str = "user-1",
    thread_id: str = "thread-1",
    run_id: str = "run-a",
    sandbox_id: str = "sandbox-1",
    snapshot_id: str | None = "a" * 64,
    consumer_id: str = "lead",
):
    reservation = coordinator.reserve_admission(
        user_id=user_id,
        thread_id=thread_id,
        reservation_id=f"reservation:{run_id}",
        snapshot_id=snapshot_id,
    )
    coordinator.promote_admission(reservation, run_id=run_id)
    return coordinator.activate(
        user_id=user_id,
        thread_id=thread_id,
        sandbox_id=sandbox_id,
        run_id=run_id,
        snapshot_id=snapshot_id,
        consumer_id=consumer_id,
    )


def test_projection_release_is_fenced_against_a_later_run() -> None:
    from deerflow.runtime.skill_projection import SkillProjectionBusyError

    coordinator = _coordinator()
    stale_a = _activate(coordinator)

    clear_a = coordinator.release(stale_a)
    assert clear_a is not None
    assert coordinator.is_busy(user_id="user-1", thread_id="thread-1")
    with pytest.raises(SkillProjectionBusyError):
        _activate(
            coordinator,
            run_id="run-b",
            snapshot_id="b" * 64,
        )
    assert coordinator.finalize_release(clear_a)

    current_b = _activate(
        coordinator,
        run_id="run-b",
        snapshot_id="b" * 64,
    )
    assert coordinator.release(stale_a) is None
    assert coordinator.current_token(user_id="user-1", thread_id="thread-1") == current_b


def test_projection_remains_bound_until_background_child_releases() -> None:
    coordinator = _coordinator()
    lead = _activate(coordinator)
    child = coordinator.retain(lead, consumer_id="subagent:task-1")

    assert coordinator.release(lead) is None
    assert coordinator.is_busy(user_id="user-1", thread_id="thread-1")

    clear = coordinator.release(child)
    assert clear is not None
    assert clear.run_id == "run-a"
    assert coordinator.is_busy(user_id="user-1", thread_id="thread-1")
    assert coordinator.finalize_release(clear)
    assert not coordinator.is_busy(user_id="user-1", thread_id="thread-1")


def test_equal_snapshot_bindings_for_different_threads_are_independent() -> None:
    coordinator = _coordinator()
    first = _activate(coordinator, thread_id="thread-1", sandbox_id="sandbox-1")
    second = _activate(coordinator, thread_id="thread-2", sandbox_id="sandbox-2")

    clear = coordinator.release(first)
    assert clear is not None
    assert coordinator.finalize_release(clear)
    assert coordinator.current_token(user_id="user-1", thread_id="thread-2") == second


def test_explicit_empty_projection_is_owned_and_fenced() -> None:
    coordinator = _coordinator()
    token = _activate(coordinator, snapshot_id=None)

    assert token.snapshot_id is None
    assert coordinator.is_busy(user_id="user-1", thread_id="thread-1")
    clear = coordinator.release(token)
    assert clear is not None
    assert coordinator.finalize_release(clear)


def test_projection_rejects_second_run_and_duplicate_consumer() -> None:
    from deerflow.runtime.skill_projection import SkillProjectionBusyError

    coordinator = _coordinator()
    lead = _activate(coordinator)
    assert (
        coordinator.activate(
            user_id="user-1",
            thread_id="thread-1",
            sandbox_id="sandbox-1",
            run_id="run-a",
            snapshot_id="a" * 64,
            consumer_id="lead",
        )
        == lead
    )

    with pytest.raises(SkillProjectionBusyError, match="skill_projection_thread_busy"):
        coordinator.reserve_admission(
            user_id="user-1",
            thread_id="thread-1",
            reservation_id="reservation:run-b",
            snapshot_id="b" * 64,
        )


def test_parallel_children_clear_only_after_exact_last_release() -> None:
    coordinator = _coordinator()
    lead = _activate(coordinator)
    first_child = coordinator.retain(lead, consumer_id="subagent:first")
    second_child = coordinator.retain(lead, consumer_id="subagent:second")

    assert coordinator.release(replace(first_child, generation=999)) is None
    assert coordinator.release(lead) is None
    assert coordinator.release(first_child) is None
    assert coordinator.release(first_child) is None

    clear = coordinator.release(second_child)
    assert clear is not None
    assert clear.generation == second_child.generation
    assert coordinator.release(second_child) == clear
    assert coordinator.finalize_release(clear)


def test_abort_admission_cannot_remove_promoted_run_owner() -> None:
    coordinator = _coordinator()
    reservation = coordinator.reserve_admission(
        user_id="user-1",
        thread_id="thread-promoted",
        reservation_id="reservation:promoted",
        snapshot_id=None,
    )
    coordinator.promote_admission(reservation, run_id="run-promoted")

    assert coordinator.abort_admission(reservation) is False
    assert coordinator.is_busy(user_id="user-1", thread_id="thread-promoted")
    assert coordinator.release_unactivated_run(
        user_id="user-1",
        thread_id="thread-promoted",
        run_id="run-promoted",
    )


def test_released_consumer_token_is_not_live_coordinator_membership() -> None:
    coordinator = _coordinator()
    token = _activate(
        coordinator,
        thread_id="thread-stale-token",
        run_id="run-stale-token",
        sandbox_id="sandbox-stale-token",
        consumer_id="lead",
    )

    assert coordinator.owns(token)
    clear = coordinator.release(token)
    assert clear is not None
    assert not coordinator.owns(token)
    assert coordinator.finalize_release(clear)
