"""Give a thread's projection back the way the worker would.

``SkillProjectionCoordinator`` is a process singleton. A test that admits a
thread and never releases it hands the next test a thread that is already
owned, and ``reserve_admission`` answers an existing matching reservation
idempotently -- so the next test silently exercises the re-reserve branch
instead of the fresh admission it reads as testing, and which branch it takes
moves with the shard split. ``backend/tests/conftest.py`` fails the test that
leaks; this is what such a test should call instead.
"""

from __future__ import annotations


def release_thread_projection(*, user_id: str, thread_id: str, run_id: str) -> None:
    """Release a thread's projection, whatever state the run left it in.

    Which call releases depends on how far the run got, and getting it wrong is
    silent: ``release_unactivated_run`` answers ``False`` once a consumer has
    activated, so a teardown that only calls it leaks the state it meant to
    drop. Releasing one token is not enough either -- a lead plus a retained
    subagent consumer leaves the thread busy, because ``release`` yields no
    clear until the last consumer goes. So: drain the consumers, then recover a
    release left part-finished, then fall back to the unactivated drop.
    """
    from deerflow.runtime.skill_projection import get_skill_projection_coordinator

    coordinator = get_skill_projection_coordinator()

    def _finish(token) -> None:
        clear = coordinator.release(token)
        if clear is not None:
            coordinator.finalize_release(clear)

    while (token := coordinator.current_token(user_id=user_id, thread_id=thread_id)) is not None:
        _finish(token)
    # A clear that was started and never finalized keeps the thread fenced with
    # no consumer to find; the clearing token is reachable only by name.
    pending = coordinator.token_for_consumer(
        user_id=user_id,
        thread_id=thread_id,
        run_id=run_id,
        consumer_id=f"run:{run_id}:lead",
    )
    if pending is not None:
        _finish(pending)
    coordinator.release_unactivated_run(user_id=user_id, thread_id=thread_id, run_id=run_id)
