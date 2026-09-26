"""A schedule held because its owner was turned off stays off until it is restored or its owner resumes it.

``disable`` pauses the owner's active schedules and ends their waiting
occurrences. The hold must survive what the scheduler writes on its own: a
launch in flight when the owner was turned off, the bookkeeping after a run
the command is cancelling, and the recovery of a claim. ``enable
--restore-held`` puts a held schedule back at its next future occurrence; a
``once`` task whose time passed stays paused.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest
import pytest_asyncio

from deerflow.config.database_config import DatabaseConfig
from deerflow.persistence.engine import close_engine, get_session_factory, init_engine_from_config
from deerflow.persistence.scheduled_task_runs import ScheduledTaskAdmissionRejected, ScheduledTaskRunRepository
from deerflow.persistence.scheduled_task_runs.model import ScheduledTaskRunRow
from deerflow.persistence.scheduled_tasks import ScheduledTaskRepository

NOW = datetime(2026, 9, 26, 12, 0, tzinfo=UTC)
HOLD_ERROR = "held: the owner's account was turned off"


@pytest_asyncio.fixture
async def repos(tmp_path):
    await init_engine_from_config(DatabaseConfig(backend="sqlite", sqlite_dir=str(tmp_path)))
    sf = get_session_factory()
    assert sf is not None
    try:
        yield ScheduledTaskRepository(sf), ScheduledTaskRunRepository(sf), sf
    finally:
        await close_engine()


async def _task(tasks: ScheduledTaskRepository, task_id: str, *, user_id: str = "user-1", once: bool = False, due: datetime | None = None) -> dict:
    return await tasks.create(
        task_id=task_id,
        user_id=user_id,
        thread_id=f"thread-{task_id}",
        context_mode="reuse_thread",
        assistant_id="lead_agent",
        title=task_id,
        prompt="Summarize the week",
        schedule_type="once" if once else "cron",
        schedule_spec={"run_at": (due or NOW - timedelta(hours=1)).isoformat()} if once else {"cron": "0 9 * * *"},
        timezone="UTC",
        next_run_at=due or NOW - timedelta(minutes=5),
    )


async def _occurrence(runs: ScheduledTaskRunRepository, task_id: str, run_id: str, *, trigger: str = "scheduled", status: str = "queued") -> None:
    await runs.create(
        run_record_id=run_id,
        task_id=task_id,
        thread_id=f"thread-{task_id}",
        scheduled_for=NOW,
        trigger=trigger,
        status=status,
    )


@pytest.mark.asyncio
async def test_hold_pauses_an_enabled_task_and_a_claimed_one_and_clears_the_claim(repos):
    tasks, _, _ = repos
    await _task(tasks, "enabled")
    await _task(tasks, "claimed")
    [claimed] = [row for row in await tasks.claim_due_tasks(now=NOW, lease_owner="scheduler-a", lease_seconds=60, limit=10) if row["id"] == "claimed"] or [None]
    assert claimed is not None

    assert await tasks.hold("enabled", user_id="user-1", now=NOW) is True
    assert await tasks.hold("claimed", user_id="user-1", now=NOW) is True

    for task_id in ("enabled", "claimed"):
        row = await tasks.get(task_id, user_id="user-1")
        assert row["status"] == "paused"
        assert row["lease_owner"] is None
        assert row["lease_expires_at"] is None
        # A resume counts as a new schedule version, and so does a hold: an
        # occurrence identity derived before it is not the one after.
        assert row["schedule_version"] == 2


@pytest.mark.asyncio
async def test_hold_leaves_a_task_already_off_and_another_owners_task_alone(repos):
    tasks, _, _ = repos
    await _task(tasks, "self-paused")
    await tasks.update("self-paused", user_id="user-1", updates={"status": "paused"})
    await _task(tasks, "theirs", user_id="user-2")

    assert await tasks.hold("self-paused", user_id="user-1", now=NOW) is False
    assert await tasks.hold("theirs", user_id="user-1", now=NOW) is False
    assert await tasks.hold("missing", user_id="user-1", now=NOW) is False
    assert (await tasks.get("theirs", user_id="user-2"))["status"] == "enabled"


@pytest.mark.asyncio
async def test_a_claim_held_before_its_occurrence_is_queued_cannot_queue_it(repos):
    tasks, runs, _ = repos
    await _task(tasks, "claimed")
    await tasks.claim_due_tasks(now=NOW, lease_owner="scheduler-a", lease_seconds=60, limit=10)
    await tasks.hold("claimed", user_id="user-1", now=NOW)

    with pytest.raises(ScheduledTaskAdmissionRejected):
        await runs.create(
            run_record_id="occurrence-after-hold",
            task_id="claimed",
            thread_id="thread-claimed",
            scheduled_for=NOW,
            trigger="scheduled",
            status="queued",
            coordinate_with_task=True,
            expected_task_user_id="user-1",
            expected_task_lease_owner="scheduler-a",
            release_task_lease_status="enabled",
        )
    assert (await tasks.get("claimed", user_id="user-1"))["status"] == "paused"


@pytest.mark.asyncio
async def test_ending_queued_occurrences_reaches_every_one_of_the_owners_including_a_manual_trigger(repos):
    tasks, runs, sf = repos
    await _task(tasks, "recurring")
    await _occurrence(runs, "recurring", "queued-scheduled")
    await _task(tasks, "paused-with-manual")
    await tasks.update("paused-with-manual", user_id="user-1", updates={"status": "paused"})
    # A manual trigger runs even on a paused task, so pausing alone would not stop this one.
    await _occurrence(runs, "paused-with-manual", "queued-manual", trigger="manual")
    await _task(tasks, "launching")
    await _occurrence(runs, "launching", "launching-occurrence")
    await runs.claim_queued_run("launching-occurrence", lease_owner="scheduler-a", now=NOW, lease_seconds=60, global_max_concurrent_runs=10)
    await _task(tasks, "theirs", user_id="user-2")
    await _occurrence(runs, "theirs", "their-occurrence")

    ended = await tasks.end_queued_occurrences(["user-1"], error=HOLD_ERROR, now=NOW)

    assert ended == 2
    async with sf() as session:
        statuses = {row_id: (await session.get(ScheduledTaskRunRow, row_id)) for row_id in ("queued-scheduled", "queued-manual", "launching-occurrence", "their-occurrence")}
    assert statuses["queued-scheduled"].status == "interrupted"
    assert statuses["queued-scheduled"].error == HOLD_ERROR
    assert statuses["queued-manual"].status == "interrupted"
    # A launch already claimed is left to its own path: the launch is refused
    # for an owner who is turned off, and the task stays held (below).
    assert statuses["launching-occurrence"].status == "launching"
    assert statuses["their-occurrence"].status == "queued"
    assert await tasks.end_queued_occurrences(["user-1"], error=HOLD_ERROR, now=NOW) == 0


@pytest.mark.asyncio
@pytest.mark.parametrize("once", [False, True])
async def test_a_launch_refused_after_the_hold_leaves_the_task_held(repos, once):
    tasks, runs, _ = repos
    await _task(tasks, "in-flight", once=once)
    await _occurrence(runs, "in-flight", "occurrence")
    await runs.claim_queued_run("occurrence", lease_owner="scheduler-a", now=NOW, lease_seconds=60, global_max_concurrent_runs=10)
    await tasks.hold("in-flight", user_id="user-1", now=NOW)

    # The launch reads the refusal and fails; nothing ran.
    assert await runs.fail_launching_run("occurrence", task_id="in-flight", lease_owner="scheduler-a", error="trusted internal launch owner's account is disabled", now=NOW) is True

    row = await tasks.get("in-flight", user_id="user-1")
    assert row["status"] == "paused"
    # Advanced past the occurrence that was refused, so a later resume does not catch it up.
    assert row["next_run_at"] is None if once else datetime.fromisoformat(row["next_run_at"]) > NOW


@pytest.mark.asyncio
async def test_bookkeeping_after_a_launch_that_beat_the_hold_leaves_a_recurring_task_held(repos):
    tasks, _, _ = repos
    await _task(tasks, "recurring")
    await tasks.hold("recurring", user_id="user-1", now=NOW)

    # The run launched just before the refusal committed; disable cancels it.
    await tasks.update_after_launch(
        "recurring",
        status="enabled",
        next_run_at=NOW + timedelta(days=1),
        last_run_at=NOW,
        last_run_id="run-before-the-refusal",
        last_thread_id="thread-recurring",
        last_error=None,
        increment_run_count=True,
        protect_terminal=True,
    )

    row = await tasks.get("recurring", user_id="user-1")
    assert row["status"] == "paused"
    # The launch itself is still recorded.
    assert row["last_run_id"] == "run-before-the-refusal"
    assert row["run_count"] == 1


@pytest.mark.asyncio
async def test_a_once_task_whose_only_occurrence_launched_records_it(repos):
    tasks, _, _ = repos
    await _task(tasks, "once", once=True)
    await tasks.hold("once", user_id="user-1", now=NOW)

    await tasks.update_after_launch(
        "once",
        status="running",
        next_run_at=None,
        last_run_at=NOW,
        last_run_id="run-before-the-refusal",
        last_thread_id="thread-once",
        last_error=None,
        increment_run_count=True,
        protect_terminal=True,
    )

    # Its one occurrence was consumed; its run's outcome decides how it ends.
    assert (await tasks.get("once", user_id="user-1"))["status"] == "running"


@pytest.mark.asyncio
async def test_recovering_a_claim_whose_run_had_launched_leaves_a_recurring_task_held(repos):
    tasks, runs, sf = repos
    from deerflow.persistence.run import RunRepository

    await _task(tasks, "recurring")
    await _occurrence(runs, "recurring", "occurrence")
    await runs.claim_queued_run("occurrence", lease_owner="scheduler-a", now=NOW - timedelta(minutes=5), lease_seconds=1, global_max_concurrent_runs=10)
    await tasks.hold("recurring", user_id="user-1", now=NOW)
    await RunRepository(sf).put(
        "run-before-the-refusal",
        thread_id="thread-recurring",
        user_id="user-1",
        status="running",
        metadata={"scheduled_task_id": "recurring", "scheduled_task_run_id": "occurrence"},
    )

    await runs.recover_expired_launch_claims(error="lease lost", now=NOW)

    assert (await tasks.get("recurring", user_id="user-1"))["status"] == "paused"


@pytest.mark.asyncio
async def test_restoring_a_held_recurring_task_puts_it_back_at_its_next_future_occurrence(repos):
    tasks, _, _ = repos
    await _task(tasks, "recurring", due=NOW - timedelta(days=3))
    await tasks.hold("recurring", user_id="user-1", now=NOW - timedelta(days=3))

    assert await tasks.restore_held("recurring", user_id="user-1", now=NOW) == "restored"

    row = await tasks.get("recurring", user_id="user-1")
    assert row["status"] == "enabled"
    # Not the occurrence it missed three days ago: the next one after now.
    assert row["next_run_at"] == (NOW + timedelta(hours=21)).isoformat()
    assert row["schedule_version"] == 3


@pytest.mark.asyncio
async def test_restoring_a_once_task_keeps_its_time_when_it_is_still_ahead_and_pauses_it_when_it_passed(repos):
    tasks, _, _ = repos
    await _task(tasks, "ahead", once=True, due=NOW + timedelta(hours=2))
    await _task(tasks, "passed", once=True, due=NOW - timedelta(hours=2))
    for task_id in ("ahead", "passed"):
        await tasks.hold(task_id, user_id="user-1", now=NOW - timedelta(hours=3))

    assert await tasks.restore_held("ahead", user_id="user-1", now=NOW) == "restored"
    assert await tasks.restore_held("passed", user_id="user-1", now=NOW) == "time_passed"

    assert (await tasks.get("ahead", user_id="user-1"))["next_run_at"] == (NOW + timedelta(hours=2)).isoformat()
    assert (await tasks.get("passed", user_id="user-1"))["status"] == "paused"


@pytest.mark.asyncio
async def test_restoring_leaves_a_task_that_is_no_longer_paused_or_no_longer_theirs(repos):
    tasks, _, _ = repos
    await _task(tasks, "resumed")
    await tasks.hold("resumed", user_id="user-1", now=NOW)
    await tasks.update("resumed", user_id="user-1", updates={"status": "enabled"})
    await _task(tasks, "theirs", user_id="user-2")
    await tasks.hold("theirs", user_id="user-2", now=NOW)

    assert await tasks.restore_held("resumed", user_id="user-1", now=NOW) == "changed_since"
    assert await tasks.restore_held("theirs", user_id="user-1", now=NOW) == "gone"
    assert await tasks.restore_held("missing", user_id="user-1", now=NOW) == "gone"
    assert (await tasks.get("theirs", user_id="user-2"))["status"] == "paused"


@pytest.mark.asyncio
async def test_an_occurrence_a_scheduler_claims_while_it_is_being_ended_is_left_to_its_launch(repos):
    tasks, runs, sf = repos
    await _task(tasks, "recurring")
    await _occurrence(runs, "recurring", "raced")
    lock_task = tasks._lock_task
    claimed: list[bool] = []

    async def _claim_first(session, task_id):
        if not claimed:
            claimed.append(True)
            await runs.claim_queued_run("raced", lease_owner="scheduler-a", now=NOW, lease_seconds=60, global_max_concurrent_runs=10)
        return await lock_task(session, task_id)

    tasks._lock_task = _claim_first
    assert await tasks.end_queued_occurrences(["user-1"], error=HOLD_ERROR, now=NOW) == 0

    async with sf() as session:
        assert (await session.get(ScheduledTaskRunRow, "raced")).status == "launching"
