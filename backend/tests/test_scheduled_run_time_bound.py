"""A scheduled occurrence that runs past ``scheduler.max_run_seconds`` is ended, once, and its run asked to stop.

Real repositories on SQLite; only the run manager's Stop and liveness are doubles.
"""

from __future__ import annotations

import asyncio
from collections.abc import Iterator
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from uuid import uuid4

import pytest
from sqlalchemy import update

from app.scheduler.run_time_bound import TimeBoundedScheduledTaskService, duration_words
from deerflow.persistence.scheduled_task_runs.model import ScheduledTaskRunRow

OWNER = "owner-1"
LIMIT_SECONDS = 900


@pytest.fixture
def stores(tmp_path) -> Iterator[SimpleNamespace]:
    from deerflow.persistence.engine import close_engine, get_session_factory, init_engine
    from deerflow.persistence.scheduled_task_runs import ScheduledTaskRunRepository
    from deerflow.persistence.scheduled_tasks import ScheduledTaskRepository

    asyncio.run(init_engine("sqlite", url=f"sqlite+aiosqlite:///{tmp_path}/bound.db", sqlite_dir=str(tmp_path)))
    session_factory = get_session_factory()
    try:
        yield SimpleNamespace(session_factory=session_factory, schedules=ScheduledTaskRepository(session_factory), occurrences=ScheduledTaskRunRepository(session_factory))
    finally:
        asyncio.run(close_engine())


async def _never_launch(**kwargs):
    raise AssertionError("no launch is expected in these tests")


def _service(stores: SimpleNamespace, *, stop_run=None, run_is_live=None, max_run_seconds: int | None = LIMIT_SECONDS) -> TimeBoundedScheduledTaskService:
    return TimeBoundedScheduledTaskService(
        task_repo=stores.schedules,
        task_run_repo=stores.occurrences,
        launch_run=_never_launch,
        poll_interval_seconds=5,
        lease_seconds=120,
        max_concurrent_runs=1,
        max_run_seconds=max_run_seconds,
        stop_run=stop_run,
        run_is_live=run_is_live,
    )


async def _running_occurrence(stores: SimpleNamespace, *, started_at: datetime, schedule_type: str = "cron") -> tuple[str, str, str]:
    """A task with one occurrence that has been running since *started_at*."""
    task_id, occurrence_id, run_id = str(uuid4()), str(uuid4()), str(uuid4())
    await stores.schedules.create(
        task_id=task_id,
        user_id=OWNER,
        thread_id=f"thread-{task_id}",
        context_mode="reuse_thread",
        assistant_id="lead_agent",
        title="Weekly numbers",
        prompt="Send me the weekly numbers",
        schedule_type=schedule_type,
        schedule_spec={"cron": "0 9 * * 1"} if schedule_type == "cron" else {"run_at": started_at.isoformat()},
        timezone="UTC",
        next_run_at=started_at + timedelta(days=7),
    )
    await stores.occurrences.create(run_record_id=occurrence_id, task_id=task_id, thread_id=f"thread-{task_id}", scheduled_for=started_at, trigger="scheduled", status="queued")
    async with stores.session_factory() as session:
        await session.execute(update(ScheduledTaskRunRow).where(ScheduledTaskRunRow.id == occurrence_id).values(status="running", run_id=run_id, started_at=started_at))
        await session.commit()
    return task_id, occurrence_id, run_id


async def _occurrence(stores: SimpleNamespace, task_id: str) -> dict:
    rows = await stores.occurrences.list_by_task(task_id)
    assert len(rows) == 1
    return rows[0]


def _completion(run_id: str, task_id: str, occurrence_id: str, status: str = "interrupted"):
    return SimpleNamespace(run_id=run_id, user_id=OWNER, status=SimpleNamespace(value=status), error=None, metadata={"scheduled_task_id": task_id, "scheduled_task_run_id": occurrence_id})


def test_the_limit_is_said_in_words() -> None:
    assert duration_words(900) == "15 minutes"
    assert duration_words(3600) == "1 hour"
    assert duration_words(7200) == "2 hours"
    assert duration_words(301) == "301 seconds"


@pytest.mark.asyncio
async def test_an_occurrence_past_its_limit_is_ended_failed_and_its_run_asked_to_stop(stores) -> None:
    now = datetime.now(UTC)
    task_id, _occurrence_id, run_id = await _running_occurrence(stores, started_at=now - timedelta(seconds=LIMIT_SECONDS + 5))
    stopped: list[str] = []

    async def stop_run(asked: str) -> str:
        stopped.append(asked)
        return "cancelled"

    await _service(stores, stop_run=stop_run)._stop_overdue_runs(now=now)

    occurrence = await _occurrence(stores, task_id)
    assert occurrence["status"] == "failed"
    assert occurrence["error"] == "the task did not finish within 15 minutes, so it was stopped"
    assert occurrence["finished_at"] is not None
    assert stopped == [run_id]


@pytest.mark.asyncio
async def test_an_occurrence_inside_its_limit_is_left_alone(stores) -> None:
    now = datetime.now(UTC)
    task_id, _occurrence_id, _run_id = await _running_occurrence(stores, started_at=now - timedelta(seconds=LIMIT_SECONDS - 5))
    stopped: list[str] = []

    async def stop_run(asked: str) -> None:
        stopped.append(asked)

    await _service(stores, stop_run=stop_run)._stop_overdue_runs(now=now)

    assert (await _occurrence(stores, task_id))["status"] == "running"
    assert stopped == []


@pytest.mark.asyncio
async def test_without_a_limit_nothing_is_ended(stores) -> None:
    now = datetime.now(UTC)
    task_id, _occurrence_id, _run_id = await _running_occurrence(stores, started_at=now - timedelta(days=2))

    async def stop_run(asked: str) -> None:
        raise AssertionError("no limit is configured")

    await _service(stores, stop_run=stop_run, max_run_seconds=None)._stop_overdue_runs(now=now)

    assert (await _occurrence(stores, task_id))["status"] == "running"


@pytest.mark.asyncio
async def test_the_occurrence_is_ended_even_when_the_stop_fails(stores) -> None:
    now = datetime.now(UTC)
    task_id, _occurrence_id, _run_id = await _running_occurrence(stores, started_at=now - timedelta(seconds=LIMIT_SECONDS + 5))

    async def stop_run(asked: str) -> None:
        raise RuntimeError("the run manager is unavailable")

    await _service(stores, stop_run=stop_run)._stop_overdue_runs(now=now)

    assert (await _occurrence(stores, task_id))["status"] == "failed"


@pytest.mark.asyncio
async def test_the_stopped_runs_own_ending_does_not_overwrite_the_outcome(stores) -> None:
    now = datetime.now(UTC)
    task_id, occurrence_id, run_id = await _running_occurrence(stores, started_at=now - timedelta(seconds=LIMIT_SECONDS + 5))

    async def stop_run(asked: str) -> str:
        return "cancelled"

    service = _service(stores, stop_run=stop_run)
    await service._stop_overdue_runs(now=now)
    await service.handle_run_completion(_completion(run_id, task_id, occurrence_id))

    occurrence = await _occurrence(stores, task_id)
    assert occurrence["status"] == "failed"
    assert "did not finish within 15 minutes" in occurrence["error"]


@pytest.mark.asyncio
@pytest.mark.parametrize("schedule_type", ["cron", "once"])
@pytest.mark.parametrize("fresh_service", [False, True], ids=["same-service", "fresh-service"])
async def test_completion_after_timeout_commit_preserves_occurrence_and_parent(stores, monkeypatch, schedule_type, fresh_service) -> None:
    """Completion runs before the timeout caller can record anything in memory."""
    now = datetime.now(UTC)
    task_id, occurrence_id, run_id = await _running_occurrence(stores, started_at=now - timedelta(seconds=LIMIT_SECONDS + 5), schedule_type=schedule_type)
    stopped = []

    async def stop_run(asked):
        stopped.append(asked)

    service = _service(stores, stop_run=stop_run)
    completing_service = _service(stores) if fresh_service else service
    complete_run = stores.schedules.complete_run
    committed = {}

    async def complete_then_callback(*args, **kwargs):
        ended = await complete_run(*args, **kwargs)
        if ended and kwargs["status"] == "failed":
            committed["occurrence"] = await _occurrence(stores, task_id)
            committed["parent"] = await stores.schedules.get(task_id, user_id=OWNER)
            await completing_service.handle_run_completion(_completion(run_id, task_id, occurrence_id))
        return ended

    monkeypatch.setattr(stores.schedules, "complete_run", complete_then_callback)
    await service._stop_overdue_runs(now=now)

    assert committed["occurrence"]["status"] == "failed"
    assert committed["occurrence"]["error"] == "the task did not finish within 15 minutes, so it was stopped"
    assert await _occurrence(stores, task_id) == committed["occurrence"]
    assert await stores.schedules.get(task_id, user_id=OWNER) == committed["parent"]
    if schedule_type == "once":
        assert committed["parent"]["status"] == "failed"
    assert stopped == [run_id]


@pytest.mark.asyncio
@pytest.mark.parametrize("status", ["success", "error", "interrupted"])
async def test_duplicate_completion_preserves_the_first_terminal_outcome(stores, status) -> None:
    now = datetime.now(UTC)
    task_id, occurrence_id, run_id = await _running_occurrence(stores, started_at=now, schedule_type="once")
    service = _service(stores)
    record = _completion(run_id, task_id, occurrence_id, status=status)
    record.error = "first failure" if status == "error" else None
    await service.handle_run_completion(record)
    occurrence = await _occurrence(stores, task_id)
    parent = await stores.schedules.get(task_id, user_id=OWNER)

    await service.handle_run_completion(_completion(run_id, task_id, occurrence_id, status="error" if status == "success" else "success"))

    assert await _occurrence(stores, task_id) == occurrence
    assert await stores.schedules.get(task_id, user_id=OWNER) == parent


@pytest.mark.asyncio
async def test_completion_winning_after_overdue_selection_is_not_stopped(stores, monkeypatch) -> None:
    now = datetime.now(UTC)
    task_id, occurrence_id, run_id = await _running_occurrence(stores, started_at=now - timedelta(seconds=LIMIT_SECONDS + 5))
    stopped = []

    async def stop_run(asked):
        stopped.append(asked)

    service = _service(stores, stop_run=stop_run)
    list_overdue = stores.occurrences.list_overdue_running

    async def select_then_complete(**kwargs):
        overdue = await list_overdue(**kwargs)
        await service.handle_run_completion(_completion(run_id, task_id, occurrence_id, status="success"))
        return overdue

    monkeypatch.setattr(stores.occurrences, "list_overdue_running", select_then_complete)
    await service._stop_overdue_runs(now=now)

    occurrence = await _occurrence(stores, task_id)
    assert occurrence["status"] == "success"
    assert occurrence["error"] is None
    assert stopped == []


@pytest.mark.asyncio
@pytest.mark.parametrize("wrong_field", ["user_id", "run_id"])
async def test_completion_cannot_end_an_occurrence_with_a_different_owner_or_run(stores, wrong_field) -> None:
    now = datetime.now(UTC)
    task_id, occurrence_id, run_id = await _running_occurrence(stores, started_at=now)
    occurrence = await _occurrence(stores, task_id)
    parent = await stores.schedules.get(task_id, user_id=OWNER)
    record = _completion(run_id, task_id, occurrence_id, status="success")
    setattr(record, wrong_field, "unrelated")

    await _service(stores).handle_run_completion(record)

    assert await _occurrence(stores, task_id) == occurrence
    assert await stores.schedules.get(task_id, user_id=OWNER) == parent


@pytest.mark.asyncio
async def test_an_occurrence_that_already_ended_keeps_its_outcome(stores) -> None:
    now = datetime.now(UTC)
    task_id, occurrence_id, run_id = await _running_occurrence(stores, started_at=now - timedelta(seconds=LIMIT_SECONDS + 5))
    assert await stores.schedules.complete_run(task_id, user_id=OWNER, task_run_id=occurrence_id, run_id=run_id, status="success", error=None, finished_at=now)

    ended = await stores.schedules.complete_run(task_id, user_id=OWNER, task_run_id=occurrence_id, run_id=run_id, status="failed", error="late", finished_at=now, only_if_active=True)

    assert ended is False
    assert (await _occurrence(stores, task_id))["status"] == "success"


@pytest.mark.asyncio
async def test_a_person_who_stops_a_scheduled_run_still_ends_it_interrupted(stores) -> None:
    now = datetime.now(UTC)
    task_id, occurrence_id, run_id = await _running_occurrence(stores, started_at=now - timedelta(seconds=30))

    await _service(stores).handle_run_completion(_completion(run_id, task_id, occurrence_id))

    assert (await _occurrence(stores, task_id))["status"] == "interrupted"


@pytest.mark.asyncio
async def test_nothing_is_launched_while_a_stopped_run_is_still_unwinding(stores) -> None:
    now = datetime.now(UTC)
    _task_id, _occurrence_id, run_id = await _running_occurrence(stores, started_at=now - timedelta(seconds=LIMIT_SECONDS + 5))
    live = {run_id: True}

    async def stop_run(asked: str) -> str:
        return "cancelled"

    async def run_is_live(asked: str) -> bool:
        return live[asked]

    service = _service(stores, stop_run=stop_run, run_is_live=run_is_live)
    await service._stop_overdue_runs(now=now)

    assert await service._a_stopped_run_is_still_unwinding(now=now) is True
    live[run_id] = False
    assert await service._a_stopped_run_is_still_unwinding(now=now) is False
    # Once it is gone it is forgotten, and scheduling is not held again for it.
    live[run_id] = True
    assert await service._a_stopped_run_is_still_unwinding(now=now) is False


@pytest.mark.asyncio
async def test_a_run_that_never_unwinds_holds_scheduling_for_the_grace_period_only(stores) -> None:
    now = datetime.now(UTC)
    await _running_occurrence(stores, started_at=now - timedelta(seconds=LIMIT_SECONDS + 5))

    async def stop_run(asked: str) -> str:
        return "cancelled"

    async def run_is_live(asked: str) -> bool:
        return True

    service = _service(stores, stop_run=stop_run, run_is_live=run_is_live)
    await service._stop_overdue_runs(now=now)

    assert await service._a_stopped_run_is_still_unwinding(now=now + timedelta(seconds=60)) is True
    assert await service._a_stopped_run_is_still_unwinding(now=now + timedelta(seconds=121)) is False
