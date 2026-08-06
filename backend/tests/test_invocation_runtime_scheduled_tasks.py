"""Scheduled Task dispatch through the application InvocationRuntime."""

from __future__ import annotations

from datetime import UTC, datetime
from types import SimpleNamespace

import pytest

from app.runtime.invocation import InternalLaunchReceipt
from app.scheduler.service import ScheduledTaskService


class _TaskRepository:
    def __init__(self) -> None:
        self.updates: list[tuple[str, dict]] = []
        self.recovery_error: str | None = None

    async def update_after_launch(self, task_id: str, **updates) -> None:
        self.updates.append((task_id, updates))

    async def cancel_stuck_once_tasks(self, *, error: str) -> int:
        self.recovery_error = error
        return 1


class _TaskRunRepository:
    def __init__(self, *, active: bool = False) -> None:
        self.active = active
        self.created: list[dict] = []
        self.updated: list[tuple[str, dict]] = []
        self.recovery_error: str | None = None

    async def has_active_runs(self, _task_id: str) -> bool:
        return self.active

    async def create(self, **row) -> dict:
        self.created.append(row)
        return row

    async def update_status(self, task_run_id: str, **updates) -> None:
        self.updated.append((task_run_id, updates))

    async def mark_stale_active_runs(self, *, error: str) -> int:
        self.recovery_error = error
        return 1


class _RuntimeSpy:
    def __init__(self) -> None:
        self.intents = []

    async def launch(self, intent):
        self.intents.append(intent)
        return InternalLaunchReceipt(
            record=SimpleNamespace(
                run_id="run-1",
                thread_id=intent.thread_id,
            )
        )


def _task(*, context_mode: str = "reuse_thread") -> dict:
    return {
        "id": "task-1",
        "user_id": "owner-1",
        "thread_id": "thread-1",
        "context_mode": context_mode,
        "assistant_id": "report-agent",
        "prompt": "prepare report",
        "schedule_type": "cron",
        "schedule_spec": {"cron": "0 9 * * *"},
        "timezone": "UTC",
        "status": "enabled",
        "overlap_policy": "skip",
    }


@pytest.mark.anyio
async def test_scheduled_occurrence_enters_runtime_with_typed_execution_facts() -> None:
    runtime = _RuntimeSpy()
    task_runs = _TaskRunRepository()
    service = ScheduledTaskService(
        task_repo=_TaskRepository(),
        task_run_repo=task_runs,
        invocation_runtime=runtime,
        poll_interval_seconds=5,
        lease_seconds=30,
        max_concurrent_runs=2,
    )

    result = await service.dispatch_task(
        _task(),
        now=datetime(2026, 8, 6, tzinfo=UTC),
        trigger="scheduled",
    )

    task_run_id = task_runs.created[0]["run_record_id"]
    assert result == {
        "outcome": "launched",
        "task_run_id": task_run_id,
        "run_id": "run-1",
        "thread_id": "thread-1",
        "error": None,
    }
    assert len(runtime.intents) == 1
    intent = runtime.intents[0]
    assert intent.source_kind == "scheduled_task"
    assert intent.trusted_task_id == "task-1"
    assert intent.task_run_id == task_run_id
    assert intent.scheduled_trigger == "scheduled"
    assert intent.owner_user_id == "owner-1"
    assert intent.assistant_id == "report-agent"
    assert intent.thread_id == "thread-1"
    assert intent.input == {"messages": [{"role": "user", "content": "prepare report"}]}
    assert intent.context == {"non_interactive": True, "user_id": "owner-1"}
    assert intent.on_disconnect == "continue"
    assert intent.multitask_strategy == "reject"


@pytest.mark.anyio
async def test_manual_occurrence_uses_fresh_thread_and_preserves_manual_facts() -> None:
    runtime = _RuntimeSpy()
    tasks = _TaskRepository()
    task_runs = _TaskRunRepository()
    service = ScheduledTaskService(
        task_repo=tasks,
        task_run_repo=task_runs,
        invocation_runtime=runtime,
        poll_interval_seconds=5,
        lease_seconds=30,
        max_concurrent_runs=2,
    )
    task = _task(context_mode="fresh_thread_per_run")
    task["status"] = "paused"

    result = await service.dispatch_task(
        task,
        now=datetime(2026, 8, 6, tzinfo=UTC),
        trigger="manual",
    )

    assert result["outcome"] == "launched"
    assert result["thread_id"] != "thread-1"
    intent = runtime.intents[0]
    assert intent.thread_id == result["thread_id"]
    assert intent.scheduled_trigger == "manual"
    assert intent.task_run_id == result["task_run_id"]
    assert tasks.updates[-1][1]["status"] == "paused"


@pytest.mark.anyio
@pytest.mark.parametrize(
    ("trigger", "outcome", "creates_tombstone"),
    [
        ("scheduled", "skipped", True),
        ("manual", "conflict", False),
    ],
)
async def test_overlap_stops_before_runtime_and_only_scheduled_creates_tombstone(
    trigger: str,
    outcome: str,
    creates_tombstone: bool,
) -> None:
    runtime = _RuntimeSpy()
    task_runs = _TaskRunRepository(active=True)
    service = ScheduledTaskService(
        task_repo=_TaskRepository(),
        task_run_repo=task_runs,
        invocation_runtime=runtime,
        poll_interval_seconds=5,
        lease_seconds=30,
        max_concurrent_runs=2,
    )

    result = await service.dispatch_task(
        _task(),
        now=datetime(2026, 8, 6, tzinfo=UTC),
        trigger=trigger,
    )

    assert result["outcome"] == outcome
    assert runtime.intents == []
    assert bool(task_runs.created) is creates_tombstone
    if creates_tombstone:
        assert task_runs.created[0]["status"] == "skipped"
        assert task_runs.updated[0][1]["status"] == "skipped"
    else:
        assert result["task_run_id"] is None


@pytest.mark.anyio
async def test_startup_recovery_keeps_current_single_scheduler_guarantee() -> None:
    tasks = _TaskRepository()
    task_runs = _TaskRunRepository()

    class RecoveryOnlyService(ScheduledTaskService):
        async def _run_loop(self) -> None:
            await self._stop.wait()

    service = RecoveryOnlyService(
        task_repo=tasks,
        task_run_repo=task_runs,
        invocation_runtime=_RuntimeSpy(),
        poll_interval_seconds=5,
        lease_seconds=30,
        max_concurrent_runs=2,
    )

    await service.start()
    await service.stop()

    expected = "interrupted: gateway restarted before the run reached a terminal state"
    assert task_runs.recovery_error == expected
    assert tasks.recovery_error == expected
