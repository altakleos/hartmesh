import asyncio
from datetime import UTC, datetime, timedelta

import pytest
from support.scheduled_task_runtime import CallbackInvocationRuntime, NeverLaunchInvocationRuntime

from app.scheduler.service import ScheduledTaskService
from deerflow.runtime import ConflictError, RunStatus
from deerflow.runtime.runs.manager import RunRecord
from deerflow.runtime.runs.schemas import DisconnectMode


class DummyTaskRepo:
    def __init__(self, rows):
        self.rows = rows
        self.claimed = False
        self.updated = None
        self.release_calls = []
        self.cancelled_stuck_once = None
        self.reconciled_stuck_once = None

    async def cancel_stuck_once_tasks(self, *, error):
        self.cancelled_stuck_once = error
        return 0

    async def reconcile_stuck_once_tasks(self, **kwargs):
        self.reconciled_stuck_once = kwargs
        return 0

    async def claim_dispatch_lease(self, task_id, **_kwargs):
        return next((dict(row) for row in self.rows if row["id"] == task_id), None)

    async def release_queued_admission_lease(self, task_id):
        return False

    async def release_dispatch_lease(self, task_id, **kwargs):
        self.release_calls.append((task_id, kwargs))
        return True

    async def claim_due_tasks(self, **_kwargs):
        if self.claimed:
            return []
        self.claimed = True
        return self.rows

    async def update_after_launch(self, *args, **kwargs):
        self.updated = (args, kwargs)

    async def get(self, task_id: str, *, user_id: str):
        row = next((item for item in self.rows if item["id"] == task_id and item["user_id"] == user_id), None)
        return dict(row) if row is not None else None

    async def get_internal(self, task_id: str):
        row = next((item for item in self.rows if item["id"] == task_id), None)
        return dict(row) if row is not None else None

    async def update(self, task_id: str, *, user_id: str, updates):
        row = next((item for item in self.rows if item["id"] == task_id and item["user_id"] == user_id), None)
        if row is None:
            return None
        row.update(updates)
        return dict(row)


class DummyRunRepo:
    def __init__(self, *, active=False, active_count=0):
        self.created = None
        self.updated = []
        self.active = active
        self.active_count = active_count
        self.stale_marked = None
        self.reconciled = None
        self.reconcile_count = 0
        self.ended = True

    async def count_active_runs(self):
        return self.active_count

    async def list_queued_runs(self, *, limit):
        return []

    async def expire_queued_runs(self, **_kwargs):
        return []

    async def recover_expired_launch_claims(self, **_kwargs):
        return 0

    async def get_active_run(self, task_id):
        if not self.active:
            return None
        return {
            "id": "task-run-active",
            "task_id": task_id,
            "thread_id": "thread-active",
            "status": "running",
        }

    async def claim_queued_run(self, run_record_id, *, global_max_concurrent_runs, **_kwargs):
        if self.active_count >= global_max_concurrent_runs:
            return None
        return {"id": run_record_id, "status": "launching"}

    async def requeue_claimed_run(self, run_record_id, **kwargs):
        self.updated.append((run_record_id, {"status": "queued", **kwargs}))
        return True

    async def create(self, **kwargs):
        self.created = kwargs
        return {"id": kwargs["run_record_id"]}

    async def update_status(self, run_record_id, **kwargs):
        self.updated.append((run_record_id, kwargs))
        return True

    async def end_active_run(self, run_record_id, **kwargs):
        """The compare-and-set the hook and the time limit end an occurrence with; ``ended`` is what the store answers."""
        self.updated.append((run_record_id, kwargs))
        return self.ended

    async def reconcile_launched_run(self, run_record_id, **kwargs):
        self.updated.append((run_record_id, {"reconciled": True, **kwargs}))
        return True

    async def fail_launching_run(self, run_record_id, **kwargs):
        self.updated.append((run_record_id, {"status": "failed", **kwargs}))
        return True

    async def has_active_runs(self, task_id):
        return self.active

    async def mark_stale_active_runs(self, *, error):
        self.stale_marked = error
        return 0

    async def reconcile_active_runs(self, **kwargs):
        self.reconcile_count += 1
        self.reconciled = kwargs
        return 0


@pytest.mark.asyncio
async def test_service_claims_and_dispatches_due_task():
    async def fake_launch(**kwargs):
        assert kwargs["owner_user_id"] == "user-1"
        assert kwargs["metadata"]["scheduled_task_id"] == "task-1"
        assert kwargs["metadata"]["scheduled_trigger"] == "scheduled"
        return {"run_id": "run-1", "thread_id": kwargs["thread_id"]}

    task_repo = DummyTaskRepo(
        [
            {
                "id": "task-1",
                "user_id": "user-1",
                "thread_id": "thread-1",
                "context_mode": "reuse_thread",
                "assistant_id": "lead_agent",
                "prompt": "Summarize thread",
                "schedule_type": "once",
                "schedule_spec": {"run_at": "2026-07-02T01:00:00+00:00"},
                "timezone": "UTC",
            }
        ]
    )
    run_repo = DummyRunRepo()
    service = ScheduledTaskService(
        task_repo=task_repo,
        task_run_repo=run_repo,
        invocation_runtime=CallbackInvocationRuntime(fake_launch),
        poll_interval_seconds=5,
        lease_seconds=120,
        max_concurrent_runs=3,
    )

    await service.run_once(now=datetime.now(UTC) + timedelta(days=1))

    assert run_repo.created["task_id"] == "task-1"
    assert run_repo.updated[0][1]["status"] == "running"
    assert run_repo.updated[0][1]["protect_terminal"] is True
    # `once` terminal status is owned by handle_run_completion, not the launch.
    assert task_repo.updated[1]["status"] == "running"


@pytest.mark.asyncio
async def test_manual_trigger_keeps_paused_cron_task_paused():
    async def fake_launch(**kwargs):
        return {"run_id": "run-2", "thread_id": kwargs["thread_id"]}

    task_repo = DummyTaskRepo(
        [
            {
                "id": "task-2",
                "user_id": "user-1",
                "thread_id": "thread-1",
                "context_mode": "reuse_thread",
                "assistant_id": "lead_agent",
                "prompt": "Summarize thread",
                "schedule_type": "cron",
                "schedule_spec": {"cron": "0 9 * * *"},
                "timezone": "UTC",
                "status": "paused",
            }
        ]
    )
    run_repo = DummyRunRepo()
    service = ScheduledTaskService(
        task_repo=task_repo,
        task_run_repo=run_repo,
        invocation_runtime=CallbackInvocationRuntime(fake_launch),
        poll_interval_seconds=5,
        lease_seconds=120,
        max_concurrent_runs=3,
    )

    await service.dispatch_task(
        task_repo.rows[0],
        now=datetime.now(UTC),
        trigger="manual",
    )

    assert task_repo.updated[1]["status"] == "paused"


@pytest.mark.asyncio
async def test_fresh_thread_per_run_creates_new_execution_thread():
    async def fake_launch(**kwargs):
        assert kwargs["thread_id"] != "thread-template"
        return {"run_id": "run-3", "thread_id": kwargs["thread_id"]}

    task_repo = DummyTaskRepo(
        [
            {
                "id": "task-3",
                "user_id": "user-1",
                "thread_id": "thread-template",
                "context_mode": "fresh_thread_per_run",
                "assistant_id": "lead_agent",
                "prompt": "Summarize thread",
                "schedule_type": "cron",
                "schedule_spec": {"cron": "0 9 * * *"},
                "timezone": "UTC",
                "status": "enabled",
            }
        ]
    )
    run_repo = DummyRunRepo()
    service = ScheduledTaskService(
        task_repo=task_repo,
        task_run_repo=run_repo,
        invocation_runtime=CallbackInvocationRuntime(fake_launch),
        poll_interval_seconds=5,
        lease_seconds=120,
        max_concurrent_runs=3,
    )

    await service.dispatch_task(
        task_repo.rows[0],
        now=datetime.now(UTC),
        trigger="scheduled",
    )

    assert run_repo.created["thread_id"] != "thread-template"
    assert task_repo.updated[1]["last_thread_id"] == run_repo.created["thread_id"]


@pytest.mark.asyncio
async def test_scheduled_overlap_conflict_is_kept_in_queue():
    async def fake_launch(**_kwargs):
        raise ConflictError("Thread thread-1 already has an active run")

    task_repo = DummyTaskRepo(
        [
            {
                "id": "task-4",
                "user_id": "user-1",
                "thread_id": "thread-1",
                "context_mode": "reuse_thread",
                "assistant_id": "lead_agent",
                "prompt": "Summarize thread",
                "schedule_type": "cron",
                "schedule_spec": {"cron": "0 9 * * *"},
                "timezone": "UTC",
                "status": "running",
                "overlap_policy": "enqueue",
                "last_run_id": "run-old",
                "last_thread_id": "thread-1",
                "last_run_at": "2026-07-01T00:00:00+00:00",
            }
        ]
    )
    run_repo = DummyRunRepo()
    service = ScheduledTaskService(
        task_repo=task_repo,
        task_run_repo=run_repo,
        invocation_runtime=CallbackInvocationRuntime(fake_launch),
        poll_interval_seconds=5,
        lease_seconds=120,
        max_concurrent_runs=3,
    )

    result = await service.dispatch_task(
        task_repo.rows[0],
        now=datetime.now(UTC),
        trigger="scheduled",
    )

    assert result["outcome"] == "queued"
    assert run_repo.created["status"] == "queued"
    assert run_repo.updated[-1][1]["status"] == "queued"
    assert task_repo.updated is None


@pytest.mark.asyncio
async def test_manual_overlap_conflict_is_kept_in_queue():
    async def fake_launch(**_kwargs):
        raise ConflictError("Thread thread-1 already has an active run")

    task_repo = DummyTaskRepo(
        [
            {
                "id": "task-5",
                "user_id": "user-1",
                "thread_id": "thread-1",
                "context_mode": "reuse_thread",
                "assistant_id": "lead_agent",
                "prompt": "Summarize thread",
                "schedule_type": "cron",
                "schedule_spec": {"cron": "0 9 * * *"},
                "timezone": "UTC",
                "status": "enabled",
                "overlap_policy": "enqueue",
            }
        ]
    )
    run_repo = DummyRunRepo()
    service = ScheduledTaskService(
        task_repo=task_repo,
        task_run_repo=run_repo,
        invocation_runtime=CallbackInvocationRuntime(fake_launch),
        poll_interval_seconds=5,
        lease_seconds=120,
        max_concurrent_runs=3,
    )

    result = await service.dispatch_task(
        task_repo.rows[0],
        now=datetime.now(UTC),
        trigger="manual",
    )

    assert result["outcome"] == "queued"
    assert run_repo.updated[-1][1]["status"] == "queued"
    assert task_repo.release_calls == []


@pytest.mark.asyncio
async def test_dispatch_task_records_failure_for_legacy_invalid_thread_id():
    """Rows persisted before the thread-id contract was centralized may store
    IDs that fail the canonical pattern (dots, >64 chars). Dispatch must record
    the failure through normal bookkeeping instead of raising — an uncaught
    ValueError surfaces as HTTP 500 on manual trigger and, in the poller,
    aborts the rest of the claimed batch every cycle."""

    async def fake_launch(**_kwargs):
        raise AssertionError("InvocationRuntime must not be called for an invalid thread_id")

    task_repo = DummyTaskRepo(
        [
            {
                "id": "task-legacy",
                "user_id": "user-1",
                "thread_id": "thread.with.dot",
                "context_mode": "reuse_thread",
                "assistant_id": "lead_agent",
                "prompt": "Summarize thread",
                "schedule_type": "cron",
                "schedule_spec": {"cron": "0 9 * * *"},
                "timezone": "UTC",
                "status": "enabled",
            }
        ]
    )
    run_repo = DummyRunRepo()
    service = ScheduledTaskService(
        task_repo=task_repo,
        task_run_repo=run_repo,
        invocation_runtime=CallbackInvocationRuntime(fake_launch),
        poll_interval_seconds=5,
        lease_seconds=120,
        max_concurrent_runs=3,
    )

    result = await service.dispatch_task(
        task_repo.rows[0],
        now=datetime.now(UTC),
        trigger="scheduled",
    )

    assert result["outcome"] == "failed"
    assert result["task_run_id"] is None
    assert result["run_id"] is None
    assert "Invalid thread_id" in result["error"]
    assert run_repo.created is None
    assert task_repo.updated[1]["last_error"] == result["error"]
    assert task_repo.updated[1]["last_thread_id"] == "thread.with.dot"
    assert task_repo.updated[1]["increment_run_count"] is False


@pytest.mark.asyncio
async def test_run_once_continues_batch_after_invalid_thread_id():
    """A poison legacy row must not prevent later claimed tasks from dispatching."""
    launched = []

    async def fake_launch(**kwargs):
        launched.append(kwargs)
        return {"run_id": "run-ok", "thread_id": kwargs["thread_id"]}

    task_repo = DummyTaskRepo(
        [
            {
                "id": "task-legacy",
                "user_id": "user-1",
                "thread_id": "thread.with.dot",
                "context_mode": "reuse_thread",
                "assistant_id": "lead_agent",
                "prompt": "Summarize thread",
                "schedule_type": "cron",
                "schedule_spec": {"cron": "0 9 * * *"},
                "timezone": "UTC",
                "status": "enabled",
            },
            {
                "id": "task-valid",
                "user_id": "user-1",
                "thread_id": "thread-ok",
                "context_mode": "reuse_thread",
                "assistant_id": "lead_agent",
                "prompt": "Summarize thread",
                "schedule_type": "cron",
                "schedule_spec": {"cron": "0 9 * * *"},
                "timezone": "UTC",
                "status": "enabled",
            },
        ]
    )
    run_repo = DummyRunRepo()
    service = ScheduledTaskService(
        task_repo=task_repo,
        task_run_repo=run_repo,
        invocation_runtime=CallbackInvocationRuntime(fake_launch),
        poll_interval_seconds=5,
        lease_seconds=120,
        max_concurrent_runs=3,
    )

    await service.run_once(now=datetime.now(UTC))

    assert len(launched) == 1
    assert launched[0]["thread_id"] == "thread-ok"


@pytest.mark.asyncio
async def test_handle_run_completion_persists_success():
    task_repo = DummyTaskRepo(
        [
            {
                "id": "task-6",
                "user_id": "user-1",
                "thread_id": None,
                "context_mode": "fresh_thread_per_run",
                "assistant_id": "lead_agent",
                "prompt": "Summarize thread",
                "schedule_type": "cron",
                "schedule_spec": {"cron": "0 9 * * *"},
                "timezone": "UTC",
                "status": "enabled",
            }
        ]
    )
    run_repo = DummyRunRepo()
    service = ScheduledTaskService(
        task_repo=task_repo,
        task_run_repo=run_repo,
        invocation_runtime=NeverLaunchInvocationRuntime(),
        poll_interval_seconds=5,
        lease_seconds=120,
        max_concurrent_runs=3,
    )

    record = RunRecord(
        run_id="run-6",
        thread_id="thread-6",
        assistant_id="lead_agent",
        status=RunStatus.success,
        on_disconnect=DisconnectMode.continue_,
        metadata={
            "scheduled_task_id": "task-6",
            "scheduled_task_run_id": "task-run-6",
        },
        user_id="user-1",
    )

    await service.handle_run_completion(record)

    assert run_repo.updated[-1][0] == "task-run-6"
    assert run_repo.updated[-1][1]["status"] == "success"
    assert task_repo.rows[0]["last_error"] is None


def _make_service(task_repo, run_repo, **kwargs):
    return ScheduledTaskService(
        task_repo=task_repo,
        task_run_repo=run_repo,
        invocation_runtime=NeverLaunchInvocationRuntime(),
        poll_interval_seconds=5,
        lease_seconds=120,
        max_concurrent_runs=3,
        **kwargs,
    )


def _once_task_row(task_id="task-once", status="running"):
    return {
        "id": task_id,
        "user_id": "user-1",
        "thread_id": None,
        "context_mode": "fresh_thread_per_run",
        "assistant_id": "lead_agent",
        "prompt": "Summarize thread",
        "schedule_type": "once",
        "schedule_spec": {"run_at": "2026-07-02T01:00:00+00:00"},
        "timezone": "UTC",
        "status": status,
    }


def _completion_record(status, *, task_id="task-once", error=None, stop_reason=None):
    record = RunRecord(
        run_id="run-x",
        thread_id="thread-x",
        assistant_id="lead_agent",
        status=status,
        on_disconnect=DisconnectMode.continue_,
        metadata={
            "scheduled_task_id": task_id,
            "scheduled_task_run_id": "task-run-x",
        },
        user_id="user-1",
        error=error,
    )
    record.stop_reason = stop_reason
    return record


@pytest.mark.asyncio
async def test_once_task_completes_only_via_completion_hook():
    task_repo = DummyTaskRepo([_once_task_row()])
    run_repo = DummyRunRepo()
    service = _make_service(task_repo, run_repo)

    await service.handle_run_completion(_completion_record(RunStatus.success))

    assert run_repo.updated[-1][1]["status"] == "success"
    assert task_repo.rows[0]["status"] == "completed"


@pytest.mark.asyncio
async def test_once_task_failed_run_marks_task_failed():
    task_repo = DummyTaskRepo([_once_task_row()])
    run_repo = DummyRunRepo()
    service = _make_service(task_repo, run_repo)

    await service.handle_run_completion(_completion_record(RunStatus.error, error="boom"))

    assert run_repo.updated[-1][1]["status"] == "failed"
    assert run_repo.updated[-1][1]["error"] == "boom"
    assert task_repo.rows[0]["status"] == "failed"
    assert task_repo.rows[0]["last_error"] == "boom"


@pytest.mark.asyncio
async def test_a_run_whose_sandbox_was_refused_is_a_failed_occurrence_not_a_completed_one():
    """Nobody reads an unattended answer, so a refusal must show in the task list.

    A sandbox tool refused for capacity ends the run as ``success`` with a
    recorded stop reason: the agent says the workspace is busy and stops. For
    a scheduled task that answer is the only trace, so the occurrence is failed
    and says why, instead of reading as a clean completion.
    """
    from deerflow.runtime.runs.worker import SANDBOX_CAPACITY_STOP_REASON

    task_repo = DummyTaskRepo([_once_task_row()])
    run_repo = DummyRunRepo()
    service = _make_service(task_repo, run_repo)

    await service.handle_run_completion(_completion_record(RunStatus.success, stop_reason=SANDBOX_CAPACITY_STOP_REASON))

    run_update = run_repo.updated[-1][1]
    assert run_update["status"] == "failed"
    assert "sandbox" in run_update["error"]
    assert task_repo.rows[0]["status"] == "failed"
    assert task_repo.rows[0]["last_error"] == run_update["error"]


_STOPPED_EARLY_CODES = [
    "turn_budget_exhausted",
    "tool_attempt_budget_exhausted",
    "sandbox_operation_budget_exhausted",
    "sandbox_runtime_budget_exhausted",
    "retrieval_budget_exhausted",
    "repeated_tool_loop",
    "no_progress_loop",
    "loop_capped",
    "token_capped",
    "safety_capped",
    "model_length_capped",
]


@pytest.mark.asyncio
@pytest.mark.parametrize("reason", [*_STOPPED_EARLY_CODES, "subagent_limit_capped", "batch_item_budget_exhausted", "an_extension_guard"])
async def test_a_run_a_limit_cut_short_is_a_failed_occurrence_not_a_completed_one(reason):
    """A run that hit a bound ends ``success`` with the bound's reason; nobody reads its answer.

    The tenant reads the task list, so an occurrence the runtime stopped at a
    turn, tool or loop limit says so instead of reading as a clean completion.
    """
    task_repo = DummyTaskRepo([_once_task_row()])
    run_repo = DummyRunRepo()
    service = _make_service(task_repo, run_repo)

    await service.handle_run_completion(_completion_record(RunStatus.success, stop_reason=reason))

    run_update = run_repo.updated[-1][1]
    assert run_update["status"] == "failed"
    assert "before it finished" in run_update["error"]
    assert task_repo.rows[0]["status"] == "failed"
    assert task_repo.rows[0]["last_error"] == run_update["error"]


@pytest.mark.asyncio
@pytest.mark.parametrize("reason", [*_STOPPED_EARLY_CODES, "subagent_limit_capped", "batch_item_budget_exhausted", "an_extension_guard"])
async def test_what_the_owner_reads_about_a_stopped_run_carries_no_internal_code(reason):
    """The task list shows this text to a business owner: words, never ``turn_budget_exhausted``."""
    task_repo = DummyTaskRepo([_once_task_row()])
    run_repo = DummyRunRepo()

    await _make_service(task_repo, run_repo).handle_run_completion(_completion_record(RunStatus.success, stop_reason=reason))

    error = run_repo.updated[-1][1]["error"]
    assert "_" not in error, error
    assert reason not in error


@pytest.mark.asyncio
async def test_each_known_limit_says_which_limit_it_was():
    reads = {}
    for reason in _STOPPED_EARLY_CODES:
        run_repo = DummyRunRepo()
        await _make_service(DummyTaskRepo([_once_task_row()]), run_repo).handle_run_completion(_completion_record(RunStatus.success, stop_reason=reason))
        reads[reason] = run_repo.updated[-1][1]["error"]

    # The same words for the same kind of limit, different words for a different one.
    assert reads["loop_capped"] == reads["repeated_tool_loop"]
    assert len(set(reads.values())) >= 7, reads
    # A limit with no phrase of its own still reads as a stop, with no code in it.
    generic = DummyRunRepo()
    await _make_service(DummyTaskRepo([_once_task_row()]), generic).handle_run_completion(_completion_record(RunStatus.success, stop_reason="an_extension_guard"))
    assert generic.updated[-1][1]["error"] == "the task stopped before it finished"


@pytest.mark.asyncio
async def test_a_run_that_finished_without_a_stop_reason_is_still_a_success():
    task_repo = DummyTaskRepo([_once_task_row()])
    run_repo = DummyRunRepo()
    service = _make_service(task_repo, run_repo)

    await service.handle_run_completion(_completion_record(RunStatus.success))

    assert run_repo.updated[-1][1]["status"] == "success"
    assert run_repo.updated[-1][1]["error"] is None


@pytest.mark.asyncio
@pytest.mark.parametrize(("status", "error", "expected"), [(RunStatus.error, "boom", "failed"), (RunStatus.interrupted, None, "interrupted")])
async def test_a_stop_reason_on_a_run_that_did_not_succeed_does_not_change_how_it_ended(status, error, expected):
    task_repo = DummyTaskRepo([_once_task_row()])
    run_repo = DummyRunRepo()

    await _make_service(task_repo, run_repo).handle_run_completion(_completion_record(status, error=error, stop_reason="loop_capped"))

    assert run_repo.updated[-1][1]["status"] == expected


@pytest.mark.asyncio
async def test_a_completion_the_scheduler_already_ended_the_occurrence_for_changes_nothing_more():
    """The occurrence was ended once, by whoever got there first; the run's later completion does not rewrite it or its task."""
    task_repo = DummyTaskRepo([_once_task_row(status="failed")])
    task_repo.rows[0]["last_error"] = "the task did not finish within 15 minutes, so it was stopped"
    run_repo = DummyRunRepo()
    run_repo.ended = False
    service = _make_service(task_repo, run_repo)

    await service.handle_run_completion(_completion_record(RunStatus.interrupted))

    assert task_repo.rows[0]["status"] == "failed"
    assert task_repo.rows[0]["last_error"] == "the task did not finish within 15 minutes, so it was stopped"


class _OverdueRunRepo(DummyRunRepo):
    """Occurrences that are still ``running`` past the cutoff the service asks about."""

    def __init__(self, overdue):
        super().__init__()
        self.overdue = overdue
        self.asked: list[tuple[datetime, int]] = []
        self.list_error: Exception | None = None

    async def list_overdue_running(self, *, started_before, limit):
        self.asked.append((started_before, limit))
        if self.list_error is not None:
            raise self.list_error
        return [dict(row) for row in self.overdue]


class _StopRecorder:
    def __init__(self, *, fail_for: tuple[str, ...] = (), hang_for: tuple[str, ...] = ()):
        self.stopped: list[str] = []
        self._fail_for = fail_for
        self._hang_for = hang_for

    async def __call__(self, run_id):
        if run_id in self._hang_for:
            await asyncio.sleep(3600)
        if run_id in self._fail_for:
            raise RuntimeError("the run manager could not be reached")
        self.stopped.append(run_id)


def _service_that_stops(run_repo, stop, *, max_run_seconds=900, rows=()):
    task_repo = DummyTaskRepo(list(rows))
    task_repo.claimed = True
    service = ScheduledTaskService(
        task_repo=task_repo,
        task_run_repo=run_repo,
        invocation_runtime=NeverLaunchInvocationRuntime(),
        poll_interval_seconds=5,
        lease_seconds=120,
        max_concurrent_runs=1,
        max_run_seconds=max_run_seconds,
        stop_run=stop,
    )
    return service, task_repo


def _occurrence(index, *, task_id=None, run_id="unset"):
    return {"id": f"task-run-{index}", "task_id": task_id or f"task-{index}", "run_id": f"run-{index}" if run_id == "unset" else run_id}


_TIME_LIMIT_ERROR = "the task did not finish within 15 minutes, so it was stopped"


@pytest.mark.asyncio
async def test_a_poll_ends_the_occurrence_that_ran_past_its_limit_and_then_stops_its_run():
    now = datetime(2026, 9, 30, 9, 0, tzinfo=UTC)
    run_repo = _OverdueRunRepo([_occurrence(1), _occurrence(2)])
    stop = _StopRecorder()
    service, _ = _service_that_stops(run_repo, stop)

    await service.run_once(now=now)

    assert run_repo.asked == [(now - timedelta(seconds=900), 16)]
    ended = [(run_id, update) for run_id, update in run_repo.updated if update.get("status") == "failed"]
    assert [run_id for run_id, _ in ended] == ["task-run-1", "task-run-2"]
    assert all(update["error"] == _TIME_LIMIT_ERROR and update["finished_at"] == now for _, update in ended)
    assert stop.stopped == ["run-1", "run-2"]


@pytest.mark.asyncio
async def test_the_occurrence_is_ended_before_its_run_is_asked_to_stop():
    """The outcome is decided before the Stop lands, so the run's own completion cannot decide it first."""
    order: list[str] = []

    class _Repo(_OverdueRunRepo):
        async def end_active_run(self, run_record_id, **kwargs):
            order.append("ended")
            return await super().end_active_run(run_record_id, **kwargs)

    async def stop(run_id):
        order.append("stopped")

    service, _ = _service_that_stops(_Repo([_occurrence(1)]), stop)

    await service.run_once(now=datetime(2026, 9, 30, 9, 0, tzinfo=UTC))

    assert order == ["ended", "stopped"]


@pytest.mark.asyncio
async def test_a_run_that_finished_first_keeps_its_own_outcome_and_is_not_stopped():
    run_repo = _OverdueRunRepo([_occurrence(1, task_id="task-once")])
    run_repo.ended = False
    stop = _StopRecorder()
    service, task_repo = _service_that_stops(run_repo, stop, rows=[_once_task_row(task_id="task-once")])

    await service.run_once(now=datetime(2026, 9, 30, 9, 0, tzinfo=UTC))

    assert stop.stopped == []
    assert task_repo.rows[0]["status"] == "running", "the task was not touched"


@pytest.mark.asyncio
async def test_a_once_task_whose_run_ran_past_its_limit_ends_failed_with_the_reason():
    run_repo = _OverdueRunRepo([_occurrence(1, task_id="task-once")])
    service, task_repo = _service_that_stops(run_repo, _StopRecorder(), rows=[_once_task_row(task_id="task-once")])

    await service.run_once(now=datetime(2026, 9, 30, 9, 0, tzinfo=UTC))

    assert task_repo.rows[0]["status"] == "failed"
    assert task_repo.rows[0]["last_error"] == _TIME_LIMIT_ERROR


@pytest.mark.asyncio
async def test_a_recurring_task_whose_run_ran_past_its_limit_stays_enabled_and_says_why():
    row = _once_task_row(task_id="task-cron")
    row.update({"schedule_type": "cron", "schedule_spec": {"cron": "0 9 * * *"}, "status": "enabled"})
    service, task_repo = _service_that_stops(_OverdueRunRepo([_occurrence(1, task_id="task-cron")]), _StopRecorder(), rows=[row])

    await service.run_once(now=datetime(2026, 9, 30, 9, 0, tzinfo=UTC))

    assert task_repo.rows[0]["status"] == "enabled"
    assert task_repo.rows[0]["last_error"] == _TIME_LIMIT_ERROR


@pytest.mark.asyncio
async def test_a_poll_without_a_time_limit_asks_for_no_overdue_runs_and_stops_none():
    run_repo = _OverdueRunRepo([_occurrence(1)])
    stop = _StopRecorder()
    service, _ = _service_that_stops(run_repo, stop, max_run_seconds=None)

    await service.run_once(now=datetime(2026, 9, 30, 9, 0, tzinfo=UTC))

    assert run_repo.asked == []
    assert stop.stopped == []


@pytest.mark.asyncio
async def test_one_run_that_cannot_be_stopped_does_not_keep_the_others_running_or_the_poll_from_finishing():
    run_repo = _OverdueRunRepo([_occurrence(1), _occurrence(2)])
    stop = _StopRecorder(fail_for=("run-1",))
    service, _ = _service_that_stops(run_repo, stop)

    await service.run_once(now=datetime(2026, 9, 30, 9, 0, tzinfo=UTC))

    assert stop.stopped == ["run-2"]
    assert {run_id for run_id, update in run_repo.updated if update.get("status") == "failed"} == {"task-run-1", "task-run-2"}


@pytest.mark.asyncio
async def test_a_stop_that_never_answers_does_not_hold_up_the_poll(monkeypatch):
    import app.scheduler.service as scheduler_service

    monkeypatch.setattr(scheduler_service, "_STOP_REQUEST_TIMEOUT_SECONDS", 0.05)
    run_repo = _OverdueRunRepo([_occurrence(1), _occurrence(2)])
    stop = _StopRecorder(hang_for=("run-1",))
    service, _ = _service_that_stops(run_repo, stop)

    await asyncio.wait_for(service.run_once(now=datetime(2026, 9, 30, 9, 0, tzinfo=UTC)), timeout=5)

    assert stop.stopped == ["run-2"]


@pytest.mark.asyncio
async def test_a_failing_look_for_overdue_runs_does_not_stop_the_poll_from_dispatching_what_is_due():
    """The bound is one duty of the poll; a store error there must not stop the others."""
    run_repo = _OverdueRunRepo([])
    run_repo.list_error = RuntimeError("database is locked")
    service, task_repo = _service_that_stops(run_repo, _StopRecorder())
    task_repo.claimed = False

    await service.run_once(now=datetime(2026, 9, 30, 9, 0, tzinfo=UTC))

    assert task_repo.claimed is True, "due tasks were still claimed"


@pytest.mark.asyncio
async def test_a_task_write_that_fails_does_not_keep_the_run_from_being_stopped():
    class _Tasks(DummyTaskRepo):
        async def update(self, *args, **kwargs):
            raise RuntimeError("database is locked")

    stop = _StopRecorder()
    service, _ = _service_that_stops(_OverdueRunRepo([_occurrence(1, task_id="task-once")]), stop, rows=[])
    service._task_repo = _Tasks([_once_task_row(task_id="task-once")])
    service._task_repo.claimed = True

    await service.run_once(now=datetime(2026, 9, 30, 9, 0, tzinfo=UTC))

    assert stop.stopped == ["run-1"]


@pytest.mark.asyncio
async def test_what_the_run_manager_answers_to_the_stop_is_logged(caplog):
    class _Answer:
        value = "not_active_locally"

    async def stop(run_id):
        return _Answer()

    service, _ = _service_that_stops(_OverdueRunRepo([_occurrence(1)]), stop)

    with caplog.at_level("INFO", logger="app.scheduler.service"):
        await service.run_once(now=datetime(2026, 9, 30, 9, 0, tzinfo=UTC))

    assert any("answered not_active_locally" in record.getMessage() and "run-1" in record.getMessage() for record in caplog.records)


@pytest.mark.parametrize(("seconds", "words"), [(300, "5 minutes"), (60, "1 minute"), (90, "90 seconds"), (3600, "1 hour"), (86400, "24 hours"), (7200, "2 hours")])
def test_a_bound_reads_in_the_largest_whole_unit(seconds, words):
    from app.scheduler.service import _duration_words

    assert _duration_words(seconds) == words


@pytest.mark.asyncio
async def test_an_occurrence_whose_run_was_never_recorded_is_left_to_recovery():
    run_repo = _OverdueRunRepo([_occurrence(1, run_id=None)])
    stop = _StopRecorder()
    service, _ = _service_that_stops(run_repo, stop)

    await service.run_once(now=datetime(2026, 9, 30, 9, 0, tzinfo=UTC))

    assert stop.stopped == []
    assert [update for _, update in run_repo.updated if update.get("status") == "failed"] == []


@pytest.mark.asyncio
async def test_interrupted_run_is_distinct_and_cancels_once_task():
    task_repo = DummyTaskRepo([_once_task_row()])
    run_repo = DummyRunRepo()
    service = _make_service(task_repo, run_repo)

    await service.handle_run_completion(_completion_record(RunStatus.interrupted))

    run_update = run_repo.updated[-1][1]
    assert run_update["status"] == "interrupted"
    assert run_update["error"] == "run was interrupted before completion"
    assert task_repo.rows[0]["status"] == "cancelled"


@pytest.mark.asyncio
async def test_interrupted_cron_run_keeps_task_enabled():
    row = _once_task_row(task_id="task-cron")
    row.update({"schedule_type": "cron", "schedule_spec": {"cron": "0 9 * * *"}, "status": "enabled"})
    task_repo = DummyTaskRepo([row])
    run_repo = DummyRunRepo()
    service = _make_service(task_repo, run_repo)

    await service.handle_run_completion(_completion_record(RunStatus.interrupted, task_id="task-cron"))

    assert run_repo.updated[-1][1]["status"] == "interrupted"
    assert task_repo.rows[0]["status"] == "enabled"


@pytest.mark.asyncio
async def test_existing_running_occurrence_blocks_duplicate_fresh_thread_run():
    launched = []

    async def fake_launch(**kwargs):
        launched.append(kwargs)
        return {"run_id": "run-9", "thread_id": kwargs["thread_id"]}

    row = _once_task_row(task_id="task-9")
    row.update({"schedule_type": "cron", "schedule_spec": {"cron": "* * * * *"}, "status": "running", "overlap_policy": "enqueue"})
    task_repo = DummyTaskRepo([row])
    run_repo = DummyRunRepo(active=True)
    service = ScheduledTaskService(
        task_repo=task_repo,
        task_run_repo=run_repo,
        invocation_runtime=CallbackInvocationRuntime(fake_launch),
        poll_interval_seconds=5,
        lease_seconds=120,
        max_concurrent_runs=3,
    )

    result = await service.dispatch_task(row, now=datetime.now(UTC), trigger="scheduled")

    assert result["outcome"] == "conflict"
    assert launched == []
    assert run_repo.created is None


@pytest.mark.asyncio
async def test_startup_sweep_reconciles_stale_runs_and_stuck_once_tasks():
    task_repo = DummyTaskRepo([])
    run_repo = DummyRunRepo()
    service = _make_service(task_repo, run_repo)

    await service.start()
    await service.stop()

    assert run_repo.stale_marked is not None
    assert task_repo.cancelled_stuck_once == run_repo.stale_marked


@pytest.mark.asyncio
async def test_multi_instance_start_uses_lease_aware_reconciliation():
    task_repo = DummyTaskRepo([])
    run_repo = DummyRunRepo()
    service = ScheduledTaskService(
        task_repo=task_repo,
        task_run_repo=run_repo,
        invocation_runtime=NeverLaunchInvocationRuntime(),
        poll_interval_seconds=5,
        lease_seconds=120,
        max_concurrent_runs=3,
        multi_instance=True,
        run_lease_grace_seconds=17,
    )

    await service.start()
    await asyncio.sleep(0)
    await service.stop()

    assert run_repo.reconcile_count == 1
    assert run_repo.reconciled is not None
    assert run_repo.reconciled["lease_grace_seconds"] == 17
    assert task_repo.reconciled_stuck_once is not None
    assert task_repo.reconciled_stuck_once["lease_grace_seconds"] == 17
    assert task_repo.cancelled_stuck_once is None


@pytest.mark.asyncio
async def test_manual_trigger_with_active_run_returns_conflict_without_launching():
    launched = []

    async def fake_launch(**kwargs):
        launched.append(kwargs)
        return {"run_id": "run-x", "thread_id": kwargs["thread_id"]}

    row = _once_task_row(task_id="task-manual-busy")
    row.update({"schedule_type": "cron", "schedule_spec": {"cron": "* * * * *"}, "status": "enabled", "overlap_policy": "enqueue"})
    task_repo = DummyTaskRepo([row])
    run_repo = DummyRunRepo(active=True)
    service = ScheduledTaskService(
        task_repo=task_repo,
        task_run_repo=run_repo,
        invocation_runtime=CallbackInvocationRuntime(fake_launch),
        poll_interval_seconds=5,
        lease_seconds=120,
        max_concurrent_runs=3,
    )

    result = await service.dispatch_task(row, now=datetime.now(UTC), trigger="manual")

    assert result["outcome"] == "conflict"
    assert launched == []
    # Nothing was scheduled to happen, so no run-history row is recorded.
    assert run_repo.created is None
    assert result["task_run_id"] is None


@pytest.mark.asyncio
async def test_run_once_admits_due_occurrences_independently_of_execution_budget():
    claim_limits = []

    class BudgetTaskRepo(DummyTaskRepo):
        async def claim_due_tasks(self, **kwargs):
            claim_limits.append(kwargs["limit"])
            return []

    task_repo = BudgetTaskRepo([])
    run_repo = DummyRunRepo(active_count=2)
    service = _make_service(task_repo, run_repo)

    await service.run_once(now=datetime.now(UTC))
    assert claim_limits == [3]

    run_repo.active_count = 3
    await service.run_once(now=datetime.now(UTC))
    assert claim_limits == [3, 3]


@pytest.mark.asyncio
async def test_launch_bookkeeping_passes_protect_terminal():
    async def fake_launch(**kwargs):
        return {"run_id": "run-pt", "thread_id": kwargs["thread_id"]}

    task_repo = DummyTaskRepo([_once_task_row(task_id="task-pt", status="enabled")])
    run_repo = DummyRunRepo()
    service = ScheduledTaskService(
        task_repo=task_repo,
        task_run_repo=run_repo,
        invocation_runtime=CallbackInvocationRuntime(fake_launch),
        poll_interval_seconds=5,
        lease_seconds=120,
        max_concurrent_runs=3,
    )

    await service.dispatch_task(task_repo.rows[0], now=datetime.now(UTC), trigger="scheduled")

    assert task_repo.updated[1]["protect_terminal"] is True


class _StatefulRunRepo:
    """Stateful fake ``ScheduledTaskRunRepository`` for the #4452 tests.

    Mirrors just enough of the real repository to let a second dispatch
    observe the active slot held by the first:

      * ``create()`` tracks each row by id, carrying its ``status`` and
        ``run_id``;
      * with ``fail_first_update=True`` the very FIRST ``update_status()``
        call raises, simulating a transient DB failure on the
        ``queued -> running`` write that fires right after ``InvocationRuntime``
        returns a live ``run_id``; every later ``update_status()`` applies;
      * ``has_active_runs()`` reflects whether any tracked row for the task
        is still in an active status (``queued``/``running``), exactly like
        the partial unique index ``uq_scheduled_task_run_active``.
    """

    _ACTIVE = {"queued", "launching", "running"}

    def __init__(self, *, fail_first_update: bool = False, fail_updates: int = 0) -> None:
        self.created: list[dict] = []
        self.updates: list[tuple[str, dict]] = []
        self.rows: dict[str, dict] = {}
        self._fail_updates = max(fail_updates, 1 if fail_first_update else 0)
        self._updates_raised = 0

    async def count_active_runs(self) -> int:
        return sum(1 for row in self.rows.values() if row["status"] in {"launching", "running"})

    async def list_queued_runs(self, *, limit: int) -> list[dict]:
        return []

    async def expire_queued_runs(self, **_kwargs) -> list[dict]:
        return []

    async def create(self, **kwargs) -> dict:
        self.created.append(kwargs)
        self.rows[kwargs["run_record_id"]] = {
            "id": kwargs["run_record_id"],
            "task_id": kwargs["task_id"],
            "thread_id": kwargs["thread_id"],
            "trigger": kwargs["trigger"],
            "status": kwargs["status"],
            "run_id": None,
        }
        return {"id": kwargs["run_record_id"]}

    async def get_active_run(self, task_id: str) -> dict | None:
        return next(
            (dict(row) for row in self.rows.values() if row["task_id"] == task_id and row["status"] in self._ACTIVE),
            None,
        )

    async def claim_queued_run(self, run_record_id: str, **_kwargs) -> dict | None:
        row = self.rows.get(run_record_id)
        if row is None or row["status"] != "queued":
            return None
        row["status"] = "launching"
        return dict(row)

    async def requeue_claimed_run(self, run_record_id: str, **_kwargs) -> bool:
        row = self.rows.get(run_record_id)
        if row is None or row["status"] != "launching":
            return False
        row["status"] = "queued"
        return True

    async def update_status(self, run_record_id: str, **kwargs) -> bool:
        self.updates.append((run_record_id, kwargs))
        if self._updates_raised < self._fail_updates:
            # The launch-path queued->running write fails AFTER _launch_run has
            # already returned a live run_id. Some tests fail both attempts to
            # pin the last-resort active-slot behavior.
            self._updates_raised += 1
            raise RuntimeError("simulated transient DB error on queued->running write")
        row = self.rows.get(run_record_id)
        if row is None:
            return False
        if "status" in kwargs:
            row["status"] = kwargs["status"]
        if kwargs.get("run_id") is not None:
            row["run_id"] = kwargs["run_id"]
        return True

    async def reconcile_launched_run(self, run_record_id: str, **kwargs) -> bool:
        row = self.rows.get(run_record_id)
        if row is None:
            return False
        row["status"] = "running"
        row["run_id"] = kwargs["run_id"]
        return True

    async def fail_launching_run(self, run_record_id: str, **kwargs) -> bool:
        row = self.rows.get(run_record_id)
        if row is None or row["status"] != "launching":
            return False
        row["status"] = "failed"
        return True

    async def has_active_runs(self, task_id: str) -> bool:
        return any(row["task_id"] == task_id and row["status"] in self._ACTIVE for row in self.rows.values())

    async def mark_stale_active_runs(self, *, error: str) -> int:
        return 0


@pytest.mark.asyncio
async def test_post_launch_bookkeeping_failure_does_not_release_active_slot():
    """Regression for issue #4452.

    A transient failure in the ``queued -> running`` bookkeeping write
    (after ``InvocationRuntime`` has already returned a live ``run_id``) must NOT
    flip the task-run row to ``failed``: ``failed`` is outside the partial
    unique index ``uq_scheduled_task_run_active``, so releasing the slot
    would let the next dispatch launch a DUPLICATE run. The fix keeps the
    row ``running`` with the launched ``run_id`` retained for recovery,
    reconciliation, and cancellation.
    """
    launched: list[dict] = []

    async def fake_launch(**kwargs):
        launched.append(kwargs)
        return {"run_id": f"run-{len(launched)}", "thread_id": kwargs["thread_id"]}

    task_repo = DummyTaskRepo(
        [
            {
                "id": "task-4452",
                "user_id": "user-1",
                "thread_id": None,
                "context_mode": "fresh_thread_per_run",
                "assistant_id": "lead_agent",
                "prompt": "do the thing",
                "schedule_type": "cron",
                "schedule_spec": {"cron": "*/5 * * * *"},
                "timezone": "UTC",
                "status": "enabled",
                "overlap_policy": "enqueue",
            }
        ]
    )
    run_repo = _StatefulRunRepo(fail_first_update=True)
    service = ScheduledTaskService(
        task_repo=task_repo,
        task_run_repo=run_repo,
        invocation_runtime=CallbackInvocationRuntime(fake_launch),
        poll_interval_seconds=5,
        lease_seconds=120,
        max_concurrent_runs=3,
    )

    now = datetime.now(UTC)
    task = dict(task_repo.rows[0])

    first = await service.dispatch_task(task, now=now, trigger="scheduled")
    # The run launched despite the post-launch bookkeeping error; the
    # outcome and run_id reflect that a live run is in flight.
    assert first["outcome"] == "launched"
    assert first["run_id"] == "run-1"
    assert first["error"] is not None  # the bookkeeping error is surfaced, not hidden

    # Second dispatch must observe the active slot held by run-1 and NOT
    # launch a duplicate. On main (bug) this would launch run-2 here.
    second = await service.dispatch_task(task, now=now, trigger="scheduled")
    assert len(launched) == 1, launched
    assert second["outcome"] == "conflict", second

    # The launched run_id is retained on the task-run row (status "running",
    # not "failed") so reconciliation / cancellation can still reach it.
    first_row_id = run_repo.created[0]["run_record_id"]
    assert run_repo.rows[first_row_id]["status"] == "running"
    assert run_repo.rows[first_row_id]["run_id"] == "run-1"

    # The bookkeeping transient is NOT surfaced as the parent task's
    # last_error: the run launched and is still in flight, so the task list
    # must not show an error on an actively running task (matching the
    # success path's clear-on-launch model). The real terminal outcome is
    # written by handle_run_completion.
    assert task_repo.updated[1]["last_error"] is None


@pytest.mark.asyncio
async def test_both_post_launch_association_writes_can_fail_without_releasing_slot():
    launched: list[dict] = []

    async def fake_launch(**kwargs):
        launched.append(kwargs)
        return {"run_id": "run-live", "thread_id": kwargs["thread_id"]}

    class FailingTaskRepo(DummyTaskRepo):
        def __init__(self, rows):
            super().__init__(rows)
            self.failures_remaining = 1

        async def update_after_launch(self, *args, **kwargs):
            if self.failures_remaining:
                self.failures_remaining -= 1
                raise RuntimeError("simulated parent bookkeeping failure")
            await super().update_after_launch(*args, **kwargs)

    task = {
        "id": "task-double-failure",
        "user_id": "user-1",
        "thread_id": None,
        "context_mode": "fresh_thread_per_run",
        "assistant_id": "lead_agent",
        "prompt": "do the thing",
        "schedule_type": "cron",
        "schedule_spec": {"cron": "*/5 * * * *"},
        "timezone": "UTC",
        "status": "enabled",
        "overlap_policy": "enqueue",
    }
    task_repo = FailingTaskRepo([task])
    run_repo = _StatefulRunRepo(fail_updates=2)
    service = ScheduledTaskService(
        task_repo=task_repo,
        task_run_repo=run_repo,
        invocation_runtime=CallbackInvocationRuntime(fake_launch),
        poll_interval_seconds=5,
        lease_seconds=120,
        max_concurrent_runs=3,
    )
    now = datetime.now(UTC)

    first = await service.dispatch_task(dict(task), now=now, trigger="scheduled")
    assert first["outcome"] == "launched"
    first_row_id = run_repo.created[0]["run_record_id"]
    assert run_repo.rows[first_row_id]["task_id"] == "task-double-failure"
    assert run_repo.rows[first_row_id]["status"] == "launching"
    assert run_repo.rows[first_row_id]["run_id"] is None

    second = await service.dispatch_task(dict(task), now=now, trigger="scheduled")
    assert len(launched) == 1
    assert second["outcome"] == "conflict"


@pytest.mark.asyncio
async def test_pre_launch_failure_still_releases_active_slot():
    """Complement to the #4452 fix: when ``InvocationRuntime`` itself fails (no run
    was ever started), the task-run row is marked ``failed`` and the active
    slot is released as before -- the post-launch retention path does not
    apply because there is no live run to protect.
    """
    launched: list[dict] = []

    async def fake_launch(**kwargs):
        launched.append(kwargs)
        raise RuntimeError("runtime refused to start the run")

    task_repo = DummyTaskRepo(
        [
            {
                "id": "task-4452-pre",
                "user_id": "user-1",
                "thread_id": None,
                "context_mode": "fresh_thread_per_run",
                "assistant_id": "lead_agent",
                "prompt": "do the thing",
                "schedule_type": "cron",
                "schedule_spec": {"cron": "*/5 * * * *"},
                "timezone": "UTC",
                "status": "enabled",
                "overlap_policy": "enqueue",
            }
        ]
    )
    run_repo = _StatefulRunRepo()
    service = ScheduledTaskService(
        task_repo=task_repo,
        task_run_repo=run_repo,
        invocation_runtime=CallbackInvocationRuntime(fake_launch),
        poll_interval_seconds=5,
        lease_seconds=120,
        max_concurrent_runs=3,
    )

    result = await service.dispatch_task(dict(task_repo.rows[0]), now=datetime.now(UTC), trigger="scheduled")

    assert result["outcome"] == "failed"
    assert result["run_id"] is None
    # launch was attempted (and raised), so exactly one launch attempt, and
    # the row is terminal -> the slot is released for the next dispatch.
    assert len(launched) == 1
    first_row_id = run_repo.created[0]["run_record_id"]
    assert run_repo.rows[first_row_id]["status"] == "failed"
    assert run_repo.rows[first_row_id]["run_id"] is None


@pytest.mark.asyncio
async def test_malformed_launch_result_still_retains_active_slot():
    """Defense-in-depth for the #4452 invariant.

    If ``InvocationRuntime`` returns a malformed receipt (e.g. missing ``run_id``),
    the unpacking line raises AFTER a live run was already created. The
    dispatch must still take the retention path (keep the row active so the
    slot stays held and no duplicate launches) rather than the pre-launch
    generic-failure path, which would mark the row ``failed`` and release
    the slot while a run is in flight. Keyed off ``launch_succeeded``, not
    ``launched_run_id is not None``.
    """
    launched: list[dict] = []

    async def fake_launch(**kwargs):
        launched.append(kwargs)
        # Live run started, but the result payload is malformed.
        return {"thread_id": kwargs["thread_id"]}

    task_repo = DummyTaskRepo(
        [
            {
                "id": "task-4452-malformed",
                "user_id": "user-1",
                "thread_id": None,
                "context_mode": "fresh_thread_per_run",
                "assistant_id": "lead_agent",
                "prompt": "do the thing",
                "schedule_type": "cron",
                "schedule_spec": {"cron": "*/5 * * * *"},
                "timezone": "UTC",
                "status": "enabled",
                "overlap_policy": "enqueue",
            }
        ]
    )
    run_repo = _StatefulRunRepo()
    service = ScheduledTaskService(
        task_repo=task_repo,
        task_run_repo=run_repo,
        invocation_runtime=CallbackInvocationRuntime(fake_launch),
        poll_interval_seconds=5,
        lease_seconds=120,
        max_concurrent_runs=3,
    )

    now = datetime.now(UTC)
    task = dict(task_repo.rows[0])

    first = await service.dispatch_task(task, now=now, trigger="scheduled")
    # Launch succeeded, so the outcome is "launched" (a run is in flight)
    # even though the result unpacking raised; run_id is unknown.
    assert first["outcome"] == "launched"
    assert first["run_id"] is None

    # Second dispatch must observe the active slot still held (row stays in
    # an active status, NOT "failed") and NOT launch a duplicate.
    second = await service.dispatch_task(task, now=now, trigger="scheduled")
    assert len(launched) == 1, launched
    assert second["outcome"] == "conflict", second

    first_row_id = run_repo.created[0]["run_record_id"]
    assert run_repo.rows[first_row_id]["status"] == "running"


@pytest.mark.asyncio
async def test_manual_trigger_is_queued_when_global_budget_exhausted():
    launched = []

    async def fake_launch(**kwargs):
        launched.append(kwargs)
        return {"run_id": "run-budget", "thread_id": kwargs["thread_id"]}

    row = _once_task_row(task_id="task-budget", status="enabled")
    row.update({"schedule_type": "cron", "schedule_spec": {"cron": "* * * * *"}, "overlap_policy": "enqueue"})
    task_repo = DummyTaskRepo([row])
    # active_count equals max_concurrent_runs → budget is exhausted
    run_repo = DummyRunRepo(active_count=3)
    service = ScheduledTaskService(
        task_repo=task_repo,
        task_run_repo=run_repo,
        invocation_runtime=CallbackInvocationRuntime(fake_launch),
        poll_interval_seconds=5,
        lease_seconds=120,
        max_concurrent_runs=3,
    )

    result = await service.dispatch_task(row, now=datetime.now(UTC), trigger="manual")

    assert result["outcome"] == "queued"
    assert launched == []
    assert run_repo.created["status"] == "queued"


@pytest.mark.asyncio
async def test_manual_trigger_proceeds_when_global_budget_available():
    """Manual trigger must launch when active count is below max_concurrent_runs."""
    launched = []

    async def fake_launch(**kwargs):
        launched.append(kwargs)
        return {"run_id": "run-ok", "thread_id": kwargs["thread_id"]}

    row = _once_task_row(task_id="task-ok", status="enabled")
    row.update({"schedule_type": "cron", "schedule_spec": {"cron": "* * * * *"}, "overlap_policy": "enqueue"})
    task_repo = DummyTaskRepo([row])
    # active_count is 2, max_concurrent_runs is 3 → one slot left
    run_repo = DummyRunRepo(active_count=2)
    service = ScheduledTaskService(
        task_repo=task_repo,
        task_run_repo=run_repo,
        invocation_runtime=CallbackInvocationRuntime(fake_launch),
        poll_interval_seconds=5,
        lease_seconds=120,
        max_concurrent_runs=3,
    )

    result = await service.dispatch_task(row, now=datetime.now(UTC), trigger="manual")

    assert result["outcome"] == "launched"
    assert len(launched) == 1


def test_scheduled_occurrence_identity_is_tenant_due_time_and_version_bound():
    task = {
        "id": "task-stable",
        "schedule_type": "cron",
        "schedule_spec": {"cron": "*/5 * * * *"},
        "schedule_version": 7,
        "timezone": "UTC",
    }
    due = datetime(2026, 9, 1, 12, 0, tzinfo=UTC)

    def service(tenant: str) -> ScheduledTaskService:
        return ScheduledTaskService(
            task_repo=DummyTaskRepo([]),
            task_run_repo=DummyRunRepo(),
            invocation_runtime=NeverLaunchInvocationRuntime(),
            poll_interval_seconds=5,
            lease_seconds=120,
            max_concurrent_runs=3,
            tenant_digest=tenant,
        )

    first = service("a" * 64)._scheduled_occurrence_id(task, scheduled_for=due)
    second = service("a" * 64)._scheduled_occurrence_id(task, scheduled_for=due)

    assert first == second
    assert first.startswith("task-run-")
    assert first != service("b" * 64)._scheduled_occurrence_id(
        task,
        scheduled_for=due,
    )
    assert first != service("a" * 64)._scheduled_occurrence_id(
        {**task, "schedule_version": 8},
        scheduled_for=due,
    )
    assert first != service("a" * 64)._scheduled_occurrence_id(
        task,
        scheduled_for=due + timedelta(minutes=5),
    )


@pytest.mark.asyncio
async def test_run_once_replaces_pod_clock_with_repository_authority_time():
    pod_time = datetime(2026, 9, 1, 10, 0, tzinfo=UTC)
    database_time = datetime(2026, 9, 1, 10, 2, tzinfo=UTC)

    class AuthorityTaskRepo(DummyTaskRepo):
        def __init__(self):
            super().__init__([])
            self.fallback = None
            self.claim_now = None

        async def authority_now(self, *, fallback):
            self.fallback = fallback
            return database_time

        async def claim_due_tasks(self, **kwargs):
            self.claim_now = kwargs["now"]
            return []

    task_repo = AuthorityTaskRepo()
    service = ScheduledTaskService(
        task_repo=task_repo,
        task_run_repo=DummyRunRepo(),
        invocation_runtime=NeverLaunchInvocationRuntime(),
        poll_interval_seconds=5,
        lease_seconds=120,
        max_concurrent_runs=3,
    )

    await service.run_once(now=pod_time)

    assert task_repo.fallback == pod_time
    assert task_repo.claim_now == database_time
    assert service.lease_stats.cycles == 1
    assert service.lease_stats.database_clock_reads == 1
    assert service.lease_stats.last_database_time == database_time


@pytest.mark.asyncio
async def test_manual_dispatch_uses_repository_authority_time():
    pod_time = datetime(2026, 9, 1, 10, 0, tzinfo=UTC)
    database_time = datetime(2026, 9, 1, 10, 2, tzinfo=UTC)

    class AuthorityTaskRepo(DummyTaskRepo):
        async def authority_now(self, *, fallback):
            assert fallback == pod_time
            return database_time

    async def fake_launch(**kwargs):
        return {"run_id": "run-authority", "thread_id": kwargs["thread_id"]}

    row = _once_task_row(task_id="task-authority", status="enabled")
    task_repo = AuthorityTaskRepo([row])
    run_repo = DummyRunRepo()
    service = ScheduledTaskService(
        task_repo=task_repo,
        task_run_repo=run_repo,
        invocation_runtime=CallbackInvocationRuntime(fake_launch),
        poll_interval_seconds=5,
        lease_seconds=120,
        max_concurrent_runs=3,
    )

    await service.dispatch_task(row, now=pod_time, trigger="manual")

    assert run_repo.created["scheduled_for"] == database_time
    assert run_repo.updated[0][1]["started_at"] == database_time
    assert service.lease_stats.database_clock_reads == 1


@pytest.mark.asyncio
async def test_multi_instance_start_reconciles_with_repository_authority_time():
    database_time = datetime(2026, 9, 1, 10, 2, tzinfo=UTC)

    class AuthorityTaskRepo(DummyTaskRepo):
        async def authority_now(self, *, fallback):
            return database_time

    task_repo = AuthorityTaskRepo([])
    run_repo = DummyRunRepo()
    service = ScheduledTaskService(
        task_repo=task_repo,
        task_run_repo=run_repo,
        invocation_runtime=NeverLaunchInvocationRuntime(),
        poll_interval_seconds=5,
        lease_seconds=120,
        max_concurrent_runs=3,
        multi_instance=True,
    )

    await service.start()
    await asyncio.sleep(0)
    await service.stop()

    assert run_repo.reconciled["now"] == database_time
    assert task_repo.reconciled_stuck_once["now"] == database_time
    assert service.lease_stats.database_clock_reads >= 1
