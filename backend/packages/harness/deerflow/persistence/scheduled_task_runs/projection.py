"""Parent projection and launch accounting under the scheduled-task row lock."""

from __future__ import annotations

from typing import TYPE_CHECKING

from sqlalchemy.orm.attributes import flag_modified

if TYPE_CHECKING:
    from deerflow.persistence.scheduled_task_runs.model import ScheduledTaskRunRow
    from deerflow.persistence.scheduled_tasks.model import ScheduledTaskRow


_TERMINAL_TASK_STATUSES = frozenset({"completed", "failed", "cancelled"})


def keeps_pause(task: ScheduledTaskRow, status: str) -> bool:
    """Whether scheduler bookkeeping writing ``status`` must leave a paused ``task`` paused.

    A pause is lifted only by a resume (the owner's) or a restore (of a hold
    ``disable`` placed). What the scheduler writes after an occurrence it had
    already started -- a launch that failed, one that beat the pause, a claim
    it recovers -- never lifts it. The one exception is a ``once`` task whose
    single occurrence launched: that occurrence is spent, and the task records
    how it went.
    """
    if task.status != "paused":
        return False
    return task.schedule_type != "once" or status not in ({"running"} | _TERMINAL_TASK_STATUSES)


def can_project(task: ScheduledTaskRow, occurrence: ScheduledTaskRunRow) -> bool:
    """Keep unsequenced history best-effort until a sequenced run is admitted."""
    if occurrence.occurrence_seq is None:
        return task.last_occurrence_seq == 0
    return occurrence.occurrence_seq == task.last_occurrence_seq


def once_run_still_scheduled(task: ScheduledTaskRow, occurrence: ScheduledTaskRunRow) -> bool:
    """Whether a ``once`` task's own run is still ahead of this trial.

    A manual trigger is a trial ("Run once now"): launched before the run
    time it must not consume that run, so the parent keeps its status rather
    than taking the once outcome. ``next_run_at`` holds the run time only
    while it was still ahead at the trial's launch; the launch write (or the
    projection repairing it) clears it otherwise. Always False for a recurring
    task, whose ``next_run_at`` is set between runs. The launch path, which
    has no occurrence row yet, encodes the same rule in
    ``ScheduledTaskService._task_status_for_launch``.
    """
    return task.schedule_type == "once" and occurrence.trigger == "manual" and task.next_run_at is not None


def account_launch(task: ScheduledTaskRow, occurrence: ScheduledTaskRunRow, run_id: str) -> bool:
    """Count a proven launch once, in the same transaction as its marker.

    Migrated NULL markers retain the old last_run_id inference for their first
    repair; historical accounting cannot be reconstructed from occurrence times.
    """
    if occurrence.launch_accounted is True:
        return False
    legacy = occurrence.launch_accounted is None
    occurrence.launch_accounted = True
    if legacy and task.last_run_id == run_id:
        return False
    task.run_count += 1
    # A stale occurrence may change the count, but not the current projection's
    # timestamp. Callers that also project a result explicitly set updated_at.
    flag_modified(task, "updated_at")
    return True
