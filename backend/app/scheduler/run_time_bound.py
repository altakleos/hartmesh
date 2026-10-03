"""A wall-clock bound on one scheduled occurrence.

Nobody watches a scheduled run. A run holds one of
``scheduler.max_concurrent_runs`` slots until it ends, so a run that never ends
holds that slot until the Gateway restarts, and the graph's ``recursion_limit``
bounds its model calls, not its clock.

``TimeBoundedScheduledTaskService`` is the scheduler service with that bound
added and nothing else changed. At each poll an occurrence still running
``scheduler.max_run_seconds`` after it started is ended ``failed`` with an
error that says how long it had, and its run is then asked to stop. The
occurrence is ended first, on the scheduler's own row, so the outcome does not
depend on the run reporting a reason, on which process owns the run, or on the
run stopping at all. A run that finished first keeps its own outcome.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable, Callable
from datetime import datetime, timedelta

from app.scheduler.service import ScheduledTaskService

logger = logging.getLogger(__name__)

# How many overdue occurrences one poll ends. Each ended occurrence leaves the
# overdue set, so the rest are ended by the next poll.
_OVERDUE_ENDS_PER_POLL = 16
# How long the poll waits for the run manager to accept one Stop. The occurrence
# is ended before the Stop is asked, so a slow Stop costs the poll time, not the
# outcome.
_STOP_REQUEST_TIMEOUT_SECONDS = 10.0
# How long the scheduler launches nothing while a run it stopped at the time
# limit is still unwinding. A run keeps its sandbox until its worker finishes,
# and the occurrence that counted against ``max_concurrent_runs`` is already
# ended, so without this the next scheduled run could start beside it. A run
# that never finishes unwinding holds scheduling for this long, not for good.
_STOPPING_GRACE_SECONDS = 120


def duration_words(seconds: int) -> str:
    for unit, size in (("hour", 3600), ("minute", 60)):
        if seconds % size == 0:
            count = seconds // size
            return f"{count} {unit}" if count == 1 else f"{count} {unit}s"
    return f"{seconds} seconds"


class TimeBoundedScheduledTaskService(ScheduledTaskService):
    def __init__(
        self,
        *,
        max_run_seconds: int | None = None,
        stop_run: Callable[[str], Awaitable[object]] | None = None,
        run_is_live: Callable[[str], Awaitable[bool]] | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self._max_run_seconds = max_run_seconds
        self._stop_run = stop_run
        # Whether the run manager still holds a worker for a run, and the runs
        # this process asked to stop at the time limit, with when it asked.
        self._run_is_live = run_is_live
        self._stopping: dict[str, datetime] = {}

    @property
    def running(self) -> bool:
        """Whether this process's scheduler loop is live."""
        return self._task is not None and not self._task.done()

    async def run_once(self, *, now: datetime) -> None:
        await self._stop_overdue_runs(now=now)
        if await self._a_stopped_run_is_still_unwinding(now=now):
            return
        await super().run_once(now=now)

    async def _stop_overdue_runs(self, *, now: datetime) -> None:
        """End every occurrence that has run past its time limit, then stop its run."""
        if self._max_run_seconds is None or self._stop_run is None:
            return
        error = f"the task did not finish within {duration_words(self._max_run_seconds)}, so it was stopped"
        try:
            overdue = await self._task_run_repo.list_overdue_running(
                started_before=now - timedelta(seconds=self._max_run_seconds),
                limit=_OVERDUE_ENDS_PER_POLL,
            )
        except Exception:
            logger.exception("Scheduled task poll could not look for runs past their time limit; retrying next poll")
            return
        for occurrence in overdue:
            occurrence_id, run_id, task_id, user_id = occurrence.get("id"), occurrence.get("run_id"), occurrence.get("task_id"), occurrence.get("user_id")
            if not run_id or not task_id or not user_id:
                continue
            try:
                ended = await self._task_repo.complete_run(
                    task_id,
                    user_id=user_id,
                    task_run_id=occurrence_id,
                    run_id=run_id,
                    status="failed",
                    error=error,
                    finished_at=now,
                    only_if_active=True,
                )
            except Exception:
                logger.exception("Scheduled task-run %s: could not end it past its time limit; retrying next poll", occurrence_id)
                continue
            if not ended:
                continue
            self._stopping[run_id] = now
            logger.warning("Scheduled task-run %s ran past %s; ended it and stopping run %s", occurrence_id, duration_words(self._max_run_seconds), run_id)
            try:
                outcome = await asyncio.wait_for(self._stop_run(run_id), timeout=_STOP_REQUEST_TIMEOUT_SECONDS)
            except Exception:
                logger.exception("Scheduled task-run %s: run %s was not confirmed stopped after its occurrence ended at the time limit", occurrence_id, run_id)
            else:
                # The Stop is asked once. Whether it took is the run manager's
                # answer, so a refusal is at least visible to an operator.
                logger.info("Scheduled task-run %s: stop of run %s at the time limit answered %s", occurrence_id, run_id, getattr(outcome, "value", outcome))

    async def _a_stopped_run_is_still_unwinding(self, *, now: datetime) -> bool:
        """Whether a run stopped at the time limit still holds its worker, and so its sandbox.

        The poll launches and claims nothing while one does, for up to
        ``_STOPPING_GRACE_SECONDS`` after the Stop. A liveness check that fails
        does not hold scheduling.
        """
        if self._run_is_live is None:
            return False
        for run_id, asked_at in list(self._stopping.items()):
            if now - asked_at >= timedelta(seconds=_STOPPING_GRACE_SECONDS):
                logger.warning("Run %s did not finish unwinding within %s seconds of its Stop; scheduling resumes", run_id, _STOPPING_GRACE_SECONDS)
                del self._stopping[run_id]
                continue
            try:
                live = await self._run_is_live(run_id)
            except Exception:
                logger.exception("Could not tell whether run %s is still unwinding; scheduling is not held for it", run_id)
                del self._stopping[run_id]
                continue
            if live:
                return True
            del self._stopping[run_id]
        return False
