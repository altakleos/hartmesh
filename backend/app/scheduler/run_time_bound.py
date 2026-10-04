"""Timeout history is terminal; execution capacity is held until worker drain.

A Stop acknowledgement, terminal runtime status, lease expiry or missing local
record does not prove cleanup. The existing occurrence lease field stores a
non-expiring retirement hold, shared by every scheduler claim. Owner-local task
completion supplies positive evidence; unconfirmable holds survive restart.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import datetime, timedelta

from app.scheduler.service import ScheduledTaskService

logger = logging.getLogger(__name__)
_OVERDUE_ENDS_PER_POLL = 16
_STOP_REQUEST_TIMEOUT_SECONDS = 10.0
_OBSERVATION_TIMEOUT_SECONDS = 2.0
_CONFIRM_TIMEOUT_SECONDS = 5.0
_STOP_RETRY_DELAYS = (5, 15, 30)


def duration_words(seconds: int) -> str:
    for unit, size in (("hour", 3600), ("minute", 60)):
        if seconds % size == 0:
            count = seconds // size
            return f"{count} {unit}" if count == 1 else f"{count} {unit}s"
    return f"{seconds} seconds"


@dataclass
class _Retirement:
    occurrence_id: str
    run_id: str
    attempts: int = 0
    next_attempt_at: datetime | None = None
    watched: bool = False
    drained: bool = False
    confirming: bool = False


async def get_owned_worker_task(manager, run_id: str) -> asyncio.Task | None:
    """Return attached owner-local evidence; hydrated/missing rows are unknown."""
    record = await manager.get(run_id, raise_on_store_error=True)
    return getattr(record, "task", None)


class TimeBoundedScheduledTaskService(ScheduledTaskService):
    def __init__(
        self,
        *,
        max_run_seconds: int | None = None,
        stop_run: Callable[[str], Awaitable[object]] | None = None,
        run_is_live: Callable[[str], Awaitable[bool | None]] | None = None,
        get_run_task: Callable[[str], Awaitable[asyncio.Task | None]] | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self._max_run_seconds = max_run_seconds
        self._stop_run = stop_run
        self._run_is_live = run_is_live
        self._get_run_task = get_run_task
        self._retiring: dict[str, _Retirement] = {}
        self._retirement_offset = 0
        self._confirmations: set[asyncio.Task] = set()
        self._closed = False

    @property
    def running(self) -> bool:
        return self._task is not None and not self._task.done()

    async def run_once(self, *, now: datetime) -> None:
        await self._stop_overdue_runs(now=now)
        await self._supervise_retirements(now=now)
        # Global budget and same-thread ordering include durable retirement
        # holds. Other free slots remain usable, including manual triggers.
        await super().run_once(now=now)

    def _state(self, occurrence: dict) -> _Retirement:
        key = occurrence["id"]
        state = self._retiring.get(key)
        if state is None or state.run_id != occurrence["run_id"]:
            state = self._retiring[key] = _Retirement(key, occurrence["run_id"])
        return state

    async def _stop_overdue_runs(self, *, now: datetime) -> None:
        if self._max_run_seconds is None or self._stop_run is None:
            return
        error = f"the task did not finish within {duration_words(self._max_run_seconds)}; execution retirement was unconfirmed at timeout"
        try:
            overdue = await self._task_run_repo.list_overdue_running(started_before=now - timedelta(seconds=self._max_run_seconds), limit=_OVERDUE_ENDS_PER_POLL)
        except Exception:
            logger.exception("Scheduled task poll could not look for overdue runs; retrying next poll")
            return
        for occurrence in overdue:
            occurrence_id, run_id, task_id, user_id = occurrence.get("id"), occurrence.get("run_id"), occurrence.get("task_id"), occurrence.get("user_id")
            if not occurrence_id or not run_id or not task_id or not user_id:
                continue
            try:
                ended = await self._task_repo.complete_run(task_id, user_id=user_id, task_run_id=occurrence_id, run_id=run_id, status="failed", error=error, finished_at=now, only_if_active=True, retirement_pending=True)
            except Exception:
                logger.exception("Scheduled task-run %s: could not commit timeout; retrying next poll", occurrence_id)
                continue
            if ended:
                logger.warning("Scheduled task-run %s exceeded its time limit; capacity held until run %s retires", occurrence_id, run_id)
                await self._observe_and_retry(self._state(occurrence), now=now)

    async def _confirm(self, state: _Retirement) -> None:
        if state.confirming:
            return
        state.confirming = True
        try:
            await asyncio.wait_for(self._task_run_repo.confirm_execution_retirement(state.occurrence_id, run_id=state.run_id), timeout=_CONFIRM_TIMEOUT_SECONDS)
        except Exception:
            # Keep positive evidence in memory even if the runtime record is
            # evicted before a later database retry can commit it.
            logger.exception("Scheduled task-run %s drained but its execution hold could not be released", state.occurrence_id)
        else:
            if self._retiring.get(state.occurrence_id) is state:
                del self._retiring[state.occurrence_id]
        finally:
            state.confirming = False

    def _worker_done(self, state: _Retirement) -> None:
        state.drained = True
        if self._closed:
            return
        helper = asyncio.create_task(self._confirm(state))
        self._confirmations.add(helper)
        helper.add_done_callback(self._confirmations.discard)

    async def _observe_and_retry(self, state: _Retirement, *, now: datetime) -> None:
        if not state.drained:
            try:
                if self._get_run_task is not None:
                    task = await asyncio.wait_for(self._get_run_task(state.run_id), timeout=_OBSERVATION_TIMEOUT_SECONDS)
                    if task is not None:
                        state.drained = task.done()
                        if not state.watched:
                            # Completion notification precedes runtime cleanup;
                            # the actual worker's done callback is the boundary.
                            task.add_done_callback(lambda _, state=state: self._worker_done(state))
                            state.watched = True
                elif self._run_is_live is not None:
                    state.drained = await asyncio.wait_for(self._run_is_live(state.run_id), timeout=_OBSERVATION_TIMEOUT_SECONDS) is False
            except Exception:
                logger.warning("Scheduled task-run %s: worker retirement is unknown; retaining capacity", state.occurrence_id, exc_info=True)
        if state.drained:
            await self._confirm(state)
            return
        if self._stop_run is None or state.attempts >= 4 or (state.next_attempt_at is not None and now < state.next_attempt_at):
            return
        state.attempts += 1
        if state.attempts <= len(_STOP_RETRY_DELAYS):
            state.next_attempt_at = now + timedelta(seconds=_STOP_RETRY_DELAYS[state.attempts - 1])
        try:
            outcome = await asyncio.wait_for(self._stop_run(state.run_id), timeout=_STOP_REQUEST_TIMEOUT_SECONDS)
        except Exception:
            logger.warning("Scheduled task-run %s: Stop attempt %d was not acknowledged; capacity remains held", state.occurrence_id, state.attempts, exc_info=True)
        else:
            logger.info("Scheduled task-run %s: Stop attempt %d answered %s; awaiting worker retirement", state.occurrence_id, state.attempts, getattr(outcome, "value", outcome))
        if state.attempts == 4:
            logger.warning("Scheduled task-run %s: Stop retry budget exhausted in this scheduler instance; awaiting worker retirement", state.occurrence_id)

    async def _supervise_retirements(self, *, now: datetime) -> None:
        # Stable bounded pages rotate so unknown old owners cannot starve later
        # local workers. Removal can shift an offset; the next pass revisits it.
        rows = await self._task_run_repo.list_retirement_pending(limit=_OVERDUE_ENDS_PER_POLL, offset=self._retirement_offset)
        self._retirement_offset = self._retirement_offset + len(rows) if len(rows) == _OVERDUE_ENDS_PER_POLL else 0
        for row in rows:
            if row.get("run_id"):
                await self._observe_and_retry(self._state(row), now=now)

    async def _a_stopped_run_is_still_unwinding(self, *, now: datetime) -> bool:
        await self._supervise_retirements(now=now)
        return bool(await self._task_run_repo.list_retirement_pending(limit=1))

    async def start(self) -> None:
        self._closed = False
        await super().start()

    async def stop(self) -> None:
        self._closed = True
        await super().stop()
        if self._confirmations:
            helpers = tuple(self._confirmations)
            _, pending = await asyncio.wait(helpers, timeout=_CONFIRM_TIMEOUT_SECONDS)
            for task in pending:
                task.cancel()
            await asyncio.gather(*helpers, return_exceptions=True)
            self._confirmations.difference_update(helpers)
