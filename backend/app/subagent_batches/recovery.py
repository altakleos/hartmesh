"""Recover work that this Gateway cannot execute before starting its batch worker."""

import asyncio
import logging
from collections.abc import Coroutine
from typing import Any

from deerflow.persistence.subagent_batches.sql import SubagentBatchRepository

logger = logging.getLogger(__name__)
_unwinding: set[asyncio.Task] = set()


def _observe_cleanup(task: asyncio.Task) -> None:
    _unwinding.discard(task)
    if not task.cancelled():
        try:
            task.result()
        except Exception as error:
            logger.warning("Batch recovery database cleanup failed (%s)", type(error).__name__)


async def _call_with_deadline[T](operation: Coroutine[Any, Any, T], timeout: float) -> T:
    # A driver may need time to roll back even after cancellation. Keep ownership
    # until cleanup completes, without making readiness wait for the DB lock.
    task = asyncio.create_task(operation)
    try:
        done, _ = await asyncio.wait({task}, timeout=timeout)
        if not done:
            raise TimeoutError("Batch recovery database call exceeded its deadline")
        return task.result()
    finally:
        if not task.done():
            _unwinding.add(task)
            task.add_done_callback(_observe_cleanup)
            task.cancel()


async def recover_unavailable_batches(repository: SubagentBatchRepository, *, agent_enabled: bool = False, time_budget: float = 5.0, call_timeout: float = 1.0) -> None:
    selection = {"legacy_only": True} if agent_enabled else {}
    loop = asyncio.get_running_loop()
    deadline = loop.time() + time_budget
    after = None
    while (remaining := deadline - loop.time()) > 0:
        try:
            batch_ids = await _call_with_deadline(repository.list_unavailable_bound_batches(after=after, limit=64, **selection), min(call_timeout, remaining))
        except Exception as error:
            logger.warning("Listing unavailable batches during startup failed (%s)", type(error).__name__)
            return
        if not batch_ids:
            return
        for batch_id in batch_ids:
            remaining = deadline - loop.time()
            if remaining <= 0:
                break
            try:
                await _call_with_deadline(repository.cancel_unavailable_bound_batch(batch_id, **selection), min(call_timeout, remaining))
            except Exception as error:
                logger.warning("Could not cancel unavailable batch %s during startup (%s)", batch_id, type(error).__name__)
            after = batch_id
    logger.warning("Startup batch recovery reached its deadline. Remaining work will be checked at the next startup.")
