"""Drive human-requested employee job items inside their one admitted run."""

import asyncio
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any
from uuid import uuid4


async def _cancel_items(active):
    for task in active:
        task.cancel()
    if active:
        await asyncio.gather(*active, return_exceptions=True)
        active.clear()


@dataclass
class BoundBatchItems:
    repository: Any
    batch_id: str
    run_id: str
    thread_id: str
    user_id: str
    instance_id: str
    work_seconds: int = 900
    _final_started: bool = field(default=False, init=False)

    _deadline: datetime | None = field(default=None, init=False)

    def items_open(self):
        return self._deadline is not None and datetime.now(UTC) < self._deadline

    async def run_items(self, execute, *, stopped: Callable[[], bool]):
        identity = {"run_id": self.run_id, "thread_id": self.thread_id, "user_id": self.user_id}
        owner = uuid4().hex
        active = {}
        try:
            while not stopped():
                batch = await self.repository.get_bound_run(self.batch_id, **identity, work_seconds=self.work_seconds)
                if batch is None or stopped():
                    break
                if self._final_started:
                    return
                now = datetime.now(UTC)
                if not active and not batch["unfinished"]:
                    if not await self.repository.begin_bound_final(self.batch_id, **identity, now=now):
                        break
                    self._final_started = True
                    return
                self._deadline = batch["items_deadline"]
                remaining = (self._deadline - now).total_seconds()
                if remaining <= 0:
                    if not await self.repository.timeout_bound_items(self.batch_id, **identity, now=now):
                        break
                    await _cancel_items(active)
                    continue
                slots = batch["max_running_items"] - len(active)
                if slots > 0:
                    items = await self.repository.claim_bound_items(self.batch_id, **identity, lease_owner=owner, now=now, limit=slots)
                    if stopped() or datetime.now(UTC) >= batch["items_deadline"]:
                        continue
                    for item in items:
                        active[asyncio.create_task(execute(item))] = item["id"]
                if not active:
                    await asyncio.sleep(min(remaining, 0.25))
                    continue
                await asyncio.wait(active, timeout=min(remaining, 0.5), return_when=asyncio.FIRST_COMPLETED)
                for task, item_id in list(active.items()):
                    if task.done() and not stopped():
                        completed_at = datetime.now(UTC)
                        if completed_at >= batch["items_deadline"]:
                            break
                        outcome = task.result()
                        del active[task]
                        if not await self.repository.finish_bound_item(self.batch_id, item_id, **identity, lease_owner=owner, **outcome, completed_at=completed_at):
                            raise RuntimeError("The conversation job was interrupted.")
            raise RuntimeError("The conversation job is no longer admitted.")
        finally:
            self._deadline = None
            await _cancel_items(active)
