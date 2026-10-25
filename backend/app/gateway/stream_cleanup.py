"""Application-owned stream retention and finite deletion lifetimes."""

import asyncio
import logging
from contextvars import Context
from dataclasses import dataclass

from deerflow.runtime.runs.capture_io import CaptureIOClosed
from deerflow.runtime.stream_bridge.base import StreamBridge
from deerflow.utils.file_io import await_drained

logger = logging.getLogger(__name__)


@dataclass(eq=False)
class _Cleanup:
    run_id: str
    started: bool = False


class StreamCleanupRegistry:
    """Close admission before draining deletes, independently of run retirement."""

    def __init__(self, bridge: StreamBridge) -> None:
        self._bridge = bridge
        self._closed = False
        self._tasks: dict[asyncio.Task[None], _Cleanup] = {}
        self._close_task: asyncio.Task[None] | None = None

    def schedule(self, run_id: str, delay: float) -> None:
        if self._closed:
            raise CaptureIOClosed() from None
        entry = _Cleanup(run_id)

        async def cleanup():
            if delay > 0:
                await asyncio.sleep(delay)
            if self._closed:
                return
            entry.started = True
            await await_drained(self._bridge.cleanup(run_id, delay=0))

        # Service work must not inherit a retiring run or request-user scope.
        task = asyncio.create_task(cleanup(), name="stream-retention-cleanup", context=Context())
        self._tasks[task] = entry

        def completed(done):
            self._tasks.pop(done, None)
            if not done.cancelled() and done.exception() is not None:
                logger.warning("Failed to clean up retained stream %s", run_id, exc_info=done.exception())

        task.add_done_callback(completed, context=Context())

    async def close(self) -> None:
        self._closed = True
        if self._close_task is None:
            tasks = tuple(self._tasks.items())
            for task, entry in tasks:
                if not entry.started:
                    task.cancel()

            async def drain():
                await asyncio.gather(*(task for task, _entry in tasks), return_exceptions=True)

            self._close_task = asyncio.create_task(drain(), name="stream-cleanup-drain", context=Context())
        await await_drained(asyncio.shield(self._close_task))
