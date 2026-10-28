"""Channel-neutral ownership of an inbound attachment's buffered bytes."""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from contextlib import contextmanager
from contextvars import ContextVar

from app.channels.execution import ChannelExecutionContext
from deerflow.utils.file_io import await_drained

_CONTEXT: ContextVar[ChannelExecutionContext | None] = ContextVar("inbound_file_context", default=None)


def current_inbound_file_context() -> ChannelExecutionContext | None:
    """Actual manager caller, separate from transport descriptors/metadata."""
    return _CONTEXT.get()


@contextmanager
def inbound_file_context(context: ChannelExecutionContext | None):
    token = _CONTEXT.set(context)
    try:
        yield
    finally:
        _CONTEXT.reset(token)


class InboundFileLease:
    """Transfer bytes and one release operation to their final consumer.

    Consumers must drain native/transport buffer users and clear their aliases
    before closing. A cancelled close waiter still owns the same release through
    completion; concurrent and repeated closes never invoke it again.
    """

    def __init__(self, data: bytes, release: Callable[[], Awaitable[None]]) -> None:
        if not isinstance(data, bytes) or not callable(release):
            raise TypeError("inbound file lease requires bytes and an async release")
        self._data: bytes | None = data
        self._release = release
        self._close_task: asyncio.Task[None] | None = None

    @property
    def data(self) -> bytes | None:
        return self._data

    async def _release_once(self) -> None:
        release, self._release = self._release, None
        await release()

    async def _join_close(self) -> None:
        await self._close_task

    async def aclose(self) -> None:
        if self._close_task is None:
            self._data = None
            self._close_task = asyncio.create_task(self._release_once())
        await await_drained(self._join_close())
