"""Host-only admission and drainage for finite captured I/O operations.

The owner does not own an arbitrary graph task. Retirement prevents a new
factory invocation, while a factory already invoked retains its complete
delegate lifetime. Resources retain their own serialized close continuation.
"""

from __future__ import annotations

import asyncio
import inspect
from collections.abc import Awaitable, Callable, Iterator
from contextlib import contextmanager
from contextvars import ContextVar
from typing import Protocol


class CaptureIOClosed(RuntimeError):
    def __init__(self):
        super().__init__("Capture I/O owner is retired")


class CaptureIOUnsupported(RuntimeError):
    def __init__(self):
        super().__init__("Capture I/O profile is unsupported")


class CaptureIOCleanupError(RuntimeError):
    def __init__(self):
        super().__init__("Capture I/O cleanup failed")


class CaptureIOResource(Protocol):
    def retire(self) -> None: ...

    async def drain(self) -> None: ...


async def _join_owned[T](task: asyncio.Task[T]) -> T:
    """Defer repeated caller cancellation until the retained task settles."""
    try:
        return await asyncio.shield(task)
    except asyncio.CancelledError:
        while not task.done():
            try:
                await asyncio.wait({task})
            except asyncio.CancelledError:
                continue
        if not task.cancelled():
            task.exception()
        raise


def _observe(task: asyncio.Task) -> None:
    if not task.cancelled():
        task.exception()


class CaptureIOOwner:
    """One loop-owned capture lifetime, separate from durable run identity."""

    def __init__(self):
        self._loop = asyncio.get_running_loop()
        self._retired = False
        self._retirement = asyncio.Event()
        self._operations: set[asyncio.Task] = set()
        # Retain the actual object as well as its id, including failed release.
        self._resources: dict[int, CaptureIOResource] = {}
        self._resource_drains: dict[int, tuple[CaptureIOResource, asyncio.Task]] = {}
        self._failed_resources: set[int] = set()
        self._drain_task: asyncio.Task[None] | None = None

    @property
    def loop(self) -> asyncio.AbstractEventLoop:
        return self._loop

    @property
    def retired(self) -> bool:
        return self._retired

    def _check_loop(self) -> None:
        try:
            current = asyncio.get_running_loop()
        except RuntimeError:
            raise CaptureIOUnsupported() from None
        if current is not self._loop:
            raise CaptureIOUnsupported() from None

    def check_open(self) -> None:
        self._check_loop()
        if self._retired:
            raise CaptureIOClosed() from None

    def _invalidate_open_drain(self) -> None:
        # An open owner's drain is a current-work observation, not retirement.
        # Retired owners cannot admit more work and retain the same drain task.
        if self._drain_task is not None and self._drain_task.done():
            self._drain_task = None

    async def run[T](self, factory: Callable[[], Awaitable[T]]) -> T:
        self.check_open()
        if not callable(factory):
            raise TypeError("Capture I/O factory is invalid") from None

        async def invoke():
            self.check_open()
            with capture_io_scope(self):
                operation = factory()
                if not inspect.isawaitable(operation):
                    raise TypeError("Capture I/O factory must return an awaitable") from None
                return await operation

        self._invalidate_open_drain()
        task = self._loop.create_task(invoke(), name="capture-io-operation")
        self._operations.add(task)

        def completed(done):
            _observe(done)
            self._operations.discard(done)

        task.add_done_callback(completed)
        return await _join_owned(task)

    def register_resource(self, resource: CaptureIOResource) -> None:
        self.check_open()
        if not callable(getattr(resource, "retire", None)) or not callable(getattr(resource, "drain", None)):
            raise TypeError("Capture I/O resource is invalid") from None
        identity = id(resource)
        current = self._resources.get(identity)
        if current is resource:
            return
        self._invalidate_open_drain()
        self._resources[identity] = resource

    def unregister_resource(self, resource: CaptureIOResource) -> None:
        self._check_loop()
        identity = id(resource)
        if identity in self._failed_resources:
            raise CaptureIOCleanupError() from None
        if self._resources.get(identity) is resource:
            self._resources.pop(identity)
        # A drain already awaiting close stays retained until its task settles.

    def retire(self) -> None:
        self._check_loop()
        if self._retired:
            return
        self._invalidate_open_drain()
        self._retired = True
        self._retirement.set()
        for identity, resource in tuple(self._resources.items()):
            try:
                resource.retire()
            except BaseException:
                # Keep failed resource identity; still retire all other resources.
                # Reporting waits for every admitted operation/close to settle.
                self._failed_resources.add(identity)

    async def wait_retired(self) -> None:
        self._check_loop()
        await self._retirement.wait()

    def _start_resource_drain(self, identity: int, resource: CaptureIOResource) -> None:
        if identity in self._resource_drains:
            return

        async def close():
            try:
                await resource.drain()
            except BaseException:
                self._failed_resources.add(identity)
                self._resources[identity] = resource
                # Do not store/report the resource exception as our public result.

        task = self._loop.create_task(close(), name="capture-io-resource-close")
        task.add_done_callback(_observe)
        self._resource_drains[identity] = (resource, task)

    async def _finish_drain(self) -> None:
        while True:
            pending_operations = {task for task in self._operations if not task.done()}
            if pending_operations:
                # Resource.retire already started independent idle-cursor
                # closes. Finish admitted calls before deferred release work.
                await asyncio.gather(*pending_operations, return_exceptions=True)
                continue
            if self._retired:
                for identity, resource in tuple(self._resources.items()):
                    self._start_resource_drain(identity, resource)
            pending = {task for _resource, task in self._resource_drains.values() if not task.done()}
            if not pending:
                break
            await asyncio.gather(*pending, return_exceptions=True)
        if self._failed_resources:
            raise CaptureIOCleanupError() from None

    async def drain(self) -> None:
        self._check_loop()
        if self._drain_task is None:
            self._drain_task = self._loop.create_task(self._finish_drain(), name="capture-io-drain")
            self._drain_task.add_done_callback(_observe)
        await _join_owned(self._drain_task)


_CURRENT_OWNER: ContextVar[CaptureIOOwner | None] = ContextVar("capture_io_owner", default=None)


def current_capture_io_owner() -> CaptureIOOwner | None:
    return _CURRENT_OWNER.get()


@contextmanager
def capture_io_scope(owner: CaptureIOOwner | None) -> Iterator[None]:
    """Bind/reset on the original task; binding a retired owner grants no I/O."""
    if owner is not None:
        if not isinstance(owner, CaptureIOOwner):
            raise CaptureIOUnsupported() from None
        owner._check_loop()
    token = _CURRENT_OWNER.set(owner)
    try:
        yield
    finally:
        _CURRENT_OWNER.reset(token)


async def run_capture_io[T](factory: Callable[[], Awaitable[T]]) -> T:
    owner = current_capture_io_owner()
    if owner is None:
        return await factory()
    return await owner.run(factory)
