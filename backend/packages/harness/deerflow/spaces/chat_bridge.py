"""Private owning-loop bridge for fresh chat storage admission.

Only a trusted host installs the callback. Completion represents the owner
coroutine finishing, including drainage of any verification it owns.
"""

from __future__ import annotations

import asyncio
import threading
from concurrent.futures import Future
from contextvars import Context, copy_context
from dataclasses import dataclass, field
from typing import Any

from deerflow_extension_api.storage import StorageUnsupported

from deerflow.utils.file_io import _FILE_IO_WORKER, await_drained


@dataclass(eq=False)
class _Request:
    user_id: str
    thread_id: str
    context: Context
    publication: dict | None = None
    completion: Future = field(default_factory=Future)
    task: asyncio.Task | None = None
    cancelled: bool = False


class ChatStorageHost:
    def __init__(self, loop, prepare, *, publish=None, pool_runtime_owner=None):
        if not callable(prepare):
            raise TypeError("Chat storage admission requires a host callback")
        self._loop, self._prepare = loop, prepare
        if publish is not None and not callable(publish):
            raise TypeError("Screenshot publication requires a host callback")
        self._publish = publish
        self.pool_runtime_owner = pool_runtime_owner
        self._lock = threading.RLock()
        self._closed = False
        self._requests: set[_Request] = set()

    @property
    def is_owner_loop(self):
        try:
            return asyncio.get_running_loop() is self._loop
        except RuntimeError:
            return False

    def _submit(self, user_id, thread_id, *, publication=None):
        request = _Request(user_id, thread_id, copy_context(), publication=publication)
        with self._lock:
            if self._closed or not self._loop.is_running() or self._loop.is_closed():
                raise StorageUnsupported("The chat storage host is unavailable")
            self._requests.add(request)
            try:
                self._loop.call_soon_threadsafe(self._start, request)
            except RuntimeError as exc:
                self._requests.discard(request)
                raise StorageUnsupported("The chat storage host cannot accept work") from exc
        return request

    def _start(self, request):
        with self._lock:
            if request.completion.done():
                return
            if self._closed:
                request.completion.set_exception(StorageUnsupported("The chat storage host is closed"))
                self._requests.discard(request)
                return
            coroutine = None
            try:
                coroutine = self._prepare(request.user_id, request.thread_id) if request.publication is None else self._publish(request.user_id, request.thread_id, **request.publication)
                request.task = self._loop.create_task(coroutine, context=request.context)
            except BaseException as exc:
                if coroutine is not None and hasattr(coroutine, "close"):
                    coroutine.close()
                request.completion.set_exception(exc)
                self._requests.discard(request)
                return
            request.task.add_done_callback(lambda task: self._complete(request, task))
            if request.cancelled:
                request.task.cancel()

    def _complete(self, request, task):
        with self._lock:
            try:
                result = task.result()
            except BaseException as exc:
                request.completion.set_exception(exc)
            else:
                request.completion.set_result(result)
            finally:
                self._requests.discard(request)

    def _cancel(self, request):
        with self._lock:
            request.cancelled = True

        def cancel_started():
            if request.task is not None and not request.task.done():
                request.task.cancel()

        try:
            self._loop.call_soon_threadsafe(cancel_started)
        except RuntimeError:
            # A stopped owner retains its request; no foreign loop runs its SQL.
            pass

    @staticmethod
    def _refuse_file_worker():
        if getattr(_FILE_IO_WORKER, "active", False):
            raise StorageUnsupported("Chat storage admission cannot wait from the bounded file I/O pool")

    async def prepare_async(self, user_id, thread_id) -> Any:
        self._refuse_file_worker()
        request = self._submit(user_id, thread_id)
        return await self._await_request(request)

    async def publish_async(self, user_id, thread_id, *, name, content, frames, expected_reference=None):
        self._refuse_file_worker()
        if self._publish is None:
            raise StorageUnsupported("The owning host has no screenshot publisher")
        publication = {"name": name, "content": content, "frames": frames}
        if expected_reference is not None:
            publication["expected_reference"] = expected_reference
        request = self._submit(user_id, thread_id, publication=publication)
        return await self._await_request(request)

    async def _await_request(self, request):
        completion = asyncio.wrap_future(request.completion)
        try:
            return await asyncio.shield(completion)
        except asyncio.CancelledError:
            self._cancel(request)
            # Cancelled concurrent futures acknowledge cancellation immediately;
            # this separate future is completed only by the owner's done callback.
            while not completion.done():
                try:
                    await asyncio.shield(completion)
                except BaseException:
                    if not completion.done():
                        continue
            if not completion.cancelled():
                completion.exception()
            raise

    def prepare_sync(self, user_id, thread_id) -> Any:
        self._refuse_file_worker()
        try:
            current = asyncio.get_running_loop()
        except RuntimeError:
            current = None
        if current is self._loop:
            raise StorageUnsupported("Chat storage admission on the owning loop requires the async API")
        request = self._submit(user_id, thread_id)
        try:
            return request.completion.result()
        except BaseException:
            self._cancel(request)
            while not request.completion.done():
                try:
                    request.completion.result()
                except BaseException:
                    if not request.completion.done():
                        continue
            request.completion.exception()
            raise

    async def close(self):
        if asyncio.get_running_loop() is not self._loop:
            raise StorageUnsupported("The owning loop must close chat storage admission")
        with self._lock:
            self._closed = True
            requests = tuple(self._requests)
            for request in requests:
                if request.task is None:
                    request.completion.set_exception(StorageUnsupported("The chat storage host is closed"))
                    self._requests.discard(request)
                else:
                    request.task.cancel()

        async def drain():
            await asyncio.gather(*(asyncio.wrap_future(request.completion) for request in requests), return_exceptions=True)

        await await_drained(drain())


_host_lock = threading.Lock()
_host: ChatStorageHost | None = None


def get_chat_storage_host():
    """The installed host callback, never a cache of users or authority."""
    with _host_lock:
        return _host


def install_chat_storage_host(host):
    global _host
    if not isinstance(host, ChatStorageHost):
        raise TypeError("Chat storage requires an owning-loop host")
    with _host_lock:
        if _host is not None and _host is not host:
            raise RuntimeError("Another lifecycle already owns chat storage admission")
        _host = host


def remove_chat_storage_host(host):
    global _host
    with _host_lock:
        if _host is host:
            _host = None
            return True
        return False
