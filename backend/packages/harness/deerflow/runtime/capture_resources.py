"""Stable public persistence facades for host-owned capture I/O.

Ordinary calls delegate to their corresponding SDK method. A capture admits one
complete delegate call, or retains an iterator across yields until native close.
The facades never own a second backend, cache, batch queue or executor.
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Callable, Iterator
from concurrent.futures import Future
from contextvars import copy_context
from typing import Any, Literal

from langgraph.checkpoint.base import BaseCheckpointSaver
from langgraph.store.base import NOT_PROVIDED, BaseStore

from deerflow.runtime.runs.capture_io import CaptureIOCleanupError, CaptureIOClosed, CaptureIOOwner, CaptureIOUnsupported, current_capture_io_owner
from deerflow.utils.file_io import await_drained

CaptureProfile = Literal["async", "memory", "unsupported"]


def checkpoint_capture_profile(inner, *, asynchronous: bool) -> CaptureProfile:
    """Qualify actual builtin factory results, before any cache wrapping."""
    from langgraph.checkpoint.memory import InMemorySaver

    if type(inner) is InMemorySaver:
        return "memory"
    if asynchronous and type(inner).__module__ == "langgraph.checkpoint.sqlite.aio":
        from langgraph.checkpoint.sqlite.aio import AsyncSqliteSaver

        if type(inner) is AsyncSqliteSaver:
            return "async"
    if asynchronous and type(inner).__module__ == "langgraph.checkpoint.postgres.aio":
        from langgraph.checkpoint.postgres.aio import AsyncPostgresSaver

        if type(inner) is AsyncPostgresSaver:
            return "async"
    return "unsupported"


def store_capture_profile(inner, *, asynchronous: bool) -> CaptureProfile:
    from langgraph.store.memory import InMemoryStore

    # An embedding-enabled Memory Store can call arbitrary synchronous code.
    if type(inner) is InMemoryStore and inner.embeddings is None:
        return "memory"
    if asynchronous and type(inner).__module__ == "langgraph.store.sqlite.aio":
        from langgraph.store.sqlite.aio import AsyncSqliteStore

        if type(inner) is AsyncSqliteStore:
            return "async"
    if asynchronous and type(inner).__module__ == "langgraph.store.postgres.aio":
        from langgraph.store.postgres.aio import AsyncPostgresStore

        if type(inner) is AsyncPostgresStore:
            return "async"
    return "unsupported"


def checkpoint_cache_capture_profile(profile: CaptureProfile, cache) -> CaptureProfile:
    """The complete configured saver includes its actual cache collaborator."""
    from deerflow.runtime.checkpoint_cache.memory import MemoryCheckpointHistoryCache

    if profile == "unsupported" or type(cache) is MemoryCheckpointHistoryCache:
        return profile
    if type(cache).__module__ == "deerflow.runtime.checkpoint_cache.redis":
        from deerflow.runtime.checkpoint_cache.redis import RedisCheckpointHistoryCache

        if type(cache) is RedisCheckpointHistoryCache:
            return "async"
    return "unsupported"


def _on_owner_loop(owner: CaptureIOOwner, call: Callable[[], Any]):
    """Marshal only atomic Memory work/registered close; never use an executor."""
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        loop = None
    if loop is owner.loop:
        return call()
    if owner.loop.is_closed() or not owner.loop.is_running():
        raise CaptureIOUnsupported() from None
    result = Future()
    context = copy_context()

    def invoke():
        try:
            value = context.run(call)
        except BaseException as error:
            result.set_exception(error)
        else:
            result.set_result(value)

    owner.loop.call_soon_threadsafe(invoke)
    return result.result()


class _Facade:
    def __init__(self, inner, *, profile: CaptureProfile, loop: asyncio.AbstractEventLoop | None):
        if profile not in {"async", "memory", "unsupported"}:
            raise CaptureIOUnsupported() from None
        self._inner, self._profile, self._loop = inner, profile, loop

    @property
    def supports_capture_io(self) -> bool:
        return self._profile != "unsupported"

    def _check(self, owner):
        owner.check_open()
        if not self.supports_capture_io or self._loop is not None and owner.loop is not self._loop:
            raise CaptureIOUnsupported() from None

    async def _async(self, method, *args, **kwargs):
        owner = current_capture_io_owner()
        if owner is None:
            return await getattr(self._inner, method)(*args, **kwargs)
        self._check(owner)
        return await owner.run(lambda: getattr(self._inner, method)(*args, **kwargs))

    def _sync(self, method, *args, **kwargs):
        owner = current_capture_io_owner()
        if owner is None:
            return getattr(self._inner, method)(*args, **kwargs)
        # Blocking a default-executor worker on SQL can starve SDK codec work
        # submitted to that same executor. No implicit thread profile is safe.
        if self._profile != "memory":
            raise CaptureIOUnsupported() from None

        def atomic():
            self._check(owner)
            return getattr(self._inner, method)(*args, **kwargs)

        return _on_owner_loop(owner, atomic)


class _OwnedAsyncIterator(AsyncIterator):
    def __init__(self, facade, factory):
        self._facade, self._factory = facade, factory
        self._inner = None
        self._started = False
        self._owner = None
        self._lock = asyncio.Lock()
        self._retired = False
        self._closing = None
        self._closed = False

    def __aiter__(self):
        return self

    async def _step(self):
        async with self._lock:
            if self._retired:
                raise CaptureIOClosed() from None
            if self._closing is not None or self._closed:
                raise StopAsyncIteration
            self._facade._check(self._owner)
            if self._inner is None:
                self._inner = self._factory()
            return await anext(self._inner)

    async def __anext__(self):
        current = current_capture_io_owner()
        if self._owner is not None and current is not None and current is not self._owner:
            raise CaptureIOUnsupported() from None
        if self._owner is not None and (self._retired or self._owner.retired):
            raise CaptureIOClosed() from None
        if self._closed:
            raise StopAsyncIteration
        if not self._started:
            if current is not None:
                self._facade._check(current)
                self._owner = current
                current.register_resource(self)
            self._started = True
        if self._owner is None:
            if current is not None:
                raise CaptureIOUnsupported() from None
            if self._inner is None:
                self._inner = self._factory()
            return await anext(self._inner)
        if self._retired or self._owner.retired:
            raise CaptureIOClosed() from None
        try:
            value = await self._owner.run(self._step)
        except BaseException as error:
            try:
                await self.aclose()
            except BaseException:
                # The owner retains failed close; preserve the step's outcome.
                pass
            if self._retired and isinstance(error, StopAsyncIteration):
                raise CaptureIOClosed() from None
            raise
        if self._retired or self._closing is not None:
            await self.aclose()
            raise CaptureIOClosed() from None
        return value

    async def _close(self):
        try:
            async with self._lock:
                if self._inner is not None:
                    await self._inner.aclose()
                self._closed = True
                self._owner.unregister_resource(self)
        except BaseException:
            raise CaptureIOCleanupError() from None

    def _start_close(self):
        if self._closing is None:
            self._closing = self._owner.loop.create_task(self._close(), name="capture-history-close")
            self._closing.add_done_callback(lambda task: None if task.cancelled() else task.exception())

    def retire(self):
        self._retired = True
        self._start_close()

    async def drain(self):
        await self.aclose()

    async def aclose(self):
        if self._owner is None:
            if self._inner is not None:
                close = getattr(self._inner, "aclose", None)
                if callable(close):
                    await close()
            self._closed = True
            return
        self._start_close()

        async def join():
            return await self._closing

        await await_drained(join())


class _OwnedSyncIterator(Iterator):
    """Atomic Memory iteration; async-backed captured sync iteration refuses."""

    def __init__(self, facade, factory):
        self._facade, self._factory = facade, factory
        self._inner = None
        self._started = False
        self._owner = None
        self._retired = False
        self._closed = False
        self._close_failed = False

    def __iter__(self):
        return self

    def _next(self):
        if self._retired:
            raise CaptureIOClosed() from None
        self._facade._check(self._owner)
        if self._closed:
            raise StopIteration
        if not self._started:
            self._owner.register_resource(self)
            self._started = True
        if self._inner is None:
            self._inner = self._factory()
        try:
            return next(self._inner)
        except StopIteration:
            self._close()
            raise
        except BaseException:
            try:
                self._close()
            except BaseException:
                # Retain the failed close for owner drainage; the delegate's
                # primary failure remains the result of this advancement.
                pass
            raise

    def __next__(self):
        current = current_capture_io_owner()
        if self._owner is not None and current is not None and current is not self._owner:
            raise CaptureIOUnsupported() from None
        if self._retired or self._owner is not None and self._owner.retired:
            raise CaptureIOClosed() from None
        if self._closed:
            raise StopIteration
        if self._owner is None and not self._started:
            if current is None:
                self._started = True
            elif self._facade._profile != "memory":
                raise CaptureIOUnsupported() from None
            else:
                self._owner = current
        if self._owner is not None:
            return _on_owner_loop(self._owner, self._next)
        if current is not None:
            raise CaptureIOUnsupported() from None
        if self._inner is None:
            self._inner = self._factory()
        return next(self._inner)

    def _close(self):
        if self._owner is None:
            close = getattr(self._inner, "close", None)
            result = close() if callable(close) else None
            self._closed = True
            return result
        if self._close_failed:
            raise CaptureIOCleanupError() from None
        if self._closed:
            return
        try:
            if self._inner is not None:
                self._inner.close()
        except BaseException:
            self._close_failed = True
            raise CaptureIOCleanupError() from None
        self._closed = True
        if self._owner is not None and self._started:
            self._owner.unregister_resource(self)

    def close(self):
        if self._owner is not None:
            return _on_owner_loop(self._owner, self._close)
        return self._close()

    def retire(self):
        self._retired = True
        self._close()

    async def drain(self):
        self._close()


class OwnedCheckpointSaver(_Facade, BaseCheckpointSaver):
    @property
    def serde(self):
        return self._inner.serde

    @serde.setter
    def serde(self, value):
        self._inner.serde = value

    @property
    def config_specs(self):
        return self._inner.config_specs

    def get_next_version(self, current, channel):
        return self._inner.get_next_version(current, channel)

    def with_allowlist(self, extra_allowlist):
        inner = self._inner.with_allowlist(extra_allowlist)
        return self if inner is self._inner else type(self)(inner, profile=self._profile, loop=self._loop)

    def get(self, config):
        return self._sync("get", config)

    def get_tuple(self, config):
        return self._sync("get_tuple", config)

    def list(self, config, *, filter=None, before=None, limit=None):
        return _OwnedSyncIterator(self, lambda: self._inner.list(config, filter=filter, before=before, limit=limit))

    def put(self, config, checkpoint, metadata, new_versions):
        return self._sync("put", config, checkpoint, metadata, new_versions)

    def put_writes(self, config, writes, task_id, task_path=""):
        return self._sync("put_writes", config, writes, task_id, task_path)

    def delete_thread(self, thread_id):
        return self._sync("delete_thread", thread_id)

    def delete_for_runs(self, run_ids):
        return self._sync("delete_for_runs", run_ids)

    def copy_thread(self, source_thread_id, target_thread_id):
        return self._sync("copy_thread", source_thread_id, target_thread_id)

    def prune(self, thread_ids, *, strategy="keep_latest"):
        return self._sync("prune", thread_ids, strategy=strategy)

    def get_delta_channel_history(self, *, config, channels):
        return self._sync("get_delta_channel_history", config=config, channels=channels)

    async def aget(self, config):
        return await self._async("aget", config)

    async def aget_tuple(self, config):
        return await self._async("aget_tuple", config)

    def alist(self, config, *, filter=None, before=None, limit=None):
        return _OwnedAsyncIterator(self, lambda: self._inner.alist(config, filter=filter, before=before, limit=limit))

    async def aput(self, config, checkpoint, metadata, new_versions):
        return await self._async("aput", config, checkpoint, metadata, new_versions)

    async def aput_writes(self, config, writes, task_id, task_path=""):
        return await self._async("aput_writes", config, writes, task_id, task_path)

    async def adelete_thread(self, thread_id):
        return await self._async("adelete_thread", thread_id)

    async def adelete_for_runs(self, run_ids):
        return await self._async("adelete_for_runs", run_ids)

    async def acopy_thread(self, source_thread_id, target_thread_id):
        return await self._async("acopy_thread", source_thread_id, target_thread_id)

    async def aprune(self, thread_ids, *, strategy="keep_latest"):
        return await self._async("aprune", thread_ids, strategy=strategy)

    async def aget_delta_channel_history(self, *, config, channels):
        return await self._async("aget_delta_channel_history", config=config, channels=channels)


class OwnedStore(_Facade, BaseStore):
    @property
    def supports_ttl(self):
        return self._inner.supports_ttl

    @supports_ttl.setter
    def supports_ttl(self, value):
        self._inner.supports_ttl = value

    @property
    def ttl_config(self):
        return self._inner.ttl_config

    @ttl_config.setter
    def ttl_config(self, value):
        self._inner.ttl_config = value

    def batch(self, ops):
        return self._sync("batch", ops)

    async def abatch(self, ops):
        return await self._async("abatch", ops)

    def get(self, namespace, key, *, refresh_ttl=None):
        return self._sync("get", namespace, key, refresh_ttl=refresh_ttl)

    async def aget(self, namespace, key, *, refresh_ttl=None):
        return await self._async("aget", namespace, key, refresh_ttl=refresh_ttl)

    def search(self, namespace_prefix, /, *, query=None, filter=None, limit=10, offset=0, refresh_ttl=None):
        return self._sync("search", namespace_prefix, query=query, filter=filter, limit=limit, offset=offset, refresh_ttl=refresh_ttl)

    async def asearch(self, namespace_prefix, /, *, query=None, filter=None, limit=10, offset=0, refresh_ttl=None):
        return await self._async("asearch", namespace_prefix, query=query, filter=filter, limit=limit, offset=offset, refresh_ttl=refresh_ttl)

    def put(self, namespace, key, value, index=None, *, ttl=NOT_PROVIDED):
        return self._sync("put", namespace, key, value, index, ttl=ttl)

    async def aput(self, namespace, key, value, index=None, *, ttl=NOT_PROVIDED):
        return await self._async("aput", namespace, key, value, index, ttl=ttl)

    def delete(self, namespace, key):
        return self._sync("delete", namespace, key)

    async def adelete(self, namespace, key):
        return await self._async("adelete", namespace, key)

    def list_namespaces(self, *, prefix=None, suffix=None, max_depth=None, limit=100, offset=0):
        return self._sync("list_namespaces", prefix=prefix, suffix=suffix, max_depth=max_depth, limit=limit, offset=offset)

    async def alist_namespaces(self, *, prefix=None, suffix=None, max_depth=None, limit=100, offset=0):
        return await self._async("alist_namespaces", prefix=prefix, suffix=suffix, max_depth=max_depth, limit=limit, offset=offset)
