"""Configured adapters and Memory thread bridging under the real loop detector."""

import asyncio
import threading

import pytest
from test_capture_resource_facades import provided_resources as provided_resources
from test_capture_resource_facades import resource_backend as resource_backend
from test_capture_resource_facades import seed_checkpoint

from deerflow.runtime.runs.capture_io import CaptureIOOwner, capture_io_scope

pytestmark = pytest.mark.asyncio


async def test_configured_capture_history_and_store_remain_on_original_loop(provided_resources):
    env = provided_resources
    owner = CaptureIOOwner()
    history = None
    try:
        with capture_io_scope(owner):
            saved = await seed_checkpoint(env.saver, "strict-capture")
            await env.store.aput(("strict-capture",), "item", {"value": 1}, index=False)
            assert (await env.store.aget(("strict-capture",), "item")).value == {"value": 1}
            history = env.saver.alist(saved)
            assert (await anext(history)).checkpoint["channel_values"] == {"value": "retained"}
        owner.retire()
        await owner.drain()
        if env.kind != "memory":
            assert not env.native_saver.lock.locked()
    finally:
        if history is not None:
            await history.aclose()
        owner.retire()
        await owner.drain()


@pytest.mark.parametrize("resource_backend", ["memory"], indirect=True)
async def test_memory_sync_worker_bridge_never_submits_nested_executor_work(provided_resources, monkeypatch):
    loop = asyncio.get_running_loop()
    owner = CaptureIOOwner()
    original = loop.run_in_executor
    submissions = []

    def submit(executor, call, *args):
        submissions.append(True)
        assert len(submissions) == 1, "Memory bridge must not need a default-executor worker to release its caller"
        return original(executor, call, *args)

    def work():
        provided_resources.store.put(("strict-memory",), "item", {"value": 7})
        return provided_resources.store.get(("strict-memory",), "item").value

    monkeypatch.setattr(loop, "run_in_executor", submit)
    try:
        with capture_io_scope(owner):
            assert await asyncio.wait_for(asyncio.to_thread(work), 3) == {"value": 7}
        assert submissions == [True]
    finally:
        owner.retire()
        await owner.drain()


@pytest.mark.parametrize("resource_backend", ["sqlite"], indirect=True)
async def test_unified_sqlite_store_resolves_derived_path_off_loop(resource_backend):
    from deerflow.config.database_config import DatabaseConfig
    from deerflow.runtime.store.async_provider import make_store

    resource_backend.config.checkpointer = None
    resource_backend.config.database = DatabaseConfig(backend="sqlite", sqlite_dir=str(resource_backend.path.parent / "unified"))
    async with make_store(resource_backend.config) as store:
        await store.aput(("unified-capture",), "item", {"value": 9}, index=False)
        assert (await store.aget(("unified-capture",), "item")).value == {"value": 9}


@pytest.mark.parametrize("resource_backend", ["sqlite", "postgres"], indirect=True)
@pytest.mark.parametrize("kind", ["saver", "store"])
async def test_factory_preparation_drains_cancelled_worker_before_any_adapter_open(resource_backend, monkeypatch, kind):
    from deerflow.runtime.checkpointer import async_provider as savers
    from deerflow.runtime.store import async_provider as stores

    module = savers if kind == "saver" else stores
    helper = (
        "_prepare_sqlite_checkpointer_path"
        if kind == "saver" and resource_backend.kind == "sqlite"
        else "_prepare_sqlite_store_path"
        if resource_backend.kind == "sqlite"
        else "_ensure_postgres_imports"
        if kind == "saver"
        else "_ensure_postgres_store_imports"
    )
    actual = getattr(module, helper)
    loop, loop_thread = asyncio.get_running_loop(), threading.get_ident()
    entered, completed = asyncio.Event(), asyncio.Event()
    release, opened = threading.Event(), []

    def preparation(*args):
        try:
            assert threading.get_ident() != loop_thread
            result = actual(*args)
            loop.call_soon_threadsafe(entered.set)
            release.wait()
            return result
        finally:
            loop.call_soon_threadsafe(completed.set)

    monkeypatch.setattr(module, helper, preparation)
    if kind == "saver" and resource_backend.kind == "postgres":
        original = module._build_postgres_pool

        def construct(*args, **kwargs):
            opened.append(True)
            return original(*args, **kwargs)

        monkeypatch.setattr(module, "_build_postgres_pool", construct)
    else:
        if kind == "saver":
            from langgraph.checkpoint.sqlite.aio import AsyncSqliteSaver as adapter
        elif resource_backend.kind == "sqlite":
            from langgraph.store.sqlite.aio import AsyncSqliteStore as adapter
        else:
            # Import fixture preparation is explicitly off-loop as in the factory.
            adapter = await asyncio.to_thread(actual)
        original = adapter.from_conn_string

        def construct(*args, **kwargs):
            opened.append(True)
            return original(*args, **kwargs)

        monkeypatch.setattr(adapter, "from_conn_string", staticmethod(construct))

    async def opening():
        factory = module.make_checkpointer if kind == "saver" else module.make_store
        async with factory(resource_backend.config):
            raise AssertionError("cancelled preparation must never reach a serving adapter")

    operation = asyncio.create_task(opening())
    try:
        await asyncio.wait_for(entered.wait(), 3)
        operation.cancel()
        first, _ = await asyncio.wait({operation}, timeout=0.05)
        operation.cancel()
        second, _ = await asyncio.wait({operation}, timeout=0.05)
        release.set()
        result = (await asyncio.wait_for(asyncio.gather(operation, return_exceptions=True), 3))[0]
        await asyncio.wait_for(completed.wait(), 3)
        assert not first and not second
        assert isinstance(result, asyncio.CancelledError)
        assert opened == []
    finally:
        release.set()
        await asyncio.gather(operation, return_exceptions=True)
        await asyncio.wait_for(completed.wait(), 3)
