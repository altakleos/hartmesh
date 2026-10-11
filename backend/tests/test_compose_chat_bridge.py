"""Owning-loop admission preserves caller context and cancellation ownership."""

import asyncio
import threading

import pytest
from deerflow_extension_api.storage import StorageUnsupported

from deerflow.spaces.chat_bridge import ChatStorageHost
from deerflow.spaces.facade import _credential_allowed, storage_credential_scope
from deerflow.utils.file_io import await_drained, run_file_io


@pytest.mark.asyncio
async def test_provider_shutdown_failure_still_drains_host_before_storage_closes(monkeypatch):
    from types import SimpleNamespace

    from app.gateway import chat_storage, deps, storage_spaces
    from deerflow.sandbox import sandbox_provider
    from deerflow.spaces.chat_bridge import get_chat_storage_host

    entered, finish, returned = (threading.Event() for _ in range(3))

    def qualification():
        entered.set()
        assert finish.wait(5)
        returned.set()

    async def prepare(*args):
        await await_drained(run_file_io(qualification))

    async def close_storage(app):
        assert returned.is_set(), "Storage closed with host qualification still running"
        app.state.storage_closed = True

    def fail_shutdown(**kwargs):
        raise RuntimeError("provider shutdown failed")

    app = SimpleNamespace(state=SimpleNamespace(storage_closed=False))
    monkeypatch.setattr(chat_storage, "prepare_chat_storage", prepare)
    monkeypatch.setattr(storage_spaces, "close_storage_spaces", close_storage)
    monkeypatch.setattr(sandbox_provider, "shutdown_sandbox_provider", fail_shutdown)
    async with chat_storage.chat_storage_lifecycle(app, enabled=True):
        host = app.state.chat_storage_host
        monkeypatch.setattr(sandbox_provider, "get_initialized_sandbox_provider", lambda: SimpleNamespace(_chat_storage_host=host))
        request = asyncio.create_task(host.prepare_async("alice", "chat-a"))
        assert await asyncio.to_thread(entered.wait, 2)
        close = asyncio.create_task(deps._close_runtime_storage(app))
        try:
            await asyncio.sleep(0.03)
            assert not close.done() and not app.state.storage_closed
            finish.set()
            with pytest.raises(RuntimeError, match="provider shutdown failed"):
                await close
            assert returned.is_set() and app.state.storage_closed
            assert get_chat_storage_host() is None
        finally:
            finish.set()
            await asyncio.gather(close, request, return_exceptions=True)


def test_gateway_run_context_supplies_host_owned_chat_scope(monkeypatch):
    from fastapi import FastAPI
    from starlette.requests import Request

    from app.gateway import deps

    app = FastAPI()
    app.state.checkpointer = object()
    app.state.run_event_store = object()
    app.state.thread_store = object()
    app.state.chat_storage_scope = object()
    monkeypatch.setattr(deps, "get_config", lambda: None)
    context = deps.get_run_context(Request({"type": "http", "app": app}))
    assert context.chat_storage_scope is app.state.chat_storage_scope


@pytest.mark.asyncio
@pytest.mark.parametrize("enabled", [False, True])
async def test_gateway_owns_host_callback_and_closes_it_on_startup_failure(monkeypatch, enabled):
    from types import SimpleNamespace
    from unittest.mock import AsyncMock

    from app.gateway import chat_storage
    from deerflow.spaces.chat_bridge import get_chat_storage_host

    app = SimpleNamespace(state=SimpleNamespace())
    prepare = AsyncMock(return_value="prepared")
    monkeypatch.setattr(chat_storage, "prepare_chat_storage", prepare)
    saved = None
    with pytest.raises(RuntimeError, match="later startup failed"):
        async with chat_storage.chat_storage_lifecycle(app, enabled=enabled):
            saved = get_chat_storage_host()
            if enabled:
                assert saved is app.state.chat_storage_host
                assert await saved.prepare_async("alice", "chat-a") == "prepared"
                prepare.assert_awaited_once_with(app, "alice", "chat-a")
                assert callable(app.state.chat_storage_scope)
            else:
                assert saved is None and app.state.chat_storage_scope is None
            raise RuntimeError("later startup failed")
    assert get_chat_storage_host() is None and app.state.chat_storage_host is None
    if enabled:
        with pytest.raises(StorageUnsupported):
            await saved.prepare_async("alice", "chat-a")


@pytest.mark.asyncio
async def test_owner_and_foreign_callers_use_one_loop_and_keep_credential_denial():
    owner = asyncio.get_running_loop()
    observed = []

    async def prepare(user_id, thread_id):
        observed.append((asyncio.get_running_loop(), _credential_allowed.get(), user_id, thread_id))
        return "prepared"

    host = ChatStorageHost(owner, prepare)
    try:
        with storage_credential_scope(False):
            assert await host.prepare_async("alice", "chat-a") == "prepared"
            assert await asyncio.to_thread(host.prepare_sync, "alice", "chat-a") == "prepared"
            assert await asyncio.to_thread(lambda: asyncio.run(host.prepare_async("alice", "chat-a"))) == "prepared"
        assert observed == [(owner, False, "alice", "chat-a")] * 3
    finally:
        await host.close()


@pytest.mark.asyncio
async def test_sync_owner_loop_and_file_io_pool_waits_are_refused_before_submission():
    owner = asyncio.get_running_loop()
    observed = []

    async def prepare(*args):
        observed.append(args)

    host = ChatStorageHost(owner, prepare)
    try:
        with pytest.raises(StorageUnsupported):
            host.prepare_sync("alice", "chat-a")
        with pytest.raises(StorageUnsupported):
            await run_file_io(host.prepare_sync, "alice", "chat-a")
        with pytest.raises(StorageUnsupported):
            await run_file_io(lambda: asyncio.run(host.prepare_async("alice", "chat-a")))
        assert observed == []
    finally:
        await host.close()


@pytest.mark.asyncio
async def test_foreign_cancellation_waits_for_owned_verification_even_when_repeated():
    owner = asyncio.get_running_loop()
    entered, release, finished, returned = (threading.Event() for _ in range(4))
    foreign = {}

    def verify():
        entered.set()
        try:
            assert release.wait(10)
        finally:
            finished.set()

    async def prepare(*args):
        await await_drained(run_file_io(verify))

    host = ChatStorageHost(owner, prepare)

    def on_foreign_loop():
        async def run():
            foreign.update(loop=asyncio.get_running_loop(), task=asyncio.current_task())
            try:
                await host.prepare_async("alice", "chat-a")
            except asyncio.CancelledError:
                assert finished.is_set()
                return "cancelled"
            finally:
                returned.set()

        return asyncio.run(run())

    task = asyncio.create_task(asyncio.to_thread(on_foreign_loop))
    try:
        assert await asyncio.to_thread(entered.wait, 5)
        foreign["loop"].call_soon_threadsafe(foreign["task"].cancel)
        await asyncio.sleep(0.03)
        foreign["loop"].call_soon_threadsafe(foreign["task"].cancel)
        await asyncio.sleep(0.03)
        assert not returned.is_set() and not task.done()
        release.set()
        assert await task == "cancelled"
    finally:
        release.set()
        await asyncio.gather(task, return_exceptions=True)
        await host.close()


@pytest.mark.asyncio
async def test_closed_host_refuses_new_work_without_calling_old_database():
    observed = []

    async def prepare(*args):
        observed.append(args)

    host = ChatStorageHost(asyncio.get_running_loop(), prepare)
    await host.close()
    with pytest.raises(StorageUnsupported):
        await host.prepare_async("alice", "chat-a")
    with pytest.raises(StorageUnsupported):
        await asyncio.to_thread(host.prepare_sync, "alice", "chat-a")
    assert observed == []


@pytest.mark.asyncio
async def test_cancellation_before_owner_task_creation_has_no_admitted_work(monkeypatch):
    observed = []
    queued = asyncio.Event()
    requests = []

    async def prepare(*args):
        observed.append(args)

    host = ChatStorageHost(asyncio.get_running_loop(), prepare)
    start = host._start

    def deferred(request):
        requests.append(request)
        queued.set()

    monkeypatch.setattr(host, "_start", deferred)
    task = asyncio.create_task(host.prepare_async("alice", "chat-a"))
    try:
        await queued.wait()
        task.cancel()
        await asyncio.sleep(0.03)
        assert not task.done()
        start(requests[0])
        with pytest.raises(asyncio.CancelledError):
            await task
        assert observed == [] and not host._requests
    finally:
        if requests and not requests[0].completion.done():
            start(requests[0])
        await asyncio.gather(task, return_exceptions=True)
        await host.close()


@pytest.mark.asyncio
async def test_lifecycle_close_waits_until_started_verification_drains():
    entered, release, finished = (threading.Event() for _ in range(3))

    def verify():
        entered.set()
        try:
            assert release.wait(10)
        finally:
            finished.set()

    async def prepare(*args):
        await await_drained(run_file_io(verify))

    host = ChatStorageHost(asyncio.get_running_loop(), prepare)
    task = asyncio.create_task(host.prepare_async("alice", "chat-a"))
    closing = None
    try:
        assert await asyncio.to_thread(entered.wait, 5)
        closing = asyncio.create_task(host.close())
        await asyncio.sleep(0.03)
        assert not closing.done()
        with pytest.raises(StorageUnsupported):
            await host.prepare_async("alice", "chat-b")
        release.set()
        await closing
        assert finished.is_set() and not host._requests
        with pytest.raises(asyncio.CancelledError):
            await task
    finally:
        release.set()
        await asyncio.gather(task, *(tuple([closing]) if closing is not None else ()), return_exceptions=True)
        await host.close()


@pytest.mark.asyncio
async def test_stopped_owner_loop_refuses_before_callback_creation():
    observed = []

    async def prepare(*args):
        observed.append(args)

    owner = asyncio.new_event_loop()
    host = ChatStorageHost(owner, prepare)
    try:
        with pytest.raises(StorageUnsupported):
            await host.prepare_async("alice", "chat-a")
        with pytest.raises(StorageUnsupported):
            await asyncio.to_thread(host.prepare_sync, "alice", "chat-a")
        assert observed == [] and not host._requests
    finally:
        owner.close()


@pytest.mark.asyncio
async def test_native_scope_without_owning_host_refuses_requestless_legacy_fallback(tmp_path, monkeypatch):
    from app.gateway.chat_storage import prepare_chat_request
    from deerflow.config.paths import Paths, paths_scope
    from deerflow.spaces.chat_paths import ChatStoragePaths
    from deerflow.spaces.contract import InvalidPrincipal

    monkeypatch.setattr("deerflow.spaces.chat_bridge.get_chat_storage_host", lambda: None)
    with paths_scope(ChatStoragePaths(Paths(tmp_path / "legacy"), user_id="alice")):
        with pytest.raises(InvalidPrincipal):
            await prepare_chat_request(None, "chat-a")
    assert not (tmp_path / "legacy").exists()
