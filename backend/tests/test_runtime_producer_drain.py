"""Owned acquisition drainage before a complete startup census, offline only."""

import asyncio
import threading
from contextlib import asynccontextmanager, contextmanager

import pytest
from _sandbox_provider_fakes import _make_provider
from test_provider_lifecycle_admission import denied, installed_gate


@pytest.mark.anyio
@pytest.mark.parametrize("asynchronous", [False, True])
async def test_bootstrap_drains_acquirer_held_before_starting_and_preserves_closed_gate(tmp_path, monkeypatch, asynchronous):
    provider, backend = _make_provider(tmp_path, monkeypatch)
    gate = installed_gate(provider, ready=True)
    entered, release = threading.Event(), threading.Event()

    @contextmanager
    def held(*_args):
        entered.set()
        assert release.wait(5)
        yield

    @asynccontextmanager
    async def held_async(*_args):
        entered.set()
        assert await asyncio.to_thread(release.wait, 5)
        yield

    monkeypatch.setattr(provider, "_chat_scope", held)
    monkeypatch.setattr(provider, "_chat_scope_async", held_async)
    task = asyncio.create_task(provider.acquire_async("thread", user_id="alice") if asynchronous else asyncio.to_thread(provider.acquire, "thread", user_id="alice"))
    drain = None
    try:
        assert await asyncio.to_thread(entered.wait, 5)
        assert not provider._starting
        provider._close_runtime_admission()
        drain = asyncio.create_task(asyncio.to_thread(provider._drain_bootstrap_producers))
        await asyncio.sleep(0.03)
        assert not drain.done()
        release.set()
        with pytest.raises(denied()):
            await asyncio.wait_for(task, 5)
        await asyncio.wait_for(drain, 5)
        with provider._lock:
            provider._require_bootstrap_producers_drained_locked()
        with pytest.raises(denied()):
            gate.require(provider)
        assert not backend.created and not backend.destroyed
    finally:
        release.set()
        await asyncio.gather(task, *([drain] if drain is not None else []), return_exceptions=True)


def test_memory_drained_proof_refuses_retained_start_or_owner_work(tmp_path, monkeypatch):
    from deerflow.spaces.runtime_owner import StorageRuntimeUnavailable

    provider, _backend = _make_provider(tmp_path, monkeypatch)
    installed_gate(provider)
    provider._starting.add("already-submitted")
    with provider._lock, pytest.raises(StorageRuntimeUnavailable):
        provider._require_bootstrap_producers_drained_locked()
    provider._starting.clear()
    reference = provider._owner_admission_gate().enter("alice")
    try:
        with provider._lock, pytest.raises(StorageRuntimeUnavailable):
            provider._require_bootstrap_producers_drained_locked()
    finally:
        provider._owner_admission_gate().end(reference)


@pytest.mark.anyio
@pytest.mark.parametrize("step", ["skills", "lock_path", "lock_file"])
async def test_cancelled_actual_offload_keeps_producer_until_worker_and_late_fd_close(tmp_path, monkeypatch, step):
    from _sandbox_provider_fakes import _aio_mod

    from deerflow.config.paths import Paths
    from deerflow.spaces.runtime_owner import StorageRuntimeUnavailable

    provider, _backend = _make_provider(tmp_path, monkeypatch)
    module = _aio_mod()
    installed_gate(provider, ready=True)
    monkeypatch.setattr(module, "get_paths", lambda: Paths(base_dir=tmp_path))
    entered, release = threading.Event(), threading.Event()
    files = []
    target, name = (module, "_open_lock_file") if step == "lock_file" else (provider, "_thread_lock_path" if step == "lock_path" else "_ensure_skills_projection")
    original = getattr(target, name)

    def held(*args, **kwargs):
        result = original(*args, **kwargs)
        if step == "lock_file":
            files.append(result)
        entered.set()
        assert release.wait(5)
        return result

    monkeypatch.setattr(target, name, held)
    task = asyncio.create_task(provider.acquire_async("thread", user_id="alice"))
    drain = None
    try:
        assert await asyncio.to_thread(entered.wait, 5)
        provider._close_runtime_admission()
        task.cancel()
        await asyncio.sleep(0.03)
        assert not task.done()
        with provider._lock, pytest.raises(StorageRuntimeUnavailable):
            provider._require_bootstrap_producers_drained_locked()
        drain = asyncio.create_task(asyncio.to_thread(provider._drain_bootstrap_producers))
        await asyncio.sleep(0.03)
        assert not drain.done()
        release.set()
        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(task, 5)
        await asyncio.wait_for(drain, 5)
        assert all(file.closed for file in files)
        assert len(files) == (1 if step == "lock_file" else 0)
    finally:
        release.set()
        await asyncio.gather(task, *([drain] if drain is not None else []), return_exceptions=True)
