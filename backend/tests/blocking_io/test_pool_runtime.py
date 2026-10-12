"""Managed qualification, inherited child execution and shutdown stay off-loop."""

import asyncio
import sys
from threading import Lock
from types import SimpleNamespace
from unittest.mock import Mock

import pytest


@pytest.mark.asyncio
async def test_gateway_pool_owner_qualification_and_child_are_drained_off_loop(tmp_path, monkeypatch):
    from app.gateway.pool_runtime import managed_pool_runtime
    from deerflow.agent_instances import runtime
    from deerflow.community.aio_sandbox.aio_sandbox_provider import AioSandboxProvider
    from deerflow.community.aio_sandbox.local_backend import LocalContainerBackend
    from deerflow.spaces.docker import DockerStorageAdapter
    from deerflow.spaces.runtime_owner import PoolRuntimeOwner, StorageRuntimeUnavailable
    from deerflow.utils.file_io import await_drained, run_file_io

    pool = tmp_path / "pool"

    def prepare():
        pool.mkdir(mode=0o750)
        (pool / ".gateway-owner.lock").touch(mode=0o600)
        (pool / ".native-creates").mkdir(mode=0o700)

    await asyncio.to_thread(prepare)
    provider = AioSandboxProvider.__new__(AioSandboxProvider)
    provider._lock = Lock()
    provider._backend = LocalContainerBackend.__new__(LocalContainerBackend)
    provider._reconcile_orphans = Mock()
    monkeypatch.setattr("deerflow.sandbox.sandbox_provider.get_baseline_sandbox_provider", lambda: provider)
    monkeypatch.setattr("deerflow.sandbox.sandbox_provider.shutdown_sandbox_provider", lambda **kwargs: None)
    monkeypatch.setattr(DockerStorageAdapter, "from_local_backend", lambda *_args, **_kwargs: SimpleNamespace(host_id="fixture", _same_host=Mock()))
    monkeypatch.setattr(runtime, "SpaceAttachments", lambda _files, adapter: SimpleNamespace(provider=adapter))
    app = SimpleNamespace(state=SimpleNamespace())
    config = SimpleNamespace(enabled=True, plan=object(), inventory_path=str(pool / "inventory.json"))
    peer = PoolRuntimeOwner(pool)
    try:
        async with managed_pool_runtime(app, config):
            files = SimpleNamespace(inventory_path=config.inventory_path, _managed_runtime_required=True, _pool_runtime_owner=app.state.pool_runtime_owner)
            baseline, _ = await runtime.qualified_attachments(files)
            assert baseline is provider
            assert provider._pool_runtime_current()
            assert await run_file_io(app.state.pool_runtime_owner.native_create_evidence.pending) == ()
            result = await await_drained(run_file_io(provider._backend._run, [sys.executable, "-c", "print('owned')"], check=True, capture_output=True, text=True, timeout=5))
            assert result.stdout.strip() == "owned"
            with pytest.raises(StorageRuntimeUnavailable):
                await run_file_io(peer.acquire)
        await run_file_io(peer.acquire)
    finally:
        await run_file_io(peer.close)
