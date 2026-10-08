"""Preparation owns its blocking SDK work until it and client cleanup settle."""

import asyncio
import threading
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from test_agent_execution_runtime import execution


@pytest.mark.asyncio
async def test_repeated_cancellation_drains_native_package_upload_before_close(monkeypatch):
    from deerflow.agent_instances import public_skills, runtime
    from deerflow.community.aio_sandbox import aio_sandbox
    from deerflow.community.aio_sandbox.aio_sandbox_provider import AioSandboxProvider
    from deerflow.community.aio_sandbox.local_backend import LocalContainerBackend
    from deerflow.spaces.attachments import Attachment
    from deerflow.spaces.docker import DockerStorageAdapter

    entered, release, finished = threading.Event(), threading.Event(), threading.Event()
    state = {"closed": 0, "late_close": False}

    class SDK:
        def __init__(self, *args, **kwargs):
            self.id = "d" * 32

        def execute_command(self, *args):
            return "OK"

        def update_file(self, path, content):
            entered.set()
            assert release.wait(5), "Fixture must release its paused upload"
            finished.set()

        def download_file(self, path):
            return b"fixture"

        def close(self):
            state["late_close"] = not finished.is_set()
            state["closed"] += 1

    class Ready:
        def __init__(self, *args, **kwargs):
            pass

        def __enter__(self):
            return self

        def __exit__(self, *args):
            pass

        def get(self, *args, **kwargs):
            return SimpleNamespace(status_code=200)

    monkeypatch.setattr(aio_sandbox, "AioSandbox", SDK)
    monkeypatch.setattr(runtime.httpx, "Client", Ready)
    monkeypatch.setattr(DockerStorageAdapter, "from_local_backend", lambda backend: SimpleNamespace(host_id="fixture"))
    monkeypatch.setattr(public_skills, "capture_public_skills", lambda *args: public_skills.PublicSkillCapture("e" * 64, frozenset({"fixture"}), (("public/fixture/SKILL.md", b"fixture"),)))
    provider = object.__new__(AioSandboxProvider)
    provider._backend = object.__new__(LocalContainerBackend)
    provider._config = {"ready_timeout": 10}
    bound = execution()
    bound.authority.validate = AsyncMock()
    bound.authority.instances = SimpleNamespace(files=SimpleNamespace(attachments=SimpleNamespace(provider=SimpleNamespace(host_id="fixture"), resume=AsyncMock(return_value=Attachment("d" * 32, "f" * 32, "fixture", "a" * 64)))))
    task = asyncio.create_task(runtime.prepare_environment(bound, baseline_provider=provider, app_config=SimpleNamespace()))
    try:
        assert await asyncio.to_thread(entered.wait, 3)
        task.cancel()
        await asyncio.sleep(0.02)
        task.cancel()
        await asyncio.sleep(0.02)
        assert not task.done() and state["closed"] == 0
        release.set()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert finished.is_set() and state == {"closed": 1, "late_close": False}
    finally:
        release.set()
        await asyncio.gather(task, return_exceptions=True)
        assert await asyncio.to_thread(finished.wait, 3)
