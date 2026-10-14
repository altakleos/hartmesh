"""Real client preparation and owned completion keep SDK work off-loop."""

import threading
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import AsyncMock

import httpx
import pytest
from test_agent_execution_runtime import execution as make_execution

from deerflow.agent_instances import docker_transport, runtime
from deerflow.community.aio_sandbox import aio_sandbox
from deerflow.runtime.runs.schemas import RunStatus


@pytest.mark.asyncio
async def test_client_readiness_aliases_package_copy_and_completion_offload_sync_sdk(monkeypatch):
    owner_thread = threading.get_ident()
    observed = []

    def offloop(name):
        assert threading.get_ident() != owner_thread
        observed.append(name)

    class Transport(httpx.MockTransport):
        def __init__(self, _container_id):
            def handle(request):
                offloop("readiness")
                return httpx.Response(200)

            super().__init__(handle)

        def prepare_home_aliases(self):
            offloop("aliases")

    class Sandbox:
        def __init__(self, identifier, *_args, **_kwargs):
            self.id = identifier
            self.requires_container_recycle = self.execution_outcome_uncertain = False

        def execute_command(self, _command):
            offloop("command")
            return "HARTMESH_HOME_READY"

        def update_file(self, _destination, content):
            offloop("update")
            self.content = content

        def download_file(self, _destination):
            offloop("download")
            return self.content

        def close(self):
            offloop("close")

    monkeypatch.setattr(docker_transport, "DockerControlTransport", Transport)
    monkeypatch.setattr(aio_sandbox, "AioSandbox", Sandbox)
    residents = SimpleNamespace(run_ended=AsyncMock())
    authority = SimpleNamespace(validate=AsyncMock(), instances=SimpleNamespace(residents=residents))
    execution = replace(make_execution(), authority=authority)
    config = SimpleNamespace(skills=SimpleNamespace(model_copy=lambda **_kwargs: "scoped"), model_copy=lambda **_kwargs: "config")
    attached = SimpleNamespace(id="d" * 32, container_id="e" * 64)
    provider = await runtime._prepare_client(execution, SimpleNamespace(sandbox_ready_timeout=lambda: 1), attached, SimpleNamespace(revision="capture", files=[("folder/README.md", b"public bytes")]), config)
    environment = runtime.ExecutionEnvironment(execution, provider, attached.id)
    await runtime.complete_environment(execution, environment, SimpleNamespace(), SimpleNamespace(run_id="run", user_id="requester", status=RunStatus.success))
    assert observed == ["readiness", "aliases", "command", "command", "update", "download", "close"]
    residents.run_ended.assert_awaited_once()
