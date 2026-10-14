"""Real worker preparation, error frames and completion with no native effects."""

import asyncio
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest
from test_agent_execution_runtime import execution as make_execution

from deerflow.agent_instances import runtime
from deerflow.agent_instances.contract import ResidentBusy
from deerflow.agent_instances.work_execution import WorkAttempt
from deerflow.config.app_config import AppConfig
from deerflow.runtime.runs.manager import RunManager
from deerflow.runtime.runs.schemas import RunStatus
from deerflow.runtime.runs.store.memory import MemoryRunStore
from deerflow.runtime.runs.worker import RunContext, run_agent
from deerflow.spaces.service import SpaceOperationPending


@pytest.mark.anyio
@pytest.mark.parametrize("readiness_fails", [False, True])
async def test_worker_records_work_readiness_before_dispatch_and_completes_failed_readiness(monkeypatch, readiness_fails):
    order = []
    residents = SimpleNamespace(attachments=SimpleNamespace(fence_reason=AsyncMock(return_value=None)), run_ended=AsyncMock(side_effect=lambda *args, **kwargs: order.append("ended")))

    async def ready(execution, scope, attachment_id):
        order.append("ready")
        assert attachment_id == "d" * 32
        if readiness_fails:
            raise RuntimeError("readiness SQL failed")

    work_service = SimpleNamespace(bind=AsyncMock(), workspace_ready=AsyncMock(side_effect=ready), finish=AsyncMock(side_effect=lambda *args, **kwargs: order.append("finish") or "failed"))
    work = WorkAttempt(work_service, "work", "attempt")
    authority = SimpleNamespace(instances=SimpleNamespace(residents=residents), validate=AsyncMock(), update_run_metadata=AsyncMock())
    execution = replace(make_execution(), authority=authority, work=work)
    config = AppConfig(sandbox={"use": "deerflow.sandbox.local.local_sandbox_provider:LocalSandboxProvider"})
    sandbox = SimpleNamespace(id="d" * 32, requires_container_recycle=False, execution_outcome_uncertain=False)
    provider = runtime.InstanceSandboxProvider(execution, sandbox, app_config=config, skill_revision="capture")
    provider.close = Mock(side_effect=lambda: order.append("close"))

    async def prepare(bound, **_kwargs):
        runtime.current_environment().attachment_id = sandbox.id
        order.append("prepare")
        return provider

    monkeypatch.setattr(runtime, "prepare_environment", prepare)
    manager = RunManager(store=MemoryRunStore())
    record = await manager.create("chat", user_id="requester")
    bridge = SimpleNamespace(publish=AsyncMock(), publish_end=AsyncMock(), cleanup=AsyncMock())

    class Agent:
        async def astream(self, *_args, **_kwargs):
            order.append("model")
            yield {"messages": []}

    await run_agent(bridge, manager, record, ctx=RunContext(checkpointer=None, event_store=None, app_config=config, agent_execution=execution), agent_factory=lambda **kwargs: Agent(), graph_input={}, config={})
    assert order == (["prepare", "ready"] + ([] if readiness_fails else ["model"]) + ["close", "finish", "ended"])
    assert record.status == (RunStatus.error if readiness_fails else RunStatus.success)


@pytest.mark.anyio
@pytest.mark.parametrize("failure", ["refusal", "fenced", "pending", "diagnostic_cancel"])
async def test_worker_maps_workspace_error_and_always_completes_registry(monkeypatch, failure):
    attachments = SimpleNamespace(fence_reason=AsyncMock(return_value="operator" if failure == "fenced" else None))
    if failure == "diagnostic_cancel":
        attachments.fence_reason.side_effect = asyncio.CancelledError
    residents = SimpleNamespace(attachments=attachments, run_ended=AsyncMock())
    execution = replace(make_execution(), authority=SimpleNamespace(instances=SimpleNamespace(residents=residents), validate=AsyncMock()))
    config = AppConfig(sandbox={"use": "deerflow.sandbox.local.local_sandbox_provider:LocalSandboxProvider"})
    sandbox = SimpleNamespace(id="d" * 32, requires_container_recycle=False, execution_outcome_uncertain=False)
    provider = runtime.InstanceSandboxProvider(execution, sandbox, app_config=config, skill_revision="capture")
    provider.close = Mock()
    preparation_ids = []

    async def prepare(bound, **kwargs):
        assert bound is execution
        preparation_ids.append(kwargs.get("run_id"))
        runtime.current_environment().attachment_id = sandbox.id
        return provider

    monkeypatch.setattr(runtime, "prepare_environment", prepare)
    manager = RunManager(store=MemoryRunStore())
    record = await manager.create("chat", user_id="requester")
    bridge = SimpleNamespace(publish=AsyncMock(), publish_end=AsyncMock(), cleanup=AsyncMock())

    class Agent:
        async def astream(self, *_args, **_kwargs):
            if failure == "refusal":
                raise ResidentBusy("Analyst", retry="later")
            if failure == "pending":
                raise SpaceOperationPending("files changed", cause="lifecycle")
            raise RuntimeError("original failure")
            yield {}  # pragma: no cover

    await run_agent(bridge, manager, record, ctx=RunContext(checkpointer=None, event_store=None, app_config=config, agent_execution=execution), agent_factory=lambda **kwargs: Agent(), graph_input={}, config={})
    assert preparation_ids == [record.run_id]
    residents.run_ended.assert_awaited_once()
    assert residents.run_ended.call_args.args[3] == sandbox.id
    frames = [call.args[2] for call in bridge.publish.call_args_list if call.args[1] == "error"]
    if failure == "diagnostic_cancel":
        assert record.status == RunStatus.interrupted and not frames
    else:
        assert record.status == RunStatus.error and len(frames) == 1
        expected = {"refusal": ("ResidentBusy", "busy"), "fenced": ("ResidentInterrupted", "operator"), "pending": ("ResidentFilesBusy", "storage-busy")}
        name, reason = expected[failure]
        assert (frames[0]["name"], frames[0]["reason"]) == (name, reason)
        assert record.stop_reason == (None if failure == "fenced" else reason)
        if failure == "refusal":
            assert frames[0]["status"] == 409 and frames[0]["retry"] == "later"
        if failure == "pending":
            assert frames[0]["cause"] == "lifecycle"
