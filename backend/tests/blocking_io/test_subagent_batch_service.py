"""Durable batch accepted-material checks must stay off the event loop."""

from __future__ import annotations

import asyncio
from enum import Enum
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from _subagent_batch_helpers import make_claimed_item, make_parent_batch_request

import deerflow.tools as tools_module
from deerflow.config.subagent_batches_config import SubagentBatchesConfig
from deerflow.config.subagent_runtime_config import SubagentRuntimeConfig
from deerflow.runtime.accepted_invocation import ResolvedAgentMaterialV1
from deerflow.runtime.tool_evidence import active_tool_receipt_context
from deerflow.subagents import batch_service as service_module
from deerflow.subagents.batch_service import SubagentBatchService

pytestmark = pytest.mark.asyncio


class _Status(Enum):
    PENDING = "pending"
    COMPLETED = "completed"

    @property
    def is_terminal(self) -> bool:
        return self is _Status.COMPLETED


async def test_acceptance_verifies_snapshot_material_off_the_event_loop(
    monkeypatch,
    tmp_path,
) -> None:
    probe = tmp_path / "accepted-snapshot"
    await asyncio.to_thread(probe.write_text, "accepted", encoding="utf-8")

    def blocking_verify(_self) -> None:
        probe.read_text(encoding="utf-8")

    monkeypatch.setattr(
        ResolvedAgentMaterialV1,
        "verify_process_material",
        blocking_verify,
    )
    repository = SimpleNamespace(accept_batch=AsyncMock(return_value={"id": "batch-1", "status": "queued"}))
    service = SubagentBatchService(
        repository=repository,
        config=SubagentBatchesConfig(),
        runtime_config=SubagentRuntimeConfig(max_running=1),
    )

    request = make_parent_batch_request()
    with active_tool_receipt_context(request.parent_tool_receipt):
        result = await service.accept(request)

    assert result["id"] == "batch-1"


async def test_tool_adapter_resolution_runs_off_the_event_loop(
    monkeypatch,
    tmp_path,
) -> None:
    probe = tmp_path / "tool-adapter"
    await asyncio.to_thread(probe.write_text, "accepted", encoding="utf-8")
    app_config = SimpleNamespace(get_model_config=lambda _name: {})
    request = make_parent_batch_request(app_config=app_config)
    result = SimpleNamespace(
        status=_Status.PENDING,
        result=None,
        stop_reason=None,
        token_usage_records=None,
        admission_failure=False,
    )

    class Repository:
        finalized = None

        async def claim_items(self, **_kwargs):
            return [make_claimed_item(request)]

        async def mark_item_running(self, *_args, **_kwargs):
            result.status = _Status.COMPLETED
            result.result = "done"
            return True

        async def finalize_item(self, *_args, **kwargs):
            self.finalized = kwargs
            return True

    callback = None

    class Executor:
        def __init__(self, **kwargs) -> None:
            nonlocal callback
            callback = kwargs["execution_admitted_callback"]

        def execute_async(self, _prompt, task_id=None):
            assert task_id is not None
            assert callback is not None
            asyncio.create_task(callback())
            return "execution-1"

    def blocking_tool_resolution(**_kwargs):
        probe.read_text(encoding="utf-8")
        return []

    repository = Repository()
    monkeypatch.setattr(
        service_module,
        "resolve_subagent_model_name",
        lambda *_args, **_kwargs: "model-a",
    )
    monkeypatch.setattr(service_module, "SubagentExecutor", Executor)
    monkeypatch.setattr(service_module, "SubagentStatus", _Status)
    monkeypatch.setattr(
        service_module,
        "get_background_task_result",
        lambda _execution_id: result,
    )
    monkeypatch.setattr(
        service_module,
        "cleanup_background_task",
        lambda _execution_id: None,
    )
    monkeypatch.setattr(
        tools_module,
        "get_available_tools",
        blocking_tool_resolution,
    )
    service = SubagentBatchService(
        repository=repository,
        config=SubagentBatchesConfig(poll_interval_seconds=0.1),
        runtime_config=SubagentRuntimeConfig(max_running=1),
        app_config=app_config,
    )

    await service.run_once(now=service_module.datetime.now(service_module.UTC))
    await asyncio.gather(*list(service._executions.values()))

    assert repository.finalized is not None
    assert repository.finalized["succeeded"] is True
