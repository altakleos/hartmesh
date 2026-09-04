import importlib
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from _subagent_batch_helpers import make_parent_batch_request
from langchain_core.messages import ToolMessage
from langgraph.types import Command

from deerflow.config.subagent_batches_config import SubagentBatchesConfig
from deerflow.runtime.agent_revision import RESOLVED_AGENT_MATERIAL_CONTEXT_KEY
from deerflow.runtime.execution_policy import ExecutionBudgetV1
from deerflow.runtime.tenant_identity import TENANT_REFERENCE_CONTEXT_KEY
from deerflow.runtime.tool_evidence import (
    TOOL_EVIDENCE_CONTEXT_KEY,
    TOOL_EVIDENCE_SINK_KEY,
    active_tool_receipt_context,
)
from deerflow.subagents.batch_acceptance import (
    PARENT_BATCH_ACCEPTANCE_CONTEXT_KEY,
)
from deerflow.tools.builtins.batch_task_tool import BatchTaskItem

tool_module = importlib.import_module("deerflow.tools.builtins.batch_task_tool")


def _runtime(request=None):
    context = {
        "thread_id": "thread-1",
        "run_id": "run-1",
        "user_id": "user-1",
        "user_role": "member",
    }
    if request is not None:
        context.update(
            {
                PARENT_BATCH_ACCEPTANCE_CONTEXT_KEY: request.accepted_parent,
                RESOLVED_AGENT_MATERIAL_CONTEXT_KEY: (request.resolved_parent_material),
                TENANT_REFERENCE_CONTEXT_KEY: request.tenant,
                TOOL_EVIDENCE_CONTEXT_KEY: request.parent_tool_binding,
                TOOL_EVIDENCE_SINK_KEY: request.parent_tool_receipt_sink,
            }
        )
    return SimpleNamespace(
        state={},
        context=context,
        config={
            "metadata": {
                "model_name": "model-a",
                "allowed_subagents": ["general-purpose"],
                "tool_groups": ["web"],
            },
            "configurable": {"thread_id": "thread-1"},
        },
    )


def _runtime_with_budget(request, budget: ExecutionBudgetV1):
    runtime = _runtime(request)
    runtime.context["accepted_execution_budget"] = budget
    return runtime


def _message(command: Command) -> ToolMessage:
    messages = command.update["messages"]
    assert len(messages) == 1 and isinstance(messages[0], ToolMessage)
    return messages[0]


def test_batch_task_schema_has_no_trusted_parent_or_tenant_inputs() -> None:
    properties = tool_module.batch_task.tool_call_schema.model_fields

    assert {
        "tenant",
        "user_id",
        "thread_id",
        "run_id",
        "tool_call_id",
        "accepted_parent",
        "execution_spec",
        "credentials",
    }.isdisjoint(properties)


@pytest.mark.asyncio
async def test_batch_task_is_explicit_idempotent_submission(monkeypatch) -> None:
    app_config = SimpleNamespace(subagent_batches=SubagentBatchesConfig())
    parent = make_parent_batch_request(app_config=app_config)
    submitter = AsyncMock()
    submitter.accept.return_value = {
        "id": "subagent-batch-1",
        "status": "queued",
        "total_items": 2,
    }
    monkeypatch.setattr(tool_module, "get_subagent_batch_submitter", lambda: submitter)

    with active_tool_receipt_context(parent.parent_tool_receipt):
        command = await tool_module.batch_task.coroutine(
            runtime=_runtime(parent),
            title="Process records",
            items=[
                BatchTaskItem(key="record-1", prompt="Process one"),
                BatchTaskItem(key="record-2", prompt="Process two"),
            ],
            subagent_type="general-purpose",
            tool_call_id="call-1",
            max_live_items=20,
            max_running_items=5,
        )

    message = _message(command)
    request = submitter.accept.await_args.args[0]
    assert request.submission_key == (f"{parent.parent_tool_receipt.receipt_id}:call-1")
    assert request.user_id == "user-1"
    assert [item.key for item in request.items] == ["record-1", "record-2"]
    assert request.limits.max_live_items == 20
    assert request.limits.max_running_items == 5
    assert request.accepted_parent is parent.accepted_parent
    assert request.resolved_parent_material is parent.resolved_parent_material
    assert message.additional_kwargs["subagent_batch_id"] == "subagent-batch-1"
    assert "running independently" in message.content


@pytest.mark.asyncio
async def test_batch_task_composes_accepted_policy_with_batch_limits(monkeypatch) -> None:
    app_config = SimpleNamespace(
        subagent_batches=SubagentBatchesConfig(
            max_attempts=5,
            max_total_runtime_seconds=500,
        )
    )
    parent = make_parent_batch_request(app_config=app_config)
    submitter = AsyncMock()
    submitter.accept.return_value = {
        "id": "subagent-batch-1",
        "status": "queued",
        "total_items": 2,
    }
    monkeypatch.setattr(tool_module, "get_subagent_batch_submitter", lambda: submitter)
    budget = ExecutionBudgetV1.build(
        max_batch_items=4,
        max_batch_concurrency=2,
        max_batch_attempts=6,
        max_batch_runtime_seconds=100,
    )

    with active_tool_receipt_context(parent.parent_tool_receipt):
        await tool_module.batch_task.coroutine(
            runtime=_runtime_with_budget(parent, budget),
            title="Policy bounded",
            items=[
                BatchTaskItem(key="record-1", prompt="Process one"),
                BatchTaskItem(key="record-2", prompt="Process two"),
            ],
            subagent_type="general-purpose",
            tool_call_id="call-1",
            max_live_items=20,
            max_running_items=5,
        )

    limits = submitter.accept.await_args.args[0].limits
    assert limits.max_live_items == 4
    assert limits.max_running_items == 2
    assert limits.max_attempts == 3
    assert limits.max_total_runtime_seconds == 100


@pytest.mark.asyncio
async def test_batch_task_rejects_duplicate_item_keys_without_submitting(monkeypatch) -> None:
    submitter = AsyncMock()
    monkeypatch.setattr(tool_module, "get_subagent_batch_submitter", lambda: submitter)

    command = await tool_module.batch_task.coroutine(
        runtime=_runtime(),
        title="Duplicates",
        items=[
            BatchTaskItem(key="same", prompt="one"),
            BatchTaskItem(key="same", prompt="two"),
        ],
        subagent_type="general-purpose",
        tool_call_id="call-1",
    )

    message = _message(command)
    assert message.status == "error"
    assert "unique" in message.content
    submitter.accept.assert_not_awaited()


@pytest.mark.asyncio
async def test_bound_batch_tools_use_the_explicit_submitter(monkeypatch) -> None:
    explicit = AsyncMock()
    explicit.get_batch.return_value = {
        "id": "subagent-batch-explicit",
        "thread_id": "thread-1",
        "status": "running",
        "total_items": 2,
        "counts": {"running": 1, "succeeded": 1},
    }
    fallback = AsyncMock()
    monkeypatch.setattr(tool_module, "get_subagent_batch_submitter", lambda: fallback)

    tools = {tool.name: tool for tool in tool_module.bind_batch_tools(explicit)}
    result = await tools["batch_status"].coroutine(
        runtime=_runtime(),
        batch_id="subagent-batch-explicit",
    )

    assert "subagent-batch-explicit" in result
    explicit.get_batch.assert_awaited_once_with(
        batch_id="subagent-batch-explicit",
        user_id="user-1",
    )
    fallback.get_batch.assert_not_awaited()


@pytest.mark.asyncio
async def test_bound_batch_task_uses_the_explicit_app_config(monkeypatch) -> None:
    app_config = SimpleNamespace(subagent_batches=SubagentBatchesConfig())
    parent = make_parent_batch_request(
        app_config=app_config,
        tool_call_id="call-explicit",
    )
    submitter = AsyncMock()
    submitter.accept.return_value = {
        "id": "subagent-batch-explicit",
        "status": "queued",
        "total_items": 1,
    }

    tools = {
        tool.name: tool
        for tool in tool_module.bind_batch_tools(
            submitter,
            app_config=app_config,
        )
    }
    with active_tool_receipt_context(parent.parent_tool_receipt):
        await tools["batch_task"].coroutine(
            runtime=_runtime(parent),
            title="Explicit config",
            items=[BatchTaskItem(key="record-1", prompt="Process one")],
            subagent_type="general-purpose",
            tool_call_id="call-explicit",
            max_live_items=None,
            max_running_items=None,
        )

    request = submitter.accept.await_args.args[0]
    assert request.app_config is app_config


@pytest.mark.asyncio
async def test_batch_task_rejects_forged_context_without_active_receipt(
    monkeypatch,
) -> None:
    submitter = AsyncMock()
    parent = make_parent_batch_request()
    monkeypatch.setattr(
        tool_module,
        "get_subagent_batch_submitter",
        lambda: submitter,
    )

    command = await tool_module.batch_task.coroutine(
        runtime=_runtime(parent),
        title="Forged",
        items=[BatchTaskItem(key="record-1", prompt="Process one")],
        subagent_type="general-purpose",
        tool_call_id="call-1",
    )

    assert _message(command).status == "error"
    assert "tool_attempt_not_active" in _message(command).content
    submitter.accept.assert_not_awaited()


@pytest.mark.asyncio
async def test_batch_task_does_not_treat_zero_limit_as_default(
    monkeypatch,
) -> None:
    submitter = AsyncMock()
    parent = make_parent_batch_request()
    monkeypatch.setattr(
        tool_module,
        "get_subagent_batch_submitter",
        lambda: submitter,
    )

    with active_tool_receipt_context(parent.parent_tool_receipt):
        command = await tool_module.batch_task.coroutine(
            runtime=_runtime(parent),
            title="Invalid limit",
            items=[BatchTaskItem(key="record-1", prompt="Process one")],
            subagent_type="general-purpose",
            tool_call_id="call-1",
            max_live_items=0,
        )

    assert _message(command).status == "error"
    assert "batch_max_live_items_invalid" in _message(command).content
    submitter.accept.assert_not_awaited()


@pytest.mark.asyncio
async def test_bound_batch_tools_do_not_fall_back_after_runtime_stops(monkeypatch) -> None:
    fallback = AsyncMock()
    monkeypatch.setattr(tool_module, "get_subagent_batch_submitter", lambda: fallback)

    tools = {
        tool.name: tool
        for tool in tool_module.bind_batch_tools(
            submitter_provider=lambda: None,
        )
    }
    result = await tools["batch_status"].coroutine(
        runtime=_runtime(),
        batch_id="subagent-batch-stopped",
    )

    assert result == "Durable subagent batches are unavailable."
    fallback.get_batch.assert_not_awaited()


@pytest.mark.asyncio
async def test_status_and_cancel_conceal_a_batch_from_another_thread(
    monkeypatch,
) -> None:
    submitter = AsyncMock()
    submitter.get_batch.return_value = {
        "id": "subagent-batch-other-thread",
        "thread_id": "thread-2",
        "status": "running",
        "total_items": 1,
        "counts": {"running": 1},
    }
    monkeypatch.setattr(
        tool_module,
        "get_subagent_batch_submitter",
        lambda: submitter,
    )

    status = await tool_module.batch_status.coroutine(
        runtime=_runtime(),
        batch_id="subagent-batch-other-thread",
    )
    cancelled = await tool_module.cancel_batch.coroutine(
        runtime=_runtime(),
        batch_id="subagent-batch-other-thread",
    )

    assert status == "Batch not found."
    assert cancelled == "Batch not found."
    submitter.cancel_batch.assert_not_awaited()
