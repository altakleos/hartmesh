"""Escaping typed capacity refusals, unlike handled tool results, end a run."""

import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest
from _run_output_store_test_support import RAW_OWNER
from _run_output_store_test_support import output_store as output_store
from langchain.agents import create_agent
from langchain_core.language_models.fake_chat_models import FakeMessagesListChatModel
from langchain_core.messages import AIMessage, HumanMessage
from langchain_core.tools import tool

from deerflow.agents.middlewares.tool_error_handling_middleware import ToolErrorHandlingMiddleware
from deerflow.config.app_config import AppConfig
from deerflow.runtime.runs import worker
from deerflow.runtime.runs.manager import RunManager
from deerflow.runtime.runs.schemas import RunStatus
from deerflow.runtime.runs.worker import RunContext, run_agent
from deerflow.runtime.user_context import reset_current_user, set_current_user
from deerflow.sandbox import middleware as sandbox_middleware
from deerflow.sandbox.exceptions import SandboxCapacityExceededError


class OfflineModel(FakeMessagesListChatModel):
    invocations: int = 0

    def bind_tools(self, tools, **kwargs):
        return self

    def _generate(self, messages, stop=None, run_manager=None, **kwargs):
        self.invocations += 1
        return super()._generate(messages, stop=stop, run_manager=run_manager, **kwargs)


class CapacityWordingError(RuntimeError):
    # A declaration on another type, or model-visible wording, is not authority.
    run_stop_reason = "sandbox_capacity_exceeded"


async def run_graph(db, monkeypatch, graph):
    manager = RunManager(db.repository)
    record = await manager.create_or_reject(uuid4().hex, "lead_agent", user_id=RAW_OWNER)
    bridge = SimpleNamespace(publish=AsyncMock(), publish_end=AsyncMock(), cleanup=AsyncMock())
    housekeeping = []
    original_task = worker._create_contextless_task

    def track(coroutine):
        task = original_task(coroutine)
        housekeeping.append(task)
        return task

    monkeypatch.setattr(worker, "_create_contextless_task", track)
    token = set_current_user(SimpleNamespace(id=RAW_OWNER))
    try:
        await run_agent(
            bridge,
            manager,
            record,
            ctx=RunContext(checkpointer=None, event_store=None, app_config=AppConfig(models=[], sandbox={"use": "deerflow.sandbox.local:LocalSandboxProvider"})),
            agent_factory=lambda *, config: graph,
            graph_input={"messages": [HumanMessage("Answer without retrying a refused tool.")]},
            config={"configurable": {"thread_id": record.thread_id}},
        )
        stored = await db.repository.get(record.run_id, user_id=RAW_OWNER)
        assert stored is not None and record.terminal_confirmed
        assert stored["status"] == record.status.value
        bridge.publish_end.assert_awaited_once_with(record.run_id)
        return record, stored
    finally:
        reset_current_user(token)
        for task in housekeeping:
            if "RunManager.cleanup" in task.get_coro().__qualname__ and not task.done():
                task.cancel()  # Only the existing post-terminal eviction delay.
        await asyncio.gather(*housekeeping, return_exceptions=True)
        await manager.shutdown(timeout=0)


@pytest.mark.asyncio
@pytest.mark.parametrize("typed", [True, False], ids=["typed-capacity", "wording-and-attribute-only"])
async def test_pre_model_capacity_has_a_durable_typed_stop_reason(output_store, monkeypatch, typed):
    model = OfflineModel(responses=[AIMessage("This model must not run.")])
    middleware = sandbox_middleware.SandboxMiddleware(lazy_init=False)
    failure = SandboxCapacityExceededError(active=2, replicas=2) if typed else CapacityWordingError("SandboxCapacityExceededError: sandbox_capacity_exceeded")
    attempts = []

    async def refuse(thread_id, *, user_id, owner_id):
        attempts.append((thread_id, user_id, owner_id))
        raise failure

    # Keep the real eager before-agent hook and SDK execution; isolate provider
    # acquisition/configuration, without creating a native sandbox or live model.
    monkeypatch.setattr(middleware, "_prepare_agent_skill_projection", lambda *args, **kwargs: None)
    monkeypatch.setattr(middleware, "_acquire_sandbox_async", refuse)
    monkeypatch.setattr(sandbox_middleware, "get_sandbox_provider", lambda: SimpleNamespace())
    monkeypatch.setattr(sandbox_middleware, "safe_app_config_async", AsyncMock(return_value=None))
    monkeypatch.setattr(sandbox_middleware, "authorize_sandbox_execution_async", AsyncMock())
    graph = create_agent(model=model, tools=[], middleware=[middleware])

    record, stored = await run_graph(output_store, monkeypatch, graph)

    assert len(attempts) == 1 and attempts[0][:2] == (record.thread_id, RAW_OWNER)
    assert model.invocations == 0
    assert record.status is RunStatus.error
    assert stored["stop_reason"] == ("sandbox_capacity_exceeded" if typed else None)
    assert record.stop_reason == stored["stop_reason"]


@pytest.mark.asyncio
async def test_handled_tool_capacity_does_not_mark_the_surviving_run_retryable(output_store, monkeypatch):
    calls = []

    @tool
    def saturated_tool() -> str:
        """A tool whose admitted provider has no free sandbox slot."""
        calls.append("tool")
        raise SandboxCapacityExceededError(active=2, replicas=2)

    model = OfflineModel(
        responses=[
            AIMessage("", tool_calls=[{"id": "capacity-call", "name": "saturated_tool", "args": {}}]),
            AIMessage("The workspace is busy; I can still answer this turn."),
        ]
    )
    graph = create_agent(model=model, tools=[saturated_tool], middleware=[ToolErrorHandlingMiddleware()])

    record, stored = await run_graph(output_store, monkeypatch, graph)

    assert calls == ["tool"] and model.invocations == 2
    assert record.status is RunStatus.success
    assert stored["stop_reason"] is None and record.stop_reason is None
