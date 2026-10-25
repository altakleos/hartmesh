"""Original worker entry refuses retired I/O without inventing an outcome."""

import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest
from _run_output_capture_test_support import capture_environment as capture_environment
from _run_output_store_test_support import RAW_OWNER
from _run_output_store_test_support import output_store as output_store
from langchain_core.language_models.fake_chat_models import FakeMessagesListChatModel
from langchain_core.messages import AIMessage, HumanMessage
from langgraph.graph import END, START, MessagesState, StateGraph
from sqlalchemy import event

from deerflow.config.app_config import AppConfig
from deerflow.runtime.runs import worker
from deerflow.runtime.runs.capture_io import CaptureIOUnsupported
from deerflow.runtime.runs.manager import RunManager
from deerflow.runtime.runs.output import UnavailableOutputReceipt
from deerflow.runtime.runs.schemas import RunStatus
from deerflow.runtime.runs.worker import RunContext, run_agent
from deerflow.runtime.user_context import reset_current_user, set_current_user


async def environment(db, monkeypatch, *, capture=True, cleanup=True):
    manager = RunManager(db.repository, event_store=db.event_store)
    record = await manager.create_or_reject(uuid4().hex, "lead_agent", user_id=RAW_OWNER, kwargs={"output_capture": {"version": 1}} if capture else {})
    bridge = SimpleNamespace(publish=AsyncMock(), publish_end=AsyncMock(), cleanup=AsyncMock(), supports_cross_process=False)
    model = FakeMessagesListChatModel(responses=[AIMessage("Exact offline answer", id="actual-offline-model-id")])
    calls, scheduled, cleanup_tasks, housekeeping = [], [], [], []

    async def node(state, config):
        calls.append("model")
        return {"messages": [await model.ainvoke(state["messages"], config)]}

    builder = StateGraph(MessagesState)
    builder.add_node("model", node)
    builder.add_edge(START, "model")
    builder.add_edge("model", END)
    graph = builder.compile()

    def factory(**kwargs):
        calls.append("factory")
        return graph

    def schedule(run_id, delay):
        scheduled.append((run_id, delay))
        cleanup_tasks.append(asyncio.create_task(bridge.cleanup(run_id, delay=delay)))

    original_task = worker._create_contextless_task

    def track(coroutine):
        task = original_task(coroutine)
        housekeeping.append(task)
        return task

    monkeypatch.setattr(worker, "_create_contextless_task", track)
    monkeypatch.setattr(worker, "capture_output_snapshot", AsyncMock(return_value=None))
    monkeypatch.setattr(worker, "capture_workspace_snapshot", AsyncMock(return_value=None))
    monkeypatch.setattr(worker, "_produced_output_paths", AsyncMock(return_value=[]))
    ctx = RunContext(checkpointer=None, event_store=db.event_store, app_config=AppConfig(models=[], sandbox={"use": "deerflow.sandbox.local:LocalSandboxProvider"}), schedule_stream_cleanup=schedule if cleanup else None)
    return SimpleNamespace(manager=manager, record=record, bridge=bridge, ctx=ctx, factory=factory, calls=calls, scheduled=scheduled, cleanup_tasks=cleanup_tasks, housekeeping=housekeeping)


async def execute(env):
    token = set_current_user(SimpleNamespace(id=RAW_OWNER))
    try:
        await run_agent(env.bridge, env.manager, env.record, ctx=env.ctx, agent_factory=env.factory, graph_input={"messages": [HumanMessage("Actual human input")]}, config={"configurable": {"thread_id": env.record.thread_id}})
    finally:
        reset_current_user(token)


async def settle(env):
    for task in env.housekeeping:
        if "RunManager.cleanup" in task.get_coro().__qualname__ and not task.done():
            task.cancel()  # Its delay/local eviction has no provider operation.
    await asyncio.gather(*env.cleanup_tasks, *env.housekeeping, return_exceptions=True)
    await env.manager.shutdown(timeout=0)


@pytest.mark.asyncio
@pytest.mark.parametrize("state", ["unstarted", "prestart_failed", "sealed_success"])
async def test_retired_original_worker_entry_has_zero_new_effects_and_preserves_SQL(capture_environment, monkeypatch, state):
    db = capture_environment
    env = await environment(db, monkeypatch)
    if state == "prestart_failed":
        assert await env.manager.fail_start_if_pending(env.record.run_id, error="Original prestart failure")
    elif state == "sealed_success":
        await env.manager.try_start(env.record.run_id)
        receipt = UnavailableOutputReceipt(version=1, kind="unavailable", owner_user_id=RAW_OWNER, thread_id=env.record.thread_id, run_id=env.record.run_id, input_message_id=None, reason="unsupported_output")
        result = await env.manager.finalize_output(env.record.run_id, RunStatus.success, output_receipt=receipt)
        assert result.output_receipt_confirmed
    before = await db.repository.get(env.record.run_id, user_id=RAW_OWNER)
    proof = (env.record.status, env.record.terminal_confirmed, env.record.prestart_outcome, dict(env.record.kwargs))
    env.manager.get_capture_io_owner(env.record).retire()
    statements = []

    def observed(_connection, _cursor, statement, _parameters, _context, _many):
        statements.append(statement.split()[0])

    event.listen(db.engine.sync_engine, "before_cursor_execute", observed)
    try:
        await execute(env)
        assert env.calls == []
        assert statements == []
        assert (env.bridge.publish.await_count, env.bridge.publish_end.await_count, env.bridge.cleanup.await_count) == (0, 0, 0)
        assert env.scheduled == [] and env.cleanup_tasks == []
        assert (env.record.status, env.record.terminal_confirmed, env.record.prestart_outcome, env.record.kwargs) == proof
    finally:
        event.remove(db.engine.sync_engine, "before_cursor_execute", observed)
        await settle(env)
    assert await db.repository.get(env.record.run_id, user_id=RAW_OWNER) == before


@pytest.mark.asyncio
async def test_missing_cleanup_capability_never_uses_raw_captured_fallback(capture_environment, monkeypatch):
    db = capture_environment
    env = await environment(db, monkeypatch, cleanup=False)
    try:
        await execute(env)
        assert env.calls == []
        assert env.bridge.cleanup.await_count == 0
        row = await db.repository.get(env.record.run_id, user_id=RAW_OWNER)
        assert row["status"] == "error" and row["kwargs"]["output_evidence"]["reason"] == "failed"
        assert env.record.prestart_outcome.confirmed
    finally:
        await settle(env)


@pytest.mark.asyncio
async def test_missing_profile_preserves_existing_confirmed_prestart_failure(capture_environment, monkeypatch):
    db = capture_environment
    env = await environment(db, monkeypatch, cleanup=False)
    assert await env.manager.fail_start_if_pending(env.record.run_id, error="Keep original failure")
    before = await db.repository.get(env.record.run_id, user_id=RAW_OWNER)
    outcome = env.record.prestart_outcome
    try:
        await execute(env)
        assert env.calls == [] and env.record.prestart_outcome is outcome
        assert await db.repository.get(env.record.run_id, user_id=RAW_OWNER) == before
    finally:
        await settle(env)


@pytest.mark.asyncio
async def test_worker_waits_for_authoritative_prestart_seal_before_retiring(capture_environment, monkeypatch):
    db = capture_environment
    env = await environment(db, monkeypatch, cleanup=False)
    entered, release = asyncio.Event(), asyncio.Event()
    original = env.manager.finalize_output

    async def held(*args, **kwargs):
        entered.set()
        await release.wait()
        return await original(*args, **kwargs)

    monkeypatch.setattr(env.manager, "finalize_output", held)
    closing = asyncio.create_task(env.manager.fail_start_if_pending(env.record.run_id, error="Actual prestart failure"))
    working = None
    premature = None
    try:
        await asyncio.wait_for(entered.wait(), 3)
        working = asyncio.create_task(execute(env))
        await asyncio.wait({working}, timeout=0.03)
        premature = env.manager.get_capture_io_owner(env.record).retired
    finally:
        release.set()
        results = await asyncio.gather(*(task for task in (closing, working) if task is not None), return_exceptions=True)
        await settle(env)
    assert premature is False, "Worker retired the lifetime while the start-lock owner was still sealing nonexecution"
    assert results == [True, None]
    row = await db.repository.get(env.record.run_id, user_id=RAW_OWNER)
    assert row["status"] == "error" and row["kwargs"]["output_evidence"]["reason"] == "failed"
    assert env.calls == []


@pytest.mark.asyncio
async def test_foreign_store_handle_cannot_mint_worker_lifetime_from_saved_capture_kwargs(capture_environment, monkeypatch):
    db = capture_environment
    env = await environment(db, monkeypatch)
    original = env.manager
    before = await db.repository.get(env.record.run_id, user_id=RAW_OWNER)
    env.manager = RunManager(db.repository, event_store=db.event_store)
    env.record = await env.manager.get(env.record.run_id)
    assert env.record.store_only
    try:
        with pytest.raises(CaptureIOUnsupported):
            await execute(env)
        assert env.calls == [] and env.scheduled == []
        assert await db.repository.get(env.record.run_id, user_id=RAW_OWNER) == before
    finally:
        await settle(env)
        await original.shutdown(timeout=0)


@pytest.mark.asyncio
async def test_ordinary_embedded_worker_keeps_its_existing_cleanup_fallback(capture_environment, monkeypatch):
    db = capture_environment
    env = await environment(db, monkeypatch, capture=False, cleanup=False)
    try:
        await execute(env)
        await asyncio.sleep(0)
        assert env.calls == ["factory", "model"]
        env.bridge.cleanup.assert_awaited_once_with(env.record.run_id, delay=60)
        row = await db.repository.get(env.record.run_id, user_id=RAW_OWNER)
        assert row["status"] == "success" and "output_evidence" not in row["kwargs"]
    finally:
        await settle(env)


@pytest.mark.asyncio
async def test_late_graph_error_after_retirement_does_not_stage_execution_failure(capture_environment, monkeypatch):
    db = capture_environment
    env = await environment(db, monkeypatch)
    owner = env.manager.get_capture_io_owner(env.record)
    failures = AsyncMock(return_value={"reason": None, "stop_reason": None})
    monkeypatch.setattr("deerflow.agent_instances.runtime.run_error", failures)
    original_set_status = env.manager.set_status_if_not_cancelled
    status_calls = []

    async def observed_status(*args, **kwargs):
        status_calls.append((args, kwargs))
        return await original_set_status(*args, **kwargs)

    monkeypatch.setattr(env.manager, "set_status_if_not_cancelled", observed_status)

    async def node(state, config):
        env.calls.append("retiring-model")
        owner.retire()
        raise ValueError("Graph continued after host retirement")

    builder = StateGraph(MessagesState)
    builder.add_node("model", node)
    builder.add_edge(START, "model")
    builder.add_edge("model", END)
    graph = builder.compile()
    env.factory = lambda **kwargs: graph
    try:
        await execute(env)
        failures.assert_not_awaited()
        assert status_calls == []
        row = await db.repository.get(env.record.run_id, user_id=RAW_OWNER)
        assert row["status"] == "running" and "output_evidence" not in row["kwargs"]
        assert env.record.status is RunStatus.running and not env.record.terminal_confirmed
    finally:
        await settle(env)
