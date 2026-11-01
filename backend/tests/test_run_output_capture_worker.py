"""Actual offline LangGraph/model callbacks and SQL output-seal boundaries."""

import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest
from _run_output_store_test_support import RAW_OWNER
from _run_output_store_test_support import output_store as output_store
from langchain_core.language_models.fake_chat_models import FakeMessagesListChatModel
from langchain_core.messages import AIMessage, HumanMessage
from langgraph.graph import END, START, MessagesState, StateGraph
from sqlalchemy import event, text, update
from sqlalchemy.util import await_only

from deerflow.persistence.models.run_event import RunEventRow
from deerflow.persistence.run.model import RunRow
from deerflow.runtime.capture_resources import OwnedCheckpointSaver
from deerflow.runtime.events.store.db import DbRunEventStore
from deerflow.runtime.runs.manager import RunManager
from deerflow.runtime.runs.output import OutputEvidenceError, canonical_text_digest
from deerflow.runtime.runs.schemas import RunStatus
from deerflow.runtime.runs.worker import RunContext, run_agent
from deerflow.runtime.user_context import reset_current_user, set_current_user


async def _environment(environment, monkeypatch):
    async with environment.engine.begin() as connection:
        if environment.dialect == "sqlite":
            await connection.execute(text("BEGIN"))
        await connection.run_sync(lambda conn: RunEventRow.metadata.create_all(conn, tables=[RunEventRow.__table__]))
    monkeypatch.setattr("deerflow.runtime.runs.worker._produced_output_paths", AsyncMock(return_value=[]))
    monkeypatch.setattr("deerflow.runtime.runs.worker.capture_output_snapshot", AsyncMock(return_value=None))
    monkeypatch.setattr("deerflow.runtime.runs.worker.capture_workspace_snapshot", AsyncMock(return_value=None))
    events = DbRunEventStore(environment.sf)
    manager = RunManager(environment.repository, event_store=events)
    human = HumanMessage("  Original question\n", additional_kwargs={"channel_turn_id": "host-publication"})
    record = await manager.create_or_reject(uuid4().hex, "lead_agent", user_id=RAW_OWNER, kwargs={"output_capture": {"version": 1}})
    model = FakeMessagesListChatModel(responses=[AIMessage("  Exact answer\nα", id="genuine-model-id")])
    calls = []

    async def node(state, config):
        calls.append(config.get("run_id"))
        return {"messages": [await model.ainvoke(state["messages"], config)]}

    builder = StateGraph(MessagesState)
    builder.add_node("model", node)
    builder.add_edge(START, "model")
    builder.add_edge("model", END)
    graph = builder.compile()
    bridge = SimpleNamespace(publish=AsyncMock(), publish_end=AsyncMock(), cleanup=AsyncMock(), supports_cross_process=False)
    return SimpleNamespace(events=events, manager=manager, record=record, human=human, model=model, calls=calls, graph=graph, bridge=bridge, graph_input={"messages": [human]})


async def _execute(env, *, completed=None):
    def factory(**kwargs):
        env.journal = kwargs["config"]["context"]["__run_journal"]
        return env.graph

    token = set_current_user(SimpleNamespace(id=RAW_OWNER))
    cleanup_tasks = []

    def schedule_cleanup(run_id, delay):
        cleanup_tasks.append(asyncio.create_task(env.bridge.cleanup(run_id, delay=delay)))

    try:
        await run_agent(
            env.bridge,
            env.manager,
            env.record,
            ctx=RunContext(checkpointer=getattr(env, "checkpointer", None), event_store=env.events, on_run_completed=completed, schedule_stream_cleanup=schedule_cleanup),
            agent_factory=factory,
            graph_input=env.graph_input,
            config={"run_id": uuid4(), "configurable": {"thread_id": env.record.thread_id}},
        )
    finally:
        reset_current_user(token)
        await asyncio.gather(*cleanup_tasks)


@pytest.mark.asyncio
async def test_actual_worker_seals_exact_callback_source_and_genuine_sdk_human_id(output_store, monkeypatch):
    env = await _environment(output_store, monkeypatch)
    assert env.human.id is None
    await _execute(env)
    row = await output_store.repository.get(env.record.run_id, user_id=RAW_OWNER)
    assert row["status"] == "success"
    receipt = row["kwargs"].get("output_evidence")
    assert receipt is not None, "A real completed capture run must seal output with its status"
    assert receipt["kind"] == "text"
    assert env.human.id and receipt["input_message_id"] == env.human.id
    assert receipt["assistant_message_id"] == "genuine-model-id"
    assert receipt["text_sha256"] == canonical_text_digest("  Exact answer\nα")
    stored = await env.events.list_messages_by_run(env.record.thread_id, env.record.run_id, user_id=RAW_OWNER)
    source = next(event for event in stored if event["seq"] == receipt["event_seq"])
    assert source["content"]["id"] == "genuine-model-id"
    assert source["content"]["content"] == "  Exact answer\nα"
    assert len(env.calls) == 1


@pytest.mark.asyncio
async def test_actual_worker_flush_uncertainty_never_terminalizes_staged_success(output_store, monkeypatch, caplog):
    env = await _environment(output_store, monkeypatch)
    completed = AsyncMock()
    calls = []

    async def fail(events):
        calls.append(len(events))
        raise RuntimeError("synthetic event write uncertainty")

    monkeypatch.setattr(env.events, "put_batch", fail)
    await _execute(env, completed=completed)
    row = await output_store.repository.get(env.record.run_id, user_id=RAW_OWNER)
    assert row["status"] == "running", "Proof failure must not become status-only success or fabricated execution failure"
    assert "output_evidence" not in row["kwargs"]
    assert env.record.ownership_lost
    assert len(env.calls) == 1
    assert len(calls) == 1, "Unknown event commits must not be blindly appended again"
    completed.assert_not_awaited()
    with pytest.raises(OutputEvidenceError):
        await env.manager.get_output_snapshot(env.record.run_id, user_id=RAW_OWNER)
    assert f"Run {env.record.run_id} output could not be confirmed: " in caplog.text, "The cause must be in the log before the run is fenced"
    assert f"Run {env.record.run_id} was fenced" in caplog.text and "lost lease" not in caplog.text
    assert "synthetic event write uncertainty" not in caplog.text


@pytest.mark.asyncio
async def test_known_local_source_fence_is_not_acknowledgeable_pending(output_store):
    manager = RunManager(output_store.repository)
    record = await manager.create_or_reject(uuid4().hex, user_id=RAW_OWNER, kwargs={"output_capture": {"version": 1}})
    await manager.try_start(record.run_id)
    await manager._mark_ownership_lost(record, require_active=False, reason="Fixed output uncertainty")
    record.terminal_write_unconfirmed = False
    with pytest.raises(OutputEvidenceError):
        await manager.get_output_snapshot(record.run_id, user_id=RAW_OWNER)
    # A peer-owned ACTIVE row is not shadowed by this worker's stale fence.
    async with output_store.sf() as session, session.begin():
        await session.execute(update(RunRow).where(RunRow.run_id == record.run_id).values(owner_worker_id="peer-worker"))
    snapshot = await manager.get_output_snapshot(record.run_id, user_id=RAW_OWNER)
    assert snapshot.status == RunStatus.running


def _graph(node, *, name="model", checkpointer=None):
    builder = StateGraph(MessagesState)
    builder.add_node(name, node)
    builder.add_edge(START, name)
    builder.add_edge(name, END)
    return builder.compile(checkpointer=checkpointer)


@pytest.mark.asyncio
@pytest.mark.parametrize("case", ["autonomous", "nested", "auxiliary_node", "middleware", "history_id", "manual", "hidden", "tool", "human_input", "reasoning", "reasoning_only", "mixed_reasoning", "mixed_image", "failure"])
async def test_actual_worker_output_selection_matrix(output_store, monkeypatch, case):
    from langchain_core.messages import ToolMessage

    env = await _environment(output_store, monkeypatch)
    expected = "text"
    expected_status = "success"
    if case == "autonomous":
        env.graph_input = {"messages": []}
    elif case == "nested":
        env.graph = _graph(env.graph.with_config(tags=["lead_agent"]))
        expected = "ambiguous_source"
    elif case == "middleware":
        env.model = env.model.with_config(tags=["middleware:synthetic", "lead_agent"])
        expected = "ambiguous_source"
    elif case == "auxiliary_node":
        expected = "ambiguous_source"
    elif case == "history_id":
        env.graph_input["messages"].insert(0, AIMessage("Old answer", id="genuine-model-id"))
        # The reducer replaces that history slot, leaving the actual human at
        # the root suffix. Never search backward for the reused AI identity.
        expected = "unsupported_output"
    elif case == "manual":
        expected = "ambiguous_source"
    elif case == "hidden":
        env.model.responses[0].additional_kwargs["hide_from_ui"] = True
        expected = "unsupported_output"
    elif case == "reasoning":
        env.model.responses[0].additional_kwargs["reasoning_content"] = "Synthetic auxiliary reasoning must never be the answer"
    elif case == "reasoning_only":
        env.model.responses[0].content = ""
        env.model.responses[0].additional_kwargs["reasoning_content"] = "Synthetic auxiliary reasoning must never be the answer"
        expected = "unsupported_output"
    elif case == "mixed_reasoning":
        env.model.responses[0].content = [{"type": "text", "text": "Visible answer"}, {"type": "reasoning", "text": "Excluded"}]
    elif case == "mixed_image":
        env.model.responses[0].content = [{"type": "text", "text": "Visible subset"}, {"type": "image", "url": "Excluded"}]
        expected = "unsupported_output"
    elif case == "tool":
        expected = "unsupported_output"
    elif case == "human_input":
        expected = "human_input_required"
    elif case == "failure":
        expected = "failed"
        expected_status = "error"

    if case != "nested":

        async def node(state, config):
            env.calls.append("actual-node")
            if case == "failure":
                raise RuntimeError("Synthetic execution failed")
            if case == "manual":
                answer = AIMessage("Manually appended prose", id="manual-id")
            else:
                answer = await env.model.ainvoke(state["messages"], config)
            messages = [answer]
            if case in {"tool", "human_input"}:
                artifact = {"human_input": {"kind": "human_input_request"}} if case == "human_input" else None
                messages.append(ToolMessage("Tool control prose", tool_call_id="synthetic-tool", artifact=artifact))
            return {"messages": messages}

        env.graph = _graph(node, name="auxiliary" if case == "auxiliary_node" else "model")

    await _execute(env)
    row = await output_store.repository.get(env.record.run_id, user_id=RAW_OWNER)
    assert row["status"] == expected_status
    receipt = row["kwargs"].get("output_evidence")
    assert receipt is not None
    assert receipt["kind"] == ("text" if expected == "text" else "unavailable")
    if expected != "text":
        assert receipt["reason"] == expected
    if case == "mixed_reasoning":
        assert receipt["text_sha256"] == canonical_text_digest("Visible answer"), "Reasoning must stay out of the delivered text"
    if case == "autonomous":
        assert receipt["input_message_id"] is None
    assert len(env.calls) <= 1


@pytest.mark.asyncio
async def test_actual_streamed_model_completion_matches_chunk_to_root_message(output_store, monkeypatch):
    from langchain_core.language_models.fake_chat_models import FakeListChatModel

    env = await _environment(output_store, monkeypatch)
    model = FakeListChatModel(responses=["  Exact streamed answer\nα"])

    async def node(state, config):
        result = None
        async for chunk in model.astream(state["messages"], config):
            result = chunk if result is None else result + chunk
        return {"messages": [result]}

    env.graph = _graph(node)
    await _execute(env)
    row = await output_store.repository.get(env.record.run_id, user_id=RAW_OWNER)
    receipt = row["kwargs"].get("output_evidence")
    assert row["status"] == "success" and receipt is not None and receipt["kind"] == "text"
    assert receipt["text_sha256"] == canonical_text_digest("  Exact streamed answer\nα")
    stored = await env.events.list_messages_by_run(env.record.thread_id, env.record.run_id, user_id=RAW_OWNER)
    source = next(event for event in stored if event["seq"] == receipt["event_seq"])
    assert source["content"]["type"] == "AIMessageChunk"
    assert source["content"]["id"] == receipt["assistant_message_id"]


@pytest.mark.asyncio
async def test_public_capture_flush_allows_later_real_model_and_root_completion(output_store, monkeypatch):
    env = await _environment(output_store, monkeypatch)
    env.model.responses = [AIMessage("Intermediate answer", id="intermediate-ai"), AIMessage("Final exact answer", id="final-ai")]

    async def node(state, config):
        first = await env.model.ainvoke(state["messages"], config)
        await env.journal.flush()
        final = await env.model.ainvoke([*state["messages"], first], config)
        return {"messages": [first, final]}

    env.graph = _graph(node)
    await _execute(env)
    row = await output_store.repository.get(env.record.run_id, user_id=RAW_OWNER)
    receipt = row["kwargs"].get("output_evidence")
    assert row["status"] == "success" and receipt is not None and receipt["kind"] == "text"
    assert receipt["assistant_message_id"] == "final-ai"
    assert receipt["text_sha256"] == canonical_text_digest("Final exact answer")


async def _until(predicate):
    async def wait():
        while not predicate():
            await asyncio.sleep(0)

    await asyncio.wait_for(wait(), 5)


@pytest.mark.asyncio
async def test_actual_journal_native_write_is_owned_through_repeated_worker_cancellation(output_store, monkeypatch):
    env = await _environment(output_store, monkeypatch)
    entered, release, native_cancelled = asyncio.Event(), asyncio.Event(), asyncio.Event()
    baseline = output_store.engine.sync_engine.pool.checkedout()

    def pause(_connection, _cursor, statement, _parameters, _context, _many):
        if "INSERT INTO run_events" in statement and not entered.is_set():

            async def hold():
                entered.set()
                try:
                    await release.wait()
                except asyncio.CancelledError:
                    native_cancelled.set()
                    raise

            await_only(hold())

    event.listen(output_store.engine.sync_engine, "after_cursor_execute", pause)
    task = asyncio.create_task(_execute(env))
    env.record.task = task
    try:
        await asyncio.wait_for(entered.wait(), 5)
        await _until(lambda: env.record.finalizing)
        snapshot = await env.manager.get_output_snapshot(env.record.run_id, user_id=RAW_OWNER)
        assert snapshot.status == RunStatus.running and snapshot.local_finalizing
        task.cancel()
        task.cancel()
        done, _ = await asyncio.wait({task}, timeout=0.05)
        assert not done and not native_cancelled.is_set()
        release.set()
        with pytest.raises(asyncio.CancelledError):
            await task
        row = await output_store.repository.get(env.record.run_id, user_id=RAW_OWNER)
        assert row["status"] == "success" and row["kwargs"]["output_evidence"]["kind"] == "text"
        assert output_store.engine.sync_engine.pool.checkedout() == baseline
    finally:
        release.set()
        await asyncio.gather(task, return_exceptions=True)
        event.remove(output_store.engine.sync_engine, "after_cursor_execute", pause)


@pytest.mark.asyncio
async def test_actual_worker_source_read_failure_fences_without_status_only_fallback(output_store, monkeypatch):
    env = await _environment(output_store, monkeypatch)
    completed = AsyncMock()

    async def fail(*_args, **_kwargs):
        raise RuntimeError("Synthetic source query uncertainty")

    monkeypatch.setattr(env.events, "list_messages_by_run", fail)
    await _execute(env, completed=completed)
    row = await output_store.repository.get(env.record.run_id, user_id=RAW_OWNER)
    assert row["status"] == "running" and "output_evidence" not in row["kwargs"]
    assert env.record.ownership_lost and len(env.calls) == 1
    completed.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("action", ["interrupt", "rollback"])
async def test_durable_cancel_wins_text_cas_then_seals_cancelled_from_active(output_store, monkeypatch, action):
    env = await _environment(output_store, monkeypatch)
    if action == "rollback":
        from langgraph.checkpoint.memory import InMemorySaver

        env.checkpointer = OwnedCheckpointSaver(InMemorySaver(), profile="memory", loop=asyncio.get_running_loop())

        async def node(state, config):
            env.calls.append("actual-node")
            return {"messages": [await env.model.ainvoke(state["messages"], config)]}

        env.graph = _graph(node, checkpointer=env.checkpointer)
    original = env.manager.finalize_output
    proposed = []

    async def race(run_id, status, *, output_receipt, **kwargs):
        proposed.append((status, output_receipt.kind, getattr(output_receipt, "reason", None)))
        if output_receipt.kind == "text":
            async with output_store.sf() as session, session.begin():
                await session.execute(update(RunRow).where(RunRow.run_id == run_id).values(cancel_action=action))
        else:
            row = await output_store.repository.get(run_id, user_id=RAW_OWNER)
            assert row["status"] == "running" and "output_evidence" not in row["kwargs"]
        return await original(run_id, status, output_receipt=output_receipt, **kwargs)

    monkeypatch.setattr(env.manager, "finalize_output", race)
    await _execute(env)
    row = await output_store.repository.get(env.record.run_id, user_id=RAW_OWNER)
    status = RunStatus.error if action == "rollback" else RunStatus.interrupted
    assert row["status"] == status.value
    assert row["kwargs"]["output_evidence"]["reason"] == "cancelled"
    assert proposed == [(RunStatus.success, "text", None), (status, "unavailable", "cancelled")]
    assert len(env.calls) == 1
    if action == "rollback":
        assert await env.checkpointer.aget_tuple({"configurable": {"thread_id": env.record.thread_id}}) is None


@pytest.mark.asyncio
async def test_direct_execution_task_cancellation_seals_real_cancelled_outcome(output_store, monkeypatch):
    env = await _environment(output_store, monkeypatch)
    entered, release = asyncio.Event(), asyncio.Event()

    async def node(state, config):
        env.calls.append("actual-node")
        answer = await env.model.ainvoke(state["messages"], config)
        entered.set()
        await release.wait()
        return {"messages": [answer]}

    env.graph = _graph(node)
    task = asyncio.create_task(_execute(env))
    env.record.task = task
    try:
        await asyncio.wait_for(entered.wait(), 5)
        assert not env.record.abort_event.is_set()
        task.cancel()
        await task
        row = await output_store.repository.get(env.record.run_id, user_id=RAW_OWNER)
        assert row["status"] == "interrupted"
        assert row["kwargs"]["output_evidence"]["reason"] == "cancelled"
        assert len(env.calls) == 1
    finally:
        release.set()
        await asyncio.gather(task, return_exceptions=True)


@pytest.mark.asyncio
async def test_real_agent_input_middleware_retains_genuine_human_identity(output_store, monkeypatch):
    from langchain.agents import create_agent

    from deerflow.agents.middlewares.dynamic_context_middleware import DynamicContextMiddleware
    from deerflow.agents.middlewares.input_sanitization_middleware import InputSanitizationMiddleware

    env = await _environment(output_store, monkeypatch)
    env.graph = create_agent(env.model, tools=[], middleware=[InputSanitizationMiddleware(), DynamicContextMiddleware(memory_enabled=False)])
    await _execute(env)
    row = await output_store.repository.get(env.record.run_id, user_id=RAW_OWNER)
    receipt = row["kwargs"].get("output_evidence")
    assert row["status"] == "success" and receipt is not None and receipt["kind"] == "text"
    assert receipt["input_message_id"] == env.human.id
    events = await env.events.list_messages_by_run(env.record.thread_id, env.record.run_id, user_id=RAW_OWNER)
    human = next(event["content"] for event in events if event["event_type"] == "llm.human.input")
    assert human["id"] == env.human.id + "__user"
    assert human["content"] == "  Original question\n"
    assert human["additional_kwargs"]["channel_turn_id"] == "host-publication"


@pytest.mark.asyncio
async def test_idless_history_ai_receives_no_new_source_authority_after_reducer_assigns_id(output_store, monkeypatch):
    env = await _environment(output_store, monkeypatch)
    previous = AIMessage("Historical answer")
    env.graph_input = {"messages": [previous]}

    async def node(state, config):
        env.model.responses[0].id = state["messages"][0].id
        return {"messages": [await env.model.ainvoke(state["messages"], config)]}

    env.graph = _graph(node)
    await _execute(env)
    row = await output_store.repository.get(env.record.run_id, user_id=RAW_OWNER)
    assert previous.id
    assert row["status"] == "success"
    assert row["kwargs"]["output_evidence"]["reason"] == "ambiguous_source"


@pytest.mark.asyncio
async def test_autonomous_capture_does_not_adopt_historical_human_journal_anchor(output_store, monkeypatch):
    from langgraph.checkpoint.memory import InMemorySaver

    env = await _environment(output_store, monkeypatch)
    env.graph_input = {"messages": []}
    env.checkpointer = OwnedCheckpointSaver(InMemorySaver(), profile="memory", loop=asyncio.get_running_loop())

    async def node(state, config):
        return {"messages": [await env.model.ainvoke(state["messages"], config)]}

    env.graph = _graph(node, checkpointer=env.checkpointer)
    await env.graph.aupdate_state({"configurable": {"thread_id": env.record.thread_id}}, {"messages": [HumanMessage("Historical question", id="old-human"), AIMessage("Historical answer", id="old-ai")]}, as_node="model")
    await _execute(env)
    row = await output_store.repository.get(env.record.run_id, user_id=RAW_OWNER)
    receipt = row["kwargs"].get("output_evidence")
    assert row["status"] == "success" and receipt is not None and receipt["kind"] == "text"
    assert receipt["input_message_id"] is None
    stored = await env.events.list_messages_by_run(env.record.thread_id, env.record.run_id, user_id=RAW_OWNER)
    assert any(event["event_type"] == "llm.human.input" and event["content"]["id"] == "old-human" for event in stored)


@pytest.mark.asyncio
@pytest.mark.parametrize("change", ["content", "marker", "identity"])
async def test_changed_consumed_input_cannot_replace_the_frozen_host_input(output_store, monkeypatch, change):
    env = await _environment(output_store, monkeypatch)

    async def node(state, config):
        current = state["messages"][-1]
        if change == "content":
            current.content = "Changed after host capture"
        elif change == "marker":
            current.additional_kwargs["channel_turn_id"] = "another-publication"
        else:
            current = current.model_copy(update={"id": "different-human"})
        return {"messages": [await env.model.ainvoke([current], config)]}

    env.graph = _graph(node)
    await _execute(env)
    row = await output_store.repository.get(env.record.run_id, user_id=RAW_OWNER)
    assert row["status"] == "running" and "output_evidence" not in row["kwargs"]
    assert env.record.ownership_lost


@pytest.mark.asyncio
async def test_malformed_completed_callback_is_uncertainty_not_a_sealed_selector_refusal(output_store, monkeypatch):
    env = await _environment(output_store, monkeypatch)
    # Simulate a provider mutating an already-created response before the SDK
    # delivers on_llm_end. This deliberately bypasses constructor validation.
    env.model.responses[0].usage_metadata = {"input_tokens": "malformed", "output_tokens": 1, "total_tokens": 0}
    await _execute(env)
    row = await output_store.repository.get(env.record.run_id, user_id=RAW_OWNER)
    assert row["status"] == "running" and "output_evidence" not in row["kwargs"]
    assert env.record.ownership_lost and len(env.calls) == 1


@pytest.mark.asyncio
async def test_unknown_event_commit_is_not_appended_again_or_sealed(output_store, monkeypatch):
    env = await _environment(output_store, monkeypatch)
    original = env.events.put_batch
    writes = []

    async def lost_response(batch):
        writes.append(len(batch))
        await original(batch)
        raise OSError("Synthetic lost event commit response")

    monkeypatch.setattr(env.events, "put_batch", lost_response)
    await _execute(env)
    row = await output_store.repository.get(env.record.run_id, user_id=RAW_OWNER)
    assert row["status"] == "running" and "output_evidence" not in row["kwargs"]
    events = await env.events.list_messages_by_run(env.record.thread_id, env.record.run_id, user_id=RAW_OWNER)
    assert len(writes) == 1
    assert len([event for event in events if event["event_type"] == "llm.ai.response"]) == 1
    assert env.record.ownership_lost and len(env.calls) == 1


@pytest.mark.asyncio
async def test_actual_worker_confirms_lost_seal_commit_response_without_reexecution(output_store, monkeypatch):
    env = await _environment(output_store, monkeypatch)
    original = output_store.repository.finalize_owned
    writes = []

    async def lost_response(*args, **kwargs):
        result = await original(*args, **kwargs)
        writes.append(result)
        if result:
            raise OSError("Synthetic lost atomic seal response")
        return result

    monkeypatch.setattr(output_store.repository, "finalize_owned", lost_response)
    await _execute(env)
    row = await output_store.repository.get(env.record.run_id, user_id=RAW_OWNER)
    assert row["status"] == "success" and row["kwargs"]["output_evidence"]["kind"] == "text"
    assert writes == [True] and len(env.calls) == 1
    assert env.record.terminal_confirmed and not env.record.ownership_lost


@pytest.mark.asyncio
@pytest.mark.parametrize("source_failure", [False, True])
async def test_goal_clearing_requires_confirmed_output_without_second_status_write(output_store, monkeypatch, source_failure, caplog):
    from typing import NotRequired

    from langgraph.checkpoint.memory import InMemorySaver

    from deerflow.runtime.goal import build_goal_state, read_thread_goal

    caplog.set_level("DEBUG", logger="deerflow.runtime.runs.worker")

    class GoalState(MessagesState):
        goal: NotRequired[dict]

    env = await _environment(output_store, monkeypatch)
    env.checkpointer = OwnedCheckpointSaver(InMemorySaver(), profile="memory", loop=asyncio.get_running_loop())

    async def node(state, config):
        env.calls.append("actual-node")
        return {"messages": [await env.model.ainvoke(state["messages"], config)]}

    graph = StateGraph(GoalState)
    graph.add_node("model", node)
    graph.add_edge(START, "model")
    graph.add_edge("model", END)
    env.graph = graph.compile(checkpointer=env.checkpointer)
    goal = build_goal_state("Synthetic goal to retain until output is proven")
    await env.graph.aupdate_state({"configurable": {"thread_id": env.record.thread_id}}, {"messages": [], "goal": goal}, as_node="model")
    monkeypatch.setattr("deerflow.runtime.runs.worker.create_goal_evaluator_model", lambda **_kwargs: object())
    evaluator = AsyncMock(return_value={"satisfied": True, "blocker": "none", "reason": "Synthetic visible evidence", "evidence_summary": "The answer is complete"})
    monkeypatch.setattr("deerflow.runtime.runs.worker.evaluate_goal_completion", evaluator)
    status_only = AsyncMock(side_effect=AssertionError("Captured output must not use status-only goal confirmation"))
    monkeypatch.setattr(env.manager, "persist_current_status", status_only)
    hook_facts = []

    async def completed(record):
        from deerflow.runtime.runs.manager import ConflictError

        blocked = False
        try:
            await env.manager.create_or_reject(record.thread_id, user_id=RAW_OWNER)
        except ConflictError:
            blocked = True
        hook_facts.append((record.finalizing, record.terminal_confirmed, blocked))

    if source_failure:

        async def fail(*_args, **_kwargs):
            raise OSError("Synthetic source failure")

        monkeypatch.setattr(env.events, "put_batch", fail)
    # Mirror production's await-free worker task attachment so the host-only
    # finalizing-run capability cannot be borrowed by a different task.
    task = asyncio.create_task(_execute(env, completed=completed))
    env.record.task = task
    await task
    saved = await read_thread_goal(env.checkpointer, env.record.thread_id)
    evaluator.assert_awaited_once()
    status_only.assert_not_awaited()
    if source_failure:
        assert saved == goal and env.record.ownership_lost
        assert not hook_facts
    else:
        assert env.record.terminal_confirmed
        assert saved is None
        assert hook_facts == [(True, True, True)]
    assert len(env.calls) == 1
