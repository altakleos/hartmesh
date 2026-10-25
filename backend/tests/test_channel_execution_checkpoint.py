"""Server-issued publication identity survives real middleware/checkpointing."""

from typing import Annotated, TypedDict

import pytest
from _channel_execution_test_support import gateway_environment as gateway_environment
from langchain_core.messages import BaseMessage, HumanMessage, SystemMessage
from langgraph.graph import END, START, StateGraph
from langgraph.graph.message import add_messages
from langgraph.runtime import Runtime

from app.gateway import services
from app.gateway.routers.thread_runs import RunCreateRequest
from deerflow.agents.middlewares.dynamic_context_middleware import DynamicContextMiddleware
from deerflow.runtime.checkpoint_state import CheckpointStateAccessor

PUBLICATION = "a" * 32


async def _checkpoint_captured_input(environment, captured):
    class State(TypedDict):
        messages: Annotated[list[BaseMessage], add_messages]

    middleware = DynamicContextMiddleware(memory_enabled=False)

    async def dynamic_context(state: State, runtime: Runtime):
        return await middleware.abefore_agent(state, runtime)

    graph = StateGraph(State)
    graph.add_node("dynamic_context", dynamic_context)
    graph.add_edge(START, "dynamic_context")
    graph.add_edge("dynamic_context", END)
    compiled = graph.compile(checkpointer=environment.checkpointer)
    await compiled.ainvoke(captured["graph_input"], captured["config"], context=captured["config"].get("context"))
    return await CheckpointStateAccessor(compiled, environment.checkpointer, "full").aget(captured["config"])


@pytest.mark.asyncio
async def test_gateway_publication_survives_real_dynamic_context_copy_and_checkpoint(gateway_environment):
    e = gateway_environment
    body = RunCreateRequest(
        assistant_id="lead_agent",
        on_disconnect="continue",
        channel_execution={"version": 1, "publication_id": PUBLICATION},
        metadata={"channel_turn_id": "forged"},
        input={
            "messages": [
                {"role": "human", "id": "visible-anchor", "content": "Owned original input", "additional_kwargs": {"channel_turn_id": "forged", "forwarded": [{"from": "synthetic sender"}], "original_user_content": "retained input"}}
            ]
        },
    )
    record = await services.start_run(body, e.thread_id, e.request, idempotency_key=PUBLICATION)
    await record.task
    # Guard the recorded old-service regression: typed admission previously
    # accepted the request without persisting a trusted receipt/stamp.
    assert record.kwargs.get("channel_execution") == {"version": 1, "publication_id": PUBLICATION, "owner_user_id": e.owner_id, "thread_id": e.thread_id, "assistant_id": "lead_agent", "agent_name": None}
    stored = await e.store.get(record.run_id)
    assert stored["kwargs"]["channel_execution"] == record.kwargs["channel_execution"]
    assert stored["metadata"]["channel_turn_id"] == record.metadata["channel_turn_id"] == PUBLICATION
    (captured,) = e.worker_inputs
    (human,) = [message for message in captured["graph_input"]["messages"] if isinstance(message, HumanMessage)]
    assert human.additional_kwargs["channel_turn_id"] == PUBLICATION
    assert captured["config"]["metadata"]["channel_turn_id"] == PUBLICATION

    snapshot = await _checkpoint_captured_input(e, captured)
    assert snapshot.metadata["channel_turn_id"] == PUBLICATION
    (visible,) = [message for message in snapshot.values["messages"] if isinstance(message, HumanMessage) and not message.additional_kwargs.get("hide_from_ui")]
    assert visible is not human and visible.id == human.id + "__user"
    assert visible.content == human.content == "Owned original input"
    assert visible.additional_kwargs["channel_turn_id"] == PUBLICATION
    assert visible.additional_kwargs["forwarded"] == [{"from": "synthetic sender"}]
    assert visible.additional_kwargs["original_user_content"] == "retained input"
    reminders = [message for message in snapshot.values["messages"] if isinstance(message, SystemMessage)]
    assert reminders and all("channel_turn_id" not in message.additional_kwargs for message in reminders)


@pytest.mark.asyncio
async def test_untyped_internal_marker_is_not_revived_by_real_middleware_checkpointing(gateway_environment):
    e = gateway_environment
    body = RunCreateRequest(assistant_id="lead_agent", input={"messages": [{"role": "human", "id": "legacy-anchor", "content": "Legacy ordinary input", "additional_kwargs": {"channel_turn_id": "forged"}}]})
    record = await services.start_run(body, e.thread_id, e.request)
    await record.task
    assert "channel_execution" not in record.kwargs
    (captured,) = e.worker_inputs
    (human,) = [message for message in captured["graph_input"]["messages"] if isinstance(message, HumanMessage)]
    assert "channel_turn_id" not in human.additional_kwargs
    snapshot = await _checkpoint_captured_input(e, captured)
    (visible,) = [message for message in snapshot.values["messages"] if isinstance(message, HumanMessage) and not message.additional_kwargs.get("hide_from_ui")]
    assert visible is not human and visible.id == human.id + "__user"
    assert visible.content == "Legacy ordinary input" and "channel_turn_id" not in visible.additional_kwargs
    assert "channel_turn_id" not in snapshot.metadata
