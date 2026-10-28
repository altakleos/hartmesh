"""Actual offline model/worker/journal/SQL response input capture."""

import pytest
from _run_output_store_test_support import output_store as output_store
from langgraph.graph import END, START, MessagesState, StateGraph
from test_output_human_input_response import response
from test_run_output_capture_worker import _environment, _execute


@pytest.mark.asyncio
@pytest.mark.parametrize("mutate", [False, True])
async def test_actual_response_turn_seals_only_unchanged_response_metadata(output_store, monkeypatch, mutate):
    e = await _environment(output_store, monkeypatch)
    e.human.additional_kwargs["human_input_response"] = response(value=e.human.content)
    if mutate:

        async def node(state, config):
            # Actual node mutation occurs after the worker captured its input,
            # before the actual model callback receives that same human text.
            e.human.additional_kwargs["human_input_response"]["request_id"] = "changed-after-capture"
            return {"messages": [await e.model.ainvoke(state["messages"], config)]}

        builder = StateGraph(MessagesState)
        builder.add_node("model", node)
        builder.add_edge(START, "model")
        builder.add_edge("model", END)
        e.graph = builder.compile()
    await _execute(e)
    row = await output_store.repository.get(e.record.run_id, user_id=e.record.user_id)
    if mutate:
        assert row["status"] == "running" and "output_evidence" not in row["kwargs"]
        assert e.record.ownership_lost
    else:
        assert row["status"] == "success"
        assert row["kwargs"]["output_evidence"]["kind"] == "text"
        assert row["kwargs"]["output_evidence"]["input_message_id"] == e.human.id
