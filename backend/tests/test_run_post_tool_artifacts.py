"""Actual model/tool callbacks and worker preserve post-tool middleware artifacts."""

import pytest
from _run_output_capture_test_support import capture_environment as capture_environment
from _run_output_store_test_support import output_store as output_store
from langchain_core.messages import AIMessage
from langchain_core.tools import tool
from langgraph.graph import END, START, MessagesState, StateGraph
from test_run_journal import journal_setup as journal_setup
from test_run_output_capture_worker import _environment, _execute


@pytest.mark.asyncio
async def test_worker_persists_the_post_tool_artifact_seen_by_the_next_model(output_store, monkeypatch):
    e = await _environment(output_store, monkeypatch)
    e.model.responses = [AIMessage("", id="call-message", tool_calls=[{"id": "actual-call", "name": "inspect_fixture", "args": {}}]), AIMessage("The answer is ready.", id="final-message")]
    seen = []

    @tool(response_format="content_and_artifact")
    def inspect_fixture():
        """Read the offline fixture."""
        return "A tool result", {"existing": "preserved"}

    async def model(state, config):
        return {"messages": [await e.model.ainvoke(state["messages"], config)]}

    async def tools(state, config):
        raw = await inspect_fixture.ainvoke(state["messages"][-1].tool_calls[0], config)
        assert raw.id, "The actual journal callback assigns the result identity"
        # Equivalent to a trusted middleware enriching the returned tool result
        # after LangChain has fired its on_tool_end callback.
        enriched = raw.model_copy(update={"artifact": {**raw.artifact, "tool_note": {"status": "restricted"}}})
        seen.append(enriched)
        return {"messages": [enriched]}

    graph = StateGraph(MessagesState)
    graph.add_node("model", model)
    graph.add_node("tools", tools)
    graph.add_edge(START, "model")
    graph.add_conditional_edges("model", lambda state: "tools" if state["messages"][-1].tool_calls else END)
    graph.add_edge("tools", "model")
    e.graph = graph.compile()
    await _execute(e)
    rows = await e.events.list_messages_by_run(e.record.thread_id, e.record.run_id, user_id=e.record.user_id)
    results = [row for row in rows if row["event_type"] == "llm.tool.result"]
    assert len(seen) == 1
    matching = [row for row in results if row["content"].get("artifact") == seen[0].artifact]
    assert len(matching) == 1, "The persisted source must include the actual enriched artifact"
    assert matching[0]["content"]["id"] == seen[0].id
    assert matching[0]["seq"] < next(row["seq"] for row in rows if row["content"].get("id") == "final-message")


@pytest.mark.asyncio
@pytest.mark.parametrize("original", [None, {"existing": {"value": "preserved"}}])
async def test_actual_assembly_seals_enriched_result_in_inventory(capture_environment, monkeypatch, tmp_path, original):
    from test_output_inventory_viewless_worker import execute

    from deerflow.runtime.runs.output_inventory import presentation_fragment_digest, validate_inventory_document
    from deerflow.sandbox.middleware import SandboxMiddleware

    @tool(response_format="content_and_artifact")
    def inspect_fixture():
        """Read the offline fixture."""
        return "A tool result", original

    # Controlled trusted-middleware transformation; actual SDK callbacks,
    # assembly profile, worker, SQL journal and terminal inventory remain real.
    async def enrich(self, request, handler):
        raw = await handler(request)
        return raw.model_copy(update={"artifact": {**(raw.artifact or {}), "tool_note": {"status": "restricted"}}})

    monkeypatch.setattr(SandboxMiddleware, "awrap_tool_call", enrich)
    result = await execute(
        capture_environment,
        monkeypatch,
        tmp_path,
        tool_objects=[inspect_fixture],
        responses=[
            AIMessage("", id="call-message", tool_calls=[{"id": "actual-call", "name": "inspect_fixture", "args": {}}]),
            AIMessage("The answer is ready.", id="final-message"),
        ],
    )
    assert result.row["status"] == "success"
    doc = validate_inventory_document(result.events[0]["content"])
    assert doc.coverage == "complete"
    rows = await capture_environment.event_store.list_messages_by_run(result.record.thread_id, result.record.run_id, user_id=result.record.user_id)
    tool_rows = [row for row in rows if row["event_type"] == "llm.tool.result"]
    assert len(tool_rows) == 2
    raw, enriched = (row["content"] for row in tool_rows)
    assert raw["artifact"] == original
    assert enriched["artifact"] == {**(original or {}), "tool_note": {"status": "restricted"}}
    assert raw["id"] == enriched["id"]
    assert len(doc.items) == 1
    assert doc.items[0].value_sha256 == presentation_fragment_digest(enriched["artifact"])
    assert doc.items[0].witness.kind == "root_snapshot" and doc.items[0].witness.message_id == enriched["id"]


@pytest.mark.asyncio
@pytest.mark.parametrize("change", ["add", "add_metadata", "body", "status", "producer_metadata", "presentation", "hidden", "other_name", "old_occurrence", "other_call", "rewrite", "remove", "overflow"])
async def test_enrichment_preserves_current_occurrence_and_original_artifact(journal_setup, change):
    from uuid import uuid4

    from langchain_core.messages import ToolMessage
    from langchain_core.outputs import ChatGeneration, LLMResult

    journal, store = journal_setup
    call = AIMessage("", id="call", tool_calls=[{"id": "call-id", "name": "inspect_fixture", "args": {}}])
    journal.on_llm_end(LLMResult(generations=[[ChatGeneration(message=call)]]), run_id=uuid4(), tags=["lead_agent"])
    raw = ToolMessage("Result", tool_call_id="call-id", name="inspect_fixture", artifact={"existing": {"value": 1}}, additional_kwargs={"producer": {"status": "success"}})
    journal.on_tool_end(raw, run_id=uuid4())
    # Committed raw events are immutable even when middleware mutates its object.
    await journal.flush()
    enriched = raw.model_copy(deep=True)
    enriched.artifact["note"] = "extra"
    if change == "body":
        enriched.content = "Changed result"
    elif change == "status":
        enriched.status = "error"
    elif change == "producer_metadata":
        enriched.additional_kwargs["producer"]["status"] = "error"
    elif change == "add_metadata":
        from deerflow.agents.middlewares.tool_result_meta import normalize_tool_message

        enriched = normalize_tool_message(enriched)
    elif change == "presentation":
        enriched.additional_kwargs["presented_files"] = ["/mnt/user-data/outputs/new.txt"]
    elif change == "hidden":
        enriched.additional_kwargs["hide_from_ui"] = True
    elif change == "other_name":
        enriched.name = "other"
    elif change == "old_occurrence":
        journal.on_llm_end(LLMResult(generations=[[ChatGeneration(message=call.model_copy(update={"id": "next-call"}))]]), run_id=uuid4(), tags=["lead_agent"])
    elif change == "other_call":
        enriched.tool_call_id = "not-current"
    elif change == "rewrite":
        enriched.artifact["existing"]["value"] = 2
    elif change == "remove":
        del enriched.artifact["existing"]
    elif change == "overflow":
        enriched.artifact["note"] = "x" * 262145
    await journal.flush()
    before = journal.get_completion_data()["message_count"]
    for _ in range(2):
        journal.on_chain_end({"messages": [call, enriched]}, run_id=uuid4())
    await journal.flush()
    rows = [row for row in await store.list_messages("t1") if row["event_type"] == "llm.tool.result"]
    assert len(rows) == (2 if change in {"add", "add_metadata"} else 1)
    assert rows[0]["content"]["artifact"] == {"existing": {"value": 1}}
    if change in {"add", "add_metadata"}:
        assert rows[-1]["content"]["artifact"] == enriched.artifact
        assert rows[-1]["content"]["id"] == raw.id
    assert journal.get_completion_data()["message_count"] == before
    await journal.close()
    assert not journal._tool_artifact_snapshots
