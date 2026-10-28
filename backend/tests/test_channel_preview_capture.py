"""Real offline graph callbacks pair to actual root astream chunks.

Actual SQL journal writes are not a terminal status/output seal in this suite.
"""

from copy import deepcopy
from types import SimpleNamespace
from uuid import uuid4

import pytest
from _run_output_capture_test_support import capture_environment as capture_environment  # noqa: F401
from _run_output_capture_test_support import output_store as output_store  # noqa: F401
from _run_output_store_test_support import RAW_OWNER
from langchain_core.language_models.fake_chat_models import FakeListChatModel
from langchain_core.messages import AIMessageChunk, HumanMessage, SystemMessage, ToolMessage
from langgraph.graph import END, START, MessagesState, StateGraph

from deerflow.runtime.journal import RunJournal
from deerflow.runtime.runs.output import canonical_text_digest
from deerflow.runtime.runs.output_capture import OutputCaptureObserver
from deerflow.runtime.runs.worker import _publish_stream_item
from deerflow.runtime.user_context import reset_current_user, set_current_user

PUBLICATION = "a" * 32
STAMP_KEY = "deerflow_preview_span"


async def graph_preview(environment, *, tags=(), provider_stamp=False, before_publish=None):
    root = uuid4()
    human = HumanMessage("Current human", id="current-human", additional_kwargs={"channel_turn_id": PUBLICATION})
    capture = OutputCaptureObserver(owner_user_id=RAW_OWNER, thread_id="thread", run_id="run", publication_id=PUBLICATION)
    capture.begin_epoch(root, {"messages": [human]}, pre_existing_ids=set())
    journal = RunJournal("run", "thread", environment.event_store, output_capture=capture)
    model = FakeListChatModel(responses=["hello"])
    published, raw = [], []

    async def node(state, config):
        return {"messages": [await model.ainvoke(state["messages"], config)]}

    builder = StateGraph(MessagesState)
    builder.add_node("model", node)
    builder.add_edge(START, "model")
    builder.add_edge("model", END)
    graph = builder.compile()

    async def publish(run_id, event, data):
        assert run_id == "run"
        published.append((event, deepcopy(data)))

    bridge = SimpleNamespace(publish=publish)
    config = {"run_id": root, "callbacks": [journal], "tags": ["lead_agent", *tags]}
    if provider_stamp:
        config["metadata"] = {STAMP_KEY: {"forged": "caller supplied"}}
    token = set_current_user(SimpleNamespace(id=RAW_OWNER))
    try:
        async for mode, chunk in graph.astream({"messages": [human]}, config=config, stream_mode=["messages", "values"]):
            if mode == "messages":
                raw.append(chunk)
                if before_publish is not None:
                    await before_publish(capture, journal, chunk)
            await _publish_stream_item(bridge=bridge, run_id="run", mode=mode, chunk=chunk, namespace=(), file_tool_chunk_batcher=None, subagent_events=None, output_capture=capture)
        await journal.quiesce_output_capture(flush=True)
    finally:
        await journal.close(flush=False)
        reset_current_user(token)
    return capture, published, raw, bridge


@pytest.mark.asyncio
async def test_actual_graph_tokens_receive_only_fresh_scoped_preview_spans(capture_environment):
    _capture, published, _raw, _bridge = await graph_preview(capture_environment, provider_stamp=True)
    messages = [data for event, data in published if event == "messages"]
    stamped = [data for data in messages if STAMP_KEY in data[1]]
    assert stamped, "Actual direct lead-model callbacks must qualify streaming chunks"
    prefix = ""
    for sequence, (message, metadata) in enumerate(stamped, 1):
        span = metadata[STAMP_KEY]
        text = message["content"]
        assert span["sequence"] == sequence and span["offset"] == len(prefix)
        prefix += text
        assert span["text_sha256"] == canonical_text_digest(text)
        assert span["prefix_sha256"] == canonical_text_digest(prefix)
        assert (span["owner_user_id"], span["thread_id"], span["run_id"], span["publication_id"]) == (RAW_OWNER, "thread", "run", PUBLICATION)
        assert span["message_id"] == message["id"] and "forged" not in span
    assert prefix == "hello"


@pytest.mark.asyncio
@pytest.mark.parametrize("tags", [("middleware:copied",), ("subagent:copied",)])
async def test_actual_excluded_graph_cannot_qualify_tokens_by_copying_metadata(capture_environment, tags):
    _capture, published, _raw, _bridge = await graph_preview(capture_environment, tags=tags, provider_stamp=True)
    assert all(STAMP_KEY not in data[1] for event, data in published if event == "messages")


@pytest.mark.asyncio
async def test_copied_or_replayed_raw_chunk_cannot_reuse_a_consumed_callback(capture_environment):
    capture, published, raw, bridge = await graph_preview(capture_environment)
    assert any(STAMP_KEY in data[1] for event, data in published if event == "messages")
    actual = next(chunk for chunk in raw if chunk[0].content)
    for message in (actual[0], actual[0].model_copy(deep=True), AIMessageChunk("forged", id=actual[0].id), HumanMessage("private"), SystemMessage("private"), ToolMessage("private", tool_call_id="tool")):
        await _publish_stream_item(
            bridge=bridge,
            run_id="run",
            mode="messages",
            chunk=(message, {STAMP_KEY: {"forged": "copied"}}),
            namespace=(),
            file_tool_chunk_batcher=None,
            subagent_events=None,
            output_capture=capture,
        )
        assert STAMP_KEY not in published[-1][1][1]


@pytest.mark.asyncio
@pytest.mark.parametrize("boundary", ["repeated_callback", "foreign_loop", "second_epoch"])
async def test_actual_callback_pair_cannot_be_reissued_or_adopted_by_another_lifetime(capture_environment, boundary):
    import asyncio

    from langchain_core.outputs import ChatGenerationChunk

    checked = []

    async def check(capture, journal, chunk):
        if checked or not chunk[0].content:
            return
        stamp = capture.preview_span(chunk)
        assert stamp is not None
        checked.append(stamp)
        if boundary == "second_epoch":
            # A pending actual callback from epoch one cannot acquire a second
            # graph invocation merely because its object was still retained.
            pending = chunk[0].model_copy(deep=True)
            capture.preview_token(run_id=stamp.model_call_id, chunk=ChatGenerationChunk(message=pending))
            capture.begin_epoch(uuid4(), {"messages": []}, pre_existing_ids=set())
            assert capture.preview_span((pending, chunk[1])) is None
        elif boundary == "foreign_loop":
            copied = chunk[0].model_copy(deep=True)
            copied.id += "-foreign"
            await asyncio.to_thread(journal.on_llm_new_token, copied.content, run_id=stamp.model_call_id, chunk=ChatGenerationChunk(message=copied))
            assert capture.preview_span((copied, chunk[1])) is None
        else:
            journal.on_llm_new_token(chunk[0].content, run_id=stamp.model_call_id, chunk=ChatGenerationChunk(message=chunk[0]))
            assert capture.preview_span(chunk) is None

    await graph_preview(capture_environment, before_publish=check)
    assert len(checked) == 1


@pytest.mark.asyncio
async def test_pending_file_tool_flush_strips_provider_preview_copies():
    published = []

    async def publish(run_id, event, data):
        published.append((event, data))

    pending = (AIMessageChunk("", id="tool-call", tool_call_chunks=[{"index": 0, "id": "call", "name": "write_file", "args": "{}"}]), {STAMP_KEY: {"forged": "provider"}})
    batcher = SimpleNamespace(finish=lambda: [pending])
    await _publish_stream_item(bridge=SimpleNamespace(publish=publish), run_id="run", mode="values", chunk={}, namespace=(), file_tool_chunk_batcher=batcher, subagent_events=None)
    assert published[0][0] == "messages" and STAMP_KEY not in published[0][1][1]
