"""Controlled callback stand-ins for canonical replay and malformed ancestry.

Actual SQL persists these callbacks. These tests isolate observer behavior;
actual SDK/worker origin is qualified separately by the worker suite.
"""

from types import SimpleNamespace
from uuid import uuid4

import pytest
from _run_output_capture_test_support import capture_environment as capture_environment
from _run_output_capture_test_support import output_store as output_store
from _run_output_store_test_support import RAW_OWNER
from langchain_core.messages import AIMessage, HumanMessage
from langchain_core.outputs import ChatGeneration, LLMResult

from deerflow.runtime.journal import RunJournal
from deerflow.runtime.runs.output import OutputEvidenceError, canonical_text_digest
from deerflow.runtime.runs.output_capture import OutputCaptureObserver
from deerflow.runtime.runs.schemas import RunStatus
from deerflow.runtime.user_context import reset_current_user, set_current_user


def _start(environment):
    root, node, model = uuid4(), uuid4(), uuid4()
    run_id = uuid4().hex
    human = HumanMessage("Canonical input", id="fixture-human")
    capture = OutputCaptureObserver(owner_user_id=RAW_OWNER, thread_id="observer-thread", run_id=run_id)
    capture.begin_epoch(root, {"messages": [human]}, pre_existing_ids=set())
    journal = RunJournal(run_id, "observer-thread", environment.event_store, output_capture=capture)
    journal.on_chain_start({}, {}, run_id=root, name="LangGraph", tags=["lead_agent"])
    journal.on_chain_start({}, {"messages": [human]}, run_id=node, parent_run_id=root, name="model", tags=["lead_agent"], metadata={"langgraph_node": "model"})
    journal.on_chat_model_start({}, [[human]], run_id=model, parent_run_id=node, tags=["lead_agent"], metadata={"langgraph_node": "model"})
    return SimpleNamespace(capture=capture, journal=journal, root=root, node=node, model=model, human=human)


def _result(message):
    return LLMResult(generations=[[ChatGeneration(message=message)]])


@pytest.mark.asyncio
async def test_provider_usage_replay_and_object_mutation_cannot_replace_first_source(capture_environment):
    token = set_current_user(SimpleNamespace(id=RAW_OWNER))
    env = _start(capture_environment)
    canonical = AIMessage("  First exact answer\nα", id="fixture-assistant", additional_kwargs={"reasoning_content": "Synthetic auxiliary first"})
    completed_root = canonical.model_copy(deep=True)
    try:
        env.journal.on_llm_end(_result(canonical), run_id=env.model, parent_run_id=env.node, tags=["lead_agent"])
        canonical.id = "provider-reused-new-id"
        canonical.content = "A later provider replay is not the canonical answer"
        canonical.additional_kwargs["reasoning_content"] = "Synthetic auxiliary replay"
        canonical.usage_metadata = {"input_tokens": 2, "output_tokens": 3, "total_tokens": 5}
        env.journal.on_llm_end(_result(canonical), run_id=env.model, parent_run_id=env.node, tags=["middleware:spoof", "lead_agent"])
        env.journal.on_chain_end({"messages": [env.human, completed_root]}, run_id=env.root)
        await env.journal.quiesce_output_capture(flush=True)
        receipt = env.capture.receipt(status=RunStatus.success, cancelled=False)
        assert receipt.kind == "text" and receipt.assistant_message_id == "fixture-assistant"
        assert receipt.text_sha256 == canonical_text_digest("  First exact answer\nα")
        rows = await capture_environment.event_store.list_messages_by_run("observer-thread", env.journal.run_id, user_id=RAW_OWNER)
        assistants = [row for row in rows if row["event_type"] == "llm.ai.response"]
        assert len(assistants) == 1
        assert assistants[0]["seq"] == receipt.event_seq
        assert assistants[0]["content"]["id"] == "fixture-assistant"
        assert assistants[0]["content"]["content"] == "  First exact answer\nα"
        assert env.journal.get_completion_data()["total_tokens"] == 5
    finally:
        await env.journal.close(flush=False)
        reset_current_user(token)


@pytest.mark.asyncio
async def test_rebound_model_run_id_latches_uncertainty_instead_of_reusing_membership(capture_environment):
    token = set_current_user(SimpleNamespace(id=RAW_OWNER))
    env = _start(capture_environment)
    message = AIMessage("Canonical answer", id="fixture-assistant")
    try:
        env.journal.on_llm_end(_result(message), run_id=env.model, parent_run_id=env.node, tags=["lead_agent"])
        env.journal.on_chat_model_start({}, [[env.human]], run_id=env.model, parent_run_id=uuid4(), tags=["lead_agent"], metadata={"langgraph_node": "model"})
        env.journal.on_chain_end({"messages": [env.human, message]}, run_id=env.root)
        await env.journal.quiesce_output_capture(flush=True)
        with pytest.raises(OutputEvidenceError):
            env.capture.receipt(status=RunStatus.success, cancelled=False)
    finally:
        await env.journal.close(flush=False)
        reset_current_user(token)


@pytest.mark.asyncio
async def test_model_parent_cannot_impersonate_an_actual_registered_graph_node(capture_environment):
    token = set_current_user(SimpleNamespace(id=RAW_OWNER))
    env = _start(capture_environment)
    pretend_node, model = uuid4(), uuid4()
    message = AIMessage("Copied lead tags do not establish origin", id="fixture-assistant")
    try:
        env.journal.on_chat_model_start({}, [[env.human]], run_id=pretend_node, parent_run_id=env.root, tags=["lead_agent"], metadata={"langgraph_node": "model"})
        env.journal.on_chat_model_start({}, [[env.human]], run_id=model, parent_run_id=pretend_node, tags=["lead_agent"], metadata={"langgraph_node": "model"})
        env.journal.on_llm_end(_result(message), run_id=model, parent_run_id=pretend_node, tags=["lead_agent"])
        env.journal.on_chain_end({"messages": [env.human, message]}, run_id=env.root)
        await env.journal.quiesce_output_capture(flush=True)
        receipt = env.capture.receipt(status=RunStatus.success, cancelled=False)
        assert receipt.kind == "unavailable" and receipt.reason == "ambiguous_source"
    finally:
        await env.journal.close(flush=False)
        reset_current_user(token)
