"""Shared exact-source rules; plain message syntax is not root finality."""

from copy import deepcopy
from types import MappingProxyType

import pytest
from langchain_core.messages import AIMessage, AIMessageChunk, HumanMessage, ToolMessage

from deerflow.runtime.runs.output import OutputEvidenceError, TextOutputReceipt, canonical_text_digest
from deerflow.runtime.runs.output_source import audit_output_source_event, inspect_output_message

TEXT = "  Exact\nCafé\u2028text  "


def message(**changes):
    return AIMessage(id="actual-ai", content=TEXT).model_dump() | changes


def receipt(**changes):
    return TextOutputReceipt(version=1, kind="text", owner_user_id="raw:owner", thread_id="thread", run_id="run", input_message_id=None, assistant_message_id="actual-ai", event_seq=3, text_sha256=canonical_text_digest(TEXT), **changes)


def event(**changes):
    return {
        "thread_id": "thread",
        "run_id": "run",
        "user_id": "raw:owner",
        "seq": 3,
        "category": "message",
        "event_type": "llm.ai.response",
        "content": message(),
        "metadata": {"caller": "lead_agent"},
        "created_at": "2026-10-25T00:00:00+00:00",
    } | changes


@pytest.mark.parametrize("kind", [AIMessage, AIMessageChunk])
def test_exact_canonical_completed_shapes_preserve_text_and_metadata(kind):
    raw = kind(id="actual-ai", content=TEXT, additional_kwargs={"reasoning_content": "private auxiliary"}, usage_metadata={"input_tokens": 1, "output_tokens": 1, "total_tokens": 2}).model_dump()
    before = deepcopy(raw)
    facts = inspect_output_message(MappingProxyType(raw))
    assert (facts.role, facts.message_id, facts.text, facts.unavailable_reason) == ("assistant", "actual-ai", TEXT, None)
    assert raw == before
    assert "private auxiliary" not in repr(facts)


@pytest.mark.parametrize("content", [["a", {"type": "text", "text": " b"}], "   "])
def test_closed_text_shapes_never_trim_or_insert_separators(content):
    assert inspect_output_message(message(content=content)).text == ("a b" if isinstance(content, list) else content)


@pytest.mark.parametrize(
    "changes",
    [
        {"content": [{"type": "reasoning", "text": "hidden"}, {"type": "text", "text": "final"}]},
        {"content": [{"type": "image", "text": "do not adopt"}]},
        {"content": "", "additional_kwargs": {"reasoning_content": "only reasoning"}},
        {"content": [{"type": "text", "text": "answer", "unknown": "data"}]},
        {"additional_kwargs": {"hide_from_ui": True}},
        {"additional_kwargs": {"deerflow_error_fallback": True}},
        {"additional_kwargs": {"refusal": "refused"}},
        {"additional_kwargs": {"function_call": {"name": "tool"}}},
        {"tool_calls": [{"name": "tool", "args": {}, "id": "call", "type": "tool_call"}]},
        {"invalid_tool_calls": [{"name": "tool", "args": "bad", "id": "call", "error": "bad", "type": "invalid_tool_call"}]},
        {"type": "AIMessageChunk", "tool_call_chunks": [{"name": "tool", "args": "", "id": "call", "index": 0, "type": "tool_call_chunk"}]},
    ],
)
def test_well_formed_unsupported_source_is_not_partial_text(changes):
    facts = inspect_output_message(message(**changes))
    assert facts.text is None
    assert facts.unavailable_reason == "unsupported_output"


@pytest.mark.parametrize(
    "changes",
    [
        {"id": 1},
        {"content": [3]},
        {"content": [{"type": "text", "text": 3}]},
        {"additional_kwargs": []},
        {"additional_kwargs": {"reasoning_content": 3}},
        {"additional_kwargs": {"hide_from_ui": 1}},
        {"tool_calls": {}},
        {"response_metadata": []},
        {"content": "\ud800"},
    ],
)
def test_malformed_source_raises_fixed_uncertainty(changes):
    with pytest.raises(OutputEvidenceError, match="^Run output evidence is unavailable$"):
        inspect_output_message(message(**changes))


def test_missing_actual_identity_is_not_invented():
    facts = inspect_output_message(message(id=None))
    assert facts.message_id is None and facts.unavailable_reason == "missing_message_identity"


def test_human_identity_uses_only_existing_suffix_normalization():
    facts = inspect_output_message(HumanMessage(id="actual__user", content="input").model_dump())
    assert (facts.role, facts.message_id, facts.unavailable_reason) == ("human", "actual", None)
    assert inspect_output_message(message(id="actual__user")).message_id == "actual__user"


def test_human_input_tool_is_control_not_its_fallback_text():
    raw = ToolMessage(id="tool", tool_call_id="call", content="readable question", artifact={"human_input": {"version": 2, "kind": "human_input_request"}}).model_dump()
    assert inspect_output_message(raw).unavailable_reason == "human_input_required"


@pytest.mark.parametrize("changes", [{"content": message(content="changed")}, {"content": message(id="other")}, {"metadata": {"caller": "subagent:other"}}, {"content": message(additional_kwargs={"hide_from_ui": True})}])
def test_valid_changed_source_does_not_supply_replacement_text(changes):
    result = audit_output_source_event(event(**changes), receipt=receipt())
    assert (result.state, result.text) == ("changed", None)


@pytest.mark.parametrize("changes", [{"user_id": "foreign"}, {"run_id": "foreign"}, {"content": "broken JSON"}, {"seq": True}, {"metadata": []}])
def test_bad_envelope_is_uncertainty_not_changed_or_missing(changes):
    with pytest.raises(OutputEvidenceError):
        audit_output_source_event(event(**changes), receipt=receipt())


def test_exact_source_audit_is_immutable_and_inert_reasoning_can_differ():
    result = audit_output_source_event(event(content=message(additional_kwargs={"reasoning_content": "different auxiliary"})), receipt=receipt())
    assert (result.state, result.text) == ("matched", TEXT)
    with pytest.raises(AttributeError):
        result.state = "changed"


def test_plain_tool_result_is_a_valid_unsupported_message():
    raw = ToolMessage(id="tool", tool_call_id="call", content="result").model_dump()
    assert inspect_output_message(raw).unavailable_reason == "unsupported_output"


def test_actual_frozen_channel_admission_mapping_is_readable():
    from app.gateway.channel_execution import receipt_from_run_kwargs

    value = MappingProxyType({"version": 1, "publication_id": "a" * 32, "owner_user_id": "raw:owner", "thread_id": "thread", "assistant_id": "lead_agent", "agent_name": None})
    result = receipt_from_run_kwargs(MappingProxyType({"channel_execution": value}))
    assert result.publication_id == "a" * 32


@pytest.mark.parametrize("value", [0, [], {}, True, None])
def test_known_provenance_kind_requires_its_actual_string_type(value):
    with pytest.raises(OutputEvidenceError):
        inspect_output_message(message(additional_kwargs={"deerflow_content_kind": value}))


@pytest.mark.parametrize("role", ["ai", "human"])
def test_non_tool_cannot_attest_a_genuine_human_input_control(role):
    raw = message(type=role, artifact={"human_input": {"version": 2, "kind": "human_input_request"}})
    result = inspect_output_message(raw)
    assert result.unavailable_reason == ("unsupported_input" if role == "human" else "unsupported_output")
