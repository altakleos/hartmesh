"""Closed protocol seeds verify parsing, not actual worker provenance."""

from copy import deepcopy
from dataclasses import replace

import pytest
from _telegram_human_input_test_support import record, source_basis
from langchain_core.messages import AIMessage, HumanMessage, ToolMessage

from deerflow.agent_instances.chat_tools import proposal_message_id
from deerflow.runtime.runs.output_inventory import build_event_inventory_evidence, output_receipt_digest, presentation_fragment_digest


def proposal_basis():
    original = record()
    original = replace(original, scope=replace(original.scope, agent_kind="instance", agent_ref="d" * 32))
    basis = source_basis(original)
    draft = {"objective": "Review the guide", "success_criteria": "Report verified gaps", "due_text": None, "due_at": None}
    messages = [
        HumanMessage(id="actual-human", content="Review the guide", additional_kwargs={"shows_cards": ["assignment_card"], "channel_turn_id": original.publication_id}),
        AIMessage(id="issuer", content="", tool_calls=[{"id": "proposal-call", "name": "propose_assignment", "args": {"proposal": draft}}]),
        ToolMessage(
            id=proposal_message_id("proposal-call", "issuer"),
            content="A proposal",
            name="propose_assignment",
            tool_call_id="proposal-call",
            artifact={"assignment_card": {"version": 1, "kind": "assignment_card", "tool_call_id": "proposal-call", "draft": draft}},
        ),
    ]
    template = basis["messages"][0]
    basis["messages"] = [
        {**deepcopy(template), "seq": index, "event_type": kind, "content": message.model_dump(), "metadata": {"caller": "lead_agent"} if index in {1, 2} else {}}
        for index, kind, message in zip((1, 2, 3), ("llm.human.input", "llm.ai.response", "llm.tool.result"), messages, strict=True)
    ]
    output = basis["projection"]["output"]
    output["reason"] = output["output_evidence"]["reason"] = "unsupported_output"
    document = basis["projection"]["inventory"]["document"]
    document["output_receipt_sha256"] = output_receipt_digest(output["output_evidence"])
    document["items"][0]["value_sha256"] = presentation_fragment_digest(messages[-1].artifact)
    document["items"][0]["witness"]["message_id"] = messages[-1].id
    reseal(basis)
    return original, basis, draft


def reseal(basis):
    output, inventory = basis["projection"]["output"], basis["projection"]["inventory"]
    inventory["document"]["output_receipt_sha256"] = output_receipt_digest(output["output_evidence"])
    inventory["evidence"] = build_event_inventory_evidence(output["output_evidence"], status="success", inventory_event_seq=9, document=inventory["document"]).model_dump(mode="json")


def api():
    from app.telegram_bots import assignment_proposal_source

    return assignment_proposal_source


def test_qualified_closed_proposal_preserves_full_draft_and_original_conversation():
    original, basis, draft = proposal_basis()
    result = api().validate_proposal_basis(original, basis)
    assert result.model_dump(mode="json") == draft
    basis["messages"][-1]["content"]["artifact"]["assignment_card"]["draft"]["objective"] = "Changed after validation"
    assert result.objective == draft["objective"]


def test_framework_human_id_rewrite_uses_the_shared_canonical_identity():
    original, basis, draft = proposal_basis()
    basis["messages"][0]["content"]["id"] += "__user"
    assert api().validate_proposal_basis(original, basis).model_dump(mode="json") == draft


@pytest.mark.parametrize("marker", [None, "f" * 32])
def test_card_support_must_belong_to_the_original_human_publication(marker):
    original, basis, _draft = proposal_basis()
    kwargs = basis["messages"][0]["content"]["additional_kwargs"]
    if marker is None:
        kwargs.pop("channel_turn_id", None)
    else:
        kwargs["channel_turn_id"] = marker
    with pytest.raises(ValueError):
        api().validate_proposal_basis(original, basis)


def test_callback_witness_requires_the_unique_actual_current_journal_result():
    original, basis, draft = proposal_basis()
    basis["projection"]["inventory"]["document"]["items"][0]["witness"] = {"kind": "callback", "callback_run_id": "44067627-570c-4c90-9503-48f14b80627f"}
    reseal(basis)
    assert api().validate_proposal_basis(original, basis).model_dump(mode="json") == draft
    duplicate = deepcopy(basis["messages"][-1])
    duplicate["seq"] = 4
    basis["messages"].append(duplicate)
    with pytest.raises(ValueError):
        api().validate_proposal_basis(original, basis)


@pytest.mark.parametrize("damage", ["duplicate-result", "duplicate-call"])
def test_ambiguous_proposal_source_is_held(damage):
    original, basis, _draft = proposal_basis()
    if damage == "duplicate-result":
        basis["messages"].append(deepcopy(basis["messages"][-1]))
        basis["messages"][-1]["seq"] = 4
    else:
        basis["messages"][1]["content"]["tool_calls"].append(deepcopy(basis["messages"][1]["content"]["tool_calls"][0]))
    reseal(basis)
    with pytest.raises(ValueError):
        api().validate_proposal_basis(original, basis)


@pytest.mark.parametrize(
    "damage",
    [
        "artifact",
        "call",
        "issuer",
        "tool-id",
        "status",
        "caller",
        "foreign-run",
        "foreign-human",
        "no-human",
        "no-mark",
        "answer-here",
        "later-message",
        "witness",
        "extra-structure",
        "page",
        "extra-card-key",
        "bool-version",
        "wrong-reason",
        "wrong-input",
        "wrong-channel",
    ],
)
def test_copied_stale_or_unaccounted_source_cannot_become_a_proposal(damage):
    original, basis, _draft = proposal_basis()
    rows = basis["messages"]
    tool, human, ai = rows[-1]["content"], rows[0]["content"], rows[1]["content"]
    document = basis["projection"]["inventory"]["document"]
    if damage == "artifact":
        tool["artifact"]["assignment_card"]["draft"]["objective"] = "Different"
    elif damage == "call":
        ai["tool_calls"][0]["args"]["proposal"]["objective"] = "Different"
    elif damage == "issuer":
        ai["id"] = "other-issuer"
    elif damage == "tool-id":
        tool["tool_call_id"] = "other-call"
    elif damage == "status":
        tool["status"] = "error"
    elif damage == "caller":
        rows[1]["metadata"]["caller"] = "subagent"
    elif damage == "foreign-run":
        rows[-1]["run_id"] = "foreign"
    elif damage == "foreign-human":
        human["id"] = "foreign-human"
    elif damage == "no-human":
        basis["messages"] = rows[1:]
    elif damage == "no-mark":
        human["additional_kwargs"] = {}
    elif damage == "answer-here":
        human["additional_kwargs"]["assignment_card_response"] = {"choice": "answer_here"}
    elif damage == "later-message":
        rows.append({**deepcopy(rows[1]), "seq": 4})
    elif damage == "witness":
        document["items"][0]["witness"]["message_id"] = "foreign"
    elif damage == "extra-structure":
        document["items"].append({**deepcopy(document["items"][0]), "value_sha256": "f" * 64})
    elif damage == "page":
        document["items"].append({"kind": "page", "key": "work", "witness": {"kind": "root_snapshot", "root_epoch": document["root_epoch"], "message_id": "page"}})
    elif damage == "extra-card-key":
        tool["artifact"]["assignment_card"]["extra"] = "value"
    elif damage == "bool-version":
        tool["artifact"]["assignment_card"]["version"] = True
    elif damage == "wrong-reason":
        basis["projection"]["output"]["reason"] = basis["projection"]["output"]["output_evidence"]["reason"] = "human_input_required"
    elif damage == "wrong-input":
        basis["projection"]["output"]["output_evidence"]["input_message_id"] = document["input_message_id"] = "different-input"
    else:
        basis["projection"]["output"]["channel_execution"]["publication_id"] = "f" * 32
    # Internally consistent inventory hashes still cannot establish message ownership.
    document["items"][0]["value_sha256"] = presentation_fragment_digest(tool["artifact"])
    reseal(basis)
    with pytest.raises(ValueError, match="assignment-proposal-source-conflict"):
        api().validate_proposal_basis(original, basis)


@pytest.mark.parametrize("kind,ref,name", [("lead_agent", "lead_agent", None), ("custom_agent", "custom", "custom")])
def test_other_conversation_kinds_never_gain_assignment_authority(kind, ref, name):
    original, basis, _draft = proposal_basis()
    original = replace(original, scope=replace(original.scope, agent_kind=kind, agent_ref=ref, agent_name=name))
    from app.telegram_bots.inbox import publication_identity

    basis["publication"] = publication_identity(original)
    basis["projection"]["output"]["channel_execution"]["agent_name"] = name
    with pytest.raises(ValueError):
        api().validate_proposal_basis(original, basis)
