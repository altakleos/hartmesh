"""Closed protocol seeds; these do not claim actual worker provenance."""

from hashlib import sha256

from langchain_core.messages import AIMessage, ToolMessage
from test_output_inventory import ROOT_EPOCH, inventory_document

from app.telegram_bots.inbox import PublicationOrigin, PublicationRecord, PublicationScope, publication_identity
from deerflow.runtime.runs.output_inventory import build_event_inventory_evidence, output_receipt_digest, presentation_fragment_digest


def request(**changes):
    return {
        "version": 1,
        "kind": "human_input_request",
        "source": "ask_clarification",
        "request_id": "clarification:call",
        "tool_call_id": "call",
        "question": "Which option?",
        "input_mode": "choice_with_other",
        "options": [{"id": "option-1", "label": "First", "value": "First"}],
        **changes,
    }


def record():
    scope = PublicationScope("a" * 32, "owner", 91, "lead_agent", "lead_agent", "lead_agent", None, "connection", 71, 71, None, 0, "thread", "e" * 64)
    origin = PublicationOrigin(scope.link_id, 701, 71, 1701, None, 0)
    return PublicationRecord(scope, "b" * 32, sha256(("b" * 32 + ":event").encode()).hexdigest()[:32], "event", (origin,), 1, "c" * 64, True, "run")


def source_basis(original=None, *, payload=None):
    original = original or record()
    payload = request() if payload is None else payload
    ai = AIMessage("", id="actual-ai", tool_calls=[{"id": "call", "name": "ask_clarification", "args": {"question": "Which option?"}}])
    tool = ToolMessage("Which option?", id="clarification:call", tool_call_id="call", name="ask_clarification", artifact={"human_input": payload})
    receipt = {"version": 1, "kind": "unavailable", "owner_user_id": original.scope.owner_user_id, "thread_id": original.scope.thread_id, "run_id": original.run_id, "input_message_id": "actual-human", "reason": "human_input_required"}
    document = inventory_document(owner_user_id=receipt["owner_user_id"], thread_id=receipt["thread_id"], run_id=receipt["run_id"], input_message_id=receipt["input_message_id"], output_receipt_sha256=output_receipt_digest(receipt))
    document["items"] = [{"kind": "structured", "carrier": "message_artifact", "value_sha256": presentation_fragment_digest(tool.artifact), "witness": {"kind": "root_snapshot", "root_epoch": ROOT_EPOCH, "message_id": tool.id}}]
    evidence = build_event_inventory_evidence(receipt, status="success", inventory_event_seq=9, document=document)
    scope = original.scope
    output = {
        "version": 1,
        "owner_user_id": scope.owner_user_id,
        "thread_id": scope.thread_id,
        "run_id": original.run_id,
        "run_status": "success",
        "state": "unavailable",
        "channel_execution": {"version": 1, "publication_id": original.publication_id, "owner_user_id": scope.owner_user_id, "thread_id": scope.thread_id, "assistant_id": scope.assistant_id, "agent_name": scope.agent_name},
        "output_evidence": receipt,
        "source": None,
        "reason": "human_input_required",
    }
    events = [
        {
            "thread_id": scope.thread_id,
            "run_id": original.run_id,
            "user_id": scope.owner_user_id,
            "seq": seq,
            "category": "message",
            "event_type": kind,
            "content": message.model_dump(),
            "metadata": metadata,
            "created_at": "2026-10-28T00:00:00+00:00",
        }
        for seq, kind, message, metadata in [(2, "llm.ai.response", ai, {"caller": "lead_agent"}), (3, "llm.tool.result", tool, {})]
    ]
    return {
        "version": 1,
        "publication": publication_identity(original),
        "projection": {"version": 2, "output": output, "inventory": {"state": "matched", "evidence": evidence.model_dump(mode="json"), "document": document, "reason": None}},
        "messages": events,
    }
