"""Exact message eligibility and durable source reads; never root-origin attestation."""

from __future__ import annotations

import math
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime
from types import MappingProxyType
from typing import Literal

from deerflow.runtime.events.store.base import RunEventStore
from deerflow.runtime.runs.output import OutputEvidenceError, OutputUnavailableReason, TextOutputReceipt, canonical_text_digest, validate_receipt
from deerflow.utils.file_io import await_drained
from deerflow.utils.messages import strip_injected_user_message_id_suffix


def _freeze(value):
    if isinstance(value, Mapping):
        if any(type(key) is not str for key in value):
            raise OutputEvidenceError()
        return MappingProxyType({key: _freeze(item) for key, item in value.items()})
    if isinstance(value, (list, tuple)):
        return tuple(_freeze(item) for item in value)
    if value is None or type(value) in {str, bool, int} or type(value) is float and math.isfinite(value):
        return value
    raise OutputEvidenceError()


@dataclass(frozen=True)
class OutputMessageEvidence:
    role: Literal["human", "assistant", "tool", "other"]
    message_id: str | None
    text: str | None
    unavailable_reason: OutputUnavailableReason | None


@dataclass(frozen=True)
class OutputSourceAudit:
    state: Literal["matched", "missing", "changed"]
    text: str | None
    input_message: Mapping[str, object] | None = None

    def __post_init__(self):
        if self.input_message is not None:
            object.__setattr__(self, "input_message", _freeze(self.input_message))


def _text(content):
    if type(content) is str:
        return content or None
    if not isinstance(content, (list, tuple)):
        raise OutputEvidenceError()
    parts = []
    unsupported = False
    for block in content:
        if type(block) is str:
            parts.append(block)
        elif isinstance(block, Mapping):
            if block.get("type") == "text":
                if type(block.get("text")) is not str:
                    raise OutputEvidenceError()
                if set(block) == {"type", "text"}:
                    parts.append(block["text"])
                else:
                    unsupported = True
            else:
                unsupported = True
        else:
            raise OutputEvidenceError()
    return None if unsupported else "".join(parts) or None


def inspect_output_message(message: object) -> OutputMessageEvidence:
    """Parse detached canonical syntax, without granting callback/root finality."""
    try:
        if not isinstance(message, Mapping):
            raise OutputEvidenceError()
        message = _freeze(message)
        kind = message.get("type")
        if type(kind) is not str or not kind:
            raise OutputEvidenceError()
        role = "assistant" if kind in {"ai", "AIMessageChunk"} else kind if kind in {"human", "tool"} else "other"
        identity = message.get("id")
        if identity is not None and type(identity) is not str:
            raise OutputEvidenceError()
        identity = identity or None
        if role == "human":
            identity = strip_injected_user_message_id_suffix(identity)
        name = message.get("name")
        if name is not None and type(name) is not str:
            raise OutputEvidenceError()
        additional = message.get("additional_kwargs", {})
        metadata = message.get("response_metadata", {})
        artifact = message.get("artifact")
        if artifact is None:
            artifact = {}
        if not isinstance(additional, Mapping) or not isinstance(metadata, Mapping) or not isinstance(artifact, Mapping):
            raise OutputEvidenceError()
        for flag in ("hide_from_ui", "deerflow_error_fallback", "dynamic_context_reminder"):
            if flag in additional and type(additional[flag]) is not bool:
                raise OutputEvidenceError()
        if "deerflow_content_kind" in additional and type(additional["deerflow_content_kind"]) is not str:
            raise OutputEvidenceError()
        if additional.get("reasoning_content") is not None and type(additional["reasoning_content"]) is not str:
            raise OutputEvidenceError()
        tool = False
        for key in ("tool_calls", "invalid_tool_calls", "tool_call_chunks"):
            value = message.get(key, ())
            if not isinstance(value, (list, tuple)):
                raise OutputEvidenceError()
            tool = tool or bool(value)
        for key, expected in (("tool_calls", (list, tuple)), ("function_call", (Mapping,))):
            value = additional.get(key)
            if value is not None and not isinstance(value, expected):
                raise OutputEvidenceError()
            tool = tool or bool(value)
        refusal = False
        for carrier in (additional, metadata):
            value = carrier.get("refusal")
            if value is not None and not isinstance(value, (str, Mapping, tuple, list)):
                raise OutputEvidenceError()
            refusal = refusal or bool(value)
        human_input = artifact.get("human_input")
        if human_input is not None and not isinstance(human_input, Mapping):
            raise OutputEvidenceError()
        control = human_input is not None and human_input.get("kind") == "human_input_request"
        text = _text(message.get("content"))
        if text is not None:
            canonical_text_digest(text)  # Reject invalid UTF-8, never normalize it.
        unavailable = None
        hidden = additional.get("hide_from_ui") or additional.get("dynamic_context_reminder") or bool(additional.get("deerflow_content_kind")) or name == "summary" or bool(name and name.startswith("__"))
        if control and role == "tool":
            unavailable = "human_input_required"
        elif control or role == "other" or role == "tool" or hidden or tool or refusal or additional.get("deerflow_error_fallback") or text is None or role == "human" and "human_input_response" in additional:
            unavailable = "unsupported_input" if role == "human" else "unsupported_output"
        elif identity is None:
            unavailable = "missing_message_identity"
        return OutputMessageEvidence(role, identity, text if unavailable is None else None, unavailable)
    except Exception:
        raise OutputEvidenceError() from None


def _receipt(value):
    result = validate_receipt(value)
    if not isinstance(result, TextOutputReceipt):
        raise OutputEvidenceError()
    return result


def _envelope(event, receipt, *, after_seq=0, before_seq=None):
    if not isinstance(event, Mapping) or any(event.get(key) != expected for key, expected in (("thread_id", receipt.thread_id), ("run_id", receipt.run_id), ("user_id", receipt.owner_user_id), ("category", "message"))):
        raise OutputEvidenceError()
    seq = event.get("seq")
    if type(seq) is not int or seq <= after_seq or before_seq is not None and seq >= before_seq:
        raise OutputEvidenceError()
    if type(event.get("event_type")) is not str or not event["event_type"] or not isinstance(event.get("metadata"), Mapping) or not isinstance(event.get("content"), Mapping) or type(event.get("created_at")) is not str:
        raise OutputEvidenceError()
    stamp = datetime.fromisoformat(event["created_at"])
    if stamp.tzinfo is None or stamp.utcoffset() is None:
        raise OutputEvidenceError()
    return inspect_output_message(event["content"])


def audit_output_source_event(event: object, *, receipt: TextOutputReceipt) -> OutputSourceAudit:
    """Audit one present original source. Caller still owns proof of its read."""
    try:
        receipt = _receipt(receipt)
        facts = _envelope(event, receipt)
        if event["seq"] != receipt.event_seq:
            raise OutputEvidenceError()
        caller = event["metadata"].get("caller")
        if caller is not None and type(caller) is not str:
            raise OutputEvidenceError()
        if (
            event["event_type"] != "llm.ai.response"
            or caller != "lead_agent"
            or facts.role != "assistant"
            or facts.unavailable_reason is not None
            or facts.message_id != receipt.assistant_message_id
            or canonical_text_digest(facts.text) != receipt.text_sha256
        ):
            return OutputSourceAudit("changed", None)
        return OutputSourceAudit("matched", facts.text)
    except Exception:
        raise OutputEvidenceError() from None


async def read_durable_output_source(event_store: RunEventStore, *, receipt: TextOutputReceipt) -> OutputSourceAudit:
    """Drain the complete exact run-prefix read; successful absence alone is missing."""
    receipt = _receipt(receipt)
    if getattr(event_store, "supports_durable_output_evidence", False) is not True:
        raise OutputEvidenceError()

    async def read():
        try:
            cursor = 0
            assistant = None
            human = None
            human_changed = False
            while True:
                rows = await event_store.list_messages_by_run(receipt.thread_id, receipt.run_id, limit=256, after_seq=cursor, before_seq=receipt.event_seq + 1, user_id=receipt.owner_user_id)
                if not isinstance(rows, list) or len(rows) > 256:
                    raise OutputEvidenceError()
                for row in rows:
                    facts = _envelope(row, receipt, after_seq=cursor, before_seq=receipt.event_seq + 1)
                    cursor = row["seq"]
                    if cursor == receipt.event_seq:
                        assistant = audit_output_source_event(row, receipt=receipt)
                    if receipt.input_message_id is not None and facts.role == "human" and facts.message_id == receipt.input_message_id and cursor < receipt.event_seq:
                        current = _freeze(row["content"])
                        caller = row["metadata"].get("caller")
                        if caller is not None and type(caller) is not str:
                            raise OutputEvidenceError()
                        if row["event_type"] != "llm.human.input" or caller != "lead_agent" or facts.unavailable_reason is not None:
                            human_changed = True
                        if human is not None and human != current:
                            human_changed = True
                        human = current
                if len(rows) < 256:
                    break
            if assistant is None or receipt.input_message_id is not None and human is None:
                return OutputSourceAudit("missing", None)
            if assistant.state != "matched" or human_changed:
                return OutputSourceAudit("changed", None)
            return OutputSourceAudit("matched", assistant.text, human)
        except Exception:
            raise OutputEvidenceError() from None

    return await await_drained(read())
