"""Exact retained proposal artifacts; bus hints never authorize draft adoption."""

import asyncio
import json
from copy import deepcopy
from datetime import datetime

from langchain_core.messages import AIMessage, HumanMessage, ToolMessage

from app.telegram_bots.assign import DraftAssignment
from app.telegram_bots.inbox import PublicationOrigin, PublicationRecord, PublicationScope, publication_identity
from app.telegram_bots.terminal_source import MAX_SOURCE_MESSAGES, capture_terminal_source, plain_source
from deerflow.agent_instances.chat_tools import successful_assignment_proposal
from deerflow.runtime.runs.output_inventory import StructuredInventoryDescriptor, content_items, presentation_fragment_digest
from deerflow.runtime.runs.output_source import inspect_output_message
from deerflow.utils.file_io import await_drained

_ISSUER = object()


def _refuse():
    raise ValueError("assignment-proposal-source-conflict") from None


def validate_proposal_basis(record, basis):
    """Check closed source consistency; the caller still needs live authority.

    A single proposal accounts for the entire publication. Mixed presentations
    are held for their owning delivery path, never silently discarded here.
    """
    from app.gateway.run_output import RunOutputProjectionV2

    try:
        identity = publication_identity(record)
        scope = PublicationScope(**identity["scope"])
        record = PublicationRecord(**{**identity, "scope": scope, "origins": tuple(PublicationOrigin(**origin) for origin in identity["origins"]), "dispatch_started": record.dispatch_started, "run_id": record.run_id})
        basis = plain_source(basis)
        if (
            scope.agent_kind != "instance"
            or record.run_id is None
            or type(basis) is not dict
            or set(basis) != {"version", "publication", "projection", "messages"}
            or type(basis["version"]) is not int
            or basis["version"] != 1
            or json.dumps(basis["publication"], sort_keys=True) != json.dumps(identity, sort_keys=True)
        ):
            _refuse()
        projection = RunOutputProjectionV2.model_validate(basis["projection"])
        output, inventory = projection.output, projection.inventory
        channel = output.channel_execution
        if (
            (output.owner_user_id, output.thread_id, output.run_id, channel.publication_id, channel.assistant_id, channel.agent_name)
            != (scope.owner_user_id, scope.thread_id, record.run_id, record.publication_id, scope.assistant_id, scope.agent_name)
            or output.run_status != "success"
            or output.state != "unavailable"
            or output.reason != "unsupported_output"
            or output.output_evidence.input_message_id is None
            or inventory.state != "matched"
            or inventory.document.coverage != "complete"
        ):
            _refuse()
        items = content_items(inventory.document)
        if len(items) != 1 or type(items[0]) is not StructuredInventoryDescriptor or items[0].carrier != "message_artifact":
            _refuse()
        # Page descriptors do not belong to content_items; refusing them as well
        # prevents an unsupported mixture from losing a promised presentation.
        if len(inventory.document.items) != 1:
            _refuse()
        structured, rows = items[0], basis["messages"]
        if type(rows) is not list or not rows or len(rows) > MAX_SOURCE_MESSAGES:
            _refuse()
        previous, messages, human_count = 0, [], 0
        classes = {"llm.human.input": HumanMessage, "llm.ai.response": AIMessage, "llm.tool.result": ToolMessage}
        for row in rows:
            if (
                type(row) is not dict
                or any(row.get(key) != expected for key, expected in (("thread_id", scope.thread_id), ("run_id", record.run_id), ("user_id", scope.owner_user_id), ("category", "message")))
                or type(row.get("seq")) is not int
                or not previous < row["seq"] < inventory.evidence.inventory_event_seq
                or type(row.get("content")) is not dict
                or type(row.get("metadata")) is not dict
                or type(row.get("created_at")) is not str
                or datetime.fromisoformat(row["created_at"]).utcoffset() is None
                or row.get("event_type") not in classes
            ):
                _refuse()
            previous = row["seq"]
            kind = row["event_type"]
            message = classes[kind].model_validate(row["content"])
            if kind == "llm.ai.response" and row["metadata"].get("caller") != "lead_agent":
                _refuse()
            if kind == "llm.human.input":
                human_count += 1
                human = inspect_output_message(row["content"])
                if messages or human.unavailable_reason is not None or human.message_id != output.output_evidence.input_message_id or row["metadata"].get("caller") != "lead_agent":
                    _refuse()
            messages.append(message)
        if human_count != 1:
            _refuse()
        tool = successful_assignment_proposal(messages)
        if tool is None or tool is not messages[-1] or tool.additional_kwargs.get("hide_from_ui") is True or set(tool.artifact) != {"assignment_card"} or presentation_fragment_digest(tool.artifact) != structured.value_sha256:
            _refuse()
        # A callback can seal the artifact before a root snapshot; all witness
        # kinds still require one digest-equal current journal result/call.
        matching = [row for row in rows if row["event_type"] == "llm.tool.result" and presentation_fragment_digest(row["content"].get("artifact")) == structured.value_sha256]
        if len(matching) != 1 or matching[0] is not rows[-1]:
            _refuse()
        witness = structured.witness
        if witness.kind not in {"callback", "root_snapshot", "message_event"}:
            _refuse()
        if witness.kind in {"root_snapshot", "message_event"} and (witness.message_id != tool.id or witness.kind == "message_event" and witness.event_seq != rows[-1]["seq"]):
            _refuse()
        return DraftAssignment.model_validate(tool.artifact["assignment_card"]["draft"])
    except Exception:
        _refuse()


class QualifiedAssignmentProposal:
    """Opaque original-attempt source; copied JSON does not acquire authority."""

    @property
    def record(self):
        return deepcopy(self._record)

    @property
    def draft(self):
        return self._draft.model_copy(deep=True)

    @property
    def basis(self):
        return deepcopy(self._basis)

    def __copy__(self):
        return self

    def __deepcopy__(self, memo):
        return self


def check_qualified_proposal(source, record):
    if type(source) is not QualifiedAssignmentProposal or source._issuer is not _ISSUER or source._basis["publication"] != publication_identity(record) or source._record.run_id != record.run_id or source._check() is not True:
        _refuse()
    return source.basis


class AssignmentProposalSource:
    def __init__(self, coordinator, http_owner, capture_attempt):
        self.coordinator, self.http_owner, self.capture_attempt = coordinator, http_owner, capture_attempt

    async def read_proposal(self, record, link, *, guard=None):
        captured = await capture_terminal_source(self, record, link, guard=guard, accept=lambda output: output.output.state == "unavailable" and output.output.reason == "unsupported_output")
        if captured is None:
            return None
        record = captured.record
        basis = {"version": 1, "publication": publication_identity(record), "projection": captured.projection.model_dump(mode="json"), "messages": captured.messages}
        draft = await await_drained(asyncio.to_thread(validate_proposal_basis, record, basis))
        if captured.current() is not True:
            _refuse()
        result = object.__new__(QualifiedAssignmentProposal)
        for key, value in {"_basis": deepcopy(basis), "_draft": draft, "_record": record, "_guard": captured.guard, "_check": captured.current, "_issuer": _ISSUER}.items():
            object.__setattr__(result, key, value)
        return result
