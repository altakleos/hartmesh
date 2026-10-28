"""Exact durable human-input offers; bus artifacts are only advisory hints."""

from __future__ import annotations

import asyncio
import json
import math
from collections.abc import Mapping
from copy import deepcopy
from dataclasses import dataclass
from datetime import datetime
from pathlib import PurePosixPath

from deerflow.utils.file_io import await_drained

MAX_SOURCE_MESSAGES = 4096
MAX_SOURCE_PAGES = 32
_ISSUER = object()
_RESERVED_NAMES = frozenset(
    {"__proto__", "constructor", "prototype", "toString", "toLocaleString", "valueOf", "hasOwnProperty", "isPrototypeOf", "propertyIsEnumerable", "__defineGetter__", "__defineSetter__", "__lookupGetter__", "__lookupSetter__"}
)


def _refuse():
    raise ValueError("human-input-source-conflict") from None


def _plain(value):
    if isinstance(value, Mapping):
        if any(type(key) is not str for key in value):
            _refuse()
        return {key: _plain(child) for key, child in value.items()}
    if type(value) in {list, tuple}:
        return [_plain(child) for child in value]
    if value is None or type(value) in {str, bool, int} or type(value) is float and math.isfinite(value):
        return value
    _refuse()


def _text(value):
    return type(value) is str and bool(value.strip())


def _options(value, *, maximum=None):
    if type(value) is not list or not value or maximum is not None and len(value) > maximum:
        _refuse()
    ids, values = set(), set()
    for item in value:
        if type(item) is not dict or set(item) != {"id", "label", "value"} or any(not _text(item[key]) for key in item) or item["id"] in ids or item["value"] in values:
            _refuse()
        ids.add(item["id"])
        values.add(item["value"])
    return value


def decode_request(value):
    """Validate a frozen request's presentation shape; this grants no authority."""
    value = _plain(value)
    required = {"version", "kind", "source", "request_id", "question", "input_mode"}
    optional = {"tool_call_id", "clarification_type", "context", "title", "options", "fields", "details"}
    if (
        type(value) is not dict
        or not required <= set(value)
        or set(value) - required - optional
        or value["kind"] != "human_input_request"
        or any(not _text(value[key]) for key in ("source", "request_id", "question", "input_mode"))
        or type(value["version"]) is not int
    ):
        _refuse()
    if any(key in value and not _text(value[key]) for key in ("tool_call_id", "title", "clarification_type")) or "context" in value and value["context"] is not None and type(value["context"]) is not str:
        _refuse()
    mode, source = value["input_mode"], value["source"]
    if source == "sandbox_network":
        details = value.get("details")
        if (
            value["version"] != 1
            or mode != "single_choice"
            or type(details) is not dict
            or set(details) != {"host", "port", "ttl_seconds"}
            or not _text(details["host"])
            or type(details["port"]) is not int
            or not 1 <= details["port"] <= 65535
            or type(details["ttl_seconds"]) is not int
            or details["ttl_seconds"] < 60
        ):
            _refuse()
        _options(value.get("options"))
        if not {"allow_temporary", "deny"} <= {option["id"] for option in value["options"]}:
            _refuse()
    elif source == "ask_clarification":
        if mode == "form":
            fields = value.get("fields")
            if value["version"] != 2 or type(fields) is not list or not 1 <= len(fields) <= 16 or "options" in value or len(json.dumps(fields, ensure_ascii=False).encode("utf-8")) > 16384:
                _refuse()
            names = set()
            for field in fields:
                if (
                    type(field) is not dict
                    or not {"name", "label", "type", "required"} <= set(field)
                    or set(field) - {"name", "label", "type", "required", "options", "placeholder"}
                    or any(not _text(field[key]) or len(field[key]) > 200 for key in ("name", "label"))
                    or field["name"] in names
                    or field["name"] in _RESERVED_NAMES
                    or type(field["required"]) is not bool
                    or type(field["type"]) is not str
                    or field["type"] not in {"text", "textarea", "number", "date", "select", "multi_select", "checkbox"}
                ):
                    _refuse()
                names.add(field["name"])
                if "placeholder" in field and (type(field["placeholder"]) is not str or len(field["placeholder"]) > 200):
                    _refuse()
                if field["type"] in {"select", "multi_select"}:
                    options = _options(field.get("options"), maximum=24)
                    if any(len(option[key]) > 200 for option in options for key in ("label", "value")):
                        _refuse()
                elif "options" in field:
                    _refuse()
        elif mode in {"free_text", "choice_with_other"} and value["version"] == 1 and "fields" not in value:
            if mode == "choice_with_other":
                _options(value.get("options"))
            elif value.get("options"):
                _refuse()
        else:
            _refuse()
    else:
        _refuse()
    return value


def validate_request_basis(record, basis):
    """Recheck stored evidence independently; DTO shape never grants a read."""
    from app.gateway.run_output import RunOutputProjectionV2
    from app.telegram_bots.inbox import publication_identity
    from deerflow.runtime.runs.output_inventory import StructuredInventoryDescriptor, presentation_fragment_digest

    try:
        basis = _plain(basis)
        if (
            type(basis) is not dict
            or set(basis) != {"version", "publication", "projection", "messages"}
            or type(basis["version"]) is not int
            or basis["version"] not in {1, 2}
            or json.dumps(basis["publication"], sort_keys=True) != json.dumps(publication_identity(record), sort_keys=True)
        ):
            _refuse()
        projection = RunOutputProjectionV2.model_validate(basis["projection"])
        output, inventory, scope = projection.output, projection.inventory, record.scope
        channel = output.channel_execution
        if (
            (output.owner_user_id, output.thread_id, output.run_id, channel.publication_id, channel.assistant_id, channel.agent_name)
            != (scope.owner_user_id, scope.thread_id, record.run_id, record.publication_id, scope.assistant_id, scope.agent_name)
            or output.run_status != "success"
            or output.state != "unavailable"
            or output.reason != "human_input_required"
            or output.output_evidence.input_message_id is None
            or inventory.state != "matched"
            or inventory.document.coverage != "complete"
        ):
            _refuse()
        items = inventory.document.items
        if basis["version"] == 1:
            if len(items) != 1:
                _refuse()
            structured = items[0]
        else:
            if type(inventory.document.version) is not int or inventory.document.version != 2:
                _refuse()
            _mixed_paths(items)
            structured_items = [item for item in items if type(item) is StructuredInventoryDescriptor]
            if len(structured_items) != 1:
                _refuse()
            structured = structured_items[0]
        if type(structured) is not StructuredInventoryDescriptor or structured.carrier != "message_artifact":
            _refuse()
        rows = basis["messages"]
        if type(rows) is not list or not rows or len(rows) > MAX_SOURCE_MESSAGES:
            _refuse()
        previous, calls, candidates = 0, {}, []
        for row in rows:
            if (
                type(row) is not dict
                or any(row.get(key) != expected for key, expected in (("thread_id", scope.thread_id), ("run_id", record.run_id), ("user_id", scope.owner_user_id), ("category", "message")))
                or type(row.get("seq")) is not int
                or row["seq"] <= previous
                or row["seq"] >= inventory.evidence.inventory_event_seq
                or type(row.get("content")) is not dict
                or type(row.get("metadata")) is not dict
                or type(row.get("created_at")) is not str
                or datetime.fromisoformat(row["created_at"]).utcoffset() is None
            ):
                _refuse()
            previous = row["seq"]
            content = row["content"]
            if row.get("event_type") == "llm.ai.response" and row["metadata"].get("caller") == "lead_agent" and content.get("type") == "ai":
                for call in content.get("tool_calls", []):
                    identity = call.get("id")
                    if not _text(identity) or identity in calls:
                        _refuse()
                    calls[identity] = (call, row["seq"])
            if row.get("event_type") == "llm.tool.result" and content.get("type") == "tool" and isinstance(content.get("artifact"), dict) and presentation_fragment_digest(content["artifact"]) == structured.value_sha256:
                candidates.append(row)
        if len(candidates) != 1 or candidates[0]["seq"] != rows[-1]["seq"]:
            _refuse()
        row = candidates[0]
        tool = row["content"]
        artifact = tool["artifact"]
        if set(artifact) != {"human_input"} or type(artifact["human_input"]) is not dict or artifact["human_input"].get("kind") != "human_input_request" or tool.get("additional_kwargs", {}).get("hide_from_ui") is True:
            _refuse()
        request = artifact["human_input"]
        call, sequence = calls[tool["tool_call_id"]]
        if sequence >= row["seq"] or call["name"] != tool["name"] or request.get("tool_call_id") != tool["tool_call_id"]:
            _refuse()
        if request.get("source") == "ask_clarification" and (tool["name"] != "ask_clarification" or tool.get("id") != request.get("request_id")):
            _refuse()
        if basis["version"] == 2 and request.get("source") not in {"ask_clarification", "sandbox_network"}:
            _refuse()
        witness = structured.witness
        if basis["version"] == 2 and witness.kind not in {"root_snapshot", "message_event"}:
            _refuse()
        if witness.kind == "root_snapshot" and witness.message_id != tool.get("id") or witness.kind == "message_event" and (witness.message_id != tool.get("id") or witness.event_seq != row["seq"]):
            _refuse()
        return deepcopy(request)
    except Exception:
        _refuse()


def _mixed_paths(items):
    from deerflow.runtime.runs.output_inventory import FileInventoryDescriptor, StructuredInventoryDescriptor

    paths = []
    for item in items:
        if type(item) is StructuredInventoryDescriptor:
            continue
        if type(item) is not FileInventoryDescriptor:
            _refuse()
        path = item.path
        parsed = PurePosixPath(path)
        if (
            not path.startswith("/mnt/user-data/outputs/")
            or str(parsed) != path
            or "\\" in path
            or any(ord(char) < 32 or ord(char) == 127 for char in path)
            or any(part in {".", ".."} for part in parsed.parts)
            or path in paths
            or not item.surfaces
            or not item.witnesses
        ):
            _refuse()
        paths.append(path)
    if not paths:
        _refuse()
    return tuple(paths)


def mixed_file_paths(record, basis):
    """Return ordered files only after qualifying the complete mixed proof."""
    from app.gateway.run_output import RunOutputProjectionV2

    validate_request_basis(record, basis)
    if type(basis["version"]) is not int or basis["version"] != 2:
        _refuse()
    return _mixed_paths(RunOutputProjectionV2.model_validate(basis["projection"]).inventory.document.items)


@dataclass(frozen=True, init=False)
class QualifiedHumanInput:
    _basis: dict
    _request: dict
    _check: object
    _issuer: object
    _record: object
    _guard: object

    @property
    def record(self):
        return deepcopy(self._record)

    @property
    def request(self):
        return deepcopy(self._request)

    @property
    def basis(self):
        return deepcopy(self._basis)

    def __copy__(self):
        return self

    def __deepcopy__(self, memo):
        return self


def check_qualified_request(source, record):
    from app.telegram_bots.inbox import publication_identity

    if type(source) is not QualifiedHumanInput or source._issuer is not _ISSUER or source._basis["publication"] != publication_identity(record) or source._check() is not True:
        _refuse()
    return source.basis


def freeze_human_input_parts(record, request, *, parts, keyboard):
    """Freeze presentation only; source and writer authority remain separate."""
    from app.telegram_bots.callbacks import canonical_keyboard
    from app.telegram_bots.producer import _canonical_primary, _safe_json
    from app.telegram_bots.split import utf16_units, visible_text

    controls = canonical_keyboard(keyboard)
    if type(parts) is not list or not parts:
        _refuse()
    frozen = []
    for index, supplied in enumerate(parts):
        part = _safe_json(deepcopy(supplied))
        if (
            part.get("kind") not in {"text", "edit"}
            or not {"html", "visible_text", "reply_to_message_id", "force_reply"} <= set(part)
            or set(part) - {"kind", "html", "visible_text", "reply_to_message_id", "force_reply", "target_message_id", "target_protect_content", "source_ref", "stored_keyboard", "label_values", "remove_keyboard"}
        ):
            _refuse()
        if (
            not _text(part["html"])
            or type(part["visible_text"]) is not str
            or part["visible_text"] != visible_text(part["html"])
            or utf16_units(part["visible_text"]) > 4096
            or type(part["force_reply"]) is not bool
            or part["force_reply"]
            and (index != len(parts) - 1 or controls)
            or type(part["reply_to_message_id"]) is not int
            or part["reply_to_message_id"] not in {origin.message_id for origin in record.origins}
        ):
            _refuse()
        if part["kind"] == "edit" and (type(part.get("target_message_id")) is not int or part["target_message_id"] <= 0) or "target_protect_content" in part and type(part["target_protect_content"]) is not bool:
            _refuse()
        if "remove_keyboard" in part and (type(part["remove_keyboard"]) is not bool or part["kind"] != "edit" or part["force_reply"] or controls):
            _refuse()
        values = part.pop("label_values", {})
        if type(values) is not dict or {"request_id", "part"}.intersection(values):
            _refuse()
        expected = {"key": "human_input.presentation", "values": {**values, "request_id": request.get("request_id") if type(request.get("request_id")) is str else "", "part": index}}
        if "source_ref" in part:
            supplied_values = part["source_ref"].get("values", {})
            expected["values"] = {**supplied_values, "request_id": expected["values"]["request_id"], "part": index}
            if _canonical_primary(part["source_ref"]) != _canonical_primary(expected):
                _refuse()
        part["source_ref"] = expected
        stored = bool(controls) and index == len(parts) - 1
        if "stored_keyboard" in part and part["stored_keyboard"] is not stored:
            _refuse()
        if stored:
            part["stored_keyboard"] = True
        frozen.append(part)
    return frozen, controls


def prepare_human_input_reply(record, source_basis, *, parts, keyboard, inputs, step_id=None):
    """Freeze host-rendered UI under the exact private request's source basis."""
    from app.telegram_bots.inbox import publication_identity, validate_publication_inputs
    from app.telegram_bots.producer import _safe_json

    request = validate_request_basis(record, source_basis)
    validate_publication_inputs(record, inputs)
    if step_id is not None and (not _text(step_id) or len(step_id) > 128 or not step_id.isprintable()):
        _refuse()
    if source_basis["version"] == 2 and step_id is None:
        _refuse()
    frozen, controls = freeze_human_input_parts(record, request, parts=parts, keyboard=keyboard)
    outcome = {"kind": "human_input", "projection": deepcopy(source_basis["projection"]), "request_basis": deepcopy(source_basis)}
    basis = {"version": 5, "publication": publication_identity(record), "outcome": outcome, "presentation": None, "parts": frozen, "keyboard": controls, "inputs": deepcopy(inputs), "step_id": step_id, "finish_route": step_id is None}
    return _safe_json(basis), frozen


def validate_human_input_reply_basis(item, part=None):
    """Validate old canonical UI bytes without silently re-rendering them."""
    from app.gateway.run_output import RunOutputProjectionV2
    from app.telegram_bots.inbox import PublicationOrigin, PublicationRecord, PublicationScope
    from app.telegram_bots.model_source import VerifiedModelReplyBasis
    from app.telegram_bots.producer import OutboxItem, _canonical_primary

    try:
        if type(item) is not OutboxItem or type(item.payload) is not dict or "product_basis" in item.payload or "delivery_kind" in item.payload or "turn_outcome_basis" in item.payload:
            _refuse()
        basis = item.payload["reply_basis"]
        if type(basis) is dict and type(basis.get("version")) is int and basis["version"] == 7:
            from app.telegram_bots.mixed_source import validate_mixed_reply_basis

            return validate_mixed_reply_basis(item, part)
        if (
            type(basis) is not dict
            or set(basis) != {"version", "publication", "outcome", "presentation", "parts", "keyboard", "inputs", "step_id", "finish_route"}
            or type(basis["version"]) is not int
            or basis["version"] != 5
            or basis["presentation"] is not None
            or type(basis["finish_route"]) is not bool
            or basis["finish_route"] is not (basis["step_id"] is None)
        ):
            _refuse()
        outcome = basis["outcome"]
        if type(outcome) is not dict or set(outcome) != {"kind", "projection", "request_basis"} or outcome["kind"] != "human_input":
            _refuse()
        output = RunOutputProjectionV2.model_validate(outcome["projection"])
        identity = basis["publication"]
        scope = PublicationScope(**identity["scope"])
        record = PublicationRecord(**{**identity, "scope": scope, "origins": tuple(PublicationOrigin(**origin) for origin in identity["origins"]), "dispatch_started": True, "run_id": output.output.run_id})
        if _canonical_primary(outcome["projection"]) != _canonical_primary(outcome["request_basis"]["projection"]):
            _refuse()
        expected, initial = prepare_human_input_reply(record, outcome["request_basis"], parts=basis["parts"], keyboard=basis["keyboard"], inputs=basis["inputs"], step_id=basis["step_id"])
        if (
            _canonical_primary(expected) != _canonical_primary(basis)
            or _canonical_primary(item.payload["source"]) != _canonical_primary(outcome)
            or _canonical_primary({"keyboard": item.payload["product_keyboard_basis"]}) != _canonical_primary({"keyboard": basis["keyboard"]})
        ):
            _refuse()
        dedupe = "reply:" + record.turn_id if basis["step_id"] is None else human_input_step_key(record, basis["step_id"])
        if (item.link_id, item.owner_user_id, item.binding_generation, item.chat_id, item.dedupe_key, item.class_) != (
            scope.link_id,
            scope.owner_user_id,
            scope.binding_generation,
            scope.chat_id,
            dedupe,
            "reply",
        ) or item.message_thread_id not in {scope.message_thread_id, None}:
            _refuse()
        receipts = {
            "part_id",
            "presentation_version",
            "ordinal",
            "state",
            "message_ids",
            "sent_at",
            "dispatched_at",
            "protect_content",
            "sent_protect_content",
            "plain_fallback",
            "invalid_token_verification_pending",
            "callback_group_id",
            "unavailable_reason",
        }

        def clean(value):
            if type(value) is not dict or any(type(value[key]) is not bool for key in ("plain_fallback", "protect_content", "sent_protect_content") if key in value):
                _refuse()
            return {key: child for key, child in value.items() if key not in receipts}

        allowed = {_canonical_primary(value) for value in initial}
        if item.payload.get("replacement_reason") in {"edit-anchor-missing", "protection", "topic-missing"}:
            for value in initial:
                relocated = {key: child for key, child in value.items() if key not in {"target_message_id", "target_protect_content"}}
                relocated["kind"] = "text"
                allowed.add(_canonical_primary(relocated))
        version = item.payload.get("presentation_version")
        if type(version) is not int or version < 0 or not item.parts:
            _refuse()
        for value in item.parts + ([part] if part is not None else []):
            if _canonical_primary(clean(value)) not in allowed:
                _refuse()
        active = [value for value in item.parts if value.get("presentation_version") == version]
        if len({_canonical_primary(value["source_ref"]) for value in active}) != len(active):
            _refuse()
        acknowledged = {_canonical_primary(value["source_ref"]) for value in item.parts if value.get("state") == "sent"}
        required = {_canonical_primary(value["source_ref"]) for value in initial}
        if required != acknowledged | {_canonical_primary(value["source_ref"]) for value in active}:
            _refuse()
        return VerifiedModelReplyBasis(record, output, None, "human_input")
    except Exception:
        _refuse()


def human_input_step_key(record, step_id):
    from hashlib import sha256

    return "notice:human-input:" + record.publication_id + "." + sha256(step_id.encode("utf-8")).hexdigest()[:32]


class HumanInputSource:
    def __init__(self, *, coordinator, http_owner, capture_attempt):
        self.coordinator, self.http_owner, self.capture_attempt = coordinator, http_owner, capture_attempt

    async def read_request(self, record, link, *, guard=None):
        from app.channels.execution_client import ChannelExecutionFacade
        from app.telegram_bots.inbox import publication_identity
        from app.telegram_bots.links import prepare_publication_guard
        from app.telegram_bots.publication import channel_execution_scope

        record, link = deepcopy(record), deepcopy(link)
        if guard is not None:
            if not callable(getattr(guard, "current", None)) or guard.current() is not True:
                _refuse()
            await prepare_publication_guard(guard)
            if guard.current() is not True:
                _refuse()
            # Recovery supplies an already matched exact run. A new unbounded
            # lookup operation cannot extend that caller's original control.
            if record.run_id is None:
                return None
        if record.run_id is None:
            observed = await self.coordinator.locate_run(record, link, max_pages=1)
            if observed.state != "matched":
                return None
            record = observed.record
        attempt = await self.capture_attempt(link, thread_id=record.scope.thread_id, endpoint_id=record.scope.endpoint_id)

        class Admission:
            def current(self, operation):
                return attempt.admission.current(operation) is True and (guard is None or guard.current() is True)

            async def authorize(self, operation):
                if self.current(operation) is not True:
                    _refuse()
                await attempt.admission.authorize(operation)
                if guard is not None:
                    await prepare_publication_guard(guard)
                if self.current(operation) is not True:
                    _refuse()

        admission = Admission()
        facade = ChannelExecutionFacade(self.http_owner, scope=channel_execution_scope(record), admission=admission, capture_output=True, capture_inventory=True, inventory_document_version=2)
        output = await facade.get_run_output(record.run_id) if guard is not None else await self.coordinator.read_output(record, link)
        if output is None or output.output.state != "unavailable" or output.output.reason != "human_input_required":
            return None
        rows, after = [], None
        for _ in range(MAX_SOURCE_PAGES):
            page = await facade.list_run_messages(record.run_id, limit=200, after_seq=after)
            rows.extend(_plain(page.data))
            if len(rows) > MAX_SOURCE_MESSAGES:
                _refuse()
            if not page.has_more:
                break
            if page.next_after_seq is None or after is not None and page.next_after_seq <= after:
                _refuse()
            after = page.next_after_seq
        else:
            _refuse()
        # A source may have changed during the page walk. Freeze only the exact
        # pair observed on both sides, with one final original-attempt fence.
        fresh = await facade.get_run_output(record.run_id) if guard is not None else await self.coordinator.read_output(record, link)
        if fresh is None or fresh.model_dump(mode="json") != output.model_dump(mode="json"):
            _refuse()
        await admission.authorize("source_read")
        mixed = getattr(fresh.inventory.document, "version", None) == 2 and any(item.kind == "file" for item in getattr(fresh.inventory.document, "items", ()))
        basis = {"version": 2 if mixed else 1, "publication": publication_identity(record), "projection": fresh.model_dump(mode="json"), "messages": rows}
        request = await await_drained(asyncio.to_thread(validate_request_basis, record, basis))
        if admission.current("source_read") is not True:
            _refuse()

        class SourceGuard:
            captured_link = attempt.read_guard.captured_link

            async def prepare(self):
                await admission.authorize("source_read")

            async def check(self, writer):
                if admission.current("source_read") is not True:
                    _refuse()
                result = await attempt.read_guard.check(writer)
                if guard is not None:
                    result = await guard.check(writer)
                if admission.current("source_read") is not True:
                    _refuse()
                return result

        result = object.__new__(QualifiedHumanInput)
        for key, value in {
            "_basis": deepcopy(basis),
            "_request": request,
            "_record": record,
            "_guard": SourceGuard() if guard is not None else attempt.read_guard,
            "_check": lambda: admission.current("source_read"),
            "_issuer": _ISSUER,
        }.items():
            object.__setattr__(result, key, value)
        return result
