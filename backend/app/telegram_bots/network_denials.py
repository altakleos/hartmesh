"""Exact-run dropped network denials, qualified from persisted tool artifacts."""

from __future__ import annotations

import asyncio
from copy import deepcopy
from dataclasses import dataclass
from datetime import datetime

from app.telegram_bots.human_input_source import MAX_SOURCE_MESSAGES, MAX_SOURCE_PAGES, _plain
from deerflow.utils.file_io import await_drained

BLOCKED_HOSTS_PER_TURN = 3
_ISSUER = object()


def _refuse():
    raise ValueError("network-denial-source-conflict") from None


def _host_denial_markers(rows):
    """Only host-recorded additive revisions can introduce the private marker."""
    from deerflow.runtime.runs.output_inventory import presentation_fragment_digest

    previous, confirmed = {}, {}
    for row in rows:
        message = row["content"]
        if row.get("event_type") != "llm.tool.result" or message.get("type") != "tool":
            continue
        identity, artifact = message.get("id"), message.get("artifact") or {}
        if type(identity) is not str or not identity or type(artifact) is not dict:
            continue
        before = previous.get(identity)
        receipt = row["metadata"].get("tool_artifact_enrichment")
        if receipt is None:
            confirmed.pop(identity, None)
        else:
            if (
                type(receipt) is not dict
                or set(receipt) != {"version", "previous_sha256", "added_keys"}
                or type(receipt["version"]) is not int
                or receipt["version"] != 1
                or before is None
                or presentation_fragment_digest(before) != receipt["previous_sha256"]
                or receipt["added_keys"] != sorted(set(artifact).difference(before))
                or not receipt["added_keys"]
                or any(key not in artifact or artifact[key] != value for key, value in before.items())
            ):
                _refuse()
            if "sandbox_network_denied" in receipt["added_keys"]:
                confirmed[identity] = artifact["sandbox_network_denied"]
        previous[identity] = artifact
    return confirmed


def _denial_projection(record, value):
    from app.gateway.run_output import RunOutputProjectionV2

    projection = RunOutputProjectionV2.model_validate(value)
    output, inventory, scope = projection.output, projection.inventory, record.scope
    channel = output.channel_execution
    if (
        scope.agent_kind != "instance"
        or (output.owner_user_id, output.thread_id, output.run_id, channel.publication_id, channel.assistant_id, channel.agent_name)
        != (scope.owner_user_id, scope.thread_id, record.run_id, record.publication_id, scope.assistant_id, scope.agent_name)
        or output.run_status != "success"
        or not (output.state == "complete" or output.state == "unavailable" and output.reason in {"unsupported_output", "human_input_required"})
        or inventory.state != "matched"
        or inventory.document.coverage != "complete"
        or inventory.document.version != 2
    ):
        _refuse()
    return projection


def _denial_entries(record, basis, *, all_entries=False):
    """Pure consistency proof; current source admission remains mandatory."""
    from app.telegram_bots.inbox import publication_identity
    from deerflow.runtime.runs.output_inventory import presentation_fragment_digest

    try:
        basis = _plain(basis)
        if type(basis) is not dict or set(basis) != {"version", "publication", "projection", "messages"} or type(basis["version"]) is not int or basis["version"] != 1 or basis["publication"] != publication_identity(record):
            _refuse()
        scope = record.scope
        inventory = _denial_projection(record, basis["projection"]).inventory
        rows = basis["messages"]
        if type(rows) is not list or len(rows) > MAX_SOURCE_MESSAGES:
            _refuse()
        previous, calls, hosts, latest, first = 0, {}, [], {}, {}
        descriptors = [item for item in inventory.document.items if item.kind == "structured" and item.carrier == "message_artifact"]
        for row in rows:
            if (
                type(row) is not dict
                or any(row.get(key) != value for key, value in (("thread_id", scope.thread_id), ("run_id", record.run_id), ("user_id", scope.owner_user_id), ("category", "message")))
                or type(row.get("seq")) is not int
                or not previous < row["seq"] < inventory.evidence.inventory_event_seq
                or type(row.get("content")) is not dict
                or type(row.get("metadata")) is not dict
                or type(row.get("created_at")) is not str
                or datetime.fromisoformat(row["created_at"]).utcoffset() is None
            ):
                _refuse()
            previous = row["seq"]
            if row.get("event_type") == "llm.tool.result":
                identity = row["content"].get("id")
                if type(identity) is str and identity:
                    latest[identity] = row["seq"]
                    first.setdefault(identity, row["seq"])
        confirmed = _host_denial_markers(rows)
        for row in rows:
            content = row["content"]
            if row.get("event_type") == "llm.ai.response" and row["metadata"].get("caller") == "lead_agent" and content.get("type") == "ai":
                for call in content.get("tool_calls", []):
                    identity = call.get("id")
                    if type(identity) is not str or not identity:
                        _refuse()
                    calls[identity] = (call.get("name"), row["seq"])
            artifact = content.get("artifact")
            if row.get("event_type") != "llm.tool.result" or content.get("type") != "tool" or type(artifact) is not dict or "sandbox_network_denied" not in artifact:
                continue
            if latest.get(content.get("id")) != row["seq"]:
                continue
            if content.get("id") not in confirmed or artifact["sandbox_network_denied"] != confirmed[content["id"]]:
                continue
            if content.get("additional_kwargs", {}).get("hide_from_ui") is True:
                continue
            call = calls.get(content.get("tool_call_id"))
            if call is None or call[0] != content.get("name") or call[1] >= first[content["id"]]:
                _refuse()
            digest = presentation_fragment_digest(artifact)
            matches = [
                item
                for item in descriptors
                if item.value_sha256 == digest
                and (
                    item.witness.kind == "message_event"
                    and item.witness.event_seq == row["seq"]
                    and item.witness.message_id == content.get("id")
                    or item.witness.kind == "root_snapshot"
                    and item.witness.message_id is not None
                    and item.witness.message_id == content.get("id")
                )
            ]
            if not matches:
                _refuse()
            value = artifact["sandbox_network_denied"]
            if (
                type(value) is not dict
                or set(value) != {"host", "port"}
                or type(value["host"]) is not str
                or not 1 <= len(value["host"]) <= 253
                or any(char.isspace() or ord(char) < 32 or ord(char) == 127 for char in value["host"])
                or type(value["port"]) is not int
                or not 1 <= value["port"] <= 65535
            ):
                _refuse()
            host = value["host"].lower().rstrip(".")
            if not host:
                _refuse()
            hosts.append({"first_seq": first[content["id"]], "event_seq": row["seq"], "message_id": content["id"], "artifact_sha256": digest, "host": host, "port": value["port"]})
        if all_entries:
            return sorted(hosts, key=lambda value: value["first_seq"])
        selected = {}
        for value in sorted(hosts, key=lambda value: value["first_seq"]):
            selected.setdefault(value["host"], value)
        return list(selected.values())[:BLOCKED_HOSTS_PER_TURN]
    except Exception:
        _refuse()


def denied_hosts(record, basis):
    return tuple(value["host"] for value in _denial_entries(record, basis))


def freeze_denials(record, basis):
    """Retain only source identities/digests and the three displayed sites."""
    return {"version": 1, "publication": deepcopy(basis["publication"]), "projection": deepcopy(basis["projection"]), "denials": _denial_entries(record, basis)}


def request_content_items(record, basis, items):
    """Separate only pure, host-proven denial artifacts from a question."""
    if record.scope.agent_kind != "instance" or basis["projection"]["inventory"]["document"].get("version") != 2:
        return items
    entries = _denial_entries(record, {**basis, "version": 1}, all_entries=True)
    latest = {row["content"].get("id"): row["content"] for row in basis["messages"] if row.get("event_type") == "llm.tool.result"}
    pure = {(value["message_id"], value["artifact_sha256"]) for value in entries if set(latest[value["message_id"]].get("artifact") or {}) == {"sandbox_network_denied"}}
    return [item for item in items if not (item.kind == "structured" and item.carrier == "message_artifact" and item.witness.kind in {"root_snapshot", "message_event"} and (item.witness.message_id, item.value_sha256) in pure)]


def notice_hosts(record, basis):
    """Validate saved recipes; fresh host qualification grants every actual read."""
    from app.telegram_bots.inbox import publication_identity

    try:
        if type(basis) is not dict or set(basis) != {"version", "publication", "projection", "denials"} or type(basis["version"]) is not int or basis["version"] != 1 or basis["publication"] != publication_identity(record):
            _refuse()
        inventory = _denial_projection(record, basis["projection"]).inventory
        entries, hosts, previous = basis["denials"], [], 0
        if type(entries) is not list or len(entries) > BLOCKED_HOSTS_PER_TURN:
            _refuse()
        for value in entries:
            if (
                type(value) is not dict
                or set(value) != {"first_seq", "event_seq", "message_id", "artifact_sha256", "host", "port"}
                or type(value["first_seq"]) is not int
                or type(value["event_seq"]) is not int
                or not previous < value["first_seq"] < value["event_seq"] < inventory.evidence.inventory_event_seq
                or type(value["message_id"]) is not str
                or not value["message_id"]
                or type(value["host"]) is not str
                or not 1 <= len(value["host"]) <= 253
                or value["host"] != value["host"].lower().rstrip(".")
                or any(char.isspace() or ord(char) < 32 or ord(char) == 127 for char in value["host"])
                or type(value["port"]) is not int
                or not 1 <= value["port"] <= 65535
                or value["host"] in hosts
            ):
                _refuse()
            if not any(
                item.kind == "structured"
                and item.carrier == "message_artifact"
                and item.value_sha256 == value["artifact_sha256"]
                and item.witness.kind in {"root_snapshot", "message_event"}
                and item.witness.message_id == value["message_id"]
                and (item.witness.kind == "root_snapshot" or item.witness.kind == "message_event" and item.witness.event_seq == value["event_seq"])
                for item in inventory.document.items
            ):
                _refuse()
            previous = value["first_seq"]
            hosts.append(value["host"])
        return tuple(hosts)
    except Exception:
        _refuse()


@dataclass(frozen=True, init=False)
class QualifiedNetworkDenials:
    _basis: dict
    _hosts: tuple[str, ...]
    _record: object
    _guard: object
    _check: object
    _issuer: object

    @property
    def basis(self):
        return deepcopy(self._basis)

    @property
    def hosts(self):
        return self._hosts

    @property
    def record(self):
        return deepcopy(self._record)


def checked_denials(source, record):
    from app.telegram_bots.inbox import publication_identity

    if type(source) is not QualifiedNetworkDenials or source._issuer is not _ISSUER or publication_identity(source.record) != publication_identity(record) or source._check() is not True:
        _refuse()
    return source.basis


def from_human_input(source, record):
    """Reuse the original host-issued read and writer guard, without nested I/O."""
    from app.telegram_bots.human_input_source import check_qualified_request

    basis = check_qualified_request(source, record)
    if basis["projection"]["inventory"]["document"].get("version") != 2:
        return None
    frozen = freeze_denials(record, {**basis, "version": 1})
    check_qualified_request(source, record)
    return _issued(record, frozen, source._guard, source._check)


def _issued(record, basis, guard, check):
    result = object.__new__(QualifiedNetworkDenials)
    for key, value in {"_basis": deepcopy(basis), "_hosts": notice_hosts(record, basis), "_record": deepcopy(record), "_guard": guard, "_check": check, "_issuer": _ISSUER}.items():
        object.__setattr__(result, key, value)
    return result


class NetworkDenialSource:
    def __init__(self, *, http_owner, capture_attempt):
        self.http_owner, self.capture_attempt = http_owner, capture_attempt

    async def read(self, record, link, *, output, guard=None):
        return await await_drained(self._read(record, link, output=output, guard=guard))

    async def _read(self, record, link, *, output, guard):
        from app.channels.execution_client import ChannelExecutionFacade
        from app.telegram_bots.inbox import publication_identity
        from app.telegram_bots.links import prepare_publication_guard
        from app.telegram_bots.publication import channel_execution_scope

        if record.scope.agent_kind != "instance":
            return None
        record, link, output = deepcopy(record), deepcopy(link), deepcopy(output)
        if (
            output.output.run_status != "success"
            or output.output.state not in {"complete", "unavailable"}
            or output.output.state == "unavailable"
            and output.output.reason not in {"unsupported_output", "human_input_required"}
            or output.inventory.state != "matched"
            or output.inventory.document.coverage != "complete"
        ):
            return None
        if not any(item.kind == "structured" and item.carrier == "message_artifact" for item in output.inventory.document.items):
            return None
        if guard is not None:
            await prepare_publication_guard(guard)
        attempt = await self.capture_attempt(link, thread_id=record.scope.thread_id, endpoint_id=record.scope.endpoint_id)

        guard = guard or attempt.read_guard

        class Admission:
            def current(self, operation):
                return attempt.admission.current(operation) is True

            async def authorize(self, operation):
                if not self.current(operation):
                    _refuse()
                await attempt.admission.authorize(operation)
                await prepare_publication_guard(guard)
                if not self.current(operation):
                    _refuse()

        admission = Admission()
        facade = ChannelExecutionFacade(self.http_owner, scope=channel_execution_scope(record), admission=admission, capture_output=True, capture_inventory=True, inventory_document_version=2)
        before = await facade.get_run_output(record.run_id)
        if before != output:
            _refuse()
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
        if await facade.get_run_output(record.run_id) != output:
            _refuse()
        await admission.authorize("source_read")
        basis = {"version": 1, "publication": publication_identity(record), "projection": output.model_dump(mode="json"), "messages": rows}
        basis = await await_drained(asyncio.to_thread(freeze_denials, record, basis))
        if admission.current("source_read") is not True:
            _refuse()

        class SourceGuard:
            captured_link = attempt.read_guard.captured_link

            async def prepare(self):
                await admission.authorize("source_read")

            async def check(self, writer):
                if admission.current("source_read") is not True:
                    _refuse()
                await attempt.read_guard.check(writer)
                result = await guard.check(writer)
                if admission.current("source_read") is not True:
                    _refuse()
                return result

        return _issued(record, basis, SourceGuard(), lambda: admission.current("source_read"))
