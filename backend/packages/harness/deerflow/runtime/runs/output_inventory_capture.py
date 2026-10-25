"""Owned first-party terminal presentation facts and a single audited publication.

This observes references and opaque presentation fragments. It grants no file
access, action permission, immutable bytes or arbitrary-effect containment.
"""

from __future__ import annotations

from collections.abc import Mapping
from datetime import UTC, datetime

from langchain_core.messages import BaseMessage, ToolMessage
from langgraph.types import Command

from deerflow.utils.file_io import await_drained
from deerflow.workspace_changes import get_changed_output_paths

from .capture_io import CaptureIOClosed, CaptureIOUnsupported, run_capture_io
from .output import OutputEvidenceError, validate_receipt
from .output_inventory import (
    INVENTORY_EVENT_CATEGORY,
    INVENTORY_EVENT_TYPE,
    MAX_INVENTORY_DESCRIPTORS,
    MAX_INVENTORY_DOCUMENT_BYTES,
    InventoryLimitExceeded,
    build_event_inventory_evidence,
    inventory_document_to_dict,
    output_receipt_digest,
    presentation_fragment_digest,
    validate_inventory_document,
)
from .output_inventory_source import audit_inventory_source_event, read_durable_inventory_source

_ROOT = "/mnt/user-data/outputs"


def _field(value, key, default=None):
    return value.get(key, default) if isinstance(value, Mapping) else getattr(value, key, default)


def _paths(value):
    if not isinstance(value, (list, tuple)) or any(type(path) is not str or not path.startswith(_ROOT + "/") or any(part in {"", ".", ".."} for part in path[len(_ROOT) + 1 :].split("/")) for path in value):
        raise OutputEvidenceError()
    return tuple(dict.fromkeys(value))


def inventory_scan(before, after):
    """Join positive facts from the same two existing finite output scans."""
    snapshots = (before, after)
    scope = None
    roots = []
    for snapshot in snapshots:
        observations = getattr(snapshot, "root_observations", ())
        roots.append(observations[0].state if len(observations) == 1 and observations[0].name == "outputs" and observations[0].virtual_prefix == _ROOT else None)
    excluded = getattr(before, "scan_excluded_dir_names", None)
    if excluded is not None and excluded == getattr(after, "scan_excluded_dir_names", None):
        scope = dict(version=1, kind="managed_outputs_regular_changes", virtual_root=_ROOT, excluded_dir_names=list(excluded))
    if scope is not None and roots == ["opened", "opened"] and all(snapshot is not None and not snapshot.truncated and snapshot.text_cache_dir is None for snapshot in snapshots):
        return dict(state="complete", source="managed_outputs", scope=scope, before_root="opened", after_root="opened", produced_paths=get_changed_output_paths(before, after))
    reason = "truncated" if any(getattr(snapshot, "truncated", False) for snapshot in snapshots) else "unavailable"
    return dict(state="incomplete", source="managed_outputs", reason=reason, scope=scope, before_root=roots[0], after_root=roots[1])


class OutputInventoryCapture:
    """Owning-loop bounded observer; no I/O, provider work or caller authority."""

    def __init__(self):
        self._profile = None
        self._root = None
        self._epochs = 0
        self._parents = {}
        self._names = {}
        self._excluded = set()
        self._baseline = set()
        self._baseline_seen = False
        self._calls = set()
        self._files = []
        self._fragments = []
        self._rich = {}
        self._current_messages = set()
        self._terminal = None
        self._complete = False
        self._reason = None
        self._closed = False
        self.final_scan = None
        self._baseline_fields = {}
        self._current_fields = {}
        self._terminal_fields = []
        self._started = False

    def observe_scan(self, snapshot):
        if not self._closed:
            self.final_scan = snapshot

    def invalidate_scan(self):
        """Cleanup changed the terminal barrier; old scans are observations only."""
        self.final_scan = None
        self._incomplete("execution_stopped")

    def qualify(self, profile, graph) -> bool:
        if self._closed:
            return False
        from deerflow.agents.output_inventory_profile import TerminalPresentationProfile

        if type(profile) is TerminalPresentationProfile and profile.accepts(graph) is True:
            self._profile = profile
            return True
        return False

    def _incomplete(self, reason="carrier_incomplete"):
        if self._reason is None or reason == "enumeration_overflow":
            self._reason = reason

    def _presentation_value(self, key, value):
        if value is None:
            return False
        if key == "structured_response":
            return True
        expected = Mapping if key == "goal" else (list, tuple)
        if not isinstance(value, expected):
            self._incomplete()
            return False
        return bool(value)

    def _valid_message(self, message):
        if not isinstance(message, (BaseMessage, Mapping)):
            self._incomplete()
            return False
        kind = _field(message, "type")
        if type(kind) is not str or kind not in {"human", "HumanMessageChunk", "ai", "AIMessageChunk", "tool", "ToolMessageChunk", "system", "SystemMessageChunk"}:
            self._incomplete()
            return False
        identity = _field(message, "id")
        additional = _field(message, "additional_kwargs", {})
        if identity is not None and (type(identity) is not str or not identity) or not isinstance(additional, Mapping):
            self._incomplete()
            return False
        for key in ("hide_from_ui", "dynamic_context_reminder", "deerflow_error_fallback"):
            if key in additional and type(additional[key]) is not bool:
                self._incomplete()
        for key in ("deerflow_content_kind", "presented_by"):
            if key in additional and type(additional[key]) is not str:
                self._incomplete()
        if "presented_files" in additional:
            try:
                _paths(additional["presented_files"])
            except OutputEvidenceError:
                self._incomplete()
        return True

    def bind_baseline(self, values):
        """Actual selected materialized checkpoint, before any graph producer."""
        if self._closed or not isinstance(values, Mapping) or self._profile is None:
            raise OutputEvidenceError() from None
        self._remember_baseline(values.get("messages", ()))
        for key in self._profile.presentation_state_keys:
            value = values.get(key)
            self._presentation_value(key, value)
            self._baseline_fields[key] = self._fragment(value) if value is not None else None

    def begin_epoch(self, root, input_payload, *, pre_existing_ids):
        if self._closed:
            return
        self._epochs += 1
        self._root = str(root)
        self._baseline.update(pre_existing_ids)
        messages = input_payload.get("messages", ()) if isinstance(input_payload, Mapping) else ()
        self._remember_baseline(messages)
        if self._profile is not None and isinstance(input_payload, Mapping):
            for key in self._profile.presentation_state_keys:
                if key in input_payload:
                    self._baseline_fields[key] = self._fragment(input_payload[key])

    def _remember_baseline(self, messages):
        if not isinstance(messages, (list, tuple)):
            self._incomplete()
            return
        if len(messages) > 8192:
            self._incomplete("enumeration_overflow")
        for message in messages[:8192]:
            identity = _field(message, "id")
            if type(identity) is str and identity:
                self._baseline.add(identity)
            elif _field(message, "artifact"):
                self._incomplete("terminal_membership_unknown")

    def _lead(self, rid):
        seen = set()
        while rid is not None and rid not in seen:
            if rid in self._excluded:
                return False
            if rid == self._root:
                return True
            seen.add(rid)
            rid = self._parents.get(rid)
        return False

    def chain_start(self, *, run_id, parent_run_id, tags, metadata, name, inputs):
        if self._closed:
            return
        rid = str(run_id)
        parent = str(parent_run_id) if parent_run_id is not None else None
        if len(self._parents) >= 8192:
            self._incomplete("enumeration_overflow")
            return
        if rid == self._root and parent is None:
            self._started = True
        self._parents[rid] = parent
        self._names[rid] = name
        if any(isinstance(tag, str) and tag.startswith(("subagent:", "middleware:")) for tag in tags or ()):
            self._excluded.add(rid)
        if parent == self._root and name == "model" and not self._baseline_seen and isinstance(inputs, Mapping):
            self._remember_baseline(inputs.get("messages", ()))
            self._baseline_seen = True

    def tool_start(self, *, run_id, parent_run_id, tags):
        self.chain_start(run_id=run_id, parent_run_id=parent_run_id, tags=tags, metadata=None, name="tool", inputs=None)

    def _fragment(self, fragment):
        try:
            return presentation_fragment_digest(fragment)
        except InventoryLimitExceeded:
            self._incomplete("enumeration_overflow")
        except OutputEvidenceError:
            self._incomplete()
        return None

    def _content(self, message):
        content = _field(message, "content")
        if type(content) is str:
            return ()
        if not isinstance(content, (list, tuple)):
            self._incomplete()
            return ()
        fragments = []
        if len(content) > 8192:
            self._incomplete("enumeration_overflow")
        for block in content[:8192]:
            if type(block) is str:
                continue
            if isinstance(block, Mapping):
                kind = block.get("type")
                if kind == "text" and set(block) == {"type", "text"} and type(block["text"]) is str:
                    continue
                # Auxiliary reasoning never enters presentation hashes. An
                # unqualified mixed-content shape still refuses completeness.
                if type(kind) is str and kind in {"tool_use", "tool_call"}:
                    argument_key = "input" if kind == "tool_use" else "args"
                    calls = _field(message, "tool_calls", ())
                    index = block.get("index", 0)
                    if (
                        _field(message, "type") in {"ai", "AIMessageChunk"}
                        and set(block) <= {"type", "id", "name", argument_key, "index"}
                        and type(block.get("id")) is str
                        and bool(block["id"])
                        and type(block.get("name")) is str
                        and bool(block["name"])
                        and isinstance(block.get(argument_key), Mapping)
                        and type(index) is int
                        and index >= 0
                        and isinstance(calls, (list, tuple))
                        and any(isinstance(call, Mapping) and call.get("id") == block["id"] and call.get("name") == block["name"] for call in calls)
                    ):
                        continue
                    self._incomplete()
                    continue
                if type(kind) is str and kind in {"reasoning", "thinking", "redacted_thinking"}:
                    self._incomplete()
                    continue
                if type(kind) is not str or kind not in {"text", "file", "image", "image_url", "audio", "video"}:
                    self._incomplete()
            else:
                self._incomplete()
            digest = self._fragment(block)
            if digest is not None:
                fragments.append(digest)
        return tuple(dict.fromkeys(fragments))

    def _remember_content(self, message, witness):
        identity = _field(message, "id")
        if type(identity) is str:
            self._current_messages.add(identity)
        if _field(message, "additional_kwargs", {}).get("hide_from_ui") is True:
            return
        for digest in self._content(message):
            key = (identity, _field(message, "tool_call_id"), digest)
            if key in self._rich:
                continue
            if len(self._rich) >= MAX_INVENTORY_DESCRIPTORS:
                self._incomplete("enumeration_overflow")
                break
            self._rich[key] = witness

    def model_response(self, message, *, run_id, parent_run_id):
        """Only the text observer's genuine direct model callback invokes this."""
        parent = str(parent_run_id)
        if self._closed or self._parents.get(parent) != self._root or self._names.get(parent) != "model" or not self._lead(parent):
            return
        if self._valid_message(message):
            self._remember_content(message, dict(kind="callback", callback_run_id=str(run_id)))

    def _remember_files(self, value, surface, witness):
        try:
            paths = _paths(value)
        except OutputEvidenceError:
            self._incomplete()
            return
        for path in paths:
            item = (path, surface, witness)
            if item in self._files:
                continue
            # A distinct callback witness alone exceeds64 canonical JSON bytes.
            # This derived ceiling is already larger than the whole wire bound.
            if len(self._files) >= MAX_INVENTORY_DOCUMENT_BYTES // 64:
                self._incomplete("enumeration_overflow")
                break
            self._files.append(item)

    def tool_end(self, output, *, run_id, parent_run_id):
        if self._closed or not self._lead(str(run_id)):
            return
        witness = dict(kind="callback", callback_run_id=str(run_id))
        messages = [output]
        if isinstance(output, Command):
            update = output.update
            if not isinstance(update, Mapping):
                self._incomplete()
                return
            if "artifacts" in update:
                self._remember_files(update["artifacts"], "artifact_reference", witness)
            for key, value in update.items():
                if self._profile is None or key not in self._profile.state_keys:
                    self._incomplete()
                elif key in self._profile.presentation_state_keys:
                    if self._presentation_value(key, value):
                        digest = self._fragment(value)
                        if digest is not None:
                            self._current_fields[key] = (digest, witness)
            messages = update.get("messages", ())
            if not isinstance(messages, (list, tuple)):
                self._incomplete()
                return
        elif not isinstance(output, ToolMessage):
            self._incomplete()
            return
        for message in messages[:8192]:
            if not self._valid_message(message):
                continue
            self._remember_content(message, witness)
            artifact = _field(message, "artifact")
            if artifact is not None:
                if not isinstance(artifact, Mapping):
                    self._incomplete()
                elif artifact:
                    digest = self._fragment(artifact)
                    if digest is not None and len(self._fragments) < MAX_INVENTORY_DESCRIPTORS:
                        self._fragments.append((_field(message, "id"), _field(message, "tool_call_id"), digest, witness))
                    elif digest is not None:
                        self._incomplete("enumeration_overflow")
            additional = _field(message, "additional_kwargs", {})
            if not isinstance(additional, Mapping):
                self._incomplete()
            elif "presented_files" in additional:
                self._remember_files(additional["presented_files"], "presentation_reference", witness)
        if len(messages) > 8192:
            self._incomplete("enumeration_overflow")

    def chain_end(self, outputs, *, run_id, parent_run_id):
        if self._closed:
            return
        rid = str(run_id)
        if self._lead(rid) and self._parents.get(rid) == self._root and self._names.get(rid) == "model":
            if isinstance(outputs, Mapping):
                updates = (outputs,)
            elif isinstance(outputs, (list, tuple)) and outputs and all(isinstance(command, Command) for command in outputs[:8192]):
                if len(outputs) > 8192:
                    self._incomplete("enumeration_overflow")
                if any(command.graph or command.resume or command.goto for command in outputs[:8192]):
                    self._incomplete()
                    return
                updates = tuple(command.update for command in outputs[:8192])
            else:
                self._incomplete()
                return
            for ordinal, update in enumerate(updates):
                if update is None and ordinal > 0:
                    continue
                if not isinstance(update, Mapping) or ordinal == 0 and "messages" not in update:
                    self._incomplete()
                    continue
                witness = dict(kind="callback", callback_run_id=rid)
                for key, value in update.items():
                    if self._profile is None or key not in self._profile.state_keys:
                        self._incomplete()
                    elif key == "artifacts":
                        self._remember_files(value, "artifact_reference", witness)
                    elif key in self._profile.presentation_state_keys:
                        if self._presentation_value(key, value):
                            digest = self._fragment(value)
                            if digest is not None:
                                self._current_fields[key] = (digest, witness)
                messages = update.get("messages", ())
                if not isinstance(messages, (list, tuple)):
                    self._incomplete()
                    continue
                if len(messages) > 8192:
                    self._incomplete("enumeration_overflow")
                for message in messages[:8192]:
                    if not self._valid_message(message):
                        continue
                    self._remember_content(message, witness)
                    calls = _field(message, "tool_calls", ())
                    if not isinstance(calls, (list, tuple)):
                        self._incomplete()
                        continue
                    if len(calls) > 8192:
                        self._incomplete("enumeration_overflow")
                    for call in calls[:8192]:
                        if isinstance(call, Mapping) and type(call.get("id")) is str:
                            self._calls.add(call["id"])
                        else:
                            self._incomplete()
        if rid != self._root or parent_run_id is not None:
            return
        if self._complete or not isinstance(outputs, Mapping):
            self._incomplete()
            return
        self._complete = True
        if not self._started:
            self._incomplete("terminal_membership_unknown")
        if self._profile is not None:
            for key, value in outputs.items():
                if key not in self._profile.state_keys:
                    self._incomplete()
                if key in self._profile.presentation_state_keys and self._presentation_value(key, value):
                    digest = self._fragment(value)
                    if digest is not None:
                        self._terminal_fields.append((key, digest))
        messages = outputs.get("messages", ())
        if not isinstance(messages, (list, tuple)):
            self._incomplete()
            messages = ()
        # Freeze only presentation fragments; auxiliary model metadata is inert.
        terminal = []
        for message in messages[:8192]:
            identity = _field(message, "id")
            if type(identity) is str and identity in self._baseline and identity not in self._current_messages:
                continue
            if not self._valid_message(message):
                continue
            artifact = _field(message, "artifact")
            additional = _field(message, "additional_kwargs", {})
            if artifact is not None and not isinstance(artifact, Mapping) or not isinstance(additional, Mapping):
                self._incomplete()
                continue
            terminal.append(
                dict(
                    id=_field(message, "id"),
                    type=_field(message, "type"),
                    tool_call_id=_field(message, "tool_call_id"),
                    artifact_digest=self._fragment(artifact) if artifact else None,
                    presented_files=tuple(additional["presented_files"]) if isinstance(additional.get("presented_files"), (list, tuple)) else additional.get("presented_files"),
                    presented_by=additional.get("presented_by"),
                    has_presentation="presented_files" in additional,
                    hidden=additional.get("hide_from_ui") is True,
                    content_digests=self._content(message) if additional.get("hide_from_ui") is not True else (),
                )
            )
        if len(messages) > 8192:
            self._incomplete("enumeration_overflow")
        try:
            artifacts = _paths(outputs.get("artifacts", ()))
        except OutputEvidenceError:
            artifacts = ()
            self._incomplete()
        self._terminal = (tuple(terminal), artifacts)

    def document(self, output_receipt, *, status, before, after, runtime_presented_files):
        receipt = validate_receipt(output_receipt)
        common = dict(
            version=1,
            profile="terminal-presentation-v1" if self._profile is not None else None,
            owner_user_id=receipt.owner_user_id,
            thread_id=receipt.thread_id,
            run_id=receipt.run_id,
            input_message_id=receipt.input_message_id,
            output_receipt_sha256=output_receipt_digest(receipt),
            root_epoch=self._root,
        )
        if self._profile is None or self._epochs != 1:
            return validate_inventory_document({**common, "coverage": "unsupported", "reason": "unsupported_profile" if self._profile is None else "multiple_epochs", "observed_items": [], "scan": None})
        scan = inventory_scan(before, after)
        files = {}
        structured = []

        def add_file(path, surface, witness):
            item = files.setdefault(path, dict(kind="file", path=path, surfaces=[], witnesses=[]))
            if surface not in item["surfaces"]:
                item["surfaces"].append(surface)
            if witness not in item["witnesses"]:
                item["witnesses"].append(witness)

        for path, surface, witness in self._files:
            if self._terminal is None or path in self._terminal[1]:
                add_file(path, surface, witness)
            else:
                self._incomplete("terminal_membership_unknown")
        if self._terminal is None:
            structured.extend(dict(kind="structured", carrier="message_artifact", value_sha256=digest, witness=witness) for _, _, digest, witness in self._fragments)
            structured.extend(dict(kind="structured", carrier="terminal_field", value_sha256=key[2], witness=witness) for key, witness in self._rich.items())
        else:
            try:
                runtime_paths = _paths(runtime_presented_files) if runtime_presented_files is not None else ()
            except OutputEvidenceError:
                runtime_paths = ()
                self._incomplete()
            if self._profile.has_runtime_delivery is True:
                for path in runtime_paths:
                    if path in self._terminal[1]:
                        # The real host registry is also a current emission
                        # when SDK message replacement left no final AI to tag.
                        add_file(path, "presentation_reference", dict(kind="root_snapshot", root_epoch=self._root, message_id=None))
                    else:
                        self._incomplete("terminal_membership_unknown")
            for message in self._terminal[0]:
                identity = message["id"]
                for digest in message["content_digests"]:
                    candidates = [witness for (mid, call, value), witness in self._rich.items() if value == digest and (mid == identity and mid is not None or mid is None and call == message["tool_call_id"] and call is not None)]
                    witness = candidates[0] if candidates else dict(kind="root_snapshot", root_epoch=self._root, message_id=identity)
                    structured.append(dict(kind="structured", carrier="terminal_field", value_sha256=digest, witness=witness))
                current = identity not in self._baseline and message["tool_call_id"] in self._calls
                matches = [
                    (mid, call, digest, witness)
                    for mid, call, digest, witness in self._fragments
                    if digest == message["artifact_digest"] and (mid == identity and mid is not None or mid is None and call == message["tool_call_id"] and call is not None and identity not in self._baseline)
                ]
                if message["artifact_digest"] is not None and not message["hidden"] and (current or matches):
                    witness = matches[0][3] if len(matches) == 1 else dict(kind="root_snapshot", root_epoch=self._root, message_id=identity)
                    structured.append(dict(kind="structured", carrier="message_artifact", value_sha256=message["artifact_digest"], witness=witness))
                elif message["artifact_digest"] is not None and identity not in self._baseline and not message["hidden"]:
                    self._incomplete("terminal_membership_unknown")
                    structured.append(dict(kind="structured", carrier="terminal_field", value_sha256=message["artifact_digest"], witness=dict(kind="root_snapshot", root_epoch=self._root, message_id=identity)))
                current_runtime = self._profile.has_runtime_delivery is True and message["presented_by"] == "runtime" and bool(runtime_paths)
                if message["has_presentation"] and (identity not in self._baseline or current_runtime):
                    try:
                        paths = _paths(message["presented_files"])
                    except OutputEvidenceError:
                        self._incomplete()
                        continue
                    if message["type"] in {"ai", "AIMessageChunk"}:
                        if self._profile.has_runtime_delivery is True and message["presented_by"] == "runtime" and all(path in runtime_paths for path in paths):
                            for path in paths:
                                add_file(path, "presentation_reference", dict(kind="root_snapshot", root_epoch=self._root, message_id=identity))
                        else:
                            self._incomplete()
                            digest = self._fragment({"presented_files": message["presented_files"], "presented_by": message["presented_by"]})
                            if digest is not None:
                                structured.append(dict(kind="structured", carrier="terminal_field", value_sha256=digest, witness=dict(kind="root_snapshot", root_epoch=self._root, message_id=identity)))
            # A current opaque tool fragment disappearing cannot prove no offer.
            retained = {message["artifact_digest"] for message in self._terminal[0]}
            if any(digest not in retained for _, _, digest, _ in self._fragments):
                self._incomplete("terminal_membership_unknown")
            retained_content = {digest for message in self._terminal[0] for digest in message["content_digests"]}
            if any(key[2] not in retained_content for key in self._rich):
                self._incomplete("terminal_membership_unknown")
        for key, digest in self._terminal_fields:
            current = self._current_fields.get(key)
            if digest != self._baseline_fields.get(key) or current is not None and current[0] == digest:
                witness = current[1] if current is not None and current[0] == digest else dict(kind="root_snapshot", root_epoch=self._root, message_id=None)
                structured.append(dict(kind="structured", carrier="terminal_field", value_sha256=digest, witness=witness))
        if scan["state"] == "complete":
            for path in scan["produced_paths"]:
                add_file(path, "output_change", dict(kind="output_scan"))
        items = list(files.values()) + structured
        reason = self._reason
        if reason is None and (not self._complete or str(status) != "success"):
            reason = "execution_stopped"
        if reason is None and scan["state"] != "complete":
            reason = "scan_incomplete"
        if len(items) > MAX_INVENTORY_DESCRIPTORS:
            reason = "enumeration_overflow"
            items = items[:MAX_INVENTORY_DESCRIPTORS]
        original_items = tuple(items)
        while True:
            data = {**common, "scan": scan, "coverage": "complete" if reason is None else "incomplete", "reason": reason, "items" if reason is None else "observed_items": items}
            try:
                return validate_inventory_document(data)
            except InventoryLimitExceeded:
                reason = "enumeration_overflow"
                if items:
                    items.pop()
                elif scan is not None:
                    # An oversized OPTIONAL full delta cannot be represented as
                    # a partial CompleteScan. Retain independently witnessed
                    # presentations and honestly omit scan-dependent features.
                    scan = None
                    for item in original_items:
                        if item["kind"] == "file":
                            surfaces = [value for value in item["surfaces"] if value != "output_change"]
                            witnesses = [value for value in item["witnesses"] if value["kind"] != "output_scan"]
                            if surfaces and witnesses:
                                items.append({**item, "surfaces": surfaces, "witnesses": witnesses})
                        elif item["witness"]["kind"] != "output_scan":
                            items.append(item)
                else:
                    raise

    def detach(self):
        self._closed = True
        self._profile = None
        self._parents.clear()
        self._names.clear()
        self._baseline.clear()
        self._calls.clear()
        self._files.clear()
        self._fragments.clear()
        self._rich.clear()
        self._current_messages.clear()
        self._terminal = None
        self.final_scan = None
        self._baseline_fields.clear()
        self._current_fields.clear()
        self._terminal_fields.clear()


async def persist_output_inventory(event_store, *, output_receipt, status, document):
    """One creating attempt; an unknown reply gets read-only exact recovery."""
    if getattr(event_store, "supports_durable_output_inventory", False) is not True:
        raise OutputEvidenceError()
    receipt = validate_receipt(output_receipt)
    document = validate_inventory_document(document)
    stamp = datetime.now(UTC).isoformat()
    metadata = {"content_is_json": True, "content_is_dict": True}
    expected = dict(thread_id=receipt.thread_id, run_id=receipt.run_id, event_type=INVENTORY_EVENT_TYPE, category=INVENTORY_EVENT_CATEGORY, content=inventory_document_to_dict(document), metadata=metadata, created_at=stamp)
    row = None
    try:
        result = await await_drained(run_capture_io(lambda: event_store.put_if_absent(**expected)))
        if not isinstance(result, tuple) or len(result) != 2 or type(result[1]) is not bool:
            raise OutputEvidenceError()
        row = result[0]
    except (CaptureIOClosed, CaptureIOUnsupported):
        raise
    except Exception:
        try:
            rows = await await_drained(run_capture_io(lambda: event_store.list_events(receipt.thread_id, receipt.run_id, event_types=[INVENTORY_EVENT_TYPE], limit=2, user_id=receipt.owner_user_id)))
        except (CaptureIOClosed, CaptureIOUnsupported):
            raise
        except Exception:
            raise OutputEvidenceError() from None
        if not isinstance(rows, list) or len(rows) != 1:
            raise OutputEvidenceError() from None
        row = rows[0]
    try:
        if not isinstance(row, Mapping) or row.get("user_id") != receipt.owner_user_id or any(row.get(key) != expected[key] for key in ("thread_id", "run_id", "event_type", "category")):
            raise OutputEvidenceError()
        if row.get("metadata") != metadata or any(type(value) is not bool for value in row["metadata"].values()) or datetime.fromisoformat(row["created_at"]) != datetime.fromisoformat(stamp):
            raise OutputEvidenceError()
        evidence = build_event_inventory_evidence(receipt, status=status, inventory_event_seq=row.get("seq"), document=document)
        if audit_inventory_source_event(row, evidence=evidence, output_receipt=receipt).state != "matched":
            raise OutputEvidenceError()
        if (await read_durable_inventory_source(event_store, evidence=evidence, output_receipt=receipt)).state != "matched":
            raise OutputEvidenceError()
        return evidence
    except (CaptureIOClosed, CaptureIOUnsupported):
        raise
    except Exception:
        raise OutputEvidenceError() from None
