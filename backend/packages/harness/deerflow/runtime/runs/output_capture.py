"""Run-local evidence from an owned graph invocation and its canonical journal writes.

This observer grants no authority to tags, stream frames or checkpoint answers.
Only the worker registers the actual graph UUID. Provider objects are inspected
synchronously and reduced to immutable facts before callbacks return.
"""

from __future__ import annotations

from collections.abc import Mapping
from copy import deepcopy
from dataclasses import dataclass
from uuid import UUID

from langchain_core.messages import BaseMessage, HumanMessage

from deerflow.runtime.runs.output import OutputEvidenceError, TextOutputReceipt, UnavailableOutputReceipt, canonical_text_digest
from deerflow.runtime.runs.output_source import OutputMessageEvidence, audit_output_source_event, inspect_output_message
from deerflow.runtime.runs.schemas import RunStatus
from deerflow.utils.messages import restore_original_human_message, strip_injected_user_message_id_suffix


def _dump(message):
    if isinstance(message, BaseMessage):
        return deepcopy(message.model_dump())
    if isinstance(message, Mapping):
        return deepcopy(dict(message))
    raise OutputEvidenceError()


def _excluded(tags):
    return any(isinstance(tag, str) and tag.startswith(("subagent:", "middleware:")) for tag in tags or ())


@dataclass
class _Response:
    facts: OutputMessageEvidence
    seq: int | None = None


class OutputCaptureObserver:
    """Capture-only, owning-loop observer; uncertainty is sticky until disposal."""

    def __init__(self, *, owner_user_id: str, thread_id: str, run_id: str):
        self.scope = {"owner_user_id": owner_user_id, "thread_id": thread_id, "run_id": run_id}
        self.uncertain = False
        self._epochs = 0
        self._root: str | None = None
        self._started = False
        self._completed = False
        self._parents: dict[str, str | None] = {}
        self._model_nodes: set[str] = set()
        self._excluded: set[str] = set()
        self._model_starts: set[str] = set()
        self._responses: dict[str, list[_Response]] = {}
        # Strong event references prevent Python identity reuse until the actual
        # write result has been paired. They contain detached journal primitives.
        self._events: dict[int, tuple[dict, _Response]] = {}
        self._history: set[str] = set()
        self._input: HumanMessage | None = None
        self._input_dump: dict | None = None
        self._input_reason: str | None = None
        self._input_bound = False
        self.input_message_id: str | None = None
        self._final: OutputMessageEvidence | None = None

    def begin_epoch(self, root: UUID, input_payload: object, *, pre_existing_ids: set[str]) -> None:
        self._epochs += 1
        self._root = str(root)
        self._history.update(pre_existing_ids)
        if self._epochs != 1:
            return
        try:
            if not isinstance(input_payload, Mapping):
                self._input_reason = "unsupported_input"
                return
            messages = input_payload.get("messages", [])
            if not isinstance(messages, (list, tuple)):
                self._input_reason = "unsupported_input"
                return
            self._remember_history(input_payload)
            if any(not isinstance(message, (BaseMessage, Mapping)) for message in messages):
                self._input_reason = "unsupported_input"
                return
            humans = [m for m in messages if isinstance(m, HumanMessage) or isinstance(m, Mapping) and m.get("type", m.get("role")) in {"human", "user"}]
            if not humans:
                self._input_bound = True  # Generic autonomous invocation.
                return
            if len(humans) != 1 or not isinstance(humans[0], HumanMessage):
                self._input_reason = "unsupported_input"
                return
            self._input = humans[0]
            self._input_dump = _dump(restore_original_human_message(self._input))
        except Exception:
            self.uncertain = True

    def _remember_history(self, state) -> None:
        messages = state.get("messages") if isinstance(state, Mapping) else None
        if not isinstance(messages, (list, tuple)):
            return
        for message in messages:
            kind = message.type if isinstance(message, BaseMessage) else message.get("type", message.get("role")) if isinstance(message, Mapping) else None
            if type(kind) is str and kind in {"ai", "assistant", "AIMessageChunk"}:
                identity = message.id if isinstance(message, BaseMessage) else message.get("id")
                if type(identity) is str and identity:
                    self._history.add(identity)

    def chain_start(self, *, run_id, parent_run_id, tags, metadata, name, inputs) -> None:
        if self._epochs != 1:
            return
        rid = str(run_id)
        parent = str(parent_run_id) if parent_run_id is not None else None
        if rid == self._root:
            if self._started or parent is not None:
                self.uncertain = True
            self._started = True
        if rid in self._parents:
            self.uncertain = True
        self._parents[rid] = parent
        if parent == self._root and name == "model" and isinstance(metadata, Mapping) and metadata.get("langgraph_node") == "model":
            self._model_nodes.add(rid)
            # The SDK has materialized reducer IDs by actual node admission.
            # Retain only those identities, including formerly idless history.
            self._remember_history(inputs)
        if _excluded(tags):
            self._excluded.add(rid)

    def model_start(self, *, run_id, parent_run_id, tags, messages, metadata) -> None:
        if self._epochs != 1:
            return
        try:
            rid, parent = str(run_id), str(parent_run_id) if parent_run_id is not None else None
            if rid in self._parents:
                self._model_starts.discard(rid)
                raise OutputEvidenceError()
            self._parents[rid] = parent
            if _excluded(tags):
                self._excluded.add(rid)
            # The initial contract supports a real model directly inside a
            # direct node of this graph. Nested graphs and auxiliary chains do
            # not become lead output merely by copying tags and message IDs.
            if (
                not self._started
                or parent not in self._model_nodes
                or self._parents.get(parent) != self._root
                or not isinstance(metadata, Mapping)
                or metadata.get("langgraph_node") != "model"
                or any(key in self._excluded for key in (rid, parent, self._root))
            ):
                return
            self._model_starts.add(rid)
            if self._input is None or self._input_bound or self._input_reason:
                return
            identity = strip_injected_user_message_id_suffix(self._input.id)
            if type(identity) is not str or not identity:
                return
            expected = {**self._input_dump, "id": identity}
            initial = inspect_output_message(expected)
            if initial.unavailable_reason is not None:
                self._input_reason = "unsupported_input"
                return
            matches = []
            for batch in messages:
                for message in batch:
                    if not isinstance(message, HumanMessage):
                        continue
                    restored = _dump(restore_original_human_message(message))
                    facts = inspect_output_message(restored)
                    if facts.message_id == identity:
                        if facts != initial or restored.get("additional_kwargs", {}).get("channel_turn_id") != expected.get("additional_kwargs", {}).get("channel_turn_id"):
                            self.uncertain = True
                            return
                        matches.append(facts)
            if len(matches) != 1:
                self.uncertain = True
                return
            self.input_message_id = identity
            self._input_bound = True
            self._input = None
            self._input_dump = None
        except Exception:
            self.uncertain = True

    def response(self, *, run_id, events: list[dict]) -> None:
        rid = str(run_id)
        if self._epochs != 1 or rid in self._responses:
            return
        try:
            observed = []
            for event in events:
                facts = inspect_output_message(event["content"])
                item = _Response(facts)
                observed.append(item)
                if rid in self._model_starts and event["metadata"].get("caller") == "lead_agent":
                    self._events[id(event)] = (event, item)
            self._responses[rid] = observed if rid in self._model_starts else []
        except Exception:
            self.uncertain = True

    def chain_end(self, outputs: object, *, run_id, parent_run_id) -> None:
        if self._epochs != 1 or str(run_id) != self._root:
            return
        try:
            if not self._started or parent_run_id is not None or self._completed:
                raise OutputEvidenceError()
            self._completed = True
            messages = outputs.get("messages") if isinstance(outputs, Mapping) else None
            if not isinstance(messages, (list, tuple)) or not messages:
                self._final = None
                return
            # Do not skip trailing tool/hidden/control messages to find prose.
            self._final = inspect_output_message(_dump(messages[-1]))
        except Exception:
            self.uncertain = True

    def persisted(self, batch: list[dict], rows: object) -> None:
        try:
            if not isinstance(rows, list) or len(rows) != len(batch):
                raise OutputEvidenceError()
            for event, row in zip(batch, rows, strict=True):
                saved = self._events.pop(id(event), None)
                if saved is None:
                    continue
                if saved[0] is not event:
                    raise OutputEvidenceError()
                item = saved[1]
                facts = item.facts
                if facts.role != "assistant" or facts.unavailable_reason is not None:
                    continue
                receipt = TextOutputReceipt(version=1, kind="text", **self.scope, input_message_id=self.input_message_id, assistant_message_id=facts.message_id, event_seq=row.get("seq"), text_sha256=canonical_text_digest(facts.text))
                if audit_output_source_event(row, receipt=receipt).state != "matched":
                    raise OutputEvidenceError()
                item.seq = receipt.event_seq
        except Exception:
            self.uncertain = True
            raise OutputEvidenceError() from None

    def receipt(self, *, status: RunStatus, cancelled: bool):
        if self.uncertain:
            raise OutputEvidenceError()
        reason = "cancelled" if cancelled else "failed" if status in {RunStatus.error, RunStatus.timeout} else None
        if reason is None:
            if self._epochs != 1:
                reason = "ambiguous_source"
            elif not self._completed:
                raise OutputEvidenceError()
            elif self._input_reason:
                reason = self._input_reason
            elif self._final is None:
                reason = "unsupported_output"
            elif self._final.unavailable_reason is not None:
                reason = self._final.unavailable_reason
            elif self._final.role != "assistant":
                reason = "unsupported_output"
            elif self._final.message_id in self._history:
                reason = "ambiguous_source"
            else:
                matches = [item for group in self._responses.values() for item in group if item.facts == self._final]
                if len(matches) != 1:
                    reason = "ambiguous_source"
                elif not self._input_bound:
                    reason = "missing_message_identity"
                elif matches[0].seq is None:
                    raise OutputEvidenceError()
                else:
                    return TextOutputReceipt(
                        version=1, kind="text", **self.scope, input_message_id=self.input_message_id, assistant_message_id=self._final.message_id, event_seq=matches[0].seq, text_sha256=canonical_text_digest(self._final.text)
                    )
        return UnavailableOutputReceipt(version=1, kind="unavailable", **self.scope, input_message_id=self.input_message_id, reason=reason)

    def detach(self) -> None:
        self._events.clear()
        self._responses.clear()
        self._parents.clear()
        self._model_nodes.clear()
        self._model_starts.clear()
        self._excluded.clear()
        self._history.clear()
        self._input = None
        self._input_dump = None
