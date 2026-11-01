"""Collector unit traces using a real host-issued profile, not SDK execution proof.

The actual first-party builder/compiled graph comes from B's offline fixture.
Callbacks and opened-root snapshots below are controlled unit facts: they do not
prove SDK invocation, physical scanning, journal persistence or terminal CAS.
"""

from dataclasses import dataclass
from hashlib import sha256

import pytest
from langchain_core.messages import AIMessage, ChatMessage, HumanMessage, ToolMessage
from langgraph.types import Command
from test_output_inventory_profile import assemble_offline

from deerflow.runtime.runs.output import TextOutputReceipt, UnavailableOutputReceipt
from deerflow.runtime.runs.output_inventory import content_items, inventory_document_digest, offered_pages, presentation_fragment_digest
from deerflow.runtime.runs.output_inventory_capture import OutputInventoryCapture
from deerflow.workspace_changes.types import FileSnapshot, WorkspaceRootObservation, WorkspaceSnapshot

ROOT = "3d9b53fb-c42c-4b80-a6a8-e43b98fe8351"
MODEL = "77c52cff-c7f0-4c98-8df2-597b806f9781"
TOOL = "ad8853fa-d499-4731-b56b-69e58afc3ac4"
PATH = "/mnt/user-data/outputs/existing.txt"
INPUT = HumanMessage(id="unit-input", content="unit input")


def snapshot(*, state="opened", excluded=(".git", "tool_output"), files=None, truncated=False):
    observations = () if state is None else (WorkspaceRootObservation("outputs", "/mnt/user-data/outputs", state),)
    return WorkspaceSnapshot(files=files or {}, truncated=truncated, root_observations=observations, scan_excluded_dir_names=excluded)


def receipt(*, status="success"):
    common = dict(version=1, owner_user_id="unit-owner", thread_id="unit-thread", run_id="unit-run", input_message_id=INPUT.id)
    if status != "success":
        return UnavailableOutputReceipt(kind="unavailable", reason="cancelled" if status == "interrupted" else "failed", **common)
    return TextOutputReceipt(kind="text", assistant_message_id="unit-answer", event_seq=3, text_sha256=sha256(b"answer").hexdigest(), **common)


def begin_capture(profile, graph, baseline):
    capture = OutputInventoryCapture()
    assert capture.qualify(profile, graph)
    capture.bind_baseline(baseline)
    baseline_ids = {message.id for message in baseline.get("messages", ()) if getattr(message, "id", None)}
    capture.begin_epoch(ROOT, {"messages": [INPUT]}, pre_existing_ids=baseline_ids)
    capture.chain_start(run_id=ROOT, parent_run_id=None, tags=(), metadata={}, name="unit-root", inputs={"messages": [INPUT]})
    return capture


@dataclass
class Trace:
    capture: OutputInventoryCapture
    before: WorkspaceSnapshot
    after: WorkspaceSnapshot
    profile: object
    graph: object

    def rebind_baseline(self, values):
        # Start a fresh collector in production order, under the exact same
        # actually issued profile; no producer precedes baseline capture.
        self.capture.detach()
        self.capture = begin_capture(self.profile, self.graph, values)

    def model_call(self, *, call_id="current-call"):
        self.capture.chain_start(run_id=MODEL, parent_run_id=ROOT, tags=(), metadata={}, name="model", inputs={"messages": [INPUT]})
        self.capture.chain_end({"messages": [AIMessage(id="unit-call", content="", tool_calls=[{"id": call_id, "name": "unit_tool", "args": {}}])]}, run_id=MODEL, parent_run_id=ROOT)

    def tool(self, output):
        self.capture.tool_start(run_id=TOOL, parent_run_id=ROOT, tags=())
        self.capture.tool_end(output, run_id=TOOL, parent_run_id=ROOT)

    def end(self, *, messages=None, artifacts=(), **fields):
        self.capture.chain_end({"messages": [INPUT, AIMessage(id="unit-answer", content="answer")] if messages is None else messages, "artifacts": artifacts, **fields}, run_id=ROOT, parent_run_id=None)

    def document(self, *, status="success", runtime_paths=()):
        return self.capture.document(receipt(status=status), status=status, before=self.before, after=self.after, runtime_presented_files=runtime_paths)


@pytest.fixture
def trace(monkeypatch):
    assembly, _built, _model = assemble_offline(monkeypatch)
    assert assembly.inventory_profile.accepts(assembly.graph)
    assert _model.invocations == 0, "Controlled callback traces must not masquerade as actual SDK invocation"
    capture = begin_capture(assembly.inventory_profile, assembly.graph, {})
    return Trace(capture, snapshot(), snapshot(), assembly.inventory_profile, assembly.graph)


def descriptors(document):
    return document.items if document.coverage == "complete" else document.observed_items


def test_qualified_opened_empty_trace_is_positive_empty(trace):
    trace.end()
    document = trace.document()
    assert document.coverage == "complete" and document.items == ()
    assert document.scan.before_root == document.scan.after_root == "opened"


@pytest.mark.parametrize("state,truncated", [("missing", False), ("unavailable", False), (None, False), ("opened", True)])
def test_missing_or_unobserved_or_partial_roots_are_not_empty(trace, state, truncated):
    trace.after = snapshot(state=state, truncated=truncated)
    trace.end()
    document = trace.document()
    assert document.coverage == "incomplete" and document.reason == "scan_incomplete"
    assert not hasattr(document, "items")


def test_actual_unit_delta_is_in_inventory_separately_from_presentations(trace):
    trace.after = snapshot(files={PATH: FileSnapshot(PATH, "outputs", 1, 1, "a" * 64)})
    trace.end()
    document = trace.document()
    assert document.coverage == "complete"
    item = document.items[0]
    assert item.path == PATH and item.surfaces == ("output_change",)
    assert item.witnesses[0].kind == "output_scan"


def test_current_raw_command_reoffers_unchanged_old_path(trace):
    trace.rebind_baseline({"messages": [], "artifacts": [PATH]})
    trace.model_call()
    trace.tool(Command(update={"artifacts": [PATH], "messages": []}))
    trace.end(artifacts=[PATH])
    document = trace.document()
    assert document.coverage == "complete" and len(document.items) == 1
    assert document.items[0].path == PATH and document.items[0].surfaces == ("artifact_reference",)
    assert document.scan.produced_paths == ()


def test_inherited_artifact_registry_alone_is_not_current_offer(trace):
    trace.rebind_baseline({"messages": [], "artifacts": [PATH], "tool_artifacts": {"old": {"path": PATH}}})
    trace.end(artifacts=[PATH], tool_artifacts={"old": {"path": PATH}})
    document = trace.document()
    assert document.coverage == "complete" and document.items == ()


@pytest.mark.parametrize("value", [False, 0, {}, []])
def test_nonnull_structured_response_cannot_disappear_when_falsey(trace, value):
    trace.end(structured_response=value)
    document = trace.document()
    assert document.coverage == "complete" and len(document.items) == 1
    assert document.items[0].carrier == "terminal_field" and document.items[0].value_sha256 == presentation_fragment_digest(value)


@pytest.mark.parametrize("key", ["goal", "todos", "background_tasks"])
@pytest.mark.parametrize("value", [False, 0])
def test_falsey_malformed_known_root_roles_are_incomplete(trace, key, value):
    trace.end(**{key: value})
    document = trace.document()
    assert document.coverage == "incomplete" and document.reason == "carrier_incomplete"


@pytest.mark.parametrize("value", [False, 0, None, [], "bad"])
def test_nonmapping_command_update_is_not_qualified(trace, value):
    trace.tool(Command(update=value))
    trace.end()
    assert trace.document().coverage == "incomplete"


@pytest.mark.parametrize("value", [False, 0, [], "bad"])
def test_malformed_artifact_even_falsey_stays_unknown(trace, value):
    trace.model_call()
    message = ToolMessage(id="unit-tool", content="unit result", tool_call_id="current-call", artifact=value)
    trace.tool(message)
    trace.end(messages=[INPUT, message, AIMessage(id="unit-answer", content="answer")])
    assert trace.document().coverage == "incomplete"


@pytest.mark.parametrize("additional", [{"hide_from_ui": 0}, {"dynamic_context_reminder": 0}, {"deerflow_content_kind": 0}, {"deerflow_error_fallback": 0}, {"presented_files": [], "presented_by": 0}])
def test_malformed_falsey_known_message_markers_are_incomplete(trace, additional):
    trace.model_call()
    message = ToolMessage(id="unit-tool", content="unit result", tool_call_id="current-call", artifact={"opaque_card": {"value": "unit"}}, additional_kwargs=additional)
    trace.tool(message)
    trace.end(messages=[INPUT, message, AIMessage(id="unit-answer", content="answer")])
    assert trace.document().coverage == "incomplete"


@pytest.mark.parametrize("message", [{}, {"type": "unknown-role", "id": "unknown", "content": "unknown terminal"}, {"type": "ai", "id": "unknown", "additional_kwargs": False}])
def test_unknown_or_malformed_terminal_message_never_claims_empty(trace, message):
    trace.end(messages=[INPUT, message, AIMessage(id="unit-answer", content="answer")])
    assert trace.document().coverage == "incomplete"


def test_current_human_input_fragment_is_retained_without_claiming_action_authority(trace):
    trace.model_call()
    artifact = {"human_input": {"kind": "human_input_request", "request_id": "unit-request", "question": "Unit question"}}
    message = ToolMessage(id="unit-tool", content="Unit question", tool_call_id="current-call", artifact=artifact)
    trace.tool(message)
    trace.end(messages=[INPUT, message, AIMessage(id="unit-answer", content="answer")])
    document = trace.document()
    assert document.coverage == "complete" and len(document.items) == 1
    assert document.items[0].value_sha256 == presentation_fragment_digest(artifact)


PAGE = {"web_page": {"version": 1, "kind": "web_page", "key": "memory"}}


def test_a_page_offer_is_named_and_is_not_output_content(trace):
    trace.model_call()
    message = ToolMessage(id="unit-tool", name="show_web_page", content="A button is shown", tool_call_id="current-call", artifact=PAGE)
    trace.tool(message)
    trace.end(messages=[INPUT, message, AIMessage(id="unit-answer", content="answer")])
    document = trace.document()
    assert document.coverage == "complete" and offered_pages(document) == ("memory",) and content_items(document) == ()


@pytest.mark.parametrize(
    ("name", "artifact"),
    [
        ("other_tool", PAGE),
        ("show_web_page", {**PAGE, "extra": True}),
        ("show_web_page", {"web_page": {"version": 2, "kind": "web_page", "key": "memory"}}),
        ("show_web_page", {"web_page": {"version": True, "kind": "web_page", "key": "memory"}}),
        ("show_web_page", {"web_page": {"version": 1, "kind": "web_page", "key": "memory", "url": "https://elsewhere.example"}}),
        ("show_web_page", {"web_page": {"version": 1, "kind": "web_page", "key": "https://elsewhere.example"}}),
    ],
)
def test_anything_but_the_exact_page_result_stays_opaque_output(trace, name, artifact):
    trace.model_call()
    message = ToolMessage(id="unit-tool", name=name, content="unit result", tool_call_id="current-call", artifact=artifact)
    trace.tool(message)
    trace.end(messages=[INPUT, message, AIMessage(id="unit-answer", content="answer")])
    document = trace.document()
    assert offered_pages(document) == () and [item.kind for item in content_items(document)] == ["structured"]


@pytest.mark.parametrize("disposition", ["removed", "hidden", "resolved-history"])
def test_removed_hidden_or_resolved_history_does_not_reappear_as_offer(trace, disposition):
    artifact = {"human_input": {"kind": "human_input_request", "request_id": "unit-request", "question": "Unit question"}}
    message = ToolMessage(id="unit-tool", content="Unit question", tool_call_id="current-call", artifact=artifact, additional_kwargs={"hide_from_ui": disposition == "hidden"})
    if disposition == "resolved-history":
        trace.rebind_baseline({"messages": [message]})
    else:
        trace.model_call()
        trace.tool(message)
    terminal = [INPUT, AIMessage(id="unit-answer", content="answer")] if disposition == "removed" else [INPUT, message, AIMessage(id="unit-answer", content="answer")]
    trace.end(messages=terminal)
    document = trace.document()
    assert all(getattr(item, "value_sha256", None) != presentation_fragment_digest(artifact) for item in descriptors(document))
    if disposition == "removed":
        assert document.coverage == "incomplete" and document.reason == "terminal_membership_unknown"


def test_valid_auxiliary_reasoning_and_usage_are_inert(trace, monkeypatch):
    trace.end(messages=[INPUT, AIMessage(id="unit-answer", content="answer", additional_kwargs={"reasoning_content": "unit private reasoning"}, usage_metadata={"input_tokens": 1, "output_tokens": 2, "total_tokens": 3})])
    first = trace.document()
    assembly, _built, _model = assemble_offline(monkeypatch)
    second_capture = OutputInventoryCapture()
    assert second_capture.qualify(assembly.inventory_profile, assembly.graph)
    second_capture.bind_baseline({})
    second_capture.begin_epoch(ROOT, {"messages": [INPUT]}, pre_existing_ids=set())
    second_capture.chain_start(run_id=ROOT, parent_run_id=None, tags=(), metadata={}, name="unit-root", inputs={})
    second = Trace(second_capture, snapshot(), snapshot(), assembly.inventory_profile, assembly.graph)
    second.end(messages=[INPUT, AIMessage(id="unit-answer", content="answer", additional_kwargs={"reasoning_content": "different unit private reasoning"}, usage_metadata={"input_tokens": 10, "output_tokens": 20, "total_tokens": 30})])
    assert inventory_document_digest(first) == inventory_document_digest(second.document())


@pytest.mark.parametrize("registry", [(), (PATH,)])
def test_provider_runtime_tag_requires_actual_qualified_registry(trace, registry):
    ai = AIMessage(id="unit-answer", content="answer", additional_kwargs={"presented_files": [PATH], "presented_by": "runtime"})
    trace.end(messages=[INPUT, ai], artifacts=[PATH])
    document = trace.document(runtime_paths=registry)
    if registry:
        assert document.coverage == "complete" and any(getattr(item, "path", None) == PATH for item in document.items)
    else:
        assert document.coverage == "incomplete"


@pytest.mark.parametrize("status", ["error", "interrupted"])
def test_failed_or_cancelled_partial_features_are_observed_subset(trace, status):
    trace.model_call()
    trace.tool(Command(update={"artifacts": [PATH], "messages": []}))
    trace.end(artifacts=[PATH])
    document = trace.document(status=status)
    assert document.coverage == "incomplete" and document.reason == "execution_stopped"
    assert any(getattr(item, "path", None) == PATH for item in document.observed_items)
    assert not hasattr(document, "items")


def test_overflow_is_explicit_bounded_subset(trace):
    paths = [f"/mnt/user-data/outputs/{i}.txt" for i in range(257)]
    trace.tool(Command(update={"artifacts": paths, "messages": []}))
    trace.end(artifacts=paths)
    document = trace.document()
    assert document.coverage == "incomplete" and document.reason == "enumeration_overflow"
    assert len(document.observed_items) == 256


def test_closed_callbacks_do_not_inspect_or_retain_new_payload(trace):
    class Poison:
        def __str__(self):
            raise AssertionError("Closed collector inspected a callback")

        def __getattribute__(self, key):
            if key.startswith("__"):
                return object.__getattribute__(self, key)
            raise AssertionError("Closed collector inspected a callback")

    trace.capture.detach()
    poison = Poison()
    trace.capture.begin_epoch(poison, poison, pre_existing_ids=poison)
    trace.capture.chain_start(run_id=poison, parent_run_id=poison, tags=poison, metadata=poison, name=poison, inputs=poison)
    trace.capture.chain_end(poison, run_id=poison, parent_run_id=poison)
    trace.capture.tool_start(run_id=poison, parent_run_id=poison, tags=poison)
    trace.capture.tool_end(poison, run_id=poison, parent_run_id=poison)
    trace.capture.observe_scan(poison)
    document = trace.document()
    assert document.coverage == "unsupported" and document.observed_items == ()


def test_actual_sdk_unknown_chat_role_never_claims_empty(trace):
    trace.end(messages=[INPUT, ChatMessage(id="unit-chat", role="unknown_role", content="unit payload"), AIMessage(id="unit-answer", content="answer")])
    assert trace.document().coverage == "incomplete"


def test_explicit_empty_or_null_clears_do_not_resurrect_old_fields(trace):
    trace.rebind_baseline({"messages": [], "goal": {"objective": "old unit goal"}, "todos": [{"content": "old unit task", "status": "pending"}], "background_tasks": ["old unit task"]})
    trace.end(goal=None, todos=[], background_tasks=None, structured_response=None)
    document = trace.document()
    assert document.coverage == "complete" and document.items == ()


def test_overbound_live_fragment_is_incomplete_not_a_fabricated_receipt(trace):
    trace.model_call()
    message = ToolMessage(id="unit-tool", content="unit result", tool_call_id="current-call", artifact={"payload": "x" * 262145})
    trace.tool(message)
    trace.end(messages=[INPUT, message, AIMessage(id="unit-answer", content="answer")])
    document = trace.document()
    assert document.coverage == "incomplete" and document.reason == "enumeration_overflow"
    assert document.observed_items == ()


@pytest.mark.parametrize("role", ["ai", "tool"])
@pytest.mark.parametrize(
    "block",
    [
        {"type": "image_url", "image_url": {"url": "https://unit.invalid/image.png"}},
        {"type": "file", "file": {"filename": "unit.txt", "file_data": "unit-data"}},
        {"type": "unknown_widget", "payload": {"selected": False}},
    ],
)
def test_current_sdk_rich_content_cannot_be_complete_empty(trace, role, block):
    trace.model_call()
    if role == "tool":
        message = ToolMessage(id="unit-rich", content=[block], tool_call_id="current-call")
        trace.tool(message)
    else:
        message = AIMessage(id="unit-rich", content=[block])
    trace.end(messages=[INPUT, message, AIMessage(id="unit-answer", content="answer")])
    document = trace.document()
    assert document.coverage != "complete" or descriptors(document), "Current SDK nontext/annotated presentation silently vanished"


def test_inherited_sdk_rich_content_alone_is_not_a_new_offer(trace):
    old = AIMessage(id="old-rich", content=[{"type": "image_url", "image_url": {"url": "https://unit.invalid/old.png"}}])
    trace.rebind_baseline({"messages": [old]})
    trace.end(messages=[old, INPUT, AIMessage(id="unit-answer", content="answer")])
    document = trace.document()
    assert document.coverage == "complete" and document.items == ()


def test_explicit_current_rich_reoffer_of_same_history_id_cannot_vanish(trace):
    old = ToolMessage(id="old-rich", content=[{"type": "file", "file": {"filename": "unit.txt", "file_data": "unit-data"}}], tool_call_id="current-call")
    trace.rebind_baseline({"messages": [old]})
    trace.model_call()
    trace.tool(old)
    trace.end(messages=[old, INPUT, AIMessage(id="unit-answer", content="answer")])
    document = trace.document()
    assert document.coverage != "complete" or descriptors(document)


def test_unhashable_message_type_is_incomplete_without_escaping_callback(trace):
    trace.end(messages=[INPUT, {"type": [], "id": "unit-malformed", "content": "unit payload"}, AIMessage(id="unit-answer", content="answer")])
    assert trace.document().coverage == "incomplete"


def test_detached_collector_cannot_be_requalified(trace, monkeypatch):
    assembly, _built, _model = assemble_offline(monkeypatch)
    trace.capture.detach()
    assert trace.capture.qualify(assembly.inventory_profile, assembly.graph) is False


def test_scan_representation_overflow_keeps_bounded_callback_subset(trace):
    paths = ["/mnt/user-data/outputs/" + f"{i:03}/" + "/".join(["d" * 200] * 7) for i in range(200)]
    assert sum(len(path.encode("utf-8")) for path in paths) > 262144
    trace.after = snapshot(files={path: FileSnapshot(path, "outputs", 1, 1, "a" * 64) for path in paths})
    trace.tool(Command(update={"artifacts": [PATH], "messages": []}))
    trace.end(artifacts=[PATH])
    document = trace.document()
    assert document.coverage == "incomplete" and document.reason == "enumeration_overflow"
    assert document.scan is None
    assert any(getattr(item, "path", None) == PATH for item in document.observed_items)
    assert all("output_change" not in getattr(item, "surfaces", ()) for item in document.observed_items)
    assert all(witness.kind != "output_scan" for item in document.observed_items for witness in getattr(item, "witnesses", ()))


def test_inactive_history_opaque_fragment_does_not_poison_current_text(trace):
    old = ToolMessage(id="old-opaque", content="old unit result", tool_call_id="old-call", artifact={"payload": "x" * 262145})
    trace.rebind_baseline({"messages": [old]})
    trace.end(messages=[old, INPUT, AIMessage(id="unit-answer", content="answer")])
    document = trace.document()
    assert document.coverage == "complete" and document.items == ()


def test_inactive_history_presentation_tags_do_not_become_current_offer(trace):
    paths = ["/mnt/user-data/outputs/" + f"{i:03}/" + "/".join(["d" * 200] * 7) for i in range(200)]
    old = AIMessage(id="old-tagged", content="old unit answer", additional_kwargs={"presented_files": paths, "presented_by": "runtime"})
    trace.rebind_baseline({"messages": [old]})
    trace.end(messages=[old, INPUT, AIMessage(id="unit-answer", content="answer")])
    document = trace.document()
    assert document.coverage == "complete" and document.items == ()


@pytest.mark.parametrize("messages", [None, False, {}, "bad"])
def test_malformed_model_messages_container_is_incomplete_without_escaping(trace, messages):
    trace.capture.chain_start(run_id=MODEL, parent_run_id=ROOT, tags=(), metadata={}, name="model", inputs={"messages": [INPUT]})
    trace.capture.chain_end({"messages": messages}, run_id=MODEL, parent_run_id=ROOT)
    trace.end()
    assert trace.document().coverage == "incomplete"


@pytest.mark.parametrize("calls", [None, False, {}, "bad"])
def test_malformed_model_tool_calls_container_is_incomplete_without_escaping(trace, calls):
    trace.capture.chain_start(run_id=MODEL, parent_run_id=ROOT, tags=(), metadata={}, name="model", inputs={"messages": [INPUT]})
    trace.capture.chain_end({"messages": [{"type": "ai", "id": "unit-call", "content": "unit", "tool_calls": calls}]}, run_id=MODEL, parent_run_id=ROOT)
    trace.end()
    assert trace.document().coverage == "incomplete"


@pytest.mark.parametrize("outputs", [None, False, 0, []])
def test_malformed_model_output_container_cannot_be_ignored(trace, outputs):
    trace.capture.chain_start(run_id=MODEL, parent_run_id=ROOT, tags=(), metadata={}, name="model", inputs={"messages": [INPUT]})
    trace.capture.chain_end(outputs, run_id=MODEL, parent_run_id=ROOT)
    trace.end()
    assert trace.document().coverage == "incomplete"


@pytest.mark.parametrize("kind", ["tool_use", "tool_call"])
def test_same_sdk_ai_tool_intent_is_not_a_terminal_user_offer(trace, kind):
    key = "input" if kind == "tool_use" else "args"
    block = {"type": kind, "id": "current-call", "name": "arbitrary_unit_name", key: {"value": 1}}
    intent = AIMessage(id="unit-intent", content=[block], tool_calls=[{"id": "current-call", "name": "arbitrary_unit_name", "args": {"value": 1}}])
    trace.end(messages=[INPUT, intent, AIMessage(id="unit-answer", content="answer")])
    document = trace.document()
    assert document.coverage == "complete" and document.items == ()


@pytest.mark.parametrize(
    "block",
    [
        pytest.param({"id": "current-call", "input": {}, "name": "arbitrary_unit_name", "type": "tool_use", "index": 1, "partial_json": '{"value": 1}'}, id="claude-stream"),
        pytest.param({"type": "function_call", "name": "arbitrary_unit_name", "arguments": '{"value": 1}', "call_id": "current-call", "id": "fc_1", "index": 1}, id="responses-stream"),
    ],
)
def test_provider_tool_blocks_are_known_by_their_call_not_their_keys(trace, block):
    intent = AIMessage(id="unit-intent", content=[block], tool_calls=[{"id": "current-call", "name": "arbitrary_unit_name", "args": {"value": 1}}])
    trace.end(messages=[INPUT, intent, AIMessage(id="unit-answer", content="answer")])
    document = trace.document()
    assert document.coverage == "complete" and document.items == ()


@pytest.mark.parametrize(
    "block",
    [
        pytest.param({"text": "answer", "type": "text", "index": 0}, id="claude-text"),
        pytest.param({"type": "text", "text": "answer", "annotations": [], "index": 0, "id": "msg_1"}, id="responses-text"),
        pytest.param({"type": "text", "text": "answer", "annotations": [{"type": "file_citation", "file_id": "unit-file"}]}, id="annotated-text"),
        pytest.param({"thinking": "private", "type": "thinking", "index": 0, "signature": "c2ln"}, id="claude-thinking"),
        pytest.param({"id": "rs_1", "summary": [], "type": "reasoning", "index": 0}, id="responses-reasoning"),
    ],
)
def test_provider_text_and_reasoning_blocks_are_not_presentation(trace, block):
    trace.end(messages=[INPUT, AIMessage(id="unit-answer", content=[block])])
    document = trace.document()
    assert document.coverage == "complete" and document.items == ()


@pytest.mark.parametrize("kind", ["tool_use", "tool_call"])
@pytest.mark.parametrize("change", ["wrong-id", "wrong-name", "invalid-args", "falsey-id"])
def test_unmatched_or_malformed_tool_intent_cannot_claim_empty(trace, kind, change):
    key = "input" if kind == "tool_use" else "args"
    block = {"type": kind, "id": "current-call", "name": "arbitrary_unit_name", key: {"value": 1}}
    if change == "wrong-id":
        block["id"] = "different-call"
    elif change == "wrong-name":
        block["name"] = "different_name"
    elif change == "invalid-args":
        block[key] = None
    else:
        block["id"] = False
    intent = AIMessage(id="unit-intent", content=[block], tool_calls=[{"id": "current-call", "name": "arbitrary_unit_name", "args": {"value": 1}}])
    trace.end(messages=[INPUT, intent, AIMessage(id="unit-answer", content="answer")])
    assert trace.document().coverage == "incomplete"


def test_tool_result_cannot_impersonate_ai_process_intent(trace):
    block = {"type": "tool_use", "id": "current-call", "name": "arbitrary_unit_name", "input": {"value": 1}}
    message = ToolMessage(id="unit-tool", content=[block], tool_call_id="current-call", tool_calls=[{"id": "current-call", "name": "arbitrary_unit_name", "args": {"value": 1}}])
    trace.end(messages=[INPUT, message, AIMessage(id="unit-answer", content="answer")])
    assert trace.document().coverage == "incomplete"


@pytest.mark.parametrize("kind,key", [("thinking", "thinking"), ("reasoning", "reasoning"), ("redacted_thinking", "data")])
def test_private_reasoning_content_never_enters_presentation_hash(trace, monkeypatch, kind, key):
    from deerflow.runtime.runs import output_inventory_capture as module

    private = "unit-private-reasoning-payload"
    hashed_private = []
    original = module.presentation_fragment_digest

    def contains_private(value):
        if isinstance(value, dict):
            return any(contains_private(item) for item in value.values())
        if isinstance(value, (list, tuple)):
            return any(contains_private(item) for item in value)
        return value == private

    def digest(value):
        if contains_private(value):
            hashed_private.append(True)
        return original(value)

    monkeypatch.setattr(module, "presentation_fragment_digest", digest)
    thought = AIMessage(id="unit-thought", content=[{"type": kind, key: private}])
    trace.end(messages=[INPUT, thought, AIMessage(id="unit-answer", content="answer")])
    document = trace.document()
    assert not hashed_private
    assert descriptors(document) == ()


@pytest.mark.parametrize("tail", [(), (Command(),), (Command(update={}),), (Command(update={"structured_response": False}),)])
def test_sdk_model_command_vector_keeps_first_messages_and_later_state(trace, tail):
    # Installed SDK _build_commands returns the model messages in the first
    # Command, followed by middleware updates. These are controlled frames.
    intent = AIMessage(id="unit-intent", content="", tool_calls=[{"id": "current-call", "name": "arbitrary_unit_name", "args": {}}])
    trace.capture.chain_start(run_id=MODEL, parent_run_id=ROOT, tags=(), metadata={}, name="model", inputs={"messages": [INPUT]})
    trace.capture.chain_end([Command(update={"messages": [intent]}), *tail], run_id=MODEL, parent_run_id=ROOT)
    artifact = {"opaque_card": {"unit": True}}
    result = ToolMessage(id="unit-tool", content="unit result", tool_call_id="current-call", artifact=artifact)
    trace.tool(result)
    fields = {"structured_response": False} if tail and tail[0].update == {"structured_response": False} else {}
    trace.end(messages=[INPUT, intent, result, AIMessage(id="unit-answer", content="answer")], **fields)
    document = trace.document()
    assert document.coverage == "complete"
    assert any(getattr(item, "value_sha256", None) == presentation_fragment_digest(artifact) for item in document.items)
    if fields:
        assert any(item.carrier == "terminal_field" and item.value_sha256 == presentation_fragment_digest(False) for item in document.items)


def test_sdk_model_command_tuple_retains_current_artifact_update(trace):
    trace.capture.chain_start(run_id=MODEL, parent_run_id=ROOT, tags=(), metadata={}, name="model", inputs={"messages": [INPUT]})
    trace.capture.chain_end((Command(update={"messages": [AIMessage(id="unit-answer", content="answer")]}), Command(update={"artifacts": [PATH]})), run_id=MODEL, parent_run_id=ROOT)
    trace.end(artifacts=[PATH])
    document = trace.document()
    assert document.coverage == "complete" and len(document.items) == 1
    assert document.items[0].path == PATH and document.items[0].surfaces == ("artifact_reference",)


@pytest.mark.parametrize(
    "frame",
    [
        [Command(update={"messages": []}), {"messages": []}],
        [Command(update={"messages": []}), None],
        [Command(update=None)],
        [Command(update={})],
        [Command(update={"messages": False})],
        [Command(update={"messages": []}), Command(update=False)],
        [Command(update={"messages": []}), Command(update={"messages": False})],
        [Command(update={"messages": []}), Command(update={"unknown_terminal_surface": {}})],
        [Command(update={"messages": []}, goto="another-node")],
        [Command(update={"messages": []}, resume="resume-value")],
        [Command(update={"messages": []}, graph=Command.PARENT)],
    ],
)
def test_malformed_mixed_or_control_model_command_vector_is_incomplete(trace, frame):
    trace.capture.chain_start(run_id=MODEL, parent_run_id=ROOT, tags=(), metadata={}, name="model", inputs={"messages": [INPUT]})
    trace.capture.chain_end(frame, run_id=MODEL, parent_run_id=ROOT)
    trace.end()
    assert trace.document().coverage == "incomplete"


def test_model_command_ai_tags_cannot_gain_tool_presentation_authority(trace):
    ai = AIMessage(id="unit-answer", content="answer", additional_kwargs={"presented_files": [PATH], "presented_by": "runtime"})
    trace.capture.chain_start(run_id=MODEL, parent_run_id=ROOT, tags=(), metadata={}, name="model", inputs={"messages": [INPUT]})
    trace.capture.chain_end([Command(update={"messages": [ai]})], run_id=MODEL, parent_run_id=ROOT)
    trace.end(messages=[INPUT, ai], artifacts=[PATH])
    document = trace.document(runtime_paths=())
    assert document.coverage == "incomplete"
    assert not any(getattr(item, "path", None) == PATH for item in descriptors(document))


@pytest.mark.parametrize("key", ["goal", "todos", "background_tasks"])
@pytest.mark.parametrize("value", [False, 0])
def test_malformed_tool_command_role_stays_incomplete_after_terminal_clear(trace, key, value):
    trace.tool(Command(update={"messages": [], key: value}))
    trace.end(**{key: None})
    document = trace.document()
    assert document.coverage == "incomplete" and document.reason == "carrier_incomplete"


@pytest.mark.parametrize("key,value", [("goal", None), ("goal", {}), ("todos", None), ("todos", []), ("background_tasks", None), ("background_tasks", [])])
def test_tool_command_empty_or_null_clear_does_not_resurrect_presentation(trace, key, value):
    previous = {"objective": "old unit goal"} if key == "goal" else ["old unit task"]
    trace.rebind_baseline({"messages": [], key: previous})
    trace.tool(Command(update={"messages": [], key: value}))
    trace.end(**{key: value})
    document = trace.document()
    assert document.coverage == "complete" and document.items == ()


def test_tool_command_false_structured_response_is_still_a_current_reoffer(trace):
    trace.rebind_baseline({"messages": [], "structured_response": False})
    trace.tool(Command(update={"messages": [], "structured_response": False}))
    trace.end(structured_response=False)
    document = trace.document()
    assert document.coverage == "complete" and len(document.items) == 1
    assert document.items[0].carrier == "terminal_field" and document.items[0].value_sha256 == presentation_fragment_digest(False)
