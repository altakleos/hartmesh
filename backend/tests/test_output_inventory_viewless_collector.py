"""Controlled profile/callback facts, not live host-view or SDK proof."""

import pytest
from langchain_core.messages import AIMessage, ToolMessage
from langgraph.types import Command
from test_output_inventory_collector import INPUT, PATH, ROOT, Trace, receipt, snapshot
from test_output_inventory_collector import trace as trace

from deerflow.runtime.runs.output_inventory import NoAdmittedOutputView
from deerflow.runtime.runs.output_inventory_capture import OutputInventoryCapture


@pytest.fixture
def viewless_trace(trace):
    capture = OutputInventoryCapture(document_version=2)
    assert capture.qualify(trace.profile, trace.graph)
    capture.bind_baseline({})
    capture.begin_epoch(ROOT, {"messages": [INPUT]}, pre_existing_ids=set())
    capture.chain_start(run_id=ROOT, parent_run_id=None, tags=(), metadata={}, name="unit-root", inputs={"messages": [INPUT]})
    return Trace(capture, snapshot(state=None), snapshot(state=None), trace.profile, trace.graph)


def document(trace, *, confirmed=True, status="success"):
    view = NoAdmittedOutputView(kind="no_admitted_output_view", scope=dict(version=1, kind="managed_output_routing", virtual_root="/mnt/user-data/outputs")) if confirmed else None
    return trace.capture.document(receipt(status=status), status=status, before=trace.before, after=trace.after, runtime_presented_files=(), output_view=view)


def test_positive_viewless_trace_can_be_complete_without_opened_roots(viewless_trace):
    viewless_trace.end()
    result = document(viewless_trace)
    assert result.version == 2 and result.coverage == "complete"
    assert result.items == () and result.output_view.kind == "no_admitted_output_view"
    assert not hasattr(result, "scan")


def test_missing_host_confirmation_is_incomplete_not_empty(viewless_trace):
    viewless_trace.end()
    result = document(viewless_trace, confirmed=False)
    assert result.coverage == "incomplete" and result.reason == "scan_incomplete"
    assert result.output_view is None


def test_failed_view_confirmation_does_not_adopt_even_complete_old_scans(viewless_trace):
    viewless_trace.before = snapshot()
    viewless_trace.after = snapshot()
    viewless_trace.end()
    result = document(viewless_trace, confirmed=False)
    assert result.coverage == "incomplete" and result.output_view is None


def test_omitted_view_uses_actual_complete_managed_scans(viewless_trace):
    trace = viewless_trace
    trace.before = snapshot()
    trace.after = snapshot()
    trace.end()
    result = trace.capture.document(receipt(), status="success", before=trace.before, after=trace.after, runtime_presented_files=())
    assert result.coverage == "complete" and result.output_view.kind == "managed_outputs"


@pytest.mark.parametrize("case", ["file", "structured_false", "content", "human_input", "unknown_role"])
def test_viewless_never_filters_independent_terminal_carriers(viewless_trace, case):
    trace = viewless_trace
    if case == "file":
        trace.model_call()
        trace.tool(Command(update={"messages": [ToolMessage("offered", tool_call_id="current-call", id="offer", artifact={"file": PATH})], "artifacts": [PATH]}))
        trace.end(messages=[INPUT, ToolMessage("offered", tool_call_id="current-call", id="offer", artifact={"file": PATH}), AIMessage("answer", id="unit-answer")], artifacts=[PATH])
    elif case == "structured_false":
        trace.end(structured_response=False)
    elif case == "content":
        trace.end(messages=[INPUT, AIMessage(id="unit-answer", content=[{"type": "image_url", "image_url": {"url": "https://unit.invalid/image"}}])])
    elif case == "human_input":
        trace.model_call()
        tool = ToolMessage("question", tool_call_id="current-call", id="control", artifact={"human_input": {"kind": "question", "question": "Continue?"}})
        trace.tool(tool)
        trace.end(messages=[INPUT, tool, AIMessage("answer", id="unit-answer")])
    else:
        trace.end(messages=[INPUT, {"type": "unknown", "id": "x", "content": "answer"}])
    result = document(trace)
    assert result.coverage != "complete" or result.items, "A view decision cannot erase a current offer or uncertainty"


@pytest.mark.parametrize("status", ["error", "interrupted"])
def test_partial_outcome_with_viewless_attestation_is_not_complete(viewless_trace, status):
    viewless_trace.end()
    result = document(viewless_trace, status=status)
    assert result.coverage == "incomplete" and result.reason == "execution_stopped"


def test_cleanup_invalidates_viewless_completion_independently(viewless_trace):
    viewless_trace.end()
    viewless_trace.capture.invalidate_scan()
    result = document(viewless_trace)
    assert result.coverage == "incomplete" and result.reason == "execution_stopped"
