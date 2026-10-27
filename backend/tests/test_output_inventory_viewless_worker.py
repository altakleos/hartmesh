"""Real SDK/worker/journal/SQL; the view issuer here is a controlled host seam.

Gateway's actual owner/storage/provider qualification is covered separately.
"""

import asyncio
from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest
from _run_output_capture_test_support import capture_environment as capture_environment
from _run_output_capture_test_support import output_store as output_store
from langchain_core.messages import AIMessage, HumanMessage
from test_output_inventory_profile import assemble_offline

from deerflow.config.paths import Paths, paths_scope
from deerflow.runtime import MemoryStreamBridge
from deerflow.runtime.runs.output import OutputEvidenceError
from deerflow.runtime.runs.output_inventory import validate_inventory_document
from deerflow.runtime.runs.output_view import NoAdmittedOutputViewLease, OutputViewQualifier
from deerflow.runtime.runs.worker import RunContext, run_agent
from deerflow.runtime.user_context import reset_current_user, set_current_user
from deerflow.spaces.chat_paths import ChatStoragePaths, ThreadMounts


async def execute(environment, monkeypatch, tmp_path, *, document_version=2, fail_refresh=False, custom=False):
    from deerflow.agents.lead_agent import agent
    from deerflow.runtime.runs import worker

    e = environment
    owner, thread = "view-owner", uuid4().hex
    base = await asyncio.to_thread(Paths, tmp_path)
    paths = ChatStoragePaths(base, user_id=owner)
    prepared = SimpleNamespace(paths=paths, mounts=ThreadMounts(owner, thread))
    assembly, _built, model = await asyncio.to_thread(assemble_offline, monkeypatch, custom=custom)
    model.responses = [AIMessage("Exact viewless answer", id="viewless-answer")]
    human = HumanMessage("Question")
    calls, leases = [], []

    @asynccontextmanager
    async def storage_scope(*, user_id, thread_id):
        assert (user_id, thread_id) == (owner, thread)
        with paths_scope(paths):
            yield prepared

    async def refresh():
        calls.append("refresh")
        if fail_refresh:
            raise OutputEvidenceError()

    async def issue(request):
        calls.append("issue")
        assert request.prepared is prepared and request.paths is paths
        assert (request.owner_user_id, request.thread_id) == (owner, thread)
        lease = NoAdmittedOutputViewLease(request, refresh=refresh)
        leases.append((lease, request))
        return lease

    # No scanner may manufacture opened facts for this intentionally viewless run.
    for name in ("capture_output_snapshot", "capture_workspace_snapshot"):
        monkeypatch.setattr(worker, name, AsyncMock(side_effect=AssertionError("No output root admitted")))
    cleanup_tasks = []
    bridge = MemoryStreamBridge()

    def cleanup(run_id, delay):
        cleanup_tasks.append(asyncio.create_task(bridge.cleanup(run_id, delay=0)))

    capture = {"version": 2, **({"inventory_document_version": 2} if document_version == 2 else {})}
    record = await e.manager.create_or_reject(thread, "lead_agent", user_id=owner, kwargs={"output_capture": capture})
    ctx = RunContext(checkpointer=None, event_store=e.event_store, app_config=agent.get_app_config(), chat_storage_scope=storage_scope, schedule_stream_cleanup=cleanup, qualify_output_view=OutputViewQualifier(issue))
    token = set_current_user(SimpleNamespace(id=owner))
    try:
        await run_agent(
            bridge,
            e.manager,
            record,
            ctx=ctx,
            agent_factory=lambda **kwargs: assembly,
            graph_input={"messages": [human]},
            config={"run_id": uuid4(), "configurable": {"thread_id": thread, **({"agent_name": "profile-agent"} if custom else {})}},
        )
    finally:
        reset_current_user(token)
        await asyncio.gather(*cleanup_tasks)
    row = await e.repository.get(record.run_id, user_id=owner)
    events = await e.event_store.list_events(thread, record.run_id, event_types=["run.output_inventory"], limit=2, user_id=owner)
    return SimpleNamespace(row=row, events=events, calls=calls, leases=leases, model=model, human=human, record=record)


@pytest.mark.asyncio
@pytest.mark.parametrize("custom", [False, True])
async def test_actual_sdk_worker_seals_document2_viewless_text(capture_environment, monkeypatch, tmp_path, custom):
    result = await execute(capture_environment, monkeypatch, tmp_path, custom=custom)
    assert result.row["status"] == "success" and len(result.events) == 1
    doc = validate_inventory_document(result.events[0]["content"])
    assert doc.version == 2, "Explicit requested format must not silently seal a format1 document"
    assert doc.coverage == "complete" and doc.items == ()
    assert doc.output_view.kind == "no_admitted_output_view"
    assert result.calls == ["issue", "refresh"] and result.model.invocations == 1
    assert result.human.id and doc.input_message_id == result.human.id
    assert result.row["kwargs"]["inventory_evidence"]["inventory_event_seq"] == result.events[0]["seq"]
    assert result.row["kwargs"]["output_evidence"]["assistant_message_id"] == "viewless-answer"


@pytest.mark.asyncio
async def test_failed_final_host_confirmation_seals_unknown_view_without_losing_valid_text(capture_environment, monkeypatch, tmp_path):
    result = await execute(capture_environment, monkeypatch, tmp_path, fail_refresh=True)
    assert result.model.invocations == 1
    assert result.calls == ["issue", "refresh"]
    assert result.row["status"] == "success" and len(result.events) == 1
    assert result.row["kwargs"]["output_evidence"]["kind"] == "text"
    doc = validate_inventory_document(result.events[0]["content"])
    assert doc.coverage == "incomplete" and doc.reason == "scan_incomplete"
    assert doc.output_view is None
    assert not result.record.ownership_lost


@pytest.mark.asyncio
async def test_default_document1_does_not_call_host_qualifier(capture_environment, monkeypatch, tmp_path):
    result = await execute(capture_environment, monkeypatch, tmp_path, document_version=1)
    assert result.row["status"] == "success" and result.model.invocations == 1
    doc = validate_inventory_document(result.events[0]["content"])
    assert doc.version == 1 and doc.coverage == "incomplete"
    assert result.calls == []


@pytest.mark.asyncio
@pytest.mark.parametrize("unsupported", ["instance", "missing_qualifier"])
async def test_identifiable_unsupported_doc2_refuses_before_environment_or_model(capture_environment, monkeypatch, unsupported):
    from deerflow.agent_instances import runtime as instances

    e = capture_environment
    record = await e.manager.create_or_reject(uuid4().hex, "lead_agent", user_id="view-owner", kwargs={"output_capture": {"version": 2, "inventory_document_version": 2}})
    prepare = AsyncMock(side_effect=AssertionError("Unsupported instance must not prepare"))
    monkeypatch.setattr(instances, "prepare_environment", prepare)
    factory = AsyncMock(side_effect=AssertionError("Unsupported view must not assemble or invoke"))

    async def issue(_request):
        raise AssertionError("Unsupported instance must not qualify ordinary paths")

    bridge = MemoryStreamBridge()
    ctx = RunContext(
        checkpointer=None,
        event_store=e.event_store,
        schedule_stream_cleanup=lambda *_args: None,
        qualify_output_view=OutputViewQualifier(issue) if unsupported == "instance" else None,
        agent_execution=SimpleNamespace(work=None) if unsupported == "instance" else None,
    )
    await run_agent(bridge, e.manager, record, ctx=ctx, agent_factory=factory, graph_input={"messages": [HumanMessage("Question")]}, config={"configurable": {"thread_id": record.thread_id}})
    prepare.assert_not_awaited()
    factory.assert_not_called()
    row = await e.repository.get(record.run_id, user_id="view-owner")
    assert row["status"] == "error"
    assert row["kwargs"]["output_capture"] == {"version": 2, "inventory_document_version": 2}
    assert row["kwargs"]["output_evidence"]["kind"] == "unavailable"
    assert row["kwargs"]["inventory_evidence"]["reason"] == "prestart_not_enumerated"
