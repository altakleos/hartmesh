"""Real instance authority, SDK graph, worker, scanner and SQL output seals.

The fixture supplies a directory-backed prepared provider instead of starting a
native container; native qualification is covered by the Compose CI acceptance.
"""

import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock
from uuid import uuid4

import pytest
from _run_output_capture_test_support import capture_environment as capture_environment
from _run_output_capture_test_support import output_store as output_store
from langchain_core.messages import AIMessage, HumanMessage
from test_agent_instances import instances as instances
from test_instance_output_presentation import prepared
from test_output_inventory_profile import assemble_offline

from deerflow.agent_instances.conversations import AGENT_EXECUTION_CONTEXT_KEY
from deerflow.agent_instances.runtime import InstanceSandboxProvider
from deerflow.runtime import MemoryStreamBridge
from deerflow.runtime.runs.output_inventory import validate_inventory_document
from deerflow.runtime.runs.output_view import OutputViewQualifier
from deerflow.runtime.runs.worker import RunContext, run_agent
from deerflow.runtime.user_context import reset_current_user, set_current_user


@pytest.mark.asyncio
@pytest.mark.parametrize("case", ["text", "legacy_text", "produced_file", "missing_root", "revoked_home"])
async def test_instance_worker_seals_current_home_inventory(instances, capture_environment, monkeypatch, case):
    from deerflow.agent_instances import runtime as instance_runtime
    from deerflow.agents.lead_agent import agent
    from deerflow.persistence.spaces.model import SpaceRow
    from deerflow.runtime.runs import worker
    from deerflow.tools.builtins.present_file_tool import present_file_tool

    execution, _, root = await prepared(instances)
    e = capture_environment
    assembly, built, model = await asyncio.to_thread(assemble_offline, monkeypatch, runtime_context={AGENT_EXECUTION_CONTEXT_KEY: execution}, tools=[present_file_tool] if case == "produced_file" else [])
    model.responses = [AIMessage("Exact instance answer", id="instance-answer")]
    if case == "produced_file":
        model.responses.insert(0, AIMessage("", tool_calls=[{"id": "present-current", "name": "present_files", "args": {"filepaths": ["/mnt/user-data/outputs/current.txt"]}}]))
    assert any(type(m).__name__ == "InstanceAuthorityMiddleware" for m in built["middleware"])
    assert assembly.inventory_profile is not None
    # No ordinary owner path or viewless qualifier may supply instance evidence.
    for name in ("capture_output_snapshot", "capture_workspace_snapshot"):
        monkeypatch.setattr(worker, name, AsyncMock(side_effect=AssertionError("Instance cannot scan requester paths")))

    async def ordinary_qualifier(_request):
        raise AssertionError("Instance Home is an admitted view")

    close = Mock()
    sandbox = SimpleNamespace(id="prepared-fixture", close=close, release_command_scope=lambda _owner: None, requires_container_recycle=False, execution_outcome_uncertain=False)

    async def prepare(bound, *, app_config, run_id):
        assert bound is execution
        return InstanceSandboxProvider(bound, sandbox, app_config=app_config, skill_revision="fixture")

    monkeypatch.setattr(instance_runtime, "prepare_environment", prepare)
    actual_generate = type(model)._agenerate

    async def generate(self, *args, **kwargs):
        result = await actual_generate(self, *args, **kwargs)
        if case == "produced_file":
            await asyncio.to_thread((root / "outputs/current.txt").write_text, "current output", encoding="utf-8")
        elif case == "missing_root":
            await asyncio.to_thread((root / "outputs").rename, root / "moved")
        elif case == "revoked_home":
            async with instances[2].begin() as session:
                (await session.get(SpaceRow, execution.instance.home_id)).generation += 1
        return result

    monkeypatch.setattr(type(model), "_agenerate", generate)
    bridge, cleanup_tasks = MemoryStreamBridge(), []

    def cleanup(run_id, _delay):
        cleanup_tasks.append(asyncio.create_task(bridge.cleanup(run_id, delay=0)))

    owner, thread = execution.requester.subject_id, execution.thread_id
    capture = {"version": 1} if case == "legacy_text" else {"version": 2, "inventory_document_version": 2}
    record = await e.manager.create_or_reject(thread, "lead_agent", user_id=owner, kwargs={"output_capture": capture})
    ctx = RunContext(checkpointer=None, event_store=e.event_store, app_config=agent.get_app_config(), agent_execution=execution, schedule_stream_cleanup=cleanup, qualify_output_view=OutputViewQualifier(ordinary_qualifier))
    token = set_current_user(SimpleNamespace(id=owner))
    try:
        await run_agent(bridge, e.manager, record, ctx=ctx, agent_factory=lambda **kwargs: assembly, graph_input={"messages": [HumanMessage("Question")]}, config={"run_id": uuid4(), "configurable": {"thread_id": thread}})
        events = await e.event_store.list_events(thread, record.run_id, event_types=["run.output_inventory"], limit=2, user_id=owner)
    finally:
        reset_current_user(token)
        await asyncio.gather(*cleanup_tasks)
    row = await e.repository.get(record.run_id, user_id=owner)
    assert model.invocations == (2 if case == "produced_file" else 1) and close.call_count == 1
    if case == "legacy_text":
        assert row["status"] == "success" and not events
        assert row["kwargs"]["output_evidence"]["kind"] == "text"
        assert "inventory_evidence" not in row["kwargs"]
        return
    assert len(events) == 1
    doc = validate_inventory_document(events[0]["content"])
    assert doc.version == 2
    assert row["kwargs"]["output_evidence"]["kind"] == ("unavailable" if case == "revoked_home" else "text")
    assert row["kwargs"]["inventory_evidence"]["inventory_event_seq"] == events[0]["seq"]
    if case == "text":
        assert row["status"] == "success" and doc.coverage == "complete" and doc.items == ()
        assert doc.output_view.kind == "managed_outputs"
    elif case == "produced_file":
        assert doc.coverage == "complete"
        assert any(getattr(item, "path", None) == "/mnt/user-data/outputs/current.txt" for item in doc.items)
    else:
        assert doc.coverage == "incomplete" and doc.reason == ("execution_stopped" if case == "revoked_home" else "scan_incomplete")
