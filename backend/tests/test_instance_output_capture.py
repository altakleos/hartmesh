"""Real instance authority, SDK graph, worker, scanner and SQL output seals.

The fixture supplies a directory-backed prepared provider instead of starting a
native container; native qualification is covered by the Compose CI acceptance.
"""

import asyncio
from dataclasses import replace
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
@pytest.mark.parametrize("case", ["text", "legacy_text", "produced_file", "network_denial", "network_question", "missing_root", "revoked_home", "proposal"])
async def test_instance_worker_seals_current_home_inventory(instances, capture_environment, monkeypatch, case):
    from deerflow.agent_instances import runtime as instance_runtime
    from deerflow.agents.lead_agent import agent
    from deerflow.persistence.spaces.model import SpaceRow
    from deerflow.runtime.runs import worker
    from deerflow.tools.builtins.clarification_tool import ask_clarification_tool
    from deerflow.tools.builtins.present_file_tool import present_file_tool

    execution, _, root = await prepared(instances)
    baseline = None
    if case in {"network_denial", "network_question"}:
        from test_sandbox_middleware import _NetworkPolicyProvider

        from deerflow.persistence.agent_instances.model import AgentInstanceGrantRow

        async with instances[2].begin() as session:
            grant = await session.get(AgentInstanceGrantRow, (execution.instance.id, execution.requester.subject_id))
            grant.permissions = 1
        execution = await execution.authority.execution(actor=execution.requester, thread_id=execution.thread_id)
        baseline = _NetworkPolicyProvider()
        baseline.events = [{"request_id": "blocked-request", "host": "example.org", "port": 443}]
    with_tool = case in {"produced_file", "network_denial", "network_question"}
    e = capture_environment
    tools = [present_file_tool] if with_tool else []
    if case == "proposal":
        from deerflow.agent_instances import chat_tools

        monkeypatch.setattr(chat_tools, "chat_tools", lambda _execution: [chat_tools.show_web_page, chat_tools.propose_assignment])
    if case == "network_question":
        tools.append(ask_clarification_tool)
    assembly, built, model = await asyncio.to_thread(assemble_offline, monkeypatch, runtime_context={AGENT_EXECUTION_CONTEXT_KEY: execution}, tools=tools)
    model.responses = [AIMessage("Exact instance answer", id="instance-answer")]
    if case == "proposal":
        model.responses = [AIMessage("", id="proposal-issuer", tool_calls=[{"id": "proposal-call", "name": "propose_assignment", "args": {"proposal": {"objective": "Review the guide", "success_criteria": "Report verified gaps"}}}])]
    if with_tool:
        filename = "current.txt" if case == "produced_file" else "result.txt"
        model.responses.insert(0, AIMessage("", tool_calls=[{"id": "present-current", "name": "present_files", "args": {"filepaths": ["/mnt/user-data/outputs/" + filename]}}]))
        if case == "network_question":
            model.responses[1] = AIMessage("", tool_calls=[{"id": "question-current", "name": "ask_clarification", "args": {"question": "Which source should I use?"}}])
    assert any(type(m).__name__ == "InstanceAuthorityMiddleware" for m in built["middleware"])
    assert assembly.inventory_profile is not None
    if case == "proposal":
        assert any(item.name == "propose_assignment" for item in built["tools"])
        assert any(type(item).__name__ == "AssignmentProposalMiddleware" for item in built["middleware"])
    # No ordinary owner path or viewless qualifier may supply instance evidence.
    for name in ("capture_output_snapshot", "capture_workspace_snapshot"):
        monkeypatch.setattr(worker, name, AsyncMock(side_effect=AssertionError("Instance cannot scan requester paths")))

    async def ordinary_qualifier(_request):
        raise AssertionError("Instance Home is an admitted view")

    close = Mock()
    sandbox = SimpleNamespace(id="prepared-fixture", close=close, release_command_scope=lambda _owner: None, requires_container_recycle=False, execution_outcome_uncertain=False)

    async def prepare(bound, *, app_config, run_id):
        assert bound is execution
        return InstanceSandboxProvider(bound, sandbox, baseline=baseline, app_config=app_config, skill_revision="fixture")

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
    kwargs = {"output_capture": capture}
    if case == "proposal":
        from _telegram_human_input_test_support import record as publication

        from app.gateway.channel_execution import ChannelExecutionReceipt

        original = publication()
        original = replace(original, scope=replace(original.scope, agent_kind="instance", agent_ref=execution.instance.id, owner_user_id=owner, thread_id=thread), run_id=None)
        kwargs["channel_execution"] = ChannelExecutionReceipt(version=1, publication_id=original.publication_id, owner_user_id=owner, thread_id=thread, assistant_id="lead_agent", agent_name=None).model_dump(mode="json")
    record = await e.manager.create_or_reject(thread, "lead_agent", user_id=owner, kwargs=kwargs)
    ctx = RunContext(checkpointer=None, event_store=e.event_store, app_config=agent.get_app_config(), agent_execution=execution, schedule_stream_cleanup=cleanup, qualify_output_view=OutputViewQualifier(ordinary_qualifier))
    token = set_current_user(SimpleNamespace(id=owner))
    try:
        graph_input = {"messages": [HumanMessage("Question")]}
        if case == "proposal":
            graph_input["messages"][0].additional_kwargs["shows_cards"] = ["assignment_card"]
            graph_input["messages"][0].additional_kwargs["channel_turn_id"] = original.publication_id
        if baseline is not None:
            graph_input["sandbox"] = {"sandbox_id": sandbox.id}
        await run_agent(bridge, e.manager, record, ctx=ctx, agent_factory=lambda **kwargs: assembly, graph_input=graph_input, config={"run_id": uuid4(), "configurable": {"thread_id": thread}})
        events = await e.event_store.list_events(thread, record.run_id, event_types=["run.output_inventory"], limit=2, user_id=owner)
    finally:
        reset_current_user(token)
        await asyncio.gather(*cleanup_tasks)
    row = await e.repository.get(record.run_id, user_id=owner)
    assert model.invocations == (2 if with_tool else 1) and close.call_count == 1
    if case == "legacy_text":
        assert row["status"] == "success" and not events
        assert row["kwargs"]["output_evidence"]["kind"] == "text"
        assert "inventory_evidence" not in row["kwargs"]
        return
    assert len(events) == 1
    doc = validate_inventory_document(events[0]["content"])
    assert doc.version == 2
    assert row["kwargs"]["output_evidence"]["kind"] == ("unavailable" if case in {"revoked_home", "network_question", "proposal"} else "text")
    assert row["kwargs"]["inventory_evidence"]["inventory_event_seq"] == events[0]["seq"]
    if case == "text":
        assert row["status"] == "success" and doc.coverage == "complete" and doc.items == ()
        assert doc.output_view.kind == "managed_outputs"
    elif case == "produced_file":
        assert doc.coverage == "complete"
        assert any(getattr(item, "path", None) == "/mnt/user-data/outputs/current.txt" for item in doc.items)
    elif case == "proposal":
        from app.telegram_bots.assignment_proposal_source import validate_proposal_basis
        from app.telegram_bots.inbox import publication_identity

        rows = await e.event_store.list_messages_by_run(thread, record.run_id, user_id=owner)
        assert row["status"] == "success" and doc.coverage == "complete" and len(doc.items) == 1
        assert row["kwargs"]["output_evidence"]["reason"] == "unsupported_output"
        original = replace(original, run_id=record.run_id)
        basis = {
            "version": 1,
            "publication": publication_identity(original),
            "messages": rows,
            "projection": {
                "version": 2,
                "output": {
                    "version": 1,
                    "owner_user_id": owner,
                    "thread_id": thread,
                    "run_id": record.run_id,
                    "run_status": "success",
                    "state": "unavailable",
                    "source": None,
                    "reason": "unsupported_output",
                    "channel_execution": kwargs["channel_execution"],
                    "output_evidence": row["kwargs"]["output_evidence"],
                },
                "inventory": {"state": "matched", "document": events[0]["content"], "evidence": row["kwargs"]["inventory_evidence"], "reason": None},
            },
        }
        result = validate_proposal_basis(original, basis)
        assert result.objective == "Review the guide" and result.success_criteria == "Report verified gaps"
        assert len([item for item in rows if item["event_type"] == "llm.tool.result"]) == 1
    elif case in {"network_denial", "network_question"}:
        from deerflow.runtime.runs.output_inventory import presentation_fragment_digest

        assert baseline.decisions == [(sandbox.id, "blocked-request", "deny")]
        assert doc.coverage == "complete"
        rows = await e.event_store.list_messages_by_run(thread, record.run_id, user_id=owner)
        tool_rows = [row for row in rows if row["event_type"] == "llm.tool.result"]
        assert tool_rows
        result = next(row for row in tool_rows if "sandbox_network_denied" in (row["content"].get("artifact") or {}))
        enriched = result["content"]
        assert result["metadata"]["tool_artifact_enrichment"]["added_keys"] == ["sandbox_network_denied"]
        assert enriched["artifact"]["sandbox_network_denied"] == {"host": "example.org", "port": 443}
        matching = [item for item in doc.items if getattr(item, "value_sha256", None) == presentation_fragment_digest(enriched["artifact"])]
        assert len(matching) == 1 and matching[0].witness.message_id == enriched["id"]
        assert any(getattr(item, "path", None) == "/mnt/user-data/outputs/result.txt" for item in doc.items)
        if case == "network_question":
            assert row["kwargs"]["output_evidence"]["reason"] == "human_input_required"
            request = tool_rows[-1]["content"]["artifact"]["human_input"]
            assert request["question"] == "Which source should I use?"
            assert len([item for item in doc.items if item.kind == "structured"]) == 2
    else:
        assert doc.coverage == "incomplete" and doc.reason == ("execution_stopped" if case == "revoked_home" else "scan_incomplete")
