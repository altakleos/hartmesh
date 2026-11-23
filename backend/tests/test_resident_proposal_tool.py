"""Only the explicitly installed resident form can turn a model call into a proposal."""

from types import SimpleNamespace

import pytest
from deerflow_extension_api.plugins import ModelTool, PluginContribution
from langchain_core.messages import AIMessage
from test_agent_instances import instances as instances

from deerflow.agent_instances.proposal_tools import observe_proposal_calls
from deerflow.extensions.registry import ExtensionRegistry


async def handle(value, context):
    return value


def test_version_and_schema_negotiation():
    form = ModelTool("change_work", "Propose a setting.", {"type": "object", "properties": {"enabled": {"type": "boolean"}}, "required": ["enabled"], "additionalProperties": False}, handle)
    reg = ExtensionRegistry()
    with reg.attributed_to("test:install"):
        with pytest.raises(ValueError):
            reg.plugin(PluginContribution("test.proposal", "Review", proposal_api_version=2, proposal_tool=form, api_version=5))
        from dataclasses import replace

        with pytest.raises(ValueError):
            reg.plugin(PluginContribution("test.proposal", "Review", proposal_api_version=1, proposal_tool=replace(form, input_schema={"type": "object"}), api_version=5))
        assert reg.plugin(PluginContribution("test.proposal", "Review", proposal_api_version=1, proposal_tool=form, api_version=5))


def test_capture_is_bound_to_current_model_result_and_exact_arguments():
    env = SimpleNamespace(proposal_calls={})
    response = AIMessage(id="issuer", content="", tool_calls=[{"id": "call", "name": "form", "args": {"enabled": True}}])
    observe_proposal_calls(env, response, {"form"}, "graph-task")
    assert env.proposal_calls == {("form", "call"): ("graph-task", '{"enabled":true}')}
    # Re-observing a host checkpoint is independent of provider-generated IDs.
    response.id = None
    observe_proposal_calls(env, response, {"form"}, "graph-task")
    assert env.proposal_calls[("form", "call")][0] == "graph-task"
    observe_proposal_calls(env, response, {"form"}, "next-task")
    assert env.proposal_calls[("form", "call")][0] == "next-task"
    observe_proposal_calls(env, response, {"form"}, None)
    assert env.proposal_calls == {}
    observe_proposal_calls(env, AIMessage(id="other", content="Finished"), {"form"}, "next-task")
    assert env.proposal_calls == {}


@pytest.mark.asyncio
async def test_installed_form_accepts_only_a_live_employee_model_issuance(instances, monkeypatch):
    from dataclasses import replace
    from pathlib import Path

    from langgraph.graph import END, START, MessagesState, StateGraph
    from langgraph.prebuilt import ToolNode
    from test_agent_configuration_proposals import setup, source

    from deerflow.agent_instances.conversations import AGENT_EXECUTION_CONTEXT_KEY
    from deerflow.agent_instances.middleware import InstanceAuthorityMiddleware
    from deerflow.agent_instances.proposal_tools import proposal_tools
    from deerflow.agent_instances.runtime import execution_scope
    from deerflow.extensions import bind_agent_build_extensions
    from deerflow.extensions.plugin_tools import build_plugin_tools
    from deerflow.runtime.runs.manager import RunRecord
    from deerflow.runtime.runs.schemas import DisconnectMode, RunStatus

    monkeypatch.syspath_prepend(str(Path(__file__).resolve().parents[1] / "extensions/sources/hartmesh-work-proposals"))
    from hartmesh_work_proposals import install

    reg = ExtensionRegistry()
    with reg.attributed_to("work-proposals"):
        install(reg, {})
    package = reg.build().plugins[0][1]

    async def mismapped(payload, context):
        return await context.submit_proposal("work_policy.enabled", not payload["enabled"])

    with reg.attributed_to("misconfigured-package"):
        reg.plugin(replace(package, namespace="test.mismapped", proposal_tool=replace(package.proposal_tool, handler=mismapped)))
    loaded = reg.build()
    monkeypatch.setattr("deerflow.agent_instances.proposal_tools.get_loaded_extensions", lambda: loaded)
    with bind_agent_build_extensions(loaded):
        assert build_plugin_tools(loaded) == [] and proposal_tools([]) == []
        tool, broken = proposal_tools(None)
    graph = StateGraph(MessagesState)
    graph.add_node("tools", ToolNode([tool, broken]))
    graph.add_edge(START, "tools")
    graph.add_edge("tools", END)
    agent = graph.compile()
    service, execution, *_ = await setup(instances)
    assert execution.for_helper().proposal_submission is False
    state = AIMessage(content="", id="lead-message", tool_calls=[{"id": "one", "name": tool.name, "args": {"enabled": True}}])
    runtime = {AGENT_EXECUTION_CONTEXT_KEY: execution, "thread_id": execution.thread_id}
    middleware = InstanceAuthorityMiddleware(lead=True)
    middleware.proposal_tools = {tool.name, broken.name}
    with execution_scope(execution) as environment:
        environment.run_record = RunRecord(run_id="run", thread_id=execution.thread_id, assistant_id=None, status=RunStatus.running, on_disconnect=DisconnectMode.continue_, user_id=execution.requester.subject_id)

        async def invoke(message, context):
            result = await agent.ainvoke({"messages": [message]}, context=context)
            return result["messages"][-1]

        assert (await invoke(state, runtime)).status == "error"
        middleware._observe_response(state, SimpleNamespace(execution_info=SimpleNamespace(task_id="host-model-task")))
        assert (await invoke(state, {**runtime, "is_subagent": True})).status == "error"
        forged = state.model_copy(update={"tool_calls": [{"id": "one", "name": tool.name, "args": {"enabled": False}}]})
        assert (await invoke(forged, runtime)).status == "error"
        result = await invoke(state, runtime)
        assert result.status == "success"
        receipt = result.artifact["configuration_proposal"]
        assert receipt["field"] == "work_policy.enabled" and receipt["proposed_value"] is True
        review = await service.detail(execution.requester, execution.instance.id, receipt["id"], source)
        assert review["status"] == "open" and not review["current_value"]
        state.id = None
        middleware._observe_response(state, SimpleNamespace(execution_info=SimpleNamespace(task_id="host-model-task")))
        assert (await invoke(state, runtime)).artifact["configuration_proposal"]["id"] == receipt["id"]
        middleware._observe_response(state, SimpleNamespace(execution_info=SimpleNamespace(task_id="later-model-task")))
        assert (await invoke(state, runtime)).artifact["configuration_proposal"]["id"] != receipt["id"]
        wrong = AIMessage(content="", tool_calls=[{"id": "bad", "name": broken.name, "args": {"enabled": False}}])
        middleware._observe_response(wrong, SimpleNamespace(execution_info=SimpleNamespace(task_id="wrong-package")))
        assert (await invoke(wrong, runtime)).status == "error"
        rows = (await service.list(execution.requester, execution.instance.id, source))["proposals"]
        assert len(rows) == 2
        environment.run_record.abort_event.set()
        assert (await invoke(state, runtime)).status == "error"
