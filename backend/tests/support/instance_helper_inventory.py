"""Exercise delegation without the parent suite's replacement helper executor."""

from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from langchain_core.messages import AIMessage
from test_agent_execution_runtime import execution
from test_compose_operator_models import catalog as catalog
from test_compose_operator_models import render_config as render_config
from test_instance_middleware_tools import HeadingMiddleware, ScriptedModel, format_heading
from test_instance_middleware_tools import stock_app_config as stock_app_config
from test_subagent_declared_tool_policy import _load_extension

from deerflow.agent_instances.conversations import AGENT_EXECUTION_CONTEXT_KEY
from deerflow.config.extensions_config import ConfiguredMiddlewareSpec
from deerflow.config.subagents_config import CustomSubagentConfig
from deerflow.extensions import EXTENSION_SNAPSHOT_CONTEXT_KEY
from deerflow.runtime.runs.input_control import RunInputControl
from deerflow.runtime.runs.inputs import INPUT_CONTEXT_KEY, RunInputs
from deerflow.subagents import executor
from deerflow.tools.builtins import task_tool


@pytest.mark.asyncio
@pytest.mark.parametrize("name", ["general-purpose", "bash", "custom"])
async def test_helper_tool_inventory(monkeypatch, tmp_path, stock_app_config, name):
    cfg = stock_app_config
    cfg.subagents.custom_agents = {"custom": CustomSubagentConfig(description="A test helper", system_prompt="Format headings", disallowed_tools=None)}
    cfg.extensions.middlewares = [ConfiguredMiddlewareSpec(class_path="test_instance_middleware_tools:HeadingMiddleware")]
    monkeypatch.setenv("DEER_FLOW_HOME", str(tmp_path))
    monkeypatch.setattr(executor, "create_chat_model", lambda **kwargs: ScriptedModel(responses=[AIMessage(content="done")]))

    bound = replace(execution(), authority=SimpleNamespace(validate=AsyncMock()))
    loaded = _load_extension(monkeypatch, HeadingMiddleware())
    context = {
        "thread_id": "chat",
        "user_id": "example",
        "app_config": cfg,
        AGENT_EXECUTION_CONTEXT_KEY: bound,
        EXTENSION_SNAPSHOT_CONTEXT_KEY: loaded,
        INPUT_CONTEXT_KEY: RunInputControl(RunInputs(None, "run", "example"), agent_runs=None, thread_id="chat", checkpointer=None),
    }
    runtime = SimpleNamespace(context=context, state={}, config={"metadata": {"model_name": cfg.models[0].name}})
    children = []

    class ChildScheduled(Exception):
        pass

    def capture(child, prompt, task_id):
        children.append(child)
        raise ChildScheduled

    # Stop at the scheduling boundary; assemble the actual child's graph below
    # without starting a command, contacting a model or registering an async run.
    monkeypatch.setattr(executor.SubagentExecutor, "execute_async", capture)
    with pytest.raises(ChildScheduled):
        await task_tool.coroutine(runtime=runtime, prompt="Review headings", subagent_type=name, tool_call_id="heading")
    (child,) = children
    assert child.agent_execution is bound and child.input_gate is not None
    guard_stack = []
    compile_graph = executor.create_agent

    def capture_chain(**kwargs):
        guard_stack.extend(kwargs["middleware"])
        return compile_graph(**kwargs)

    monkeypatch.setattr(executor, "create_agent", capture_chain)
    _, tools, deferred = await child._build_initial_state("Review headings")
    graph = await child._create_agent(tools, deferred_setup=deferred, extensions=child.extensions)
    compiled = graph.get_graph().nodes["tools"].data.tools_by_name
    assert "format_heading" not in compiled
    assert "bash" in compiled
    assert guard_stack[0].allowed_tool_names == compiled.keys()
    for tool_name in compiled:
        guard_stack[0]._check_tool(SimpleNamespace(tool_call={"name": tool_name}))
    assert HeadingMiddleware.tools == [format_heading]
