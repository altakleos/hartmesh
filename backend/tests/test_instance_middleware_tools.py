"""Compiled instance tool inventories stay local to the current graph."""

import asyncio
import os
import subprocess
import sys
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace
from typing import NotRequired
from unittest.mock import AsyncMock, Mock

import pytest
from _config_singleton_guard import restore_config_singletons  # noqa: F401 -- autouse fixture
from deerflow_extension_api import AgentScope
from langchain.agents import create_agent
from langchain.agents.middleware import AgentMiddleware, AgentState
from langchain_core.language_models.fake_chat_models import FakeMessagesListChatModel
from langchain_core.messages import AIMessage
from langchain_core.tools import tool
from test_agent_execution_runtime import execution
from test_compose_operator_models import TEMPLATE, _base_environ
from test_compose_operator_models import catalog as catalog
from test_compose_operator_models import render_config as render_config
from test_subagent_declared_tool_policy import _app_config, _load_extension

from deerflow.agent_instances.contract import AgentDenied
from deerflow.agent_instances.conversations import AGENT_EXECUTION_CONTEXT_KEY
from deerflow.agents.lead_agent import agent
from deerflow.agents.middlewares.tool_declarations import DeclaredToolViewError
from deerflow.config.app_config import AppConfig
from deerflow.config.extensions_config import ConfiguredMiddlewareSpec
from deerflow.extensions import bind_agent_build_extensions, get_agent_build_extensions
from deerflow.extensions.loader import load_extensions


@tool
def format_heading(text: str) -> str:
    """Format a short heading."""
    return text.upper()


class HeadingState(AgentState):
    heading_observed: NotRequired[bool]


class HeadingMiddleware(AgentMiddleware):
    state_schema = HeadingState
    tools = [format_heading]

    def before_model(self, state, runtime):
        return {"heading_observed": True}


class ScriptedModel(FakeMessagesListChatModel):
    def bind_tools(self, tools, **kwargs):
        return self


def build_graph(monkeypatch, *, bound=None, middleware=HeadingMiddleware, mode="full", package=False, app_config=None, checkpointer=None):
    cfg = app_config or _app_config()
    monkeypatch.setattr(agent, "get_app_config", lambda: cfg)
    cfg.database.checkpoint_channel_mode = mode
    if app_config is None:
        cfg.extensions.middlewares = [] if package else [ConfiguredMiddlewareSpec(class_path=f"{middleware.__module__}:{middleware.__name__}")]
        loaded = _load_extension(monkeypatch, middleware(), AgentScope.BOTH) if package else get_agent_build_extensions()
        monkeypatch.setattr("deerflow.tools.get_available_tools", lambda **kwargs: [])
    else:
        loaded, diagnostics = load_extensions(cfg.plugins)
        assert not diagnostics
    chain = []

    def compile_graph(**kwargs):
        chain.extend(kwargs["middleware"])
        return create_agent(checkpointer=checkpointer, **kwargs)

    monkeypatch.setattr(agent, "create_agent", compile_graph)
    monkeypatch.setattr(agent, "create_chat_model", lambda **kwargs: ScriptedModel(responses=[AIMessage(content="done")]))
    config = {"configurable": {"thread_id": "chat"}, "context": {AGENT_EXECUTION_CONTEXT_KEY: bound} if bound else {}}
    with bind_agent_build_extensions(loaded):
        return agent.assemble_lead_agent(config, app_config=cfg).graph, chain


@pytest.mark.parametrize("package", [False, True], ids=["configured", "package"])
@pytest.mark.parametrize("work", [False, True], ids=["chat", "assignment"])
def test_extension_tools_stay_in_personal_graphs(monkeypatch, work, package):
    personal, _ = build_graph(monkeypatch, package=package)
    assert personal.get_graph().nodes["tools"].data.tools_by_name["format_heading"].invoke({"text": "example"}) == "EXAMPLE"
    graph, chain = build_graph(monkeypatch, bound=replace(execution(), work=SimpleNamespace() if work else None), package=package)
    assert "format_heading" not in graph.get_graph().nodes["tools"].data.tools_by_name
    assert any("HeadingMiddleware" in name and name.endswith(".before_model") for name in graph.get_graph().nodes)
    assert chain[0].allowed_tool_names == graph.get_graph().nodes["tools"].data.tools_by_name.keys()
    assert HeadingMiddleware.tools == [format_heading]


@pytest.mark.asyncio
@pytest.mark.parametrize("package", [False, True], ids=["configured", "package"])
async def test_narrowed_middleware_hook_executes_in_an_instance_graph(monkeypatch, tmp_path, package):
    monkeypatch.setenv("DEER_FLOW_HOME", str(tmp_path))
    bound = replace(execution(), owner_loop=asyncio.get_running_loop(), authority=SimpleNamespace(validate=AsyncMock()))
    graph, _ = build_graph(monkeypatch, bound=bound, package=package)
    (hook,) = [name for name in graph.get_graph().nodes if "HeadingMiddleware" in name and name.endswith(".before_model")]
    state = await graph.ainvoke({"messages": [{"role": "user", "content": "Example heading"}]}, config={"configurable": {"thread_id": "chat"}}, context={AGENT_EXECUTION_CONTEXT_KEY: bound}, interrupt_after=[hook])
    assert state["heading_observed"] is True


class NamedHeadingMiddleware(HeadingMiddleware):
    @property
    def name(self):
        return "Heading formatter"


class RestoringHeadingMiddleware(NamedHeadingMiddleware):
    def __copy__(self):
        return type(self)()


@pytest.mark.asyncio
@pytest.mark.parametrize("read", [False, True], ids=["run", "read"])
@pytest.mark.parametrize("mode", ["full", "delta"])
async def test_verify_external_positions_after_schema_normalization(monkeypatch, mode, read):
    bound = replace(execution(), execution_allowed=not read)
    if mode == "delta" and not read:
        with pytest.raises(DeclaredToolViewError, match="Heading formatter"):
            build_graph(monkeypatch, bound=bound, middleware=RestoringHeadingMiddleware, mode=mode)
        return
    from langgraph.checkpoint.memory import InMemorySaver

    from app.gateway import services, transcript
    from deerflow.runtime import CheckpointStateAccessor

    saver = InMemorySaver()
    saved = {"configurable": {"thread_id": "archived"}}
    if read:
        existing, _ = build_graph(monkeypatch, bound=execution(), middleware=NamedHeadingMiddleware, mode=mode, checkpointer=saver)
        await existing.aupdate_state(saved, {"messages": [{"role": "user", "content": "Example heading"}]}, as_node="__start__")
    graph, _ = build_graph(monkeypatch, bound=bound, middleware=RestoringHeadingMiddleware, mode=mode, checkpointer=saver)
    assert "format_heading" not in graph.get_graph().nodes["tools"].data.tools_by_name
    assert "Heading formatter.before_model" in graph.get_graph().nodes
    if read:
        scope = SimpleNamespace()
        ctx = SimpleNamespace(app_config=agent.get_app_config(), checkpoint_channel_mode=mode, checkpointer=saver, store=None)
        monkeypatch.setattr(services, "get_run_context", lambda scope: ctx)
        monkeypatch.setattr(services, "resolve_agent_factory", lambda _: agent.make_lead_agent)
        accessor, config = services.build_checkpoint_state_accessor(scope, thread_id="archived", agent_inspection=bound)
        assert isinstance(accessor, CheckpointStateAccessor)
        assert (await accessor.aget(config)).values["messages"][0].content == "Example heading"
        assert (await accessor.ahistory(config, limit=1))[0].values["messages"][0].content == "Example heading"
        monkeypatch.setattr("app.gateway.agent_conversations.inspection_for_read", AsyncMock(return_value=bound))
        monkeypatch.setattr(transcript, "get_run_manager", lambda _: None)
        monkeypatch.setattr(transcript, "get_run_event_store", lambda _: None)
        monkeypatch.setattr(transcript, "feed_messages", AsyncMock(return_value=[]))
        conversation = await transcript.read_conversation(scope, "archived", {}, user_id="example")
        assert conversation.messages[0]["content"] == "Example heading"


@pytest.mark.asyncio
async def test_dispatch_only_delegates_names_from_the_compiled_graph(monkeypatch):
    bound = replace(execution(), owner_loop=asyncio.get_running_loop(), authority=SimpleNamespace(validate=AsyncMock()))
    graph, chain = build_graph(monkeypatch, bound=bound)
    guard = chain[0]
    runtime = SimpleNamespace(context={AGENT_EXECUTION_CONTEXT_KEY: bound})
    handler = AsyncMock(return_value="observed")
    sync_handler = Mock(return_value="observed")
    for name in graph.get_graph().nodes["tools"].data.tools_by_name:
        request = SimpleNamespace(tool_call={"name": name}, runtime=runtime)
        assert await guard.awrap_tool_call(request, handler) == "observed"
        assert await asyncio.to_thread(guard.wrap_tool_call, request, sync_handler) == "observed"
    handler.reset_mock()
    sync_handler.reset_mock()
    with pytest.raises(AgentDenied, match="unavailable"):
        await guard.awrap_tool_call(SimpleNamespace(tool_call={"name": "format_heading"}, runtime=runtime), handler)
    with pytest.raises(AgentDenied, match="unavailable"):
        await asyncio.to_thread(guard.wrap_tool_call, SimpleNamespace(tool_call={"name": "format_heading"}, runtime=runtime), sync_handler)
    handler.assert_not_called()
    sync_handler.assert_not_called()


@pytest.fixture
def stock_app_config(monkeypatch, tmp_path, render_config, catalog):
    env = _base_environ(OPENAI_API_KEY="offline-model")
    for key, value in env.items():
        monkeypatch.setenv(key, value)
    rendered, _ = render_config.render_text(TEMPLATE.read_text(encoding="utf-8"), catalog, env)
    path = tmp_path / "config.yaml"
    path.write_text(rendered, encoding="utf-8")
    return AppConfig.from_file(str(path))


@pytest.mark.parametrize("work", [False, True], ids=["chat", "assignment"])
def test_stock_profile_authorizes_each_compiled_tool(monkeypatch, stock_app_config, work):
    bound = replace(execution(), work=SimpleNamespace() if work else None)
    graph, chain = build_graph(monkeypatch, bound=bound, app_config=stock_app_config)
    names = graph.get_graph().nodes["tools"].data.tools_by_name.keys()
    assert {"ls", "bash", "present_files"} <= names
    assert ("report_work" in names) == work
    assert chain[0].allowed_tool_names == names
    for name in names:
        chain[0]._check_tool(SimpleNamespace(tool_call={"name": name}))


def test_real_helper_graphs_in_a_clean_interpreter():
    backend = Path(__file__).resolve().parents[1]
    subprocess.run(
        [sys.executable, "-m", "pytest", "--noconftest", "-q", "tests/support/instance_helper_inventory.py"],
        cwd=backend,
        env={**os.environ, "PYTHONPATH": os.pathsep.join((str(backend), str(backend / "tests")))},
        timeout=90,
        check=True,
    )
