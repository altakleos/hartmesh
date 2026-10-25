"""Host-issued inventory qualification; graph/config strings grant no profile."""

import pytest
from langchain_core.language_models.fake_chat_models import FakeMessagesListChatModel
from langchain_core.messages import AIMessage


class OfflineProfileModel(FakeMessagesListChatModel):
    invocations: int = 0

    def bind_tools(self, tools, **kwargs):
        return self

    def _generate(self, messages, stop=None, run_manager=None, **kwargs):
        self.invocations += 1
        return super()._generate(messages, stop=stop, run_manager=run_manager, **kwargs)


def assemble_offline(monkeypatch, *, mode="full", custom=False, tools=(), transform_middlewares=None, bootstrap=False, plan=False, tool_objects=None):
    """Actual first-party builder/SDK; isolate operator files and provider I/O."""
    import deerflow.tools
    from deerflow.agents.lead_agent import agent
    from deerflow.config.agents_config import AgentConfig
    from deerflow.config.app_config import AppConfig
    from deerflow.config.memory_config import MemoryConfig
    from deerflow.config.model_config import ModelConfig
    from deerflow.config.sandbox_config import SandboxConfig
    from deerflow.config.summarization_config import SummarizationConfig
    from deerflow.config.title_config import TitleConfig
    from deerflow.extensions import bind_agent_build_extensions
    from deerflow.extensions.registry import ExtensionRegistry
    from deerflow.runtime import checkpoint_mode

    config = AppConfig(
        models=[ModelConfig(name="offline-profile", display_name="offline-profile", use="langchain_openai:ChatOpenAI", model="offline-profile")],
        sandbox=SandboxConfig(use="deerflow.sandbox.local:LocalSandboxProvider"),
        memory=MemoryConfig(enabled=False),
        title=TitleConfig(enabled=False),
        summarization=SummarizationConfig(enabled=False),
    )
    config.database.checkpoint_channel_mode = mode
    monkeypatch.setattr(checkpoint_mode, "_frozen_checkpoint_channel_mode", None)
    monkeypatch.setattr(checkpoint_mode, "_frozen_checkpoint_snapshot_frequency", None)
    monkeypatch.setattr(agent, "get_app_config", lambda: config)
    monkeypatch.setattr(agent, "load_agent_config", lambda *args, **kwargs: AgentConfig(name="profile-agent", memory_enabled=False))
    monkeypatch.setattr(agent, "_load_enabled_available_skills", lambda *args, **kwargs: [])
    monkeypatch.setattr(agent, "build_tracing_callbacks", lambda: [])
    monkeypatch.setattr(agent, "apply_prompt_template", lambda **kwargs: "Offline profile fixture")
    selected_tools = tools if tool_objects is None else (tool_objects or ())
    monkeypatch.setattr(deerflow.tools, "get_available_tools", lambda **kwargs: list(selected_tools))
    model = OfflineProfileModel(responses=[AIMessage(content="offline profile answer")])
    monkeypatch.setattr(agent, "create_chat_model", lambda **kwargs: model)
    captured = {}
    actual_create = agent.create_agent

    def create(**kwargs):
        if transform_middlewares is not None:
            transform_middlewares(kwargs["middleware"])
        captured.update(kwargs)
        return actual_create(**kwargs)

    monkeypatch.setattr(agent, "create_agent", create)
    with bind_agent_build_extensions(ExtensionRegistry().build()):
        result = agent.assemble_lead_agent(
            {"configurable": {"thread_id": "profile-fixture", "thinking_enabled": False, "is_bootstrap": bootstrap, "is_plan_mode": plan, **({"agent_name": "profile-agent"} if custom else {})}},
            app_config=config,
        )
    return result, captured, model


@pytest.mark.parametrize("mode", ["full", "delta"])
@pytest.mark.parametrize("custom", [False, True])
def test_actual_no_observer_assembly_qualifies_exact_sdk_objects(monkeypatch, mode, custom):
    from deerflow.agents.output_inventory_profile import TerminalPresentationProfile

    assembly, built, _model = assemble_offline(monkeypatch, mode=mode, custom=custom)
    assert assembly.descriptor is None
    profile = getattr(assembly, "inventory_profile", None)
    assert type(profile) is TerminalPresentationProfile, "Actual first-party assembly must qualify without observer hashes"
    assert profile.accepts(assembly.graph) is True
    assert profile.accepts(object()) is False
    assert profile.has_runtime_delivery is True
    assert "messages" in profile.state_keys and "artifacts" in profile.state_keys
    assert profile.presentation_state_keys == frozenset({"todos", "goal", "background_tasks", "structured_response"})
    assert profile._state_schema is built["state_schema"]
    assert all(actual is bound for actual, bound in zip(built["middleware"], profile._middlewares, strict=True))
    assert all(actual is bound for actual, bound in zip(built["tools"], profile._tools, strict=True))


def test_legacy_positional_assembly_and_bare_graph_contract_stay_compatible():
    from deerflow.agents.lead_agent.agent import LeadAgentAssembly, unwrap_agent_graph

    graph = object()
    assembly = LeadAgentAssembly(graph, None, "model")
    assert getattr(assembly, "inventory_profile", None) is None
    assert unwrap_agent_graph(assembly) is graph
    assert unwrap_agent_graph(graph) is graph


def test_unknown_middleware_tool_adapter_is_not_qualified(monkeypatch):
    from langchain_core.tools import BaseTool

    from deerflow.agents.middlewares.title_middleware import TitleMiddleware

    class UnknownTool(BaseTool):
        name: str = "unknown_presentation"
        description: str = "Unknown adapter"

        def _run(self):
            return "ordinary result"

    def add_tool(middlewares):
        next(item for item in middlewares if type(item) is TitleMiddleware).tools = [UnknownTool()]

    assembly, _built, _model = assemble_offline(monkeypatch, transform_middlewares=add_tool)
    assert assembly.inventory_profile is None


@pytest.mark.parametrize("contributed", [False, True])
def test_standard_structured_tool_is_bound_by_identity(monkeypatch, contributed):
    from langchain_core.tools import tool

    from deerflow.agents.middlewares.title_middleware import TitleMiddleware

    @tool
    def ordinary_tool() -> str:
        """Return a standard SDK terminal result."""
        return "plain result"

    def add_tool(middlewares):
        next(item for item in middlewares if type(item) is TitleMiddleware).tools = [ordinary_tool]

    assembly, _built, _model = assemble_offline(
        monkeypatch,
        tools=() if contributed else (ordinary_tool,),
        transform_middlewares=add_tool if contributed else None,
    )
    assert assembly.inventory_profile.accepts(assembly.graph) is True
    bound = assembly.inventory_profile._middleware_tools if contributed else assembly.inventory_profile._tools
    assert any(actual is ordinary_tool for actual in bound)


def test_unknown_final_tool_adapter_stays_ordinary_but_unsupported(monkeypatch):
    from langchain_core.tools import BaseTool

    class UnknownTool(BaseTool):
        name: str = "present_file"
        description: str = "A familiar name supplies no qualification"

        def _run(self):
            return "ordinary result"

    assembly, _built, _model = assemble_offline(monkeypatch, tools=(UnknownTool(),))
    assert assembly.inventory_profile is None
    assert assembly.graph is not None


@pytest.mark.parametrize("mode", ["full", "delta"])
@pytest.mark.parametrize("unknown", ["subclass", "schema"])
def test_unknown_middleware_type_or_state_is_not_qualified(monkeypatch, mode, unknown):
    from typing import TypedDict

    from langchain.agents import AgentState

    from deerflow.agents.middlewares.title_middleware import TitleMiddleware

    class TitleLookalike(TitleMiddleware):
        pass

    # Neither a familiar module/name nor inheritance makes an actual adapter.
    TitleLookalike.__module__ = TitleMiddleware.__module__
    TitleLookalike.__name__ = TitleMiddleware.__name__
    UnknownState = TypedDict("UnknownState", {**AgentState.__annotations__, "unknown_offer": dict})

    def alter(middlewares):
        for index, item in enumerate(middlewares):
            if type(item) is TitleMiddleware:
                if unknown == "subclass":
                    item.__class__ = TitleLookalike
                else:
                    item.state_schema = UnknownState
                assert middlewares[index] is item
                break
        else:
            raise AssertionError("Expected actual first-party title middleware")

    assembly, _built, _model = assemble_offline(monkeypatch, mode=mode, transform_middlewares=alter)
    assert assembly.inventory_profile is None


def test_profile_is_immutable_host_issued_and_does_not_describe_graph(monkeypatch):
    from dataclasses import FrozenInstanceError

    from deerflow.agents.output_inventory_profile import TerminalPresentationProfile

    assembly, _built, _model = assemble_offline(monkeypatch)
    profile = assembly.inventory_profile
    with pytest.raises(TypeError, match="issued by the host"):
        TerminalPresentationProfile()
    forged = object.__new__(TerminalPresentationProfile)
    object.__setattr__(forged, "_graph", assembly.graph)
    assert forged.accepts(assembly.graph) is False
    with pytest.raises(FrozenInstanceError):
        profile._graph = object()
    assert repr(profile) == "TerminalPresentationProfile()"
    assert type(profile.state_keys) is frozenset
    assert type(profile.presentation_state_keys) is frozenset


def test_no_observer_profile_does_not_build_descriptor(monkeypatch):
    from deerflow.agents import assembly_descriptor

    def forbidden(**kwargs):
        raise AssertionError("No observer descriptor should be constructed")

    monkeypatch.setattr(assembly_descriptor, "build_assembly_descriptor", forbidden)
    assembly, _built, _model = assemble_offline(monkeypatch)
    assert assembly.descriptor is None
    assert assembly.inventory_profile.accepts(assembly.graph) is True


@pytest.mark.asyncio
@pytest.mark.parametrize("mode", ["full", "delta"])
async def test_actual_profile_graph_invokes_offline_sdk_model(monkeypatch, tmp_path, mode):
    from langchain_core.messages import HumanMessage

    from deerflow.config.paths import Paths, paths_scope

    with paths_scope(Paths(str(tmp_path))):
        assembly, _built, model = assemble_offline(monkeypatch, mode=mode)
        assert assembly.inventory_profile.accepts(assembly.graph) is True
        result = await assembly.graph.ainvoke(
            {"messages": [HumanMessage(content="Answer once.")]},
            config={"configurable": {"thread_id": "profile-fixture"}},
            context={"thread_id": "profile-fixture"},
        )
    assert any(isinstance(message, AIMessage) and message.content == "offline profile answer" for message in result["messages"])
    assert model.invocations == 1


def test_profile_leaf_executes_without_runtime_or_graph_imports(tmp_path):
    import subprocess
    import sys
    from pathlib import Path

    source = Path(__file__).resolve().parents[1] / "packages/harness/deerflow/agents/output_inventory_profile.py"
    script = """
import importlib.abc
import importlib.util
import sys
class BlockRuntime(importlib.abc.MetaPathFinder):
    def find_spec(self, fullname, path=None, target=None):
        if fullname.split('.')[0] in {'langchain', 'langchain_core', 'langgraph', 'deerflow'}:
            raise AssertionError('Leaf tried a runtime import: ' + fullname)
sys.meta_path.insert(0, BlockRuntime())
spec = importlib.util.spec_from_file_location('isolated_inventory_profile', sys.argv[1])
module = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = module
spec.loader.exec_module(module)
assert module.TerminalPresentationProfile.__module__ == spec.name
"""
    result = subprocess.run([sys.executable, "-c", script, str(source)], cwd=tmp_path, capture_output=True, text=True, timeout=20)
    assert result.returncode == 0, result.stderr


@pytest.mark.parametrize("mode", ["full", "delta"])
@pytest.mark.parametrize("bootstrap", [False, True])
def test_bootstrap_and_plan_middleware_tools_are_actual_sdk_adapters(monkeypatch, mode, bootstrap):
    assembly, built, _model = assemble_offline(monkeypatch, mode=mode, bootstrap=bootstrap, plan=True)
    assert assembly.inventory_profile.accepts(assembly.graph) is True
    assert assembly.descriptor is None
    contributed = [tool for item in built["middleware"] for tool in getattr(item, "tools", ())]
    assert contributed, "Real plan middleware should contribute its actual SDK tool"
    assert all(actual is bound for actual, bound in zip(contributed, assembly.inventory_profile._middleware_tools, strict=True))


@pytest.mark.parametrize("malformed", [None, {"name": "server_tool"}])
def test_present_malformed_middleware_tools_are_unsupported(monkeypatch, malformed):
    from deerflow.agents.middlewares.title_middleware import TitleMiddleware

    assembly, built, _model = assemble_offline(monkeypatch)
    title = next(item for item in built["middleware"] if type(item) is TitleMiddleware)
    title.tools = malformed
    from deerflow.agents.lead_agent.agent import _qualify_inventory_profile
    from deerflow.extensions.registry import ExtensionRegistry

    assert (
        _qualify_inventory_profile(
            assembly.graph,
            state_schema=built["state_schema"],
            middlewares=built["middleware"],
            tools=built["tools"],
            mode="full",
            extensions=ExtensionRegistry().build(),
        )
        is None
    )


@pytest.mark.parametrize("field", ["middleware_contributors", "plugins"])
def test_extension_contributor_presence_cannot_assert_a_profile(monkeypatch, field):
    from dataclasses import replace

    from deerflow.agents.lead_agent.agent import _qualify_inventory_profile
    from deerflow.extensions.registry import ExtensionRegistry

    assembly, built, _model = assemble_offline(monkeypatch)
    extensions = replace(ExtensionRegistry().build(), **{field: (("claimed_first_party", object()),)})
    assert (
        _qualify_inventory_profile(
            assembly.graph,
            state_schema=built["state_schema"],
            middlewares=built["middleware"],
            tools=built["tools"],
            mode="full",
            extensions=extensions,
        )
        is None
    )
