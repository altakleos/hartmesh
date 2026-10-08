"""Host-only instance context reaches assembly without requester-private inputs."""

from types import SimpleNamespace

import pytest
from test_agent_assembly_descriptor import TestLeadAgentAssembly as _LeadAssembly

from deerflow.agent_instances.contract import DefinitionSnapshot, InstanceIdentity
from deerflow.agent_instances.conversations import AGENT_EXECUTION_CONTEXT_KEY, AgentExecution
from deerflow.spaces.contract import PrincipalRef


def execution():
    definition = DefinitionSnapshot.capture(owner_id="creator", config={"name": "analyst", "skills": [], "mcp_plugins": [], "memory_enabled": True}, soul="ADOPTED INSTANCE INSTRUCTIONS")
    instance = InstanceIdentity("a" * 32, PrincipalRef("nonhuman", "agent:" + "a" * 32), "Analyst", "company", None, "creator", PrincipalRef("human", "supervisor"), definition.revision, "b" * 32, "active", 2)
    return AgentExecution(instance, PrincipalRef("human", "requester"), definition, 1, SimpleNamespace(), "chat", "c" * 32)


def test_worker_rejects_forged_execution_and_uses_only_host_capability():
    from deerflow.runtime.runs.worker import _build_runtime_context, _install_runtime_context

    bound = execution()
    forged = {"instance": "forged"}
    context = _build_runtime_context("chat", "run", {AGENT_EXECUTION_CONTEXT_KEY: forged}, agent_execution=bound)
    assert context[AGENT_EXECUTION_CONTEXT_KEY] is bound
    assert AGENT_EXECUTION_CONTEXT_KEY not in _build_runtime_context("chat", "run", {AGENT_EXECUTION_CONTEXT_KEY: forged})
    config = {"configurable": {AGENT_EXECUTION_CONTEXT_KEY: forged}, "context": {AGENT_EXECUTION_CONTEXT_KEY: forged}}
    _install_runtime_context(config, context)
    assert AGENT_EXECUTION_CONTEXT_KEY not in config["configurable"]
    assert config["context"][AGENT_EXECUTION_CONTEXT_KEY] is bound


def test_real_lead_assembly_uses_adopted_soul_and_disables_legacy_memory(monkeypatch):
    from deerflow.agents.lead_agent import agent
    from deerflow.extensions import bind_agent_build_extensions

    app_config = _LeadAssembly._isolate_from_the_ambient_config(monkeypatch)
    bound = execution()

    def private_definition(*args, **kwargs):
        raise AssertionError("A bound instance must not load the requester's definition")

    monkeypatch.setattr(agent, "load_agent_config", private_definition)
    config = {"configurable": {"thread_id": "chat", "agent_name": "requester-private"}, "context": {"user_id": "requester", AGENT_EXECUTION_CONTEXT_KEY: bound}}
    with bind_agent_build_extensions(_LeadAssembly._extensions_with_an_agent_assembly_observer()):
        assembly = agent.assemble_lead_agent(config, app_config=app_config)
    assert "ADOPTED INSTANCE INSTRUCTIONS" in assembly.graph["system_prompt"]
    assert config["metadata"]["agent_instance_id"] == bound.instance.id
    assert config["metadata"]["agent_definition_revision"] == bound.definition.revision
    assert config["metadata"]["memory_enabled"] is False
    assert not any(tool.name in {"update_agent", "memory_search", "memory_update", "list_uploaded_files"} for tool in assembly.graph["tools"])
    assert assembly.descriptor.effective_policies["agent_instance"]["principal_id"] == bound.instance.principal.subject_id


def test_assembly_keeps_native_capture_revision_distinct_from_live_host_catalog(monkeypatch):
    from deerflow.agent_instances.runtime import execution_scope
    from deerflow.agents.lead_agent import agent
    from deerflow.extensions import bind_agent_build_extensions

    config = _LeadAssembly._isolate_from_the_ambient_config(monkeypatch)
    bound = execution()
    with execution_scope(bound) as environment, bind_agent_build_extensions(_LeadAssembly._extensions_with_an_agent_assembly_observer()):
        environment.provider = SimpleNamespace(skill_revision="d" * 64)
        assembly = agent.assemble_lead_agent({"configurable": {"thread_id": "chat"}, "context": {AGENT_EXECUTION_CONTEXT_KEY: bound}}, app_config=config)
    policies = assembly.descriptor.effective_policies
    assert policies["public_package_capture_revision"] == "d" * 64
    assert policies["skill_catalog_source"] == "current_host_activation" and policies["native_package_copy"] == "mutable_home_data"


def test_thread_data_is_an_instance_space_view_not_requester_paths():
    from deerflow.agents.middlewares.thread_data_middleware import ThreadDataMiddleware

    bound = execution()
    runtime = SimpleNamespace(context={"thread_id": "chat", "run_id": "run", "user_id": "requester", AGENT_EXECUTION_CONTEXT_KEY: bound})
    update = ThreadDataMiddleware().before_agent({"messages": []}, runtime)
    assert update["thread_data"] == bound.thread_paths
    assert all("requester" not in path for path in update["thread_data"].values())


def test_instance_bash_does_not_resolve_requester_integration_credentials(monkeypatch):
    from deerflow.integrations import lark_cli
    from deerflow.sandbox import tools

    calls = []

    def private(*args, **kwargs):
        calls.append((args, kwargs))
        return {"PRIVATE_INTEGRATION": "must-not-inherit"}

    monkeypatch.setattr(lark_cli, "lark_cli_env_overlay", private)
    runtime = SimpleNamespace(context={"user_id": "requester", AGENT_EXECUTION_CONTEXT_KEY: execution()})
    assert tools._lark_cli_env_from_runtime(runtime, "lark-cli list", sandbox_paths=True) is None
    assert calls == []


def test_instance_presentation_maps_only_its_home_outputs():
    from deerflow.tools.builtins.present_file_tool import resolve_presented_filepath

    runtime = SimpleNamespace(context={"thread_id": "chat", "user_id": "requester", AGENT_EXECUTION_CONTEXT_KEY: execution()}, state={"thread_data": execution().thread_paths})
    for path in ("/mnt/spaces/home/outputs/result.md", "/mnt/user-data/outputs/result.md"):
        virtual, actual = resolve_presented_filepath(runtime, path)
        assert virtual == "/mnt/user-data/outputs/result.md"
        assert str(actual) == "/mnt/spaces/home/outputs/result.md"
    import pytest

    with pytest.raises(ValueError):
        resolve_presented_filepath(runtime, "/mnt/spaces/home/outputs/../private.md")


def test_runtime_cleanup_handles_legacy_missing_configurable():
    from deerflow.runtime.runs.worker import _release_run_scoped_references

    for config in ({}, {"configurable": None}, {"configurable": []}, {"configurable": {AGENT_EXECUTION_CONTEXT_KEY: execution()}}):
        context = {AGENT_EXECUTION_CONTEXT_KEY: execution()}
        config["context"] = dict(context)
        _release_run_scoped_references([config], context, None)
        assert AGENT_EXECUTION_CONTEXT_KEY not in context
        assert AGENT_EXECUTION_CONTEXT_KEY not in config["context"]


@pytest.mark.asyncio
async def test_instance_runtime_delivery_never_snapshots_requester_directories(monkeypatch):
    from unittest.mock import AsyncMock

    from deerflow.agents.middlewares import runtime_delivery_middleware as module

    scan = AsyncMock(side_effect=AssertionError("Requester directories are outside instance execution"))
    monkeypatch.setattr(module, "capture_output_snapshot", scan)
    runtime = SimpleNamespace(context={"thread_id": "chat", "run_id": "run", AGENT_EXECUTION_CONTEXT_KEY: execution()})
    assert await module.RuntimeDeliveryMiddleware()._snapshot(runtime) is None
    scan.assert_not_awaited()
