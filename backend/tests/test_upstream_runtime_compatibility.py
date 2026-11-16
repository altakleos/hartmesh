"""Upstream runtime additions preserve HartMesh's storage and authority boundaries."""

import json

from langchain_core.messages import AIMessage, HumanMessage

from deerflow.runtime.context_keys import AGENT_AUTH_CONTEXT_KEY
from deerflow.runtime.runs.worker import _build_runtime_context, _install_runtime_context, _release_run_scoped_references
from deerflow.runtime.secret_context import redact_config_secrets


def test_agent_mcp_authority_is_host_only_and_released_after_the_run():
    authority = object()
    config = {"context": {AGENT_AUTH_CONTEXT_KEY: "forged"}, "configurable": {AGENT_AUTH_CONTEXT_KEY: "forged"}}
    context = _build_runtime_context("thread", "run", config["context"], agent_mcp_authority=authority)
    assert context[AGENT_AUTH_CONTEXT_KEY] is authority
    assert AGENT_AUTH_CONTEXT_KEY not in _build_runtime_context("thread", "run", config["context"])
    _install_runtime_context(config, context)
    assert AGENT_AUTH_CONTEXT_KEY not in config["configurable"]
    assert AGENT_AUTH_CONTEXT_KEY not in redact_config_secrets(config)["context"]
    assert context[AGENT_AUTH_CONTEXT_KEY] is authority
    _release_run_scoped_references([config], context, None)
    assert AGENT_AUTH_CONTEXT_KEY not in config["context"]
    assert AGENT_AUTH_CONTEXT_KEY not in context


def test_employee_runtime_never_inherits_a_human_mcp_credential_grant():
    employee = object()
    context = _build_runtime_context("thread", "run", None, agent_execution=employee, agent_mcp_authority=object())
    assert AGENT_AUTH_CONTEXT_KEY not in context


def test_portable_memory_storage_binding_does_not_retain_ambient_project_writes(tmp_path):
    from deerflow.agents.memory.backends.deermem.deer_mem import DeerMem
    from deerflow.agents.memory.backends.deermem.deermem.config import DeerMemConfig
    from deerflow.agents.memory.backends.deermem.deermem.core.storage import FileMemoryStorage

    project_lookups = []

    def project_hook(messages, thread_id, user_id):
        project_lookups.append((thread_id, user_id))
        return None

    class Extractor:
        def invoke(self, prompt, config=None):
            return AIMessage(
                content=json.dumps({"user": {}, "history": {}, "newFacts": [{"content": "User prefers dark mode", "category": "preference", "confidence": 0.9, "scope": "user", "durability": "durable", "authority": "descriptive"}]})
            )

    human = DeerMem(backend_config={"storage_path": str(tmp_path / "human"), "retrieval_adapter": "", "project_memory": project_hook, "host_llm": Extractor()})
    bound = human.with_storage(FileMemoryStorage(DeerMemConfig(storage_path=str(tmp_path / "bound"), retrieval_adapter="")))
    messages = [HumanMessage("Remember that I prefer dark mode", id="human-input")]
    try:
        assert bound._updater.update_memory(messages, thread_id="bound-thread", agent_name="researcher", user_id="alice") is True
        assert [fact["content"] for fact in bound.get_memory(agent_name="researcher", user_id="alice")["facts"]] == ["User prefers dark mode"]
        assert project_lookups == []
        assert human._updater.update_memory(messages, thread_id="human-thread", agent_name="researcher", user_id="alice") is True
        assert project_lookups == [("human-thread", "alice")]
    finally:
        bound.close()
        human.close()
