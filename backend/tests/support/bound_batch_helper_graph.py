"""Exercise the employee pre-model gate, shared helper graph and Home file checks."""

import asyncio
import shlex
from dataclasses import replace
from types import SimpleNamespace

import pytest
from _config_singleton_guard import restore_config_singletons  # noqa: F401 -- autouse
from _storage_spaces_test_support import ALICE, operation
from langchain.agents import create_agent
from langchain_core.messages import AIMessage
from test_agent_conversations import conversations
from test_agent_instances import create
from test_agent_instances import instances as instances
from test_bound_batch_execution import Repository
from test_instance_middleware_tools import HeadingMiddleware, ScriptedModel
from test_subagent_declared_tool_policy import _app_config, _load_extension

from app.subagent_batches.execution import BoundBatchItems
from deerflow.agent_instances.conversations import AGENT_EXECUTION_CONTEXT_KEY, ConnectionOrigin
from deerflow.agent_instances.middleware import InstanceAuthorityMiddleware
from deerflow.agent_instances.runtime import InstanceSandboxProvider, current_environment, execution_scope
from deerflow.agents.thread_state import ThreadState
from deerflow.config.app_config import set_app_config
from deerflow.extensions import EXTENSION_SNAPSHOT_CONTEXT_KEY, bind_agent_build_extensions
from deerflow.sandbox.tools import write_file_tool


@pytest.mark.asyncio
async def test_job_helpers_use_the_employees_home_and_fenced_inventory(instances, tmp_path, monkeypatch):

    agents, authority, _, _ = await conversations(instances)
    instance = await create(agents)
    thread = (await authority.create(actor=ALICE, instance_id=instance.id, thread_id=operation()))["thread_id"]
    employee = replace(await authority.execution(actor=ALICE, thread_id=thread), connection_origin=ConnectionOrigin("batch_followup"))
    home = next(volume.data_path for volume in instances[3].verified.values() if (volume.data_path / "outputs").is_dir())
    other = tmp_path / "other"
    other.mkdir()
    (other / "absent.txt").write_text("Only the human owns this copy.")

    class DirectoryTransport:
        id = "prepared"

        def release_command_scope(self, _owner):
            pass

        def local(self, path):
            return home / path.removeprefix("/mnt/user-data/workspace/") if path.startswith("/mnt/user-data/workspace/") else home / path.removeprefix("/mnt/spaces/home/")

        def read_file(self, path, **_):
            return self.local(path).read_text()

        def write_file(self, path, content, append=False):
            assert not append
            self.local(path).write_text(content)

        def execute_command(self, command, **_):
            path = self.local(shlex.split(command)[-2])
            return str(path.stat().st_size) if path.is_file() else "NOFILE"

    repo = Repository(1)
    original = repo.claim_bound_items

    async def claim(*args, **kwargs):
        rows = await original(*args, **kwargs)
        for item in rows:
            item["acceptance_criteria"] = ["file:report.txt non-empty", "file:absent.txt exists"]
        return rows

    repo.claim_bound_items = claim
    app_config = _app_config()
    app_config.memory.enabled = False
    app_config.tool_search.enabled = False
    monkeypatch.setenv("DEER_FLOW_HOME", str(tmp_path / "runtime"))
    extensions = _load_extension(monkeypatch, HeadingMiddleware())
    seen = []
    helpers = []

    class Helper(ScriptedModel):
        def bind_tools(self, tools, **kwargs):
            child = current_environment()
            assert child.execution.instance.id == employee.instance.id
            assert child.execution.connection_origin is None and child.bound_batch_items is None
            helpers.append(child.execution)
            seen.append({tool.name for tool in tools})
            return self

    responses = [AIMessage(content="", tool_calls=[{"name": "write_file", "args": {"path": "/mnt/spaces/home/report.txt", "content": "Employee report"}, "id": "write"}]), AIMessage(content="Wrote report.")]
    monkeypatch.setattr("deerflow.subagents.executor.create_chat_model", lambda **_: Helper(responses=responses))

    def inventory(**kwargs):
        assert kwargs["include_mcp"] is False
        return [write_file_tool]

    monkeypatch.setattr("deerflow.tools.get_available_tools", inventory)

    class Lead(ScriptedModel):
        def _generate(self, *args, **kwargs):
            assert repo.final == 1 and len(repo.results) == 1
            return super()._generate(*args, **kwargs)

    with execution_scope(employee) as env, bind_agent_build_extensions(extensions):
        set_app_config(app_config)
        env.provider = InstanceSandboxProvider(employee, DirectoryTransport(), app_config=app_config)
        env.run_record = SimpleNamespace(ownership_lost=False, abort_event=asyncio.Event())
        env.bound_batch_items = BoundBatchItems(repo, "job", "run", thread, "alice", instance.id)
        graph = create_agent(model=Lead(responses=[AIMessage(content="Finished.")]), middleware=[InstanceAuthorityMiddleware(lead=True)], state_schema=ThreadState)
        state = {"messages": [{"role": "user", "content": "Check the report"}], "thread_data": employee.thread_paths, "sandbox": {"sandbox_id": "prepared"}}
        context = {AGENT_EXECUTION_CONTEXT_KEY: employee, "app_config": app_config, "thread_id": thread, "user_id": "alice", EXTENSION_SNAPSHOT_CONTEXT_KEY: extensions}
        stream = [entry async for entry in graph.astream(state, config={"configurable": {"thread_id": thread}}, context=context, stream_mode=["custom", "messages", "values"])]
        assert not any("Wrote report." in str(data) or "Employee report" in str(data) or (kind == "custom" and str(data.get("type", "")).startswith("task_")) for kind, data in stream)
        output = [data for kind, data in stream if kind == "values"][-1]
    assert output["messages"][-1].content == "Finished."
    from deerflow.agent_instances.contract import AgentDenied

    for child in helpers:
        with pytest.raises(AgentDenied, match="no longer running"):
            await child.validate()
    assert seen and all("write_file" in names and "format_heading" not in names for names in seen)
    stored = repo.results[0][2]
    assert stored["result"] == "Wrote report."
    assert (home / "report.txt").read_text() == "Employee report"
    assert not (other / "report.txt").exists()
    assert [leaf["holds"] for leaf in stored["acceptance_verdict"]["leaves"]] == [True, False]
