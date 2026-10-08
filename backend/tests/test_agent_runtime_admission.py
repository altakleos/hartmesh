"""Real admission and worker preserve the instance and human scopes together."""

from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from _storage_spaces_test_support import BOB, operation
from langchain_core.messages import AIMessage
from test_agent_conversations import conversations
from test_agent_instances import create
from test_agent_instances import instances as instances
from test_gateway_services import _make_start_run_persistence_context
from test_run_worker_delivery import _make_bridge

from app.gateway.authz import AuthContext
from app.gateway.run_models import RunCreateRequest
from deerflow.config.app_config import AppConfig, reset_app_config, set_app_config
from deerflow.runtime.runs.schemas import RunStatus
from deerflow.runtime.user_context import reset_current_user, set_current_user


@pytest.mark.asyncio
async def test_real_admission_and_worker_keep_requester_attribution_and_instance_scope(instances, monkeypatch):
    from app.gateway import services
    from deerflow.agent_instances import runtime
    from deerflow.agent_instances.contract import AgentPermission
    from deerflow.agent_instances.conversations import AGENT_EXECUTION_CONTEXT_KEY
    from deerflow.persistence.agent_instances.model import AgentInstanceGrantRow
    from deerflow.runtime.runs import worker
    from deerflow.sandbox.sandbox_provider import get_sandbox_provider
    from deerflow.spaces.facade import _current_actor

    agents, authority, threads, sf = await conversations(instances)
    agent = await create(agents, custody="company", supervisor=BOB)
    async with sf() as session, session.begin():
        (await session.get(AgentInstanceGrantRow, (agent.id, "bob"))).permissions = int(AgentPermission.USE)
    chat = await authority.create(actor=BOB, instance_id=agent.id, creation_id=operation())
    request, _, _ = _make_start_run_persistence_context()
    state = request.app.state
    state.thread_store = threads
    state.agent_conversations = authority
    state.stream_bridge = _make_bridge()
    state.mcp_task_repo = SimpleNamespace(list_by_thread=AsyncMock(side_effect=AssertionError("Private tasks cannot be inherited")))
    user = SimpleNamespace(id="bob", system_role="user", role="user")
    request.state.user = user
    request.state.auth = AuthContext(user, ["runs:create", "runs:read"])
    request.state.auth_source = "session"
    request.url = "http://test/api/threads/" + chat["thread_id"] + "/runs"
    config = AppConfig.model_validate({"sandbox": {"use": "deerflow.sandbox.local:LocalSandboxProvider"}})
    provider = SimpleNamespace(app_config=config, skill_revision="e" * 64, close=lambda: None)
    captured = []
    private_scans = []

    async def private_scan(*args, **kwargs):
        private_scans.append(args)
        return None

    monkeypatch.setattr(worker, "capture_output_snapshot", private_scan)
    monkeypatch.setattr(worker, "capture_workspace_snapshot", private_scan)

    async def prepare(execution, **kwargs):
        await execution.validate()
        return provider

    class Graph:
        async def aget_state(self, config):
            return SimpleNamespace(config={}, values={}, metadata={}, next=())

        async def astream(self, graph_input, config=None, **kwargs):
            execution = config["context"][AGENT_EXECUTION_CONTEXT_KEY]
            assert execution.instance.id == agent.id and execution.requester == BOB
            from deerflow.spaces.facade import storage_credential_scope

            with storage_credential_scope(True):
                assert _current_actor() == agent.principal
            assert get_sandbox_provider() is provider
            assert config["context"]["user_id"] == "bob"
            assert "background_tasks" not in graph_input
            assert (await threads.get(chat["thread_id"], user_id="bob"))["status"] == "running"
            captured.append(execution)
            yield {"messages": [AIMessage("Instance work complete")]}

    monkeypatch.setattr(runtime, "prepare_environment", prepare)
    monkeypatch.setattr(services, "resolve_agent_factory", lambda *_: lambda **kwargs: Graph())
    token = set_current_user(user)
    set_app_config(config)
    try:
        record = await services.start_run(RunCreateRequest(input={"messages": [{"role": "user", "content": "work"}]}, context={"agent_name": "private", "__agent_execution": {"forged": True}}), chat["thread_id"], request)
        await record.task
        assert record.status == RunStatus.success, record.error
        assert record.user_id == "bob" and len(captured) == 1
        assert not private_scans
        assert "__agent_execution" not in str(record.kwargs)
        state.mcp_task_repo.list_by_thread.assert_not_awaited()
        evidence = await state.run_event_store.list_events(chat["thread_id"], record.run_id)
        admitted = next(event["content"] for event in evidence if event["event_type"] == "agent.environment")
        assert admitted["public_package_capture_revision"] == "e" * 64 and admitted["native_package_copy"] == "mutable_home_data"
        assert (await threads.get(chat["thread_id"], user_id="bob"))["status"] == "idle"
    finally:
        reset_current_user(token)
        reset_app_config()
