"""Real creating HTTP -> service/graph/worker/SQL -> private output proof.

The configured offline resolver is the only agent substitution. Actual auth/CSRF,
service admission, attachment, RunJournal, worker and SQL source/seal stay real.
The account provider returns an actual fixture User (credential-store stand-in).
Workspace/file snapshot helpers are disabled: this case has no sandbox/files.
"""

import asyncio
import json
from unittest.mock import AsyncMock
from uuid import UUID

import httpx
import pytest
from _channel_execution_test_support import TABLES, ControlledProvider
from _run_output_capture_test_support import capture_environment as capture_environment
from _run_output_capture_test_support import output_store as output_store
from fastapi import FastAPI
from langchain_core.language_models.fake_chat_models import FakeMessagesListChatModel
from langchain_core.messages import AIMessage
from langgraph.checkpoint.memory import InMemorySaver
from langgraph.graph import END, START, MessagesState, StateGraph
from sqlalchemy import text
from test_gateway_services import _make_start_run_request

from app.channels.execution import ChannelExecutionScope
from app.channels.execution_client import ChannelExecutionFacade
from app.gateway import channel_execution_reads, deps, services
from app.gateway.auth.models import User
from app.gateway.auth_middleware import AuthMiddleware
from app.gateway.csrf_middleware import CSRFMiddleware
from app.gateway.routers import run_output, thread_runs
from app.gateway.stream_cleanup import StreamCleanupRegistry
from app.telegram_bots.gateway_client import GatewayClientOwner
from deerflow.agent_instances.conversations import AgentConversations
from deerflow.config import app_config
from deerflow.runtime import MemoryStreamBridge
from deerflow.runtime.capture_resources import OwnedCheckpointSaver, OwnedStore
from deerflow.runtime.runs.output import canonical_text_digest
from deerflow.runtime.runs.worker import run_agent

OWNER = "00000000-0000-0000-0000-00000000000a"
PUBLICATION = "ba" * 16
THREAD = "composition-output"
QUESTION = "  Original HTTP question\n"
ANSWER = "  Exact offline answer\nα"


class Admission:
    async def authorize(self, operation):
        assert operation in {"run_create", "run_read", "source_read"}

    def current(self, operation):
        return operation in {"run_create", "run_read", "source_read"}


@pytest.mark.asyncio
async def test_creating_http_runs_the_real_worker_and_returns_exact_private_output(capture_environment, monkeypatch):
    e = capture_environment
    # Pin the no-double promise before any request/fixture work.
    assert services.run_agent is run_agent
    original_start = services.start_run
    async with e.engine.begin() as connection:
        if e.dialect == "sqlite":
            await connection.execute(text("BEGIN"))
        await connection.run_sync(lambda sync: TABLES[0].metadata.create_all(sync, tables=TABLES))
    config = app_config.AppConfig.model_validate({"sandbox": {"use": "deerflow.sandbox.local:LocalSandboxProvider"}})
    for name in ("_app_config", "_app_config_path", "_app_config_mtime", "_app_config_signature", "_app_config_is_custom"):
        monkeypatch.setattr(app_config, name, getattr(app_config, name))
    app_config.set_app_config(config)
    monkeypatch.setenv("DEER_FLOW_AUTH_DISABLED", "0")
    user = User(id=UUID(OWNER), email="composition@example.com", system_role="user")
    provider = ControlledProvider(user)
    for module in (deps, services, channel_execution_reads):
        monkeypatch.setattr(module, "get_local_provider", lambda: provider)
    await e.thread_store.create(THREAD, assistant_id="lead_agent", user_id=OWNER)
    assert await AgentConversations(None, session_factory=e.sf).binding(THREAD) is None

    model = FakeMessagesListChatModel(responses=[AIMessage(ANSWER)])
    calls, humans, factories, requests, responses = [], [], [], [], []

    async def node(state, config):
        calls.append(config.get("run_id"))
        humans.extend(message for message in state["messages"] if message.type == "human")
        return {"messages": [await model.ainvoke(state["messages"], config)]}

    graph = StateGraph(MessagesState)
    graph.add_node("model", node)
    graph.add_edge(START, "model")
    graph.add_edge("model", END)
    compiled = graph.compile()

    def factory(**kwargs):
        factories.append(tuple(type(callback).__name__ for callback in kwargs["config"]["callbacks"]))
        return compiled

    def resolve(assistant_id):
        assert assistant_id == "lead_agent"
        return factory

    monkeypatch.setattr(services, "resolve_agent_factory", resolve)
    for name in ("capture_output_snapshot", "capture_workspace_snapshot"):
        monkeypatch.setattr("deerflow.runtime.runs.worker." + name, AsyncMock(return_value=None))
    monkeypatch.setattr("deerflow.runtime.runs.worker._produced_output_paths", AsyncMock(return_value=[]))

    bridge = MemoryStreamBridge()
    prototype = _make_start_run_request(e.manager, thread_store=e.thread_store)
    app = FastAPI()
    app.add_middleware(CSRFMiddleware)
    app.add_middleware(AuthMiddleware)
    for key, value in vars(prototype.app.state).items():
        setattr(app.state, key, value)
    app.state.stream_bridge = bridge
    app.state.stream_cleanups = StreamCleanupRegistry(bridge)
    app.state.checkpointer = OwnedCheckpointSaver(InMemorySaver(), profile="memory", loop=asyncio.get_running_loop())
    app.state.store = OwnedStore(prototype.app.state.store, profile="memory", loop=asyncio.get_running_loop())
    app.state.run_event_store = e.event_store
    app.state.run_store = e.repository
    app.state.agent_conversations = AgentConversations(None, session_factory=e.sf)
    app.include_router(thread_runs.router)
    app.include_router(run_output.router, prefix="/api/threads")

    class RecordingASGI(httpx.ASGITransport):
        async def handle_async_request(self, request):
            # Never store authority headers/cookies in diagnostic artifacts.
            requests.append((request.method, request.url.path, json.loads(request.content) if request.content else None))
            response = await super().handle_async_request(request)
            responses.append((request.method, request.url.path, response.status_code, response.headers.get("X-DeerFlow-Channel-Execution-Version")))
            return response

    owner = GatewayClientOwner(gateway_url="http://gateway.test", langgraph_url="http://gateway.test/api", transport=RecordingASGI(app=app))
    admission = Admission()
    scope = ChannelExecutionScope(PUBLICATION, OWNER, None, "chat", THREAD, owner.endpoint_id, "lead_agent", None)
    facade = ChannelExecutionFacade(owner, scope=scope, admission=admission, capture_output=True)
    body = dict(
        assistant_id="lead_agent",
        input={"messages": [{"role": "human", "content": QUESTION}]},
        output_capture={"version": 1},
        channel_execution={"version": 1, "publication_id": PUBLICATION},
        on_disconnect="continue",
        on_completion=None,
        multitask_strategy="reject",
    )
    try:
        response = await owner.request_json(
            operation="run_create",
            owner_user_id=OWNER,
            method="POST",
            path=f"threads/{THREAD}/runs",
            body=body,
            headers={"Idempotency-Key": PUBLICATION},
            before_dispatch=lambda: admission.authorize("run_create"),
            after_response=lambda: admission.authorize("run_read"),
        )
        assert response.status_code == 200, response.body
        created = response.body
        run_id = created["run_id"]
        expected_admission = dict(version=1, publication_id=PUBLICATION, owner_user_id=OWNER, thread_id=THREAD, assistant_id="lead_agent", agent_name=None)
        assert created["kwargs"]["channel_execution"] == expected_admission
        assert created["kwargs"]["output_capture"] == {"version": 1}
        record = await e.manager.get(run_id, user_id=OWNER)
        assert record.task is not None
        await asyncio.wait_for(asyncio.shield(record.task), 10)
        stored = await e.repository.get(run_id, user_id=OWNER)
        assert stored["status"] == "success"
        assert services.start_run is original_start and services.run_agent is run_agent
        assert len(calls) == len(factories) == len(humans) == 1
        assert "RunJournal" in factories[0]
        human = humans[0]
        assert human.id and human.content == QUESTION
        assert human.additional_kwargs["channel_turn_id"] == PUBLICATION
        receipt = stored["kwargs"]["output_evidence"]
        assert receipt["kind"] == "text" and receipt["input_message_id"] == human.id
        assert receipt["assistant_message_id"] and receipt["text_sha256"] == canonical_text_digest(ANSWER)
        source_rows = await e.event_store.list_messages_by_run(THREAD, run_id, user_id=OWNER)
        sealed = next(row for row in source_rows if row["seq"] == receipt["event_seq"])
        assert sealed["content"]["id"] == receipt["assistant_message_id"]
        assert sealed["content"]["content"] == ANSWER
        projection = await facade.get_run_output(run_id)
        assert projection.state == "complete" and projection.source.text == ANSWER
        assert projection.channel_execution.model_dump(mode="json") == expected_admission
        assert projection.output_evidence.model_dump(mode="json") == receipt
        assert sum(method == "POST" for method, _path, _body in requests) == 1
        assert responses[-1][-1] == "1"
        assert len(await e.repository.list_by_thread(THREAD, user_id=OWNER)) == 1

    finally:
        await e.manager.shutdown(timeout=2)
        await owner.stop()
        await app.state.stream_cleanups.close()
        await bridge.close()
