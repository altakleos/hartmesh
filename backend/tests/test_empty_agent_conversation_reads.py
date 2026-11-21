"""A new employee conversation can open before its first checkpoint or graph build."""

import asyncio
import os
import threading

import httpx
import pytest
import pytest_asyncio
from _storage_spaces_test_support import ALICE, BOB, operation
from langgraph.checkpoint.memory import InMemorySaver
from langgraph.graph import END, START, MessagesState, StateGraph
from sqlalchemy import text
from test_agent_conversation_http import app_for
from test_agent_conversations import conversations
from test_agent_instances import create
from test_agent_instances import instances as instances

from app.gateway import deps, services
from app.gateway.routers import threads as thread_routes
from deerflow.config.app_config import AppConfig
from deerflow.config.checkpointer_config import CheckpointerConfig
from deerflow.runtime import checkpoint_mode
from deerflow.runtime.checkpointer.async_provider import make_checkpointer


@pytest_asyncio.fixture
async def checkpoint_store(instances, tmp_path):
    engine = instances[2].kw["bind"]
    if engine.dialect.name == "postgresql":
        async with engine.connect() as conn:
            schema = await conn.scalar(text("SELECT current_schema()"))
        settings = CheckpointerConfig(type="postgres", connection_string=os.environ["TEST_POSTGRES_URI"], postgres_schema=schema)
    else:
        settings = CheckpointerConfig(type="sqlite", connection_string=str(tmp_path / "checkpoints.sqlite"))
    config = AppConfig.model_validate({"sandbox": {"use": "deerflow.sandbox.local:LocalSandboxProvider"}, "checkpointer": settings})
    async with make_checkpointer(config) as saver:
        yield saver


@pytest.mark.asyncio
@pytest.mark.parametrize("custody", ["company", "personal"])
async def test_empty_bound_thread_can_open_while_agent_assembly_is_parked(instances, monkeypatch, custody, checkpoint_store):
    agents, authority, threads, _ = await conversations(instances)
    employee = await create(agents, custody=custody)
    chat = await authority.create(actor=ALICE, instance_id=employee.id, thread_id=operation())
    thread = chat["thread_id"]
    app = app_for(agents, authority, threads, monkeypatch, actor=ALICE)
    saver = app.state.checkpointer = checkpoint_store
    workflow = StateGraph(MessagesState)
    workflow.add_node("answer", lambda state: {})
    workflow.add_edge(START, "answer")
    workflow.add_edge("answer", END)
    release, called = threading.Event(), []
    entered = asyncio.Event()
    loop = asyncio.get_running_loop()

    def factory(*, config):
        called.append(config["configurable"]["thread_id"])
        loop.call_soon_threadsafe(entered.set)
        assert release.wait(10), "test did not release the factory"
        return workflow.compile()

    monkeypatch.setattr(services, "resolve_agent_factory", lambda _: factory)
    url = f"/api/threads/{thread}"
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test", cookies={"access_token": "fixture"}) as client:
        both = asyncio.gather(client.get(url), client.post(url + "/history", json={"limit": 1}))
        attempted = asyncio.create_task(entered.wait())
        try:
            done, _ = await asyncio.wait([both, attempted], timeout=8, return_when=asyncio.FIRST_COMPLETED)
        finally:
            release.set()
            attempted.cancel()
            await asyncio.gather(attempted, return_exceptions=True)
        metadata, history = await both
        assert both in done and not called, "opening an empty bound thread waited on the employee graph"
        assert metadata.status_code == history.status_code == 200
        assert metadata.json()["agent_instance"]["id"] == employee.id
        assert metadata.json()["values"] == {} and history.json() == []

        # A historical selector still uses the graph even when the conversation
        # has no root checkpoint. The fast path never changes its semantics.
        previous = await client.post(url + "/history", json={"before": "00000000-0000-0000-0000-000000000001", "limit": 1})
        assert previous.status_code == 200 and previous.json() == []
        assert called == [thread]

        # Keep a real head read open across the very first write. The racing
        # reader can use its observed absence; subsequent reads must see the save.
        head_read, saved = asyncio.Event(), asyncio.Event()
        read_head = thread_routes.root_checkpoint_exists

        async def observe_head(checkpointer, thread_id):
            head = await read_head(checkpointer, thread_id)
            if head is False and not head_read.is_set():
                head_read.set()
                await saved.wait()
            return head

        monkeypatch.setattr(thread_routes, "root_checkpoint_exists", observe_head)
        racing = asyncio.create_task(client.get(url))
        await asyncio.wait_for(head_read.wait(), timeout=8)
        graph = workflow.compile(checkpointer=saver)
        root = {"configurable": {"thread_id": thread}}
        try:
            stored = await graph.aupdate_state(root, {"messages": [{"role": "user", "content": "A saved note."}]}, as_node="__start__")
        finally:
            saved.set()
        assert (await racing).json()["values"] == {}
        checkpoint = stored["configurable"]["checkpoint_id"]
        metadata, history, pinned = await asyncio.gather(client.get(url), client.post(url + "/history", json={"limit": 1}), client.post(url + "/history", json={"before": checkpoint, "limit": 1}))
        assert metadata.json()["values"]["messages"][0]["content"] == "A saved note."
        assert history.json()[0]["values"]["messages"][0]["content"] == "A saved note."
        assert pinned.json()[0]["checkpoint_id"] == checkpoint
        assert called == [thread] * 4

    another = app_for(agents, authority, threads, monkeypatch, actor=BOB)
    another.state.checkpointer = saver
    unopened = await authority.create(actor=ALICE, instance_id=employee.id, thread_id=operation())
    private_url = f"/api/threads/{unopened['thread_id']}"
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=another), base_url="http://test", cookies={"access_token": "fixture"}) as client:
        refused = await asyncio.gather(client.get(private_url), client.post(private_url + "/history", json={"limit": 1}))
        assert [r.status_code for r in refused] == [404, 404]


@pytest.mark.asyncio
async def test_changed_delta_snapshot_cadence_requires_restart_even_before_first_reply(instances, monkeypatch):
    agents, authority, threads, _ = await conversations(instances)
    employee = await create(agents, custody="company")
    chat = await authority.create(actor=ALICE, instance_id=employee.id, thread_id=operation())
    app = app_for(agents, authority, threads, monkeypatch, actor=ALICE)
    app.state.checkpointer = InMemorySaver()
    app.state.checkpoint_channel_mode = "delta"
    config = deps.get_config()
    config.database.checkpoint_channel_mode = "delta"
    config.database.checkpoint_delta.snapshot_frequency = 6
    monkeypatch.setattr(checkpoint_mode, "_frozen_checkpoint_channel_mode", "delta")
    monkeypatch.setattr(checkpoint_mode, "_frozen_checkpoint_snapshot_frequency", 5)
    url = f"/api/threads/{chat['thread_id']}"
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test", cookies={"access_token": "fixture"}) as client:
        for response in (await client.get(url), await client.post(url + "/history", json={"limit": 1})):
            assert response.status_code == 503
            assert "restart-required" in response.json()["detail"]


@pytest.mark.asyncio
async def test_custom_saver_keeps_ordinary_errors_when_the_optional_probe_is_unavailable(instances, monkeypatch):
    class UnreadableSaver(InMemorySaver):
        async def aget_tuple(self, config):
            raise RuntimeError("unavailable fixture")

        async def alist(self, *args, **kwargs):
            raise checkpoint_mode.CheckpointModeMismatchError("incompatible fixture")
            yield

    agents, authority, threads, _ = await conversations(instances)
    employee = await create(agents, custody="company")
    chat = await authority.create(actor=ALICE, instance_id=employee.id, thread_id=operation())
    app = app_for(agents, authority, threads, monkeypatch, actor=ALICE)
    app.state.checkpointer = UnreadableSaver()
    assert await thread_routes.root_checkpoint_exists(app.state.checkpointer, chat["thread_id"]) is None
    url = f"/api/threads/{chat['thread_id']}"
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test", cookies={"access_token": "fixture"}) as client:

        async def failed_probe(*args):
            raise OSError("optional index unavailable")

        for probe in (thread_routes.root_checkpoint_exists, failed_probe):
            monkeypatch.setattr(thread_routes, "root_checkpoint_exists", probe)
            metadata, history = await asyncio.gather(client.get(url), client.post(url + "/history", json={"limit": 1}))
            assert metadata.status_code == 500 and metadata.json()["detail"] == "Failed to get thread"
            assert history.status_code == 409 and "incompatible fixture" in history.json()["detail"]
