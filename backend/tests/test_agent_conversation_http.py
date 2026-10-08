"""Actual HTTP routes and mandatory middleware use durable instance authority."""

import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock

import httpx
import pytest
from _storage_spaces_test_support import ALICE, BOB, operation
from fastapi import FastAPI
from starlette.responses import StreamingResponse
from test_agent_conversations import conversations
from test_agent_instances import create
from test_agent_instances import instances as instances

from deerflow.agent_instances.contract import AgentPermission
from deerflow.persistence.agent_instances.model import AgentInstanceGrantRow, AgentProtectedContextRow
from deerflow.runtime.events.store.memory import MemoryRunEventStore
from deerflow.runtime.runs.manager import RunManager
from deerflow.runtime.runs.store.memory import MemoryRunStore


def app_for(agents, authority, threads, monkeypatch, *, actor=BOB, permissions=None):
    from app.gateway import auth_middleware, deps
    from app.gateway.routers import agent_instances, artifacts, runs, thread_runs
    from app.gateway.routers import threads as thread_routes

    app = FastAPI()
    from deerflow.config.app_config import AppConfig

    config = AppConfig.model_validate({"sandbox": {"use": "deerflow.sandbox.local:LocalSandboxProvider"}})
    monkeypatch.setattr(deps, "get_config", lambda: config)
    app.state.agent_instances = agents
    app.state.agent_conversations = authority
    app.state.thread_store = threads
    app.state.run_store = MemoryRunStore()
    app.state.run_manager = RunManager(store=app.state.run_store)
    app.state.run_event_store = MemoryRunEventStore()
    user = SimpleNamespace(id=actor.subject_id, system_role="user")
    monkeypatch.setattr(deps, "get_current_user_from_request", AsyncMock(return_value=user))
    monkeypatch.setattr(auth_middleware, "is_auth_disabled", lambda: False)
    monkeypatch.setattr(auth_middleware, "is_valid_internal_auth_token", lambda token: False)
    monkeypatch.setattr(
        auth_middleware,
        "resolve_route_permissions",
        AsyncMock(return_value=permissions if permissions is not None else ["agents:read", "agents:write", "threads:read", "threads:write", "threads:delete", "runs:read", "runs:create", "runs:cancel"]),
    )
    monkeypatch.setattr(agent_instances, "get_agents_api_config", lambda: SimpleNamespace(enabled=True))
    app.add_middleware(auth_middleware.AuthMiddleware)
    app.include_router(agent_instances.router)
    app.include_router(thread_runs.router)
    app.include_router(runs.router)
    app.include_router(thread_routes.router)
    app.include_router(artifacts.router)
    return app


@pytest.mark.asyncio
async def test_http_server_mints_bound_chat_and_ceiling_cannot_be_widened(instances, monkeypatch):
    agents, authority, threads, sf = await conversations(instances)
    agent = await create(agents, custody="company", supervisor=BOB)
    app = app_for(agents, authority, threads, monkeypatch)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test", cookies={"access_token": "fixture"}) as client:
        endpoint = f"/api/agent-instances/{agent.id}/conversations"
        forged = await client.post(endpoint, json={"creation_id": operation(), "thread_id": "old-private-history"})
        assert forged.status_code == 422
        key = operation()
        first = await client.post(endpoint, json={"creation_id": key})
        assert first.status_code == 201, first.text
        assert await authority.binding(first.json()["thread_id"]) == agent.id
        again = await client.post(endpoint, json={"creation_id": key})
        assert again.status_code == 201 and again.json()["thread_id"] == first.json()["thread_id"]
    limited = app_for(agents, authority, threads, monkeypatch, permissions=["agents:read"])
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=limited), base_url="http://test", cookies={"access_token": "fixture"}) as client:
        assert (await client.post(endpoint, json={"creation_id": operation()})).status_code == 403


@pytest.mark.asyncio
async def test_http_shared_run_evidence_is_visible_to_inspector_but_mutations_are_not(instances, monkeypatch):
    agents, authority, threads, sf = await conversations(instances)
    agent = await create(agents, custody="company", supervisor=BOB)
    chat = await authority.create(actor=ALICE, instance_id=agent.id, creation_id=operation())
    async with sf() as session, session.begin():
        (await session.get(AgentInstanceGrantRow, (agent.id, "bob"))).permissions = int(AgentPermission.INSPECT)
    app = app_for(agents, authority, threads, monkeypatch)
    record = await app.state.run_manager.create(chat["thread_id"], user_id="alice")
    endpoint = f"/api/threads/{chat['thread_id']}"
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test", cookies={"access_token": "fixture"}) as client:
        response = await client.get(endpoint + "/runs/" + record.run_id)
        assert response.status_code == 200 and response.json()["run_id"] == record.run_id and record.user_id == "alice", response.text
        for method, suffix in (("POST", "/runs/" + record.run_id + "/cancel"), ("DELETE", ""), ("PATCH", "")):
            denied = await client.request(method, endpoint + suffix, json={})
            assert denied.status_code == 404
        async with sf() as session, session.begin():
            await session.delete(await session.get(AgentInstanceGrantRow, (agent.id, "bob")))
        assert (await client.get(endpoint + "/runs/" + record.run_id)).status_code == 404


@pytest.mark.asyncio
@pytest.mark.parametrize("protected", [False, True])
async def test_stream_stops_before_a_frame_after_current_grant_is_revoked(instances, monkeypatch, protected):
    agents, authority, threads, sf = await conversations(instances)
    agent = await create(agents, custody="company", supervisor=BOB)
    chat = await authority.create(actor=BOB if protected else ALICE, instance_id=agent.id, creation_id=operation())
    if protected:
        async with sf() as session, session.begin():
            session.add(AgentProtectedContextRow(thread_id=chat["thread_id"]))
    app = app_for(agents, authority, threads, monkeypatch)
    opened, release = asyncio.Event(), asyncio.Event()

    @app.get("/api/threads/{thread_id}/qualification-stream")
    async def stream(thread_id: str):
        async def frames():
            yield b"data: first\n\n"
            opened.set()
            await release.wait()
            yield b"data: must-not-disclose-after-revocation\n\n"

        return StreamingResponse(frames(), media_type="text/event-stream")

    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test", cookies={"access_token": "fixture"}) as client:
        task = asyncio.create_task(client.get(f"/api/threads/{chat['thread_id']}/qualification-stream"))
        try:
            await asyncio.wait_for(opened.wait(), 3)
            async with sf() as session, session.begin():
                if protected:
                    (await session.get(AgentInstanceGrantRow, (agent.id, "bob"))).permissions = int(AgentPermission.USE)
                else:
                    await session.delete(await session.get(AgentInstanceGrantRow, (agent.id, "bob")))
            release.set()
            response = await asyncio.wait_for(task, 3)
            assert "must-not-disclose" not in response.text
        finally:
            release.set()
            await asyncio.gather(task, return_exceptions=True)


@pytest.mark.asyncio
async def test_protected_own_use_cannot_read_http_history_or_run_evidence(instances, monkeypatch):
    agents, authority, threads, sf = await conversations(instances)
    agent = await create(agents, custody="company", supervisor=BOB)
    chat = await authority.create(actor=BOB, instance_id=agent.id, creation_id=operation())
    async with sf() as session, session.begin():
        session.add(AgentProtectedContextRow(thread_id=chat["thread_id"]))
        (await session.get(AgentInstanceGrantRow, (agent.id, "bob"))).permissions = int(AgentPermission.USE)
    app = app_for(agents, authority, threads, monkeypatch)
    record = await app.state.run_manager.create(chat["thread_id"], user_id="bob")
    endpoint = f"/api/threads/{chat['thread_id']}"
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test", cookies={"access_token": "fixture"}) as client:
        for method, suffix in (("GET", ""), ("POST", "/history"), ("GET", "/runs/" + record.run_id)):
            response = await client.request(method, endpoint + suffix)
            assert response.status_code == 404, response.text


@pytest.mark.asyncio
async def test_shared_chat_delete_removes_all_requester_evidence_and_retains_home(instances, monkeypatch):
    from app.gateway.routers import threads as routes
    from deerflow.runtime.runs.schemas import RunStatus

    agents, authority, threads, sf = await conversations(instances)
    agent = await create(agents, custody="company", supervisor=BOB)
    chat = await authority.create(actor=ALICE, instance_id=agent.id, creation_id=operation())
    app = app_for(agents, authority, threads, monkeypatch)
    monkeypatch.setattr(routes, "_delete_thread_data", lambda *args, **kwargs: pytest.fail("An instance chat must never clean requester-owned directories"))
    one = await app.state.run_manager.create(chat["thread_id"], user_id="alice")
    await app.state.run_event_store.put(thread_id=chat["thread_id"], run_id=one.run_id, category="message", event_type="llm.ai.response", content={"type": "ai", "content": "shared"})
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test", cookies={"access_token": "fixture"}) as client:
        endpoint = f"/api/threads/{chat['thread_id']}"
        assert (await client.delete(endpoint)).status_code == 409
        await app.state.run_manager.set_status(one.run_id, RunStatus.success)
        response = await client.delete(endpoint)
        assert response.status_code == 200, response.text
        assert response.json()["success"] is True
        assert await threads.get(chat["thread_id"], user_id=None) is None
        assert await authority.binding(chat["thread_id"]) == agent.id
        assert await app.state.run_store.get(one.run_id, user_id=None) is None
        assert await app.state.run_event_store.list_messages(chat["thread_id"]) == []
        assert (await agents.get(actor=BOB, instance_id=agent.id)).home_id == agent.home_id


@pytest.mark.asyncio
async def test_instance_artifact_legacy_writers_and_archives_never_resolve_requester_directories(instances, monkeypatch):
    from app.gateway.routers import artifacts, thread_runs

    agents, authority, threads, sf = await conversations(instances)
    agent = await create(agents, custody="company", supervisor=BOB)
    chat = await authority.create(actor=ALICE, instance_id=agent.id, creation_id=operation())
    app = app_for(agents, authority, threads, monkeypatch)
    monkeypatch.setattr(artifacts, "resolve_outputs_confined_path", lambda *args, **kwargs: pytest.fail("Must use Home Space mutation"))
    monkeypatch.setattr(thread_runs, "get_paths", lambda: pytest.fail("Must use Home Space downloads"))
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test", cookies={"access_token": "fixture"}) as client:
        endpoint = f"/api/threads/{chat['thread_id']}"
        edit = await client.put(endpoint + "/artifacts/mnt/user-data/outputs/result.md", json={"content": "new", "expected_sha256": "a" * 64})
        assert edit.status_code == 501, edit.text
        archive = await client.post(endpoint + "/runs/fixture/artifacts/archive")
        assert archive.status_code == 501, archive.text


@pytest.mark.asyncio
async def test_use_only_staff_can_stop_own_run_but_cannot_stop_other_requesters(instances, monkeypatch):
    agents, authority, threads, sf = await conversations(instances)
    agent = await create(agents, custody="company", supervisor=BOB)
    chat = await authority.create(actor=BOB, instance_id=agent.id, creation_id=operation())
    async with sf() as session, session.begin():
        (await session.get(AgentInstanceGrantRow, (agent.id, "bob"))).permissions = int(AgentPermission.USE)
    app = app_for(agents, authority, threads, monkeypatch)
    own = await app.state.run_manager.create(chat["thread_id"], user_id="bob")
    # Different attribution in a shared chat must not turn Use into Manage.
    other_id = operation()
    await app.state.run_store.put(other_id, thread_id=chat["thread_id"], user_id="alice")
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test", cookies={"access_token": "fixture"}) as client:
        endpoint = f"/api/threads/{chat['thread_id']}/runs/"
        assert (await client.post(endpoint + other_id + "/cancel")).status_code == 404
        stopped = await client.post(endpoint + own.run_id + "/cancel")
        assert stopped.status_code in (202, 204), stopped.text


@pytest.mark.asyncio
async def test_checkpoint_branch_retains_instance_and_never_clones_requester_workspace(instances, monkeypatch):
    from langchain_core.messages import AIMessage, HumanMessage
    from langgraph.checkpoint.base import uuid6
    from langgraph.checkpoint.memory import InMemorySaver
    from langgraph.store.memory import InMemoryStore
    from test_threads_router import _write_checkpoint

    from app.gateway import services
    from app.gateway.routers import threads as routes
    from deerflow.runtime.checkpoint_state import build_state_mutation_graph

    agents, authority, threads, sf = await conversations(instances)
    agent = await create(agents, custody="company", supervisor=BOB)
    chat = await authority.create(actor=ALICE, instance_id=agent.id, creation_id=operation())
    app = app_for(agents, authority, threads, monkeypatch)
    app.state.checkpointer = InMemorySaver()
    app.state.store = InMemoryStore()
    app.state.checkpoint_channel_mode = "full"
    graph = build_state_mutation_graph("branch", "full")
    monkeypatch.setattr(services, "resolve_agent_factory", lambda *_: lambda **kwargs: graph)
    monkeypatch.setattr(routes, "_copy_branch_user_data", AsyncMock(side_effect=AssertionError("An instance branch reuses Home")))
    human, answer = HumanMessage(id="human", content="work"), AIMessage(id="answer", content="done")
    parent = await _write_checkpoint(app.state.checkpointer, chat["thread_id"], str(uuid6()), [human], step=1)
    await _write_checkpoint(app.state.checkpointer, chat["thread_id"], str(uuid6()), [human, answer], step=2, parent_config=parent)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test", cookies={"access_token": "fixture"}) as client:
        response = await client.post(f"/api/threads/{chat['thread_id']}/branches", json={"message_id": "answer", "title": "New branch"})
        assert response.status_code == 200, response.text
        new_id = response.json()["thread_id"]
        assert await authority.binding(new_id) == agent.id
        assert response.json()["workspace_clone_mode"] == "shared_agent_home"
        assert (await threads.get(new_id, user_id="bob"))["display_name"] == "New branch"
        assert (await authority.execution(actor=BOB, thread_id=new_id)).instance.home_id == agent.home_id
        assert (await app.state.run_event_store.list_messages(new_id))[0]["content"]["content"] == "work"


@pytest.mark.asyncio
async def test_browser_socket_ownership_cannot_reopen_requester_sessions_for_bound_chats(instances):
    from app.gateway.routers.browser import _browser_thread_owned_by

    agents, authority, threads, sf = await conversations(instances)
    agent = await create(agents)
    chat = await authority.create(actor=ALICE, instance_id=agent.id, creation_id=operation())
    assert not await _browser_thread_owned_by(threads, chat["thread_id"], "alice")


@pytest.mark.asyncio
async def test_registered_instance_outputs_recheck_human_read_export_and_revocation(instances, monkeypatch):
    from app.gateway.routers import artifacts, spaces
    from deerflow.spaces.contract import Permission
    from deerflow.tools.builtins.present_file_tool import present_file_tool

    agents, authority, threads, _ = await conversations(instances)
    agent = await create(agents, custody="company", supervisor=BOB)
    files = agents.files
    home = await files.registry.get(actor=ALICE, space_id=agent.home_id)
    await files.mkdir(actor=ALICE, space_id=home.id, expected_generation=home.generation, operation_id=operation(), path="outputs")
    for path, content in (("outputs/result.txt", b"result"), ("outputs/page.html", b"<html>active</html>")):
        await files.write(actor=ALICE, space_id=home.id, expected_generation=home.generation, operation_id=operation(), path=path, content=content, create=True)
    chat = await authority.create(actor=ALICE, instance_id=agent.id, creation_id=operation())
    execution = await authority.execution(actor=ALICE, thread_id=chat["thread_id"])
    from deerflow.agent_instances.conversations import AGENT_EXECUTION_CONTEXT_KEY

    runtime = SimpleNamespace(context={"thread_id": chat["thread_id"], AGENT_EXECUTION_CONTEXT_KEY: execution}, state={"thread_data": execution.thread_paths})
    reference = "/mnt/user-data/outputs/result.txt"
    # Registration is valid even when the path is missing; retrieval has its own result.
    registered = present_file_tool.func(runtime=runtime, filepaths=[reference, "/mnt/user-data/outputs/missing.txt"], tool_call_id="present")
    assert registered.update["artifacts"] == [reference, "/mnt/user-data/outputs/missing.txt"]
    app = app_for(agents, authority, threads, monkeypatch)
    app.state.storage_spaces = files
    monkeypatch.setattr(spaces, "get_current_user_from_request", AsyncMock(return_value=SimpleNamespace(id=BOB.subject_id)))
    monkeypatch.setattr(artifacts, "get_paths", lambda: pytest.fail("No requester path fallback"), raising=False)
    home = await files.registry.set_grant(actor=ALICE, space_id=home.id, subject=BOB, permissions=Permission.READ, expected_generation=home.generation)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test", cookies={"access_token": "fixture"}) as client:
        endpoint = f"/api/threads/{chat['thread_id']}/artifacts/mnt/user-data/outputs/"
        read = await client.get(endpoint + "result.txt")
        assert read.status_code == 200 and read.content == b"result"
        assert (await client.get(endpoint + "missing.txt")).status_code == 404
        assert (await client.get(endpoint + "result.txt", params={"download": "true"})).status_code == 403
        safe = await client.get(endpoint + "page.html")
        assert safe.status_code == 200 and safe.headers["content-disposition"].startswith("attachment")
        home = await files.registry.set_grant(actor=ALICE, space_id=home.id, subject=BOB, permissions=Permission.READ | Permission.EXPORT, expected_generation=home.generation, acknowledge_existing_data=True)
        exported = await client.get(endpoint + "result.txt", params={"download": "true"})
        assert exported.status_code == 200 and exported.content == b"result"
        await files.registry.set_grant(actor=ALICE, space_id=home.id, subject=BOB, permissions=Permission(0), expected_generation=home.generation)
        assert (await client.get(endpoint + "result.txt")).status_code == 404
        assert registered.update["artifacts"][0] == reference
