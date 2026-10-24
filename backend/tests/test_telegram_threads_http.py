"""Public SDK, real Gateway auth/CSRF and stored thread/binding authority."""

import asyncio
import json
from types import SimpleNamespace
from uuid import uuid4

import httpx
import pytest
from _telegram_repository_test_support import db as db
from fastapi import FastAPI
from langgraph.checkpoint.memory import InMemorySaver
from sqlalchemy import select, text
from test_telegram_bot_adapter import build as build_channel
from test_telegram_bot_adapter import start as start_channel
from test_telegram_supervisor import _supervisor_logging as _supervisor_logging
from test_telegram_threads import close, environment, inbound, origin
from test_threads_router import _RawStateAccessor

from app.gateway.auth_middleware import AuthMiddleware
from app.gateway.csrf_middleware import CSRFMiddleware
from app.gateway.routers import threads
from app.telegram_bots.gateway_client import GatewayClientOwner
from deerflow.agent_instances.conversations import AgentConversations
from deerflow.agent_instances.directory import InstanceDirectory
from deerflow.agent_instances.service import AgentInstances
from deerflow.persistence.agent_instances.model import AgentConversationRow, AgentDefinitionRevisionRow, AgentInstanceGrantRow, AgentInstanceRow
from deerflow.persistence.base import Base
from deerflow.persistence.channel_connections.model import ChannelConversationRow
from deerflow.persistence.spaces.model import SpaceRow
from deerflow.persistence.thread_meta.model import ThreadMetaRow
from deerflow.persistence.thread_meta.sql import ThreadMetaRepository
from deerflow.spaces.contract import ResolvedPrincipal

pytestmark = pytest.mark.asyncio


async def http_environment(db, monkeypatch):
    e = await environment(db, monkeypatch)
    monkeypatch.setenv("DEER_FLOW_AUTH_DISABLED", "0")
    tables = [SpaceRow.__table__, AgentDefinitionRevisionRow.__table__, AgentInstanceRow.__table__, AgentInstanceGrantRow.__table__, AgentConversationRow.__table__, ThreadMetaRow.__table__]
    async with db.engine.begin() as connection:
        if db.dialect == "sqlite":
            await connection.execute(text("BEGIN"))
        await connection.run_sync(lambda sync: Base.metadata.create_all(sync, tables=tables))
    app = FastAPI()
    app.add_middleware(CSRFMiddleware)
    app.add_middleware(AuthMiddleware)
    app.state.thread_store = ThreadMetaRepository(db.sessions)
    app.state.checkpointer = InMemorySaver()

    async def human(reference):
        from app.gateway.auth.mode import account_refusal

        user = await e.live.users.get_user_by_id(reference.subject_id)
        return ResolvedPrincipal(reference) if user is not None and account_refusal(user, sign_on_only_mode=False) is None else None

    instances = AgentInstances(None, InstanceDirectory(db.sessions, human=human))
    app.state.agent_conversations = AgentConversations(instances)
    app.state.run_manager = SimpleNamespace()
    app.include_router(threads.router)
    e.observed = []

    @app.middleware("http")
    async def observe(request, next_handler):
        response = await next_handler(request)
        e.observed.append((request.method, request.url.path, str(getattr(getattr(request.state, "user", None), "id", "")), request.headers.get("x-csrf-token") == request.cookies.get("csrf_token")))
        return response

    async def accessor(request, *, thread_id, assistant_id=None, checkpoint_id=None):
        # Graph execution is not activated by this PR. Materialize the actual
        # checkpointer without constructing a model; auth/stores remain real.
        return _RawStateAccessor(app.state.checkpointer), {"configurable": {"thread_id": thread_id, "checkpoint_ns": ""}}

    monkeypatch.setattr(threads, "abuild_checkpoint_state_accessor", accessor)
    await e.http_client.stop()
    e.http_client = GatewayClientOwner(gateway_url="http://gateway.example", langgraph_url="http://runtime.example/api", transport=httpx.ASGITransport(app=app))
    e.threads.client = e.http_client
    e.http_app = app
    return e


async def test_sdk_create_replay_and_get_prove_actual_stored_owner_with_real_middleware(db, monkeypatch):
    e = await http_environment(db, monkeypatch)
    try:
        identity = await e.threads.ensure_thread(e.channel.link, inbound(e), origin=await origin(e))
        record = await e.http_app.state.thread_store.get(identity, user_id=e.link.owner_user_id)
        assert record["user_id"] == e.link.owner_user_id
        assert e.observed == [("POST", "/api/threads", e.link.owner_user_id, True), ("GET", f"/api/threads/{identity}", e.link.owner_user_id, True)]
        replay = await e.http_client.create_thread(e.link.owner_user_id, thread_id=identity, metadata={"owner_user_id": "forged-owner"})
        assert replay["owner_user_id"] == e.link.owner_user_id
        proof = await e.http_client.get_thread(e.link.owner_user_id, identity)
        assert proof["owner_user_id"] == e.link.owner_user_id and proof["agent_instance"] is None
    finally:
        await close(e)


async def test_create_replay_default_is_not_proof_when_real_binding_exists(db, monkeypatch):
    e = await http_environment(db, monkeypatch)
    try:
        row = await origin(e)
        owner_check = await e.live.authority.prepare_live_owner(e.link.owner_user_id)
        intent = await db.links.reserve_thread_preparation(e.link, row, purpose="ensure", endpoint_id=e.http_client.endpoint_id, check_live_owner=owner_check, check_attempt_current=lambda: True)
        identity, agent, home = intent.creation_id, uuid4().hex, uuid4().hex
        await e.http_app.state.thread_store.create(identity, user_id=e.link.owner_user_id, metadata={"channel_agent_name": "lead_agent"})
        async with db.sessions.begin() as writer:
            writer.add(
                SpaceRow(
                    id=home,
                    backing_handle=uuid4().hex,
                    name="Fixture Home",
                    custody_kind="company",
                    custodian_kind=None,
                    custodian_id=None,
                    mode="native",
                    status="active",
                    generation=1,
                    feature_namespace=None,
                    feature_controller=None,
                    feature_metadata_version=None,
                )
            )
            writer.add(AgentDefinitionRevisionRow(revision="a" * 64, owner_id=e.link.owner_user_id, config={}, soul=""))
            await writer.flush()
            writer.add(
                AgentInstanceRow(
                    id=agent,
                    principal_id="agent:" + agent,
                    name="Fixture AI employee",
                    custody="company",
                    owner_id=None,
                    creator_id=e.link.owner_user_id,
                    supervisor_id=e.link.owner_user_id,
                    definition_revision="a" * 64,
                    home_id=home,
                    status="active",
                    generation=1,
                    creation_id=uuid4().hex,
                    creation_request={},
                )
            )
            await writer.flush()
            writer.add(AgentInstanceGrantRow(instance_id=agent, user_id=e.link.owner_user_id, permissions=2))
            writer.add(AgentConversationRow(thread_id=identity, instance_id=agent, requester_id=e.link.owner_user_id, creation_id=uuid4().hex))
        replay = await e.http_client.create_thread(e.link.owner_user_id, thread_id=identity, metadata={"channel_agent_name": "lead_agent"})
        assert replay["agent_instance"] is None  # the POST's default is unsafe proof
        proof = await e.http_client.get_thread(e.link.owner_user_id, identity)
        assert proof["agent_instance"]["id"] == agent
        with pytest.raises(ValueError, match="invalid-thread-proof"):
            await e.threads.ensure_thread(e.link, inbound(e), origin=row)
        assert await db.connections.get_thread_id(e.link.connection_id, str(e.link.chat_id)) is None
    finally:
        await close(e)


async def test_sdk_strict_get_refuses_absent_remote_binding_authority(db, monkeypatch):
    e = await http_environment(db, monkeypatch)
    del e.http_app.state.agent_conversations
    try:
        with pytest.raises(httpx.HTTPStatusError) as failure:
            await e.threads.ensure_thread(e.link, inbound(e), origin=await origin(e))
        assert failure.value.response.status_code == 503
        assert await db.connections.get_thread_id(e.link.connection_id, str(e.link.chat_id)) is None
    finally:
        await close(e)


@pytest.mark.parametrize("lost", [False, True])
async def test_actual_committed_http_creation_survives_token_rotation_and_response_loss(db, monkeypatch, lost):
    entered, release = asyncio.Event(), asyncio.Event()
    e = await http_environment(db, monkeypatch)

    class CommitBarrier(httpx.AsyncBaseTransport):
        def __init__(self):
            self.inner = httpx.ASGITransport(app=e.http_app)
            self.created_ids = []

        async def handle_async_request(self, request):
            response = await self.inner.handle_async_request(request)
            if request.method == "POST":
                self.created_ids.append(json.loads(request.content)["thread_id"])
                if len(self.created_ids) == 1:
                    entered.set()
                    await release.wait()
                    if lost:
                        await response.aclose()
                        raise httpx.ReadError("synthetic response loss after committed POST")
            return response

        async def aclose(self):
            await self.inner.aclose()

    barrier = CommitBarrier()
    await e.http_client.stop()
    e.http_client = GatewayClientOwner(gateway_url="http://gateway.example", langgraph_url="http://runtime.example/api", transport=barrier)
    e.threads.client = e.http_client
    task, successor = None, None
    try:
        row = await origin(e)
        task = asyncio.create_task(e.threads.ensure_thread(e.link, inbound(e), origin=row))
        await asyncio.wait_for(entered.wait(), 3)
        async with db.sessions() as reader:
            committed = list(await reader.scalars(select(ThreadMetaRow)))
        assert len(committed) == 1 and committed[0].thread_id == barrier.created_ids[0]
        current = await db.links.store_token(e.link.id, "synthetic-new-wrapped-token", actor_id=e.link.owner_user_id, expected_revision=e.link.token_revision, token_changed=True)
        successor = await build_channel(db, connected=False, link=current, construct_off_loop=True)
        await start_channel(successor)
        e.current[0] = successor.channel
        release.set()
        with pytest.raises(httpx.ReadError if lost else ValueError):
            await task
        assert await db.connections.get_thread_id(e.link.connection_id, str(e.link.chat_id)) is None
        ready = await e.threads.ensure_thread(current, inbound(e), origin=row)
        assert barrier.created_ids == [ready, ready]
        async with db.sessions() as reader:
            assert list(await reader.scalars(select(ThreadMetaRow.thread_id))) == [ready]
            assert list(await reader.scalars(select(ChannelConversationRow.thread_id))) == [ready]
    finally:
        release.set()
        if task is not None:
            await asyncio.gather(task, return_exceptions=True)
        if successor is not None:
            await successor.channel.stop()
        await close(e)
