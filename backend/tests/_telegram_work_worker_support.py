"""Default Telegram owners and real Gateway workers on one isolated SQL engine.

Provider/Bot API transports, prepared directory storage, a ready host status and
health route, memory checkpoints and stream transport are explicit adapters.
This qualifies worker provenance, not a full Gateway lifespan or containment.
"""

import asyncio
import json
import secrets
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace
from uuid import uuid4
from zoneinfo import ZoneInfo

import httpx
from _storage_spaces_test_support import FixtureCatalog
from _telegram_bot_test_support import ManualClock, RecordingRequest, update_payload
from _telegram_compose_turn_support import BOT, OWNER, PERSON, ProviderWire, _answer_every_call, _delivered, _finish
from _telegram_repository_test_support import code_of
from fastapi import FastAPI
from langgraph.checkpoint.memory import InMemorySaver
from sqlalchemy import select
from telegram import Update
from test_gateway_services import _make_start_run_request

from app.channels.manager import ChannelManager
from app.channels.message_bus import MessageBus
from app.channels.store import JsonChannelStore
from app.gateway import chat_storage, deps, storage_spaces
from app.gateway.auth.local_provider import LocalAuthProvider
from app.gateway.auth.repositories.sqlite import SQLiteUserRepository
from app.gateway.auth_middleware import AuthMiddleware
from app.gateway.csrf_middleware import CSRFMiddleware
from app.gateway.refusals import refusal_handler
from app.gateway.routers import agent_instances, agent_work, artifacts, features, run_output, thread_runs, threads, uploads
from app.gateway.stream_cleanup import StreamCleanupRegistry
from app.telegram_bots import turns
from app.telegram_bots.cipher import token_cipher, wrap_token
from app.telegram_bots.config import load_config
from app.telegram_bots.gateway_client import GatewayClientOwner
from app.telegram_bots.supervisor import TelegramSupervisor
from deerflow.agent_instances.contract import DefinitionSnapshot, Refusal
from deerflow.agent_instances.conversations import AgentConversations
from deerflow.agent_instances.directory import InstanceDirectory
from deerflow.agent_instances.runtime import InstanceSandboxProvider
from deerflow.agent_instances.service import AgentInstances
from deerflow.config import app_config
from deerflow.config.paths import Paths, paths_scope
from deerflow.extensions import get_loaded_extensions, load_extensions, set_loaded_extensions
from deerflow.features.plugins import with_default_features
from deerflow.persistence import models  # noqa: F401 -- register complete SQL metadata
from deerflow.persistence.base import Base
from deerflow.persistence.channel_connections.sql import ChannelConnectionRepository
from deerflow.persistence.spaces.files import SpaceBackingRow
from deerflow.persistence.thread_meta.sql import ThreadMetaRepository
from deerflow.persistence.user.model import UserRow
from deerflow.runtime import MemoryStreamBridge
from deerflow.runtime.capture_resources import OwnedCheckpointSaver, OwnedStore
from deerflow.runtime.runs.manager import RunManager
from deerflow.spaces.contract import PrincipalRef
from deerflow.spaces.filesystem import ConfinedFilesystem
from deerflow.spaces.principals import HostPrincipalResolver
from deerflow.spaces.registry import SpaceRegistry
from deerflow.spaces.service import SpaceFiles
from deerflow.utils.file_io import await_drained


@dataclass(frozen=True)
class PreparedDirectory:
    spec: object
    data_path: Path
    control_path: Path
    device: int
    root_inode: int
    control_inode: int

    def filesystem(self):
        return ConfinedFilesystem(self.data_path, self.control_path)


def directory_catalog(root):
    catalog = FixtureCatalog(root)
    catalog.verified = {key: PreparedDirectory(volume.spec, volume.data_path, volume.control_path, volume.device, volume.root_inode, volume.control_inode) for key, volume in catalog.verified.items()}
    return catalog


class WorkerProviderWire(ProviderWire):
    """The provider SDK receives its requested streaming or JSON wire shape."""

    def __init__(self, *args):
        super().__init__(*args)
        self.errors = []

    def respond(self, request):
        try:
            return self._respond(request)
        except Exception as error:
            self.errors.append((type(error).__name__, str(error)))
            raise

    def _respond(self, request):
        response = super().respond(request)
        if json.loads(request.content or b"{}").get("stream") is True or response.headers.get("content-type") != "text/event-stream":
            return response
        chunks = [json.loads(line[6:]) for line in response.read().decode().splitlines() if line.startswith("data: ") and line != "data: [DONE]"]
        message, calls, finish = {"role": "assistant", "content": ""}, {}, None
        for chunk in chunks:
            for choice in chunk.get("choices", []):
                delta = choice["delta"]
                message["content"] += delta.get("content") or ""
                finish = choice.get("finish_reason") or finish
                for change in delta.get("tool_calls", []):
                    call = calls.setdefault(change["index"], {"id": "", "type": "function", "function": {"name": "", "arguments": ""}})
                    call["id"] += change.get("id", "")
                    for key in ("name", "arguments"):
                        call["function"][key] += change.get("function", {}).get(key, "")
        if calls:
            message["tool_calls"] = [calls[key] for key in sorted(calls)]
        first = chunks[0]
        for index, call in enumerate(message.get("tool_calls", [])):
            call["id"] = first["id"] + "-call-" + str(index)
        return httpx.Response(
            200,
            json={
                "id": first["id"],
                "object": "chat.completion",
                "created": first["created"],
                "model": first["model"],
                "choices": [{"index": 0, "message": message, "finish_reason": finish}],
                "usage": {"prompt_tokens": 10, "completion_tokens": 5, "total_tokens": 15},
            },
            request=request,
        )


@asynccontextmanager
async def installed_worker(e, monkeypatch, tmp_path):
    config = await asyncio.to_thread(app_config.get_app_config)
    async with e.engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)
    users = SQLiteUserRepository(e.sf)
    async with e.sf.begin() as writer:
        writer.add(UserRow(id=OWNER, email="work-worker@example.com", password_hash="unusable-fixture", system_role="user"))
    monkeypatch.setattr(deps, "_cached_repo", users)
    monkeypatch.setattr(deps, "_cached_local_provider", LocalAuthProvider(users))
    human = storage_spaces.human_lookup(users)
    directory = InstanceDirectory(e.sf, human=human)
    root = tmp_path / "prepared-directories"
    await asyncio.to_thread(root.mkdir)
    catalog = await asyncio.to_thread(directory_catalog, root)
    files = SpaceFiles(SpaceRegistry(e.sf, HostPrincipalResolver(human=human, nonhuman=directory.lookup, nonhuman_in_session=directory.lookup_in_session)), catalog)
    agents = AgentInstances(files, directory)
    authority = AgentConversations(agents)
    owner = PrincipalRef("human", OWNER)
    instance = await agents.create(
        actor=owner,
        creation_id=uuid4().hex,
        name="Documentation assistant",
        custody="personal",
        supervisor=owner,
        definition=DefinitionSnapshot.capture(owner_id=OWNER, config={"name": "analyst", "skills": [], "mcp_plugins": [], "work_policy": {"enabled": True}}, soul="Review documentation and report verified findings."),
    )
    async with e.sf() as reader:
        backing = await reader.scalar(select(SpaceBackingRow).where(SpaceBackingRow.space_id == instance.home_id))
    home = catalog.verified[backing.slot_id].data_path

    async def prepare(bound, *, app_config, run_id):
        # Actual execution authority supplies every binding and output root.
        assert bound.instance.id == instance.id and bound.authority.instances.files is files
        await bound.validate()
        sandbox = SimpleNamespace(id="directory-prepared", close=lambda: None, release_command_scope=lambda _owner: None, requires_container_recycle=False, execution_outcome_uncertain=False)
        return InstanceSandboxProvider(bound, sandbox, app_config=app_config, skill_revision="directory-fixture")

    monkeypatch.setattr("deerflow.agent_instances.runtime.prepare_environment", prepare)
    # The real CopyGuard still journals and compares its host observation. Its
    # external Docker identity read alone is scripted; no daemon is contacted.
    monkeypatch.setattr("app.telegram_bots.deployment.host_identity", lambda: "a" * 64)
    previous = get_loaded_extensions()
    extensions, _diagnostics = await asyncio.to_thread(lambda: load_extensions(with_default_features(config.plugins)))
    set_loaded_extensions(extensions)
    bridge = MemoryStreamBridge()
    runs = RunManager(store=e.repository, run_ownership_config=config.run_ownership, event_store=e.event_store)
    thread_store = ThreadMetaRepository(e.sf, instance_authority=authority)
    prototype = _make_start_run_request(runs, thread_store=thread_store)
    app = FastAPI()
    app.add_exception_handler(Refusal, refusal_handler)
    app.add_middleware(CSRFMiddleware)
    app.add_middleware(AuthMiddleware)
    for key, value in vars(prototype.app.state).items():
        setattr(app.state, key, value)
    app.state.extensions = extensions
    app.state.stream_bridge = bridge
    app.state.stream_cleanups = StreamCleanupRegistry(bridge)
    app.state.checkpointer = OwnedCheckpointSaver(InMemorySaver(), profile="memory", loop=asyncio.get_running_loop())
    app.state.store = OwnedStore(prototype.app.state.store, profile="memory", loop=asyncio.get_running_loop())
    app.state.run_event_store, app.state.run_store = e.event_store, e.repository
    app.state.agent_instances, app.state.agent_conversations = agents, authority
    app.state.storage_spaces, app.state.storage_spaces_enabled = files, True
    app.state.storage_spaces_config = config.storage_spaces.model_copy(update={"user_spaces": False})
    app.state.agent_execution = storage_spaces.ExecutionStatus(host_id="directory-prepared-host")
    for router in (threads.router, thread_runs.router, artifacts.router, uploads.router, features.router, agent_instances.router, agent_work.router):
        app.include_router(router)
    app.include_router(run_output.router, prefix="/api/threads")

    @app.get("/health")
    async def health():
        return {"status": "ok"}

    errors, held_errors = [], []
    actual_held = turns._Turn.held

    def held(turn, event, error=None, **facts):
        if error is not None:
            held_errors.append(error)
        return actual_held(turn, event, error, **facts)

    monkeypatch.setattr(turns._Turn, "held", held)

    class GatewayTransport(httpx.AsyncBaseTransport):
        def __init__(self):
            self.inner = httpx.ASGITransport(app=app)

        async def handle_async_request(self, request):
            response = await self.inner.handle_async_request(request)
            if response.status_code >= 400:
                errors.append((request.url.path, response.status_code, (await response.aread()).decode()))
            return response

        async def aclose(self):
            await self.inner.aclose()

    transport = GatewayTransport()
    gateway = GatewayClientOwner(gateway_url="http://gateway.test", langgraph_url="http://gateway.test/api", transport=transport)
    connections = ChannelConnectionRepository(e.sf)
    bus = MessageBus()
    store = await asyncio.to_thread(JsonChannelStore, str(tmp_path / "channels.json"))
    manager = ChannelManager(bus, store, connection_repo=connections, langgraph_url="http://gateway.test/api", gateway_url="http://gateway.test", max_concurrency=1)
    service = SimpleNamespace(bus=bus, manager=manager)
    normal, updates = RecordingRequest(), RecordingRequest()
    _answer_every_call(normal, monkeypatch)
    default_request = normal.do_request

    async def sdk_request(url, method, request_data=None, **kwargs):
        if url.rsplit("/", 1)[-1] == "getMe" and not normal.replies["getMe"]:
            normal.enqueue_get_me(bot_id=BOT)
        return await default_request(url, method, request_data, **kwargs)

    monkeypatch.setattr(normal, "do_request", sdk_request)
    clock = ManualClock(datetime.now(UTC))
    env = {"HARTMESH_PROVIDER_KEYS_SECRET": secrets.token_urlsafe(48)}
    host = TelegramSupervisor(
        app,
        config=load_config(config),
        channel_service=service,
        session_factory=e.sf,
        app_config=config,
        environ=env,
        thread_client=gateway,
        request_factory=lambda **kw: normal if kw["connection_pool_size"] == 8 else updates,
        health_client_factory=lambda **kw: httpx.AsyncClient(transport=transport),
        clock=clock.now,
        monotonic=clock.monotonic,
        sleep=clock.sleep,
    )
    paths = await asyncio.to_thread(Paths, tmp_path / "data")
    storage = chat_storage.chat_storage_lifecycle(app, enabled=True)
    storage_entered = False
    with paths_scope(paths):
        try:
            await storage.__aenter__()
            storage_entered = True
            await host.start()
            assert host.cause() is None
            await manager.start()
            await asyncio.to_thread(ZoneInfo, "UTC")
            await host.settings.ensure(OWNER, timezone="UTC", locale="en-US")
            token = str(BOT) + ":" + secrets.token_urlsafe(30)
            link = await host.links.add(owner_user_id=OWNER, agent_kind="instance", agent_ref=instance.id, bot_id=BOT, bot_username="work_worker_bot", encrypted_token=wrap_token(token_cipher(env), BOT, token), actor_id=OWNER)
            code = code_of(await host.links.mint_code(link))
            normal.enqueue_get_me(bot_id=BOT)
            updates.enqueue("getUpdates", result=[], release=asyncio.Event())
            await host.add(link, token)
            channel = host.channel(link.id)
            assert channel is not None and host.work_chat is not None
            start = Update.de_json(update_payload(201, message_id=101, account=PERSON, text="/start " + code), channel.bot)
            assert await channel.handle_update(start)
            await channel._drain_ingress()
            link = await host.links.get(link.id)
            assert link.state == "connected" and link.telegram_user_id == PERSON, link.state
            result = SimpleNamespace(e=e, app=app, host=host, manager=manager, link=link, channel=channel, clock=clock, normal=normal, home=home, instance=instance, gateway=gateway, http_errors=errors, held_errors=held_errors)
            await asyncio.wait_for(_delivered(e, host, clock), 30)
            yield result
        finally:

            async def drain_workers():
                await runs.shutdown(timeout=2)
                # Shutdown bounds its grace; the test retains any finalizer
                # beyond that grace until it finishes with SQL still open.
                tasks = [record.task for record in runs._runs.values() if record.task is not None]
                if tasks:
                    await asyncio.gather(*tasks, return_exceptions=True)
                assert all(task.done() for task in tasks)

            failures = []

            async def close_storage():
                if storage_entered:
                    await storage.__aexit__(None, None, None)

            try:
                for close in (host.begin_shutdown, manager.stop, host.stop, drain_workers, app.state.stream_cleanups.close, bridge.close, close_storage):
                    try:
                        await await_drained(close())
                    except BaseException as error:
                        failures.append(error)
            finally:
                set_loaded_extensions(previous)
            if failures:
                raise BaseExceptionGroup("Installed Telegram worker cleanup failed", failures)


async def propose(e):
    update = Update.de_json(update_payload(2701, message_id=2701, account=PERSON, text="Review the guide and give me a result to review."), e.channel.bot)
    assert await e.channel.handle_update(update)
    await e.channel._drain_ingress()
    turn = next(iter(e.host.turns._turns.values()))
    await asyncio.wait_for(_finish(turn, e.clock), 45)
    if e.held_errors:
        raise e.held_errors[0]
    await asyncio.wait_for(_delivered(e.e, e.host, e.clock), 30)
    return turn
