"""One Telegram message through the whole stack, on the configuration Compose ships.

Real: the Compose template through its renderer, AppConfig, the plugin loader,
lead-agent assembly, the provider SDK classes, a person seated on a Space, the
Gateway routes with auth and CSRF, the run manager, worker, SQL stores and output
proof, and the Telegram adapter, inbox, turn controller, planner, outbox and sender.

Scripted: the provider's HTTP exchange and the Bot API, because neither may be
reached from a test. The stream bridge is in memory and no sandbox container starts.

httpx's ASGI transport hands over a run's stream only when the run ends, so its
frames race the end of the turn here. Live previews are pinned frame by frame in
`test_telegram_run_observer.py`.
"""

import asyncio
import importlib.util
import json
import sys
from collections.abc import AsyncIterator
from pathlib import Path
from types import SimpleNamespace
from uuid import UUID

import httpx
import yaml
from _channel_execution_test_support import TABLES as INSTANCE_TABLES
from _storage_spaces_test_support import make_storage_fixture
from _telegram_bot_test_support import update_payload
from _telegram_repository_test_support import TABLES as TELEGRAM_TABLES
from _telegram_repository_test_support import code_of, welcome
from fastapi import FastAPI
from langgraph.checkpoint.memory import InMemorySaver
from sqlalchemy import select, text
from telegram import Update
from test_gateway_services import _make_start_run_request
from test_telegram_supervisor import build

from app.channels.manager import ChannelManager
from app.channels.store import ChannelStore
from app.gateway import chat_storage, deps
from app.gateway import storage_spaces as spaces
from app.gateway.auth.local_provider import LocalAuthProvider
from app.gateway.auth.models import User
from app.gateway.auth.repositories.sqlite import SQLiteUserRepository
from app.gateway.auth_middleware import AuthMiddleware
from app.gateway.csrf_middleware import CSRFMiddleware
from app.gateway.routers import artifacts, run_output, thread_runs, threads
from app.gateway.stream_cleanup import StreamCleanupRegistry
from app.telegram_bots.authority import TelegramAuthority
from app.telegram_bots.cipher import token_cipher, wrap_token
from app.telegram_bots.config import load_config
from app.telegram_bots.gateway_client import GatewayClientOwner
from app.telegram_bots.links import BotLinkRepository
from app.telegram_bots.settings import SettingsRepository
from deerflow.agent_instances.conversations import AgentConversations
from deerflow.config import agents_api_config, app_config, memory_config, stream_bridge_config
from deerflow.config.paths import Paths, paths_scope
from deerflow.config.storage_spaces_config import StoragePlan, StorageSpacesConfig
from deerflow.extensions import get_loaded_extensions, load_extensions, set_loaded_extensions
from deerflow.features.plugins import with_default_features
from deerflow.features.resources import StorageFeatureLinkRow
from deerflow.persistence.agent_instances.model import AgentDefinitionRevisionRow, AgentInstanceGrantRow, AgentInstanceRow, AgentLifecycleRow
from deerflow.persistence.channel_connections.sql import ChannelConnectionRepository
from deerflow.persistence.run.model import RunRow
from deerflow.persistence.telegram_bots.model import TelegramOutboxRow
from deerflow.persistence.user.access import DisabledIdentityRow, IdentityHoldRow, RoleLimitRow
from deerflow.persistence.user.model import UserRow
from deerflow.runtime import MemoryStreamBridge, checkpoint_mode
from deerflow.runtime.capture_resources import OwnedCheckpointSaver, OwnedStore
from deerflow.runtime.runs.manager import RunManager
from deerflow.spaces.contract import Custody, MutationMode
from deerflow.spaces.principals import HostPrincipalResolver

REPO = Path(__file__).resolve().parents[2]
OWNER = "00000000-0000-0000-0000-00000000000a"
BOT = 9_000_000_091
PERSON = 7_000_000_091
SENDS = {"sendMessage", "editMessageText", "sendDocument", "sendPhoto", "sendMediaGroup"}


def _sse(events) -> bytes:
    lines = []
    for name, data in events:
        if name:
            lines.append(f"event: {name}\n")
        lines.append(f"data: {data if isinstance(data, str) else json.dumps(data)}\n\n")
    return "".join(lines).encode()


def _halves(text_value):
    middle = max(1, len(text_value) // 2)
    return [piece for piece in (text_value[:middle], text_value[middle:]) if piece]


def anthropic_stream(*, text_value="", thinking=None, tool=None, message_id="msg_1") -> bytes:
    usage = {"input_tokens": 25, "cache_creation_input_tokens": 0, "cache_read_input_tokens": 0, "output_tokens": 1, "service_tier": "standard"}
    events = [
        ("message_start", {"type": "message_start", "message": {"id": message_id, "type": "message", "role": "assistant", "model": "claude-sonnet-4-20250514", "content": [], "stop_reason": None, "stop_sequence": None, "usage": usage}})
    ]
    index = 0

    def block(head, deltas):
        nonlocal index
        events.append(("content_block_start", {"type": "content_block_start", "index": index, "content_block": head}))
        events.extend(("content_block_delta", {"type": "content_block_delta", "index": index, "delta": delta}) for delta in deltas)
        events.append(("content_block_stop", {"type": "content_block_stop", "index": index}))
        index += 1

    if thinking is not None:
        block({"type": "thinking", "thinking": "", "signature": ""}, [{"type": "thinking_delta", "thinking": thinking}, {"type": "signature_delta", "signature": "c3ludGhldGlj"}])
    if text_value:
        block({"type": "text", "text": ""}, [{"type": "text_delta", "text": piece} for piece in _halves(text_value)])
    if tool is not None:
        name, arguments = tool
        block({"type": "tool_use", "id": "toolu_1", "name": name, "input": {}}, [{"type": "input_json_delta", "partial_json": piece} for piece in _halves(json.dumps(arguments))])
    events.append(("message_delta", {"type": "message_delta", "delta": {"stop_reason": "tool_use" if tool else "end_turn", "stop_sequence": None}, "usage": {"output_tokens": 15}}))
    events.append(("message_stop", {"type": "message_stop"}))
    return _sse(events)


def chat_completions_stream(*, text_value="", tool=None, identity="chatcmpl-1") -> bytes:
    common = {"id": identity, "object": "chat.completion.chunk", "created": 1800000000, "model": "gpt-4", "system_fingerprint": "fp_1"}

    def chunk(delta, finish=None):
        return (None, {**common, "choices": [{"index": 0, "delta": delta, "logprobs": None, "finish_reason": finish}]})

    events = [chunk({"role": "assistant", "content": ""})]
    events.extend(chunk({"content": piece}) for piece in _halves(text_value) if text_value)
    if tool is not None:
        name, arguments = tool
        events.append(chunk({"tool_calls": [{"index": 0, "id": "call_1", "type": "function", "function": {"name": name, "arguments": ""}}]}))
        events.append(chunk({"tool_calls": [{"index": 0, "function": {"arguments": json.dumps(arguments)}}]}))
    events.append(chunk({}, "tool_calls" if tool else "stop"))
    events.append((None, {**common, "choices": [], "usage": {"prompt_tokens": 10, "completion_tokens": 5, "total_tokens": 15}}))
    events.append((None, "[DONE]"))
    return _sse(events)


def responses_stream(*, text_value="", tool=None, identity="resp_1") -> bytes:
    base = {
        "id": identity,
        "object": "response",
        "created_at": 1800000000,
        "status": "in_progress",
        "error": None,
        "incomplete_details": None,
        "instructions": None,
        "max_output_tokens": None,
        "model": "gpt-5",
        "output": [],
        "parallel_tool_calls": True,
        "previous_response_id": None,
        "reasoning": {"effort": None, "summary": None},
        "store": True,
        "temperature": 1.0,
        "text": {"format": {"type": "text"}},
        "tool_choice": "auto",
        "tools": [],
        "top_p": 1.0,
        "truncation": "disabled",
        "usage": None,
        "user": None,
        "metadata": {},
    }
    events = []

    def push(kind, **fields):
        events.append((kind, {"type": kind, "sequence_number": len(events), **fields}))

    push("response.created", response=base)
    push("response.in_progress", response=base)
    output = []

    def item(start, *middle, done):
        position = len(output)
        push("response.output_item.added", output_index=position, item=start)
        for kind, fields in middle:
            push(kind, output_index=position, **fields)
        push("response.output_item.done", output_index=position, item=done)
        output.append(done)

    thought = {"id": "rs_1", "type": "reasoning", "summary": []}
    item(thought, done=thought)
    if tool is not None:
        name, arguments = tool
        raw = json.dumps(arguments)
        call = {"id": "fc_1", "type": "function_call", "status": "in_progress", "arguments": "", "call_id": "call_1", "name": name}
        item(call, ("response.function_call_arguments.delta", {"item_id": "fc_1", "delta": raw}), ("response.function_call_arguments.done", {"item_id": "fc_1", "arguments": raw}), done={**call, "status": "completed", "arguments": raw})
    if text_value:
        message = {"id": "msg_1", "type": "message", "status": "in_progress", "content": [], "role": "assistant"}
        part = {"type": "output_text", "annotations": [], "logprobs": [], "text": ""}
        finished = {**part, "text": text_value}
        where = {"content_index": 0, "item_id": "msg_1"}
        item(
            message,
            ("response.content_part.added", {**where, "part": part}),
            *(("response.output_text.delta", {**where, "delta": piece, "logprobs": []}) for piece in _halves(text_value)),
            ("response.output_text.done", {**where, "logprobs": [], "text": text_value}),
            ("response.content_part.done", {**where, "part": finished}),
            done={**message, "status": "completed", "content": [finished]},
        )
    usage = {"input_tokens": 10, "input_tokens_details": {"cached_tokens": 0}, "output_tokens": 5, "output_tokens_details": {"reasoning_tokens": 0}, "total_tokens": 15}
    push("response.completed", response={**base, "status": "completed", "output": output, "usage": usage})
    return _sse(events)


class _Body(httpx.AsyncByteStream, httpx.SyncByteStream):
    """Real SSE arrives in arbitrary chunks, so the SDK's line parsers get exercised."""

    def __init__(self, body):
        self.body = body

    def _chunks(self):
        return (self.body[start : start + 512] for start in range(0, len(self.body), 512))

    async def __aiter__(self) -> AsyncIterator[bytes]:
        for chunk in self._chunks():
            yield chunk

    def __iter__(self):
        return self._chunks()

    async def aclose(self) -> None:
        return None

    def close(self) -> None:
        return None


class ProviderWire:
    """The lead model's scripted replies; a request without tools is a title or memory call."""

    def __init__(self, replies, auxiliary):
        self.replies, self.auxiliary, self.lead_requests = list(replies), auxiliary, 0

    def respond(self, request):
        if request.url.host == "searxng":
            body = json.dumps({"results": [{"title": "Synthetic result", "url": "https://example.invalid/result", "content": "Synthetic snippet"}]}).encode()
            return httpx.Response(200, headers={"content-type": "application/json"}, stream=_Body(body), request=request)
        body = self.auxiliary
        if json.loads(request.content or b"{}").get("tools"):
            self.lead_requests += 1
            assert self.replies, "The lead model was called more often than this test scripted"
            reply = self.replies.pop(0)
            body = reply() if callable(reply) else reply
        return httpx.Response(200, headers={"content-type": "text/event-stream"}, stream=_Body(body), request=request)


def install_provider_wire(monkeypatch, wire):
    """Replaces only outbound transports; the Gateway's own ASGI client never uses them."""

    async def handle_async(self, request):
        return wire.respond(request)

    monkeypatch.setattr(httpx.AsyncHTTPTransport, "handle_async_request", handle_async)
    monkeypatch.setattr(httpx.HTTPTransport, "handle_request", lambda self, request: wire.respond(request))


def _renderer(monkeypatch):
    spec = importlib.util.spec_from_file_location("hartmesh_render_config", REPO / "deploy/compose/gateway/render_config.py")
    module = importlib.util.module_from_spec(spec)
    monkeypatch.setitem(sys.modules, spec.name, module)
    spec.loader.exec_module(module)
    return module


def render_compose_config(tmp_path, monkeypatch, *, keys, tweak=None):
    """The real template and renderer; only paths that exist on a tenant VM are redirected."""
    renderer = _renderer(monkeypatch)
    environ = {
        "HARTMESH_LOCAL_PASSWORDS": "allowed",
        "HARTMESH_LOCAL_REGISTRATION": "closed",
        "DATABASE_URL": "postgresql://synthetic:synthetic@postgres.invalid:5432/synthetic",
        "HARTMESH_DATA_DIR": str(tmp_path / "data"),
        "DEER_FLOW_STREAM_BRIDGE_REDIS_URL": "redis://redis.invalid:6379/0",
        **keys,
    }
    template = (REPO / "deploy/compose/config.yaml").read_text(encoding="utf-8")
    rendered, _included = renderer.render_text(template, renderer.load_catalog(REPO / "deploy/compose/providers"), environ)
    document = yaml.safe_load(rendered)
    document["skills"]["path"] = str(REPO / "skills")
    document["tenant_bundle"]["path"] = str(tmp_path / "tenant")
    document["storage_spaces"]["inventory_path"] = str(tmp_path / "inventory.v2.json")
    document["memory"]["enabled"] = False  # A memory update is a timer that would outlive this test.
    if tweak is not None:
        tweak(document)
    path = tmp_path / "config.yaml"
    path.write_text(yaml.safe_dump(document, sort_keys=False), encoding="utf-8")
    extensions = tmp_path / "extensions_config.json"
    extensions.write_text('{"mcpServers":{},"skills":{}}', encoding="utf-8")
    (tmp_path / "home").mkdir(exist_ok=True)
    for name, value in {**environ, "DEER_FLOW_CONFIG_PATH": str(path), "DEER_FLOW_EXTENSIONS_CONFIG_PATH": str(extensions), "DEER_FLOW_HOME": str(tmp_path / "home"), "DEER_FLOW_AUTH_DISABLED": "0"}.items():
        monkeypatch.setenv(name, value)
    for name in ("_app_config", "_app_config_path", "_app_config_mtime", "_app_config_signature", "_app_config_is_custom"):
        monkeypatch.setattr(app_config, name, getattr(app_config, name))
    # Loading the file also writes these, and `reset_app_config` does not put them back.
    for module, name in ((memory_config, "_memory_config"), (agents_api_config, "_agents_api_config"), (stream_bridge_config, "_stream_bridge_config")):
        monkeypatch.setattr(module, name, getattr(module, name))
    app_config.reset_app_config()
    monkeypatch.setattr(checkpoint_mode, "_frozen_checkpoint_channel_mode", None)
    monkeypatch.setattr(checkpoint_mode, "_frozen_checkpoint_snapshot_frequency", None)


async def _seat_person(app, root, monkeypatch):
    """A real Space assignment. Only the catalogue is directory-backed, with no loop volume."""
    root.mkdir(parents=True, exist_ok=True)
    fixture = make_storage_fixture(root, SimpleNamespace(param="sqlite"))
    files, sessions, catalog = await fixture.__anext__()
    async with sessions.kw["bind"].begin() as connection:
        for model in (UserRow, DisabledIdentityRow, IdentityHoldRow, RoleLimitRow, StorageFeatureLinkRow, AgentDefinitionRevisionRow, AgentInstanceRow, AgentInstanceGrantRow, AgentLifecycleRow):
            await connection.run_sync(model.__table__.create)
    users = SQLiteUserRepository(sessions)
    monkeypatch.setattr("app.gateway.auth.mode.sign_on_only", lambda: False)
    files.registry._resolver = HostPrincipalResolver(human=spaces.human_lookup(users))
    for volume, kind in zip(catalog.volumes.values(), ("user", "user", "user", "company"), strict=True):
        volume.kind, volume.max_bytes, volume.max_inodes = kind, 1 << 30, 1 << 20
    await files.registry.host_create(name="Company", custody=Custody.company(), mode=MutationMode.NATIVE, feature=None, kind="company")
    plan = StoragePlan(users=3, user_gib=1, ai_assistants_per_user=0, ai_assistant_gib=1, ai_employees=0, ai_employee_gib=1, company_gib=1)
    app.state.storage_spaces, app.state.storage_spaces_config = files, StorageSpacesConfig(enabled=True, inventory_path=str(root / "inventory.v2.json"), user_spaces=False, plan=plan)
    app.state.agent_execution = spaces.ExecutionStatus(host_id="qualified-test-host")
    await spaces.assign_user_space(app, await users.create_user(User(id=UUID(OWNER), email="turn@example.com")))
    return fixture.aclose


def produce_file(root, name, data, reply):
    """A scripted reply with a side effect: the file a real agent run would leave in outputs."""

    def respond():
        outputs = list(root.glob("*/data/conversations/*/user-data/outputs"))
        assert outputs, "The turn's output root must exist before the model is called"
        for directory in outputs:
            (directory / name).write_bytes(data)
        return reply

    return respond


def _bot_result(endpoint, counter):
    counter[0] += 1
    if endpoint in SENDS:
        return {"message_id": 5000 + counter[0], "date": 1_893_456_000, "chat": {"id": PERSON, "type": "private"}, "text": "Delivered"}
    return True


def _answer_every_call(normal, monkeypatch):
    original, counter = normal.do_request, [0]

    async def do_request(url, method, request_data=None, **kwargs):
        endpoint = url.rsplit("/", 1)[-1]
        if not normal.replies[endpoint]:
            normal.enqueue(endpoint, result=_bot_result(endpoint, counter))
        return await original(url, method, request_data, **kwargs)

    monkeypatch.setattr(normal, "do_request", do_request)


async def _connect_bot(db, supervisor, boot):
    await db.settings.ensure(OWNER, timezone="UTC", locale="en-US")
    token = f"{BOT}:" + "synthetic_" * 5
    link = await db.links.add(owner_user_id=OWNER, agent_kind="lead_agent", agent_ref="lead_agent", bot_id=BOT, bot_username="turn_fixture_bot", encrypted_token=wrap_token(token_cipher(boot.env), BOT, token), actor_id=OWNER)
    code = code_of(await db.links.mint_code(link))
    link = await db.links.bind(
        await db.links.get(link.id),
        telegram_user_id=PERSON,
        chat_id=PERSON,
        first_name="Fixture",
        username="turn_person",
        code=code,
        start_identity={"update_id": 201, "message_id": 101, "chat_id": PERSON},
        hello_parts=welcome("hello.chat"),
    )
    boot.normal.enqueue_get_me(bot_id=BOT)
    boot.updates.enqueue("getUpdates", result=[], release=asyncio.Event())
    await supervisor.add(link, token)
    async with db.sessions.begin() as writer:
        for row in await writer.scalars(select(TelegramOutboxRow)):
            row.state = "sent"  # The welcome is not what these tests deliver.
    return link


async def _finish(turn, clock):
    """The driver paces itself on the injected clock, so time has to pass for it to finish."""
    while not turn.done.is_set():
        clock.advance(1)
        await asyncio.sleep(0.05)


async def drive_message(e, monkeypatch, tmp_path, *, prompts=("What is two plus two?",)):
    """Sends each prompt as a real Telegram update and returns what came of it."""
    async with e.engine.begin() as connection:
        if e.dialect == "sqlite":
            await connection.execute(text("BEGIN"))
        tables = list(dict.fromkeys([*TELEGRAM_TABLES, *INSTANCE_TABLES, UserRow.__table__, DisabledIdentityRow.__table__, IdentityHoldRow.__table__, RoleLimitRow.__table__]))
        await connection.run_sync(lambda sync: tables[0].metadata.create_all(sync, tables=tables))
    config = app_config.get_app_config()
    settings = load_config(config)
    db = SimpleNamespace(engine=e.engine, sessions=e.sf, dialect=e.dialect, config=settings, connections=ChannelConnectionRepository(e.sf))
    db.settings = SettingsRepository(e.sf, settings)
    db.links = BotLinkRepository(e.sf, db.connections, settings)
    async with db.sessions.begin() as session:
        session.add(UserRow(id=OWNER, email="turn@example.com", password_hash="unusable-fixture", system_role="user"))
    users = SQLiteUserRepository(db.sessions)
    monkeypatch.setattr(deps, "_cached_repo", users)
    monkeypatch.setattr(deps, "_cached_local_provider", LocalAuthProvider(users))
    previous_extensions = get_loaded_extensions()
    paths = await asyncio.to_thread(Paths, tmp_path / "data")
    with paths_scope(paths):
        extensions, _diagnostics = load_extensions(with_default_features(config.plugins))
        set_loaded_extensions(extensions)
        bridge = MemoryStreamBridge()
        runs = RunManager(store=e.repository, run_ownership_config=config.run_ownership, event_store=e.event_store)
        prototype = _make_start_run_request(runs, thread_store=e.thread_store)
        app = FastAPI()
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
        app.state.agent_conversations = AgentConversations(None, session_factory=e.sf)
        app.state.storage_spaces_enabled = True
        close_storage = await _seat_person(app, tmp_path / "spaces", monkeypatch)
        storage = chat_storage.chat_storage_lifecycle(app, enabled=True)
        app.include_router(threads.router)
        app.include_router(thread_runs.router)
        app.include_router(run_output.router, prefix="/api/threads")
        app.include_router(artifacts.router)
        gateway = GatewayClientOwner(gateway_url="http://gateway.test", langgraph_url="http://gateway.test/api", transport=httpx.ASGITransport(app=app))
        boot = build(db, monkeypatch)
        supervisor = boot.supervisor
        supervisor.app, supervisor.authority, supervisor._app_config = app, TelegramAuthority(links=db.links, connections=db.connections), config
        supervisor.thread_client = gateway
        store = await asyncio.to_thread(ChannelStore, str(tmp_path / "manager.json"))
        manager = ChannelManager(boot.service.bus, store, connection_repo=db.connections, langgraph_url="http://gateway.test/api", gateway_url=gateway.gateway_url, max_concurrency=1)
        boot.service.manager = manager
        finished = asyncio.Event()

        async def outbound(message):
            if message.is_final:
                finished.set()

        boot.service.bus.subscribe_outbound(outbound)
        await storage.__aenter__()
        try:
            await supervisor.check()
            await supervisor.start()
            await asyncio.wait_for(supervisor._send_event.wait(), 3)
            link = await _connect_bot(db, supervisor, boot)
            channel = supervisor.channel(link.id)
            await manager.start()
            _answer_every_call(boot.normal, monkeypatch)
            for number, prompt in enumerate(prompts):
                finished.clear()
                update = Update.de_json(update_payload(1701 + number, message_id=711 + number, account=PERSON, text=prompt), channel.bot)
                assert await channel.handle_update(update)
                await asyncio.sleep(0)
                turn = next(iter(supervisor.turns._turns.values()))
                await asyncio.wait_for(finished.wait(), 30)
                await asyncio.wait_for(_finish(turn, boot.clock), 30)
                for _ in range(6):
                    boot.clock.advance(5)
                    await supervisor.sender.tick()
                    await supervisor.sender.drain()
            async with e.sf() as reader:
                facts = SimpleNamespace(
                    plugins=[plugin.namespace for _source, plugin in extensions.plugins],
                    runs=[row.status for row in await reader.scalars(select(RunRow))],
                    sent=[(call.endpoint, call.parameters.get("text") or call.parameters.get("caption")) for call in boot.normal.calls if call.endpoint in SENDS],
                    bot_calls=repr([(call.endpoint, call.parameters) for call in boot.normal.calls]),
                )
            return facts
        finally:
            await supervisor.begin_shutdown()
            await manager.stop()
            await supervisor.stop()
            await runs.shutdown(timeout=2)
            await app.state.stream_cleanups.close()
            await bridge.close()
            await storage.__aexit__(None, None, None)
            await close_storage()
            set_loaded_extensions(previous_extensions)
