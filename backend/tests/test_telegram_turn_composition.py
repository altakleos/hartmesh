"""Actual managed bus/HTTP/SDK worker evidence reaches the recorded Bot API.

Operator config, prompts, selected tools/skills, tracing and model I/O are
controlled. Telegram requests, health and copy discovery stay offline. The real
graph/profile, ordinary task-Paths scans, SQL guards, creating route, proofs and
sender run; this is not native storage-volume or User-grant qualification.
"""

import asyncio
import json
from types import SimpleNamespace

import httpx
import pytest
from _channel_execution_test_support import TABLES as INSTANCE_TABLES
from _run_output_capture_test_support import capture_environment as capture_environment
from _run_output_capture_test_support import output_store as output_store
from _telegram_ingress_test_support import install_live_owner
from _telegram_repository_test_support import TABLES as TELEGRAM_TABLES
from _telegram_repository_test_support import code_of, welcome
from fastapi import FastAPI
from langchain_core.messages import AIMessage
from langgraph.checkpoint.memory import InMemorySaver
from sqlalchemy import select, text
from telegram import Chat, Message, Update, User
from test_gateway_services import _make_start_run_request
from test_output_inventory_profile import assemble_offline
from test_telegram_supervisor import _supervisor_logging as _supervisor_logging
from test_telegram_supervisor import build

from app.channels.manager import ChannelManager
from app.channels.message_bus import InboundMessage
from app.channels.store import ChannelStore
from app.gateway import chat_storage, services
from app.gateway.auth_middleware import AuthMiddleware
from app.gateway.csrf_middleware import CSRFMiddleware
from app.gateway.routers import run_output, thread_runs, threads
from app.gateway.stream_cleanup import StreamCleanupRegistry
from app.telegram_bots.cipher import token_cipher, wrap_token
from app.telegram_bots.config import TelegramBotsConfig
from app.telegram_bots.gateway_client import GatewayClientOwner
from app.telegram_bots.links import BotLinkRepository
from app.telegram_bots.settings import SettingsRepository
from deerflow.agent_instances.conversations import AgentConversations
from deerflow.agents.lead_agent import agent
from deerflow.config import app_config
from deerflow.config.paths import Paths, get_paths, paths_scope
from deerflow.persistence.channel_connections.sql import ChannelConnectionRepository
from deerflow.persistence.telegram_bots.model import TelegramInboundMarkRow, TelegramOutboxRow
from deerflow.runtime import MemoryStreamBridge
from deerflow.runtime.capture_resources import OwnedCheckpointSaver, OwnedStore
from deerflow.runtime.runs.worker import run_agent
from deerflow.utils.messages import restore_original_human_message, strip_injected_user_message_id_suffix

OWNER = "00000000-0000-0000-0000-00000000000a"
ANSWER = "**Durable answer**"


@pytest.mark.asyncio
@pytest.mark.parametrize("case", ["text", "custom_text", "produced_file", "phase_one", "no_user_text", "no_user_custom_text", "no_user_uncertain"])
async def test_actual_managed_turn_creates_once_and_delivers_only_verified_output(capture_environment, monkeypatch, tmp_path, case):
    e = capture_environment
    async with e.engine.begin() as connection:
        if e.dialect == "sqlite":
            await connection.execute(text("BEGIN"))
        tables = list(dict.fromkeys([*TELEGRAM_TABLES, *INSTANCE_TABLES]))
        await connection.run_sync(lambda sync: tables[0].metadata.create_all(sync, tables=tables))
    config = TelegramBotsConfig()
    db = SimpleNamespace(engine=e.engine, sessions=e.sf, dialect=e.dialect, config=config, connections=ChannelConnectionRepository(e.sf))
    db.settings = SettingsRepository(e.sf, config)
    db.links = BotLinkRepository(e.sf, db.connections, config)
    live = await install_live_owner(db, monkeypatch, owner=OWNER)
    paths = await asyncio.to_thread(Paths, tmp_path / "data")
    with paths_scope(paths):
        custom = case in {"custom_text", "recovery_custom_text", "no_user_custom_text"}
        no_user = case.startswith("no_user_")
        target = "profile-agent" if custom else "lead_agent"
        if custom:
            from deerflow.persistence.agents.file import FileAgentStore

            confined = await asyncio.to_thread(lambda: paths.user_agent_dir(OWNER, target).resolve().is_relative_to(tmp_path.resolve()))
            assert confined
            await asyncio.to_thread(FileAgentStore().create, target, {"name": target, "memory_enabled": False}, "Synthetic custom configuration", user_id=OWNER)
        assembly, _built, model = await asyncio.to_thread(assemble_offline, monkeypatch, custom=custom)
        assert assembly.inventory_profile is not None and services.run_agent is run_agent
        model.responses = [AIMessage(ANSWER)]
        configured = agent.get_app_config()
        for name in ("_app_config", "_app_config_path", "_app_config_mtime", "_app_config_signature", "_app_config_is_custom"):
            monkeypatch.setattr(app_config, name, getattr(app_config, name))
        app_config.set_app_config(configured)
        monkeypatch.setenv("DEER_FLOW_AUTH_DISABLED", "0")

        def factory(**kwargs):
            return assembly

        monkeypatch.setattr(services, "resolve_agent_factory", lambda identity: factory if identity == "lead_agent" else None)
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
        app.state.run_event_store, app.state.run_store = e.event_store, e.repository
        app.state.agent_conversations = AgentConversations(None, session_factory=e.sf)
        app.state.storage_spaces_enabled = no_user
        if no_user:
            from unittest.mock import AsyncMock

            monkeypatch.setattr(chat_storage, "storage_state", AsyncMock(return_value=SimpleNamespace(state="unavailable", reason="preparing")))
        # Observe the actual worker scope without creating a sandbox. User
        # identity is the live SQL account installed above, even without files.
        monkeypatch.setattr("deerflow.sandbox.sandbox_provider.get_initialized_sandbox_provider", lambda: None)
        storage_lifetime = chat_storage.chat_storage_lifecycle(app, enabled=no_user)
        storage_entered = False
        app.include_router(threads.router)
        app.include_router(thread_runs.router)
        app.include_router(run_output.router, prefix="/api/threads")
        http_calls = []
        http_responses = []

        class RecordedASGI(httpx.ASGITransport):
            async def handle_async_request(self, request):
                # Never retain authentication headers/cookies in diagnostics.
                http_calls.append((request.method, request.url.path, json.loads(request.content) if request.content else None))
                response = await super().handle_async_request(request)
                http_responses.append((request.method, request.url.path, response.status_code))
                return response

        owner = GatewayClientOwner(gateway_url="http://gateway.test", langgraph_url="http://gateway.test/api", transport=RecordedASGI(app=app))
        boot = build(db, monkeypatch)
        supervisor = boot.supervisor
        supervisor.app, supervisor.authority, supervisor._app_config = app, live.authority, configured
        supervisor.thread_client = owner
        channel_store = await asyncio.to_thread(ChannelStore, str(tmp_path / "manager.json"))
        manager = ChannelManager(boot.service.bus, channel_store, connection_repo=db.connections, langgraph_url="http://gateway.test/api", gateway_url=owner.gateway_url, max_concurrency=1)
        boot.service.manager = manager
        final, model_entered, model_release = asyncio.Event(), asyncio.Event(), asyncio.Event()
        frames = []
        model_inputs = []

        async def outbound(message):
            frames.append(message)
            if message.is_final:
                final.set()

        boot.service.bus.subscribe_outbound(outbound)
        original_generate = type(model)._agenerate
        thread_id = None

        async def generate(self, *args, **kwargs):
            messages = args[0] if args else kwargs["messages"]
            model_inputs.extend(message for message in messages if message.type == "human" and "channel_turn_id" in message.additional_kwargs)
            model_entered.set()
            if no_user:
                from deerflow.spaces.chat_paths import ChatStoragePaths

                assert type(get_paths()) is ChatStoragePaths
                assert get_paths().thread_data(thread_id, user_id=OWNER) == {}
                if case == "no_user_uncertain":
                    app.state.storage_spaces_enabled = False
            if case == "phase_one":
                await model_release.wait()
            if case == "produced_file":
                await asyncio.to_thread(lambda: (paths.sandbox_outputs_dir(thread_id, user_id=OWNER) / "current.txt").write_text("Actual generated file", encoding="utf-8"))
            return await original_generate(self, *args, **kwargs)

        monkeypatch.setattr(type(model), "_agenerate", generate)
        try:
            await storage_lifetime.__aenter__()
            storage_entered = True
            await supervisor.check()
            await supervisor.start()
            await asyncio.wait_for(supervisor._send_event.wait(), 3)
            await db.settings.ensure(OWNER, timezone="UTC", locale="en-US")
            bot = 9_000_000_091
            synthetic_token = str(bot) + ":" + "synthetic_" * 5
            # Runtime concatenation keeps even synthetic token-shaped strings
            # out of committed credential scans.
            link = await db.links.add(
                owner_user_id=OWNER,
                agent_kind="custom_agent" if custom else "lead_agent",
                agent_ref=target,
                bot_id=bot,
                bot_username="turn_fixture_bot",
                encrypted_token=wrap_token(token_cipher(boot.env), bot, synthetic_token),
                actor_id=OWNER,
            )
            code = code_of(await db.links.mint_code(link))
            link = await db.links.bind(
                await db.links.get(link.id),
                telegram_user_id=7_000_000_091,
                chat_id=7_000_000_091,
                first_name="Fixture",
                username="turn_person",
                code=code,
                start_identity={"update_id": 201, "message_id": 101, "chat_id": 7_000_000_091},
                hello_parts=welcome("hello.custom_agent" if custom else "hello.chat"),
            )
            boot.normal.enqueue_get_me(bot_id=bot)
            boot.updates.enqueue("getUpdates", result=[], release=asyncio.Event())
            await supervisor.add(link, synthetic_token)
            channel = supervisor.channel(link.id)
            assert channel is not None
            origins = []
            inbox = supervisor.outbox.inbox
            for message_id, update_id, content in [(711, 1701, "First accepted question"), (710, 1702, "Second accepted question")]:
                incoming = Update(update_id, message=Message(message_id, link.created_at, Chat(link.chat_id, "private"), from_user=User(link.telegram_user_id, "Fixture", False), text=content))
                row, _ = await inbox.accept(link, incoming, sanitized_input={"text": content, "type": "text"})
                origins.append(await inbox.route(row, kind="turn", target_id=target, basis={"input_profile": {"version": 1, "kind": "plain_text", "forwarded": False}}, operation_ids=[]))
            inbound = InboundMessage(channel_name="telegram", chat_id=str(link.chat_id), user_id=str(link.telegram_user_id), text="First accepted question", connection_id=link.connection_id, owner_user_id=OWNER, workspace_id=str(bot))
            thread_id = await supervisor.threads.ensure_thread(channel.link, inbound, origin=origins[0])
            if not no_user:
                await asyncio.to_thread(paths.ensure_thread_dirs, thread_id, user_id=OWNER)
            async with e.sf.begin() as writer:
                for row in await writer.scalars(select(TelegramOutboxRow)):
                    row.state = "sent"  # Initial welcome is outside this model-delivery test.
            await manager.start()
            record = await supervisor.publications.dispatch(channel.link, origins, thread_id=thread_id)
            try:
                await asyncio.wait_for(model_entered.wait(), 15)
            except TimeoutError:
                pytest.fail(f"Model never entered; HTTP outcomes: {http_responses!r}")
            if case == "phase_one":
                await supervisor.begin_shutdown()
                assert not owner.closed and supervisor._subscribed
                model_release.set()
            await asyncio.wait_for(final.wait(), 15)
            if case == "no_user_uncertain":
                app.state.storage_spaces_enabled = True
            assert any(message.text == ANSWER for message in frames if message.is_final)
            if case in {"recovery_text", "recovery_custom_text"}:
                # Recreate only the recovery/coordinator/source consumer. The
                # worker's real durable SQL seal/event/pair survives; no remote
                # response or output proof is substituted by this fixture.
                from app.telegram_bots.model_source import ModelSource
                from app.telegram_bots.producer import Outbox
                from app.telegram_bots.publication import PublicationCoordinator
                from app.telegram_bots.recovery import PublicationRecoveryService

                await supervisor.recovery.stop()
                await supervisor.publications.stop()
                async with e.sf() as reader:
                    previous_primary = await reader.scalar(select(TelegramOutboxRow.id).where(TelegramOutboxRow.link_id == link.id, TelegramOutboxRow.dedupe_key == "reply:" + record.turn_id))
                assert previous_primary is None, "The old consumer had already planned this publication"
                fresh_outbox = Outbox(e.sf)
                supervisor.publications = PublicationCoordinator(inbox=fresh_outbox.inbox, outbox=fresh_outbox, bus=boot.service.bus, http_owner=owner, capture_attempt=supervisor.publication_authority.capture)
                supervisor.model_source = ModelSource(read_output=supervisor.publications.read_output)
                supervisor.recovery = PublicationRecoveryService(inbox=fresh_outbox.inbox, outbox=fresh_outbox, coordinator=supervisor.publications, links=db.links, allowed=supervisor.send_allowed)
                await supervisor.recovery.tick()
                async with e.sf() as reader:
                    recovered = await reader.scalar(select(TelegramOutboxRow).where(TelegramOutboxRow.link_id == link.id, TelegramOutboxRow.dedupe_key == "reply:" + record.turn_id))
                assert recovered is not None, "The fresh recovery service did not plan the persisted completed publication"
                observed = SimpleNamespace(state="planned", outbox_id=recovered.id)
            else:
                observed = await supervisor.publications.observe(record, channel.link, max_pages=2)
            assert model.invocations == 1
            assert len(model_inputs) == 1
            assert restore_original_human_message(model_inputs[0]).content == "First accepted question\n\nSecond accepted question"
            assert model_inputs[0].additional_kwargs["channel_turn_id"] == record.publication_id
            runs = await e.repository.list_by_thread(thread_id, user_id=OWNER)
            assert len(runs) == 1 and runs[0]["kwargs"]["output_capture"] == {"version": 2, "inventory_document_version": 2}
            assert runs[0]["kwargs"]["channel_execution"]["publication_id"] == record.publication_id
            assert runs[0]["kwargs"]["channel_execution"]["agent_name"] == (target if custom else None)
            assert runs[0]["kwargs"]["output_evidence"]["input_message_id"] == strip_injected_user_message_id_suffix(model_inputs[0].id)
            creates = [body for method, path, body in http_calls if method == "POST" and path.endswith(("/runs/stream", "/runs/wait", "/runs"))]
            assert len(creates) == 1
            assert creates[0]["channel_execution"]["publication_id"] == record.publication_id
            if case in {"produced_file", "no_user_uncertain"}:
                assert observed.state == "held" and observed.reason == "inventory_unhandled"
                assert not any(call.endpoint == "sendMessage" for call in boot.normal.calls)
            else:
                assert observed.state == "planned"
                async with e.sf() as reader:
                    planned = await reader.get(TelegramOutboxRow, observed.outbox_id)
                proof = planned.payload["reply_basis"]["outcome"]["projection"]
                assert planned.payload["reply_basis"]["version"] == 2
                assert proof["inventory"]["document"]["coverage"] == "complete" and proof["inventory"]["document"]["items"] == []
                assert proof["inventory"]["document"]["version"] == 2
                assert proof["inventory"]["document"]["output_view"]["kind"] == ("no_admitted_output_view" if no_user else "managed_outputs")
                if case == "phase_one":
                    assert not owner.closed and planned.state == "pending"
                    assert not any(call.endpoint == "sendMessage" for call in boot.normal.calls)
                else:
                    boot.normal.enqueue("sendMessage", result={"message_id": 501, "date": 1_893_456_000, "chat": {"id": link.chat_id, "type": "private"}, "text": "Durable answer"})
                    await supervisor.sender.tick()
                    await supervisor.sender.drain()
                    sent = [call for call in boot.normal.calls if call.endpoint == "sendMessage"]
                    assert len(sent) == 1 and sent[0].parameters["text"] == "<b>Durable answer</b>"
                    assert sent[0].parameters["reply_parameters"]["message_id"] == 711
                    async with e.sf() as reader:
                        completed = await reader.get(TelegramOutboxRow, observed.outbox_id)
                        saved = await reader.get(TelegramInboundMarkRow, (link.id, 711, link.chat_id))
                    assert completed.state == "sent" and completed.parts[0]["message_ids"] == [501]
                    assert saved.state == "done" and saved.run_id == runs[0]["run_id"]
                    assert sum(method == "GET" and path.endswith("/output") for method, path, _body in http_calls) >= 2
        finally:
            model_release.set()
            await supervisor.begin_shutdown()
            await manager.stop()
            await supervisor.stop()
            await e.manager.shutdown(timeout=2)
            await app.state.stream_cleanups.close()
            await bridge.close()
            if storage_entered:
                await storage_lifetime.__aexit__(None, None, None)
