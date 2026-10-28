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
@pytest.mark.parametrize("case", ["text", "custom_text", "produced_file", "custom_file", "produced_album", "phase_one", "no_user_text", "no_user_custom_text", "no_user_uncertain"])
async def test_actual_managed_turn_creates_once_and_delivers_only_verified_output(capture_environment, monkeypatch, tmp_path, case, *, managed_ingress=False, dialogue=None, inbound_file=False):
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
        produced_file = case in {"produced_file", "custom_file", "produced_album"}
        album = case == "produced_album"
        if album:
            import base64

            output_bytes = base64.b64decode("iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAwMCAO+aBz0AAAAASUVORK5CYII=")
        else:
            output_bytes = b"Actual generated file"
        output_names = ("first.png", "second.png") if album else ("current.txt",)
        custom = case in {"custom_file", "custom_text", "recovery_custom_text", "no_user_custom_text"}
        no_user = case.startswith("no_user_")
        target = "profile-agent" if custom else "lead_agent"
        if custom:
            from deerflow.persistence.agents.file import FileAgentStore

            confined = await asyncio.to_thread(lambda: paths.user_agent_dir(OWNER, target).resolve().is_relative_to(tmp_path.resolve()))
            assert confined
            await asyncio.to_thread(FileAgentStore().create, target, {"name": target, "memory_enabled": False}, "Synthetic custom configuration", user_id=OWNER)
        tools = []
        if dialogue is not None:
            from deerflow.tools.builtins.clarification_tool import ask_clarification_tool

            tools = [ask_clarification_tool]
        assembly, _built, model = await asyncio.to_thread(assemble_offline, monkeypatch, custom=custom, tools=tools)
        assert assembly.inventory_profile is not None and services.run_agent is run_agent
        model.responses = [AIMessage(ANSWER)]
        if dialogue is not None:
            args = {"question": "Choose a value", "options": ["First", "Second"]}
            if dialogue == "form":
                args = {"question": "Supply a value", "fields": [{"name": "value", "label": "A value", "type": "text", "required": True}]}
            model.responses = [AIMessage("", tool_calls=[{"id": "actual-dialogue-call", "name": "ask_clarification", "args": args}]), AIMessage(ANSWER)]
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
        if produced_file:
            from app.gateway.routers import artifacts

            app.include_router(artifacts.router)
        feature_readers, file_downloads, charged_writes = [], [], []
        inbound_bytes = b"a,b\n1,2\n"
        if inbound_file:
            from app.gateway.routers import features as feature_routes

            # Only the provider availability projection is controlled. The
            # actual features route, internal-owner headers and owner decoding
            # run over the same bound Gateway client as thread/run requests.
            async def available_storage(request, config):
                actual_owner = await feature_routes._feature_owner(request)
                assert actual_owner == OWNER
                feature_readers.append(actual_owner)
                return feature_routes.StorageSpacesFeature(enabled=True, state="ready", own="ready"), feature_routes.AccountsFeature(), feature_routes.AgentInstancesFeature()

            monkeypatch.setattr(feature_routes, "_storage_features", available_storage)
            app.include_router(feature_routes.router)
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
        uploaded_outputs = []
        if produced_file:
            original_request = boot.normal.do_request

            async def observe_output_upload(url, method, request_data=None, **kwargs):
                if url.rsplit("/", 1)[-1] in {"sendDocument", "sendMediaGroup"}:
                    import hashlib

                    uploads = tuple(request_data.multipart_data.values())
                    try:
                        assert tuple(upload[0] for upload in uploads) == output_names
                        for _name, handle, _mime in uploads:
                            # These synthetic fixtures fit one bounded chunk;
                            # real HTTPX multipart streaming is tested separately.
                            handle.seek(0)
                            assert handle.read() == output_bytes
                            handle.seek(0)
                            uploaded_outputs.append(hashlib.sha256(output_bytes).hexdigest())
                        assert supervisor.file_bytes_budget.used == len(output_names) * len(output_bytes)
                    finally:
                        uploads = None
                return await original_request(url, method, request_data, **kwargs)

            monkeypatch.setattr(boot.normal, "do_request", observe_output_upload)
        if inbound_file:
            from urllib.parse import parse_qs

            from app.telegram_bots.capped_download import ManagedHTTPXRequest
            from deerflow.uploads import manager as upload_manager

            async def telegram_transport(request):
                if request.method == "GET":
                    file_downloads.append("actual-capped-get")
                    return httpx.Response(200, content=inbound_bytes)
                parameters = {}
                for key, values in parse_qs(request.content.decode("utf-8"), keep_blank_values=True).items():
                    value = values[0]
                    try:
                        value = json.loads(value)
                    except ValueError:
                        pass
                    parameters[key] = value
                endpoint = request.url.path.rsplit("/", 1)[-1]
                status, body = await boot.normal.do_request("https://offline.invalid/" + endpoint, request.method, SimpleNamespace(parameters=parameters))
                return httpx.Response(status, content=body)

            def file_request_factory(**kwargs):
                if kwargs["connection_pool_size"] == 1:
                    return boot.updates
                # Supervisor invokes channel construction (and this factory)
                # off-loop. This is the real prepared normal HTTPX client for
                # both actual SDK Bot API posts and streamed file GETs.
                kwargs["httpx_kwargs"] = {**kwargs["httpx_kwargs"], "transport": httpx.MockTransport(telegram_transport)}
                return ManagedHTTPXRequest(**kwargs)

            supervisor.request_factory = file_request_factory
            actual_write = upload_manager.write_upload_file_no_symlink
            actual_prepare_uploads = upload_manager.ensure_uploads_dir
            prepared_threads = []

            def prepare_actual_uploads(thread, *, user_id=None):
                # The offline graph has no sandbox startup to prepare its
                # outputs/workspace roots. Preserve the existing fixture's
                # precondition only once the manager has the actual admitted
                # thread, on its native preparation worker.
                assert isinstance(thread, str) and user_id == OWNER
                paths.ensure_thread_dirs(thread, user_id=user_id)
                prepared_threads.append(thread)
                return actual_prepare_uploads(thread, user_id=user_id)

            monkeypatch.setattr(upload_manager, "ensure_uploads_dir", prepare_actual_uploads)

            def write_actual_upload(directory, filename, data):
                charged_writes.append(supervisor.file_bytes_budget.used)
                assert data == inbound_bytes and supervisor.file_bytes_budget.used == len(inbound_bytes)
                return actual_write(directory, filename, data)

            monkeypatch.setattr(upload_manager, "write_upload_file_no_symlink", write_actual_upload)
        if dialogue is not None or produced_file:
            supervisor.outbox.clock = boot.clock.now
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
            nonlocal thread_id
            messages = args[0] if args else kwargs["messages"]
            model_inputs.extend(message for message in messages if message.type == "human" and "channel_turn_id" in message.additional_kwargs)
            if managed_ingress:
                # The actual controller has reserved the original publication
                # before its bus handoff can enter this real model callback.
                active_turn = next(iter(supervisor.turns._turns.values()))
                thread_id = active_turn.record.scope.thread_id
            if inbound_file:
                uploaded = model_inputs[-1].additional_kwargs.get("files")
                assert uploaded == [{"filename": "input.csv", "size": len(inbound_bytes), "path": "/mnt/user-data/uploads/input.csv", "is_image": False}]
                actual_path = paths.sandbox_uploads_dir(thread_id, user_id=OWNER) / "input.csv"
                assert await asyncio.to_thread(actual_path.read_bytes) == inbound_bytes
                assert supervisor.file_bytes_budget.used == 0 and prepared_threads == [thread_id]
            model_entered.set()
            if no_user:
                from deerflow.spaces.chat_paths import ChatStoragePaths

                assert type(get_paths()) is ChatStoragePaths
                assert get_paths().thread_data(thread_id, user_id=OWNER) == {}
                if case == "no_user_uncertain":
                    app.state.storage_spaces_enabled = False
            if case == "phase_one":
                await model_release.wait()
            if produced_file:

                def generate_files():
                    for filename in output_names:
                        (paths.sandbox_outputs_dir(thread_id, user_id=OWNER) / filename).write_bytes(output_bytes)

                await asyncio.to_thread(generate_files)
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
            for message_id, update_id, content in [] if managed_ingress else [(711, 1701, "First accepted question"), (710, 1702, "Second accepted question")]:
                incoming = Update(update_id, message=Message(message_id, link.created_at, Chat(link.chat_id, "private"), from_user=User(link.telegram_user_id, "Fixture", False), text=content))
                row, _ = await inbox.accept(link, incoming, sanitized_input={"text": content, "type": "text"})
                origins.append(await inbox.route(row, kind="turn", target_id=target, basis={"input_profile": {"version": 1, "kind": "plain_text", "forwarded": False}}, operation_ids=[]))
            if not managed_ingress:
                inbound = InboundMessage(channel_name="telegram", chat_id=str(link.chat_id), user_id=str(link.telegram_user_id), text="First accepted question", connection_id=link.connection_id, owner_user_id=OWNER, workspace_id=str(bot))
                thread_id = await supervisor.threads.ensure_thread(channel.link, inbound, origin=origins[0])
            if not no_user and thread_id is not None:
                await asyncio.to_thread(paths.ensure_thread_dirs, thread_id, user_id=OWNER)
            async with e.sf.begin() as writer:
                for row in await writer.scalars(select(TelegramOutboxRow)):
                    row.state = "sent"  # Initial welcome is outside this model-delivery test.
            await manager.start()
            if managed_ingress:
                from _telegram_bot_test_support import update_payload

                boot.normal.enqueue("sendChatAction")
                boot.normal.enqueue("sendMessageDraft")
                payload = update_payload(1701, message_id=711, account=link.telegram_user_id, text="First accepted question")
                if inbound_file:
                    payload["message"]["caption"] = payload["message"].pop("text")
                    payload["message"]["document"] = {"file_id": "actual-inbound-file", "file_unique_id": "actual-inbound-unique", "file_name": "input.csv", "mime_type": "text/csv", "file_size": len(inbound_bytes)}
                    boot.normal.enqueue("getFile", result={"file_id": "actual-inbound-file", "file_unique_id": "actual-inbound-unique", "file_size": len(inbound_bytes), "file_path": "documents/input.csv"})
                incoming = Update.de_json(payload, channel.bot)
                assert await channel.handle_update(incoming), "Actual SDK ingress must commit admission before advancing polling"
                active_turn = next(iter(supervisor.turns._turns.values()))
            else:
                record = await supervisor.publications.dispatch(channel.link, origins, thread_id=thread_id)
            try:
                await asyncio.wait_for(model_entered.wait(), 15)
            except TimeoutError:
                pytest.fail(f"Model never entered; HTTP outcomes: {http_responses!r}")
            if managed_ingress:
                record = active_turn.record
                assert record is not None and record.dispatch_started
            if case == "phase_one":
                await supervisor.begin_shutdown()
                assert not owner.closed and supervisor._subscribed
                model_release.set()
            await asyncio.wait_for(final.wait(), 15)
            if dialogue is not None:
                await _complete_actual_human_input_dialogue(
                    SimpleNamespace(e=e, boot=boot, supervisor=supervisor, channel=channel, link=link, model=model, model_inputs=model_inputs, record=record, first=active_turn, http_calls=http_calls, http_responses=http_responses),
                    dialogue=dialogue,
                )
                return
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
            elif managed_ingress:
                await asyncio.wait_for(active_turn.done.wait(), 15)
                async with e.sf() as reader:
                    completed_primary = await reader.scalar(select(TelegramOutboxRow).where(TelegramOutboxRow.link_id == link.id, TelegramOutboxRow.dedupe_key == "reply:" + record.turn_id))
                assert completed_primary is not None
                observed = SimpleNamespace(state="planned", outbox_id=completed_primary.id)
            else:
                observed = await supervisor.publications.observe(record, channel.link, max_pages=2)
            assert model.invocations == 1
            assert len(model_inputs) == 1
            assert restore_original_human_message(model_inputs[0]).content == ("First accepted question" if managed_ingress else "First accepted question\n\nSecond accepted question")
            assert model_inputs[0].additional_kwargs["channel_turn_id"] == record.publication_id
            if inbound_file:
                assert len(feature_readers) >= 2 and set(feature_readers) == {OWNER}
                assert any(method == "GET" and path == "/api/features" for method, path, _body in http_calls)
                assert file_downloads == ["actual-capped-get"] and charged_writes == [len(inbound_bytes)]
                assert [call.endpoint for call in boot.normal.calls].count("getFile") == 1
                assert isinstance(channel._requests[0], ManagedHTTPXRequest) and channel._requests[1] is boot.updates
                assert supervisor.file_bytes_budget.used == 0
            runs = await e.repository.list_by_thread(thread_id, user_id=OWNER)
            assert len(runs) == 1 and runs[0]["kwargs"]["output_capture"] == {"version": 2, "inventory_document_version": 2}
            assert runs[0]["kwargs"]["channel_execution"]["publication_id"] == record.publication_id
            assert runs[0]["kwargs"]["channel_execution"]["agent_name"] == (target if custom else None)
            assert runs[0]["kwargs"]["output_evidence"]["input_message_id"] == strip_injected_user_message_id_suffix(model_inputs[0].id)
            creates = [body for method, path, body in http_calls if method == "POST" and path.endswith(("/runs/stream", "/runs/wait", "/runs"))]
            assert len(creates) == 1
            assert creates[0]["channel_execution"]["publication_id"] == record.publication_id
            if case == "no_user_uncertain":
                assert observed.state == "held" and observed.reason == "inventory_unhandled"
                assert not any(call.endpoint == "sendMessage" for call in boot.normal.calls)
            else:
                assert observed.state == "planned"
                async with e.sf() as reader:
                    planned = await reader.get(TelegramOutboxRow, observed.outbox_id)
                proof = planned.payload["reply_basis"]["outcome"]["projection"]
                assert planned.payload["reply_basis"]["version"] in ({6} if produced_file else {2, 4} if managed_ingress else {2})
                assert proof["inventory"]["document"]["coverage"] == "complete"
                inventory_files = proof["inventory"]["document"]["items"]
                assert len(inventory_files) == (len(output_names) if produced_file else 0)
                assert proof["inventory"]["document"]["version"] == 2
                assert proof["inventory"]["document"]["output_view"]["kind"] == ("no_admitted_output_view" if no_user else "managed_outputs")
                if case == "phase_one":
                    assert not owner.closed and planned.state == "pending"
                    assert not any(call.endpoint == "sendMessage" for call in boot.normal.calls)
                else:
                    boot.normal.enqueue("sendDocument" if produced_file and not album else "sendMessage", result={"message_id": 501, "date": 1_893_456_000, "chat": {"id": link.chat_id, "type": "private"}, "text": "Durable answer"})
                    if album:
                        boot.normal.enqueue("sendMediaGroup", result=[{"message_id": identity, "date": 1_893_456_000, "chat": {"id": link.chat_id, "type": "private"}} for identity in (502, 503)])
                    await supervisor.sender.tick()
                    await supervisor.sender.drain()
                    if album:
                        # The real sender persists a one-second destination
                        # pacing floor after the text ACK. Advance the fixture
                        # clock, then admit its next durable delivery attempt.
                        boot.clock.advance(1)
                        await supervisor.sender.tick()
                        await supervisor.sender.drain()
                    sent = [call for call in boot.normal.calls if call.endpoint == ("sendDocument" if produced_file and not album else "sendMessage")]
                    assert len(sent) == 1 and sent[0].parameters["caption" if produced_file and not album else "text"] == "<b>Durable answer</b>"
                    if produced_file:
                        assert supervisor.file_bytes_budget.used == 0 and len(uploaded_outputs) == len(output_names)
                        artifact_reads = [(method, path) for method, path, _body in http_calls if "/artifacts/" in path]
                        assert any(method == "HEAD" for method, _path in artifact_reads)
                        assert sum(method == "GET" for method, _path in artifact_reads) == len(output_names)
                    assert sent[0].parameters["reply_parameters"]["message_id"] == 711
                    async with e.sf() as reader:
                        completed = await reader.get(TelegramOutboxRow, observed.outbox_id)
                        saved = await reader.get(TelegramInboundMarkRow, (link.id, 711, link.chat_id))
                    assert completed.state == "sent" and completed.parts[0]["message_ids"] == [501]
                    if produced_file:
                        media = completed.parts[-1] if album else completed.parts[0]
                        assert [file["sha256"] for file in media["source_ref"]["output_files"]] == uploaded_outputs
                        assert media["message_ids"] == ([502, 503] if album else [501])
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


@pytest.mark.asyncio
@pytest.mark.parametrize("case", ["no_user_text", "no_user_custom_text"])
async def test_actual_sdk_ingress_controller_worker_and_sender_compose(capture_environment, monkeypatch, tmp_path, case):
    """No scripted run/output proof or substituted controller/view/worker."""
    await test_actual_managed_turn_creates_once_and_delivers_only_verified_output(capture_environment, monkeypatch, tmp_path, case, managed_ingress=True)


async def _complete_actual_human_input_dialogue(env, *, dialogue):
    from test_telegram_callback_commands import press

    from deerflow.persistence.telegram_bots.model import TelegramCallbackRow

    e, supervisor, channel, boot, link = env.e, env.supervisor, env.channel, env.boot, env.link

    async def until(predicate):
        async with asyncio.timeout(20):
            while not predicate():
                await asyncio.sleep(0.01)

    def sent(identity):
        return {"message_id": identity, "date": 1_893_456_000, "chat": {"id": link.chat_id, "type": "private"}, "text": "Offline delivery"}

    await asyncio.wait_for(env.first.done.wait(), 20)
    async with e.sf() as reader:
        primary = await reader.scalar(select(TelegramOutboxRow).where(TelegramOutboxRow.link_id == link.id, TelegramOutboxRow.dedupe_key == "reply:" + env.record.turn_id))
    assert primary is not None and primary.payload["reply_basis"]["version"] == 5
    assert primary.payload["reply_basis"]["outcome"]["projection"]["output"]["reason"] == "human_input_required"
    request = primary.payload["reply_basis"]["outcome"]["request_basis"]["messages"][-1]["content"]["artifact"]["human_input"]
    boot.normal.enqueue("sendMessage", result=sent(501))
    boot.clock.advance(2)
    await supervisor.sender.tick()
    await supervisor.sender.drain()
    card = [call for call in boot.normal.calls if call.endpoint == "sendMessage"][-1]
    assert "value" in card.parameters["text"]
    if dialogue == "choice":
        callback_id = card.parameters["reply_markup"]["inline_keyboard"][0][0]["callback_data"].removeprefix("h1:")
        expected = "First"
        expected_kind = "option"
    else:
        assert card.parameters["reply_markup"]["force_reply"] is True
        from _telegram_bot_test_support import update_payload

        errors = []

        async def record_ingress_error(update, context):
            errors.append(context.error)

        channel.application.add_error_handler(record_ingress_error)
        incoming = Update.de_json(update_payload(1702, message_id=712, account=link.telegram_user_id, text="Actual field value"), channel.bot)
        assert await channel.handle_update(incoming), [repr(error) for error in errors]
        boot.normal.enqueue("editMessageText", result=sent(501))
        boot.clock.advance(2)
        await supervisor.sender.tick()
        await supervisor.sender.drain()
        review = [call for call in boot.normal.calls if call.endpoint == "editMessageText"][-1]
        assert "Actual field value" in review.parameters["text"]
        callback_id = review.parameters["reply_markup"]["inline_keyboard"][0][0]["callback_data"].removeprefix("h1:")
        expected = 'A value: Actual field value [values: {"value":"Actual field value"}]'
        expected_kind = "text"
    boot.normal.enqueue("answerCallbackQuery")
    boot.normal.enqueue("editMessageText", result=sent(501))
    boot.normal.enqueue("sendMessageDraft")
    assert await channel.handle_update(press(SimpleNamespace(link=link, channel=channel), 1703, callback_id, message_id=501))
    await channel._drain_ingress()
    async with e.sf() as reader:
        staged_choice = await reader.get(TelegramCallbackRow, callback_id)
    assert staged_choice.payload.get("publication_owner"), {
        "acknowledgements": [call.parameters for call in boot.normal.calls if call.endpoint == "answerCallbackQuery"],
        "controller_turns": len(supervisor.turns._turns),
        "model_invocations": env.model.invocations,
    }
    try:
        await until(lambda: env.model.invocations == 2)
    except TimeoutError:
        saved_runs = await e.repository.list_by_thread(env.first.record.scope.thread_id, user_id=OWNER)
        async with e.sf() as reader:
            staged_choice = await reader.get(TelegramCallbackRow, callback_id)
        owner = staged_choice.payload.get("publication_owner", {})
        pytest.fail(
            str(
                {
                    "model_invocations": env.model.invocations,
                    "turns": [
                        {
                            "record": turn.record.publication_id if turn.record else None,
                            "task_done": turn.task.done() if turn.task else None,
                            "capacity_waiting": turn.capacity_waiting,
                            "refusal": turn.refusal is not None,
                            "error_type": type(turn.task.exception()).__name__ if turn.task and turn.task.done() and not turn.task.cancelled() and turn.task.exception() else None,
                        }
                        for turn in supervisor.turns._turns.values()
                    ],
                    "saved_input": {key: owner.get(key) for key in ("current_publication_id", "confirmed_publication_id", "route_complete", "state")},
                    "runs": [{key: run.get(key) for key in ("run_id", "status", "stop_reason")} for run in saved_runs],
                    "creating_requests": [(method, path) for method, path, body in env.http_calls if method == "POST" and "/runs" in path],
                    "creating_responses": [(method, path, status) for method, path, status in env.http_responses if method == "POST" and "/runs" in path],
                }
            )
        )
    await until(lambda: not supervisor.turns._turns)
    actual = env.model_inputs[-1]
    response = actual.additional_kwargs["human_input_response"]
    assert restore_original_human_message(actual).content == expected
    assert response["value"] == expected and response["response_kind"] == expected_kind
    assert response["source"] == request["source"] == "ask_clarification" and response["request_id"] == request["request_id"]
    if dialogue == "choice":
        assert response["option_id"] == request["options"][0]["id"]
    assert actual.additional_kwargs["channel_turn_id"] != env.record.publication_id
    runs = await e.repository.list_by_thread(env.first.record.scope.thread_id, user_id=OWNER)
    assert len(runs) == 2 and all(run["status"] == "success" for run in runs)
    assert len([1 for method, path, body in env.http_calls if method == "POST" and path.endswith(("/runs/stream", "/runs/wait", "/runs"))]) == 2
    async with e.sf() as reader:
        selected = await reader.get(TelegramCallbackRow, callback_id)
        answer = await reader.scalar(select(TelegramOutboxRow).where(TelegramOutboxRow.link_id == link.id, TelegramOutboxRow.dedupe_key != primary.dedupe_key, TelegramOutboxRow.dedupe_key.like("reply:%")))
    assert selected.consumed_at is not None and answer is not None
    boot.normal.enqueue("sendMessage", result=sent(503))
    # Exercise the production pacing floor with an explicit clock.
    for _ in range(4):
        boot.clock.advance(2)
        await supervisor.sender.tick()
        await supervisor.sender.drain()
        async with e.sf() as reader:
            saved = await reader.get(TelegramOutboxRow, answer.id)
        if saved.state == "sent":
            break
    assert saved.state == "sent"
    assert any(call.endpoint == "sendMessage" and call.parameters["text"] == "<b>Durable answer</b>" for call in boot.normal.calls)


@pytest.mark.asyncio
@pytest.mark.parametrize("dialogue,case", [("choice", "no_user_text"), ("form", "no_user_custom_text")])
async def test_actual_worker_request_and_sdk_answer_complete_the_next_qualified_run(capture_environment, monkeypatch, tmp_path, dialogue, case):
    await test_actual_managed_turn_creates_once_and_delivers_only_verified_output(capture_environment, monkeypatch, tmp_path, case, managed_ingress=True, dialogue=dialogue)
