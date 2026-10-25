"""Real offline SDK output -> worker seal -> Telegram SQL primary planning.

Only the model is offline and file snapshots are disabled: there are no files in
this turn. The assistant identity comes from the SDK message before execution;
the test never replaces an identity or receipt after the worker has sealed it.
"""

from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest
from _run_output_store_test_support import RAW_OWNER
from _telegram_repository_test_support import code_of, rows, welcome
from _telegram_repository_test_support import db as db
from langchain_core.language_models.fake_chat_models import FakeMessagesListChatModel
from langchain_core.messages import AIMessage, HumanMessage
from langgraph.graph import END, START, MessagesState, StateGraph
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError
from telegram import Chat, Message, Update, User
from test_telegram_publication_custody import publication_environment as publication_environment

from app.gateway.channel_execution import ChannelExecutionReceipt
from app.gateway.run_output import RunOutputProjection
from app.gateway.stream_cleanup import StreamCleanupRegistry
from app.telegram_bots.inbox import InboxRepository
from app.telegram_bots.producer import Outbox
from deerflow.config.run_ownership_config import RunOwnershipConfig
from deerflow.persistence.models.run_event import RunEventRow
from deerflow.persistence.run.model import RunChangeClockRow, RunRow
from deerflow.persistence.run.sql import RunRepository
from deerflow.persistence.telegram_bots.model import TelegramInboundMarkRow, TelegramOutboxRow
from deerflow.runtime import MemoryStreamBridge
from deerflow.runtime.events.store.db import DbRunEventStore
from deerflow.runtime.runs.manager import RunManager
from deerflow.runtime.runs.output import canonical_text_digest, validate_receipt
from deerflow.runtime.runs.output_source import read_durable_output_source
from deerflow.runtime.runs.worker import RunContext, run_agent
from deerflow.runtime.user_context import reset_current_user, set_current_user

ANSWER = "Exact SDK answer 😀\nα"
LONG_SDK_ID = "provider-message-" + "opaque-identity-" * 32


@pytest.mark.asyncio
@pytest.mark.parametrize("sdk_id", ["sdk-message", LONG_SDK_ID], ids=["short-id", "long-id"])
async def test_genuine_worker_assistant_id_survives_primary_planning(db, monkeypatch, sdk_id):
    await db.settings.ensure(RAW_OWNER, timezone="UTC", locale="en-US")
    link = await db.links.add(
        owner_user_id=RAW_OWNER,
        agent_kind="lead_agent",
        agent_ref="lead_agent",
        bot_id=9_000_000_001,
        bot_username="fixture_publication_bot",
        encrypted_token="wrapped-fixture-publication",
        actor_id=RAW_OWNER,
    )
    code = code_of(await db.links.mint_code(link))
    link = await db.links.bind(
        await db.links.get(link.id),
        telegram_user_id=7_000_000_001,
        chat_id=7_000_000_001,
        first_name="Fixture",
        username="fixture_person",
        code=code,
        start_identity={"update_id": 201, "message_id": 101, "chat_id": 7_000_000_001},
        hello_parts=welcome("hello.chat"),
    )
    inbox = InboxRepository(db.sessions)
    update = Update(202, message=Message(102, link.created_at, Chat(link.chat_id, "private"), from_user=User(link.telegram_user_id, "Fixture", False), text="Original SDK question"))
    mark, _ = await inbox.accept(link, update, sanitized_input={"text": update.message.text})
    mark = await inbox.route(mark, kind="turn", target_id=link.agent_ref, basis={"kind": link.agent_kind, "target": link.agent_ref}, operation_ids=[])
    turn_id, thread_id = uuid4().hex, uuid4().hex
    await inbox.bind_turn([mark], turn_id=turn_id, thread_id=thread_id)
    mark, _ = await inbox.accept(link, update, sanitized_input={"text": "ignored redelivery"})
    publication_id = mark.route_basis["current_publication_id"]

    async with captured_projection(db, monkeypatch, link=link, thread_id=thread_id, publication_id=publication_id, sdk_id=sdk_id, input_text=update.message.text) as projection:
        await inbox.bind_run(turn_id=turn_id, publication_id=publication_id, run_id=projection.run_id)
        outbox = Outbox(db.sessions, inbox=inbox)
        before_ids = {row.id for row in await rows(db, TelegramOutboxRow)}
        source = {"thread_id": thread_id, "run_id": projection.run_id, "publication_id": publication_id, "assistant_message_id": projection.output_evidence.assistant_message_id}
        try:
            identity = await outbox.plan_reply(link, turn_id=turn_id, source=source, parts=[{"kind": "text", "source_ref": {key: source[key] for key in ("thread_id", "run_id", "assistant_message_id")}}], inbox_rows=[mark])
        except DBAPIError as error:
            # A capacity failure must roll back the primary and route together.
            assert db.dialect == "postgres" and getattr(error.orig, "sqlstate", None) == "22001"
            assert {row.id for row in await rows(db, TelegramOutboxRow)} == before_ids
            saved = next(row for row in await rows(db, TelegramInboundMarkRow) if row.message_id == mark.message_id)
            assert saved.assistant_message_id is None and saved.outbox_id is None and not saved.route_complete
            raise
        saved = next(row for row in await rows(db, TelegramInboundMarkRow) if row.message_id == mark.message_id)
        assert saved.assistant_message_id == sdk_id and saved.outbox_id == identity and saved.route_complete
        assert identity not in before_ids


@asynccontextmanager
async def captured_projection(db, monkeypatch, *, link, thread_id, publication_id, sdk_id, input_text):
    async with db.engine.begin() as connection:
        if db.dialect == "sqlite":
            await connection.execute(text("BEGIN"))
        await connection.run_sync(lambda sync: RunRow.metadata.create_all(sync, tables=[RunRow.__table__, RunChangeClockRow.__table__, RunEventRow.__table__]))
    for name in ("capture_output_snapshot", "capture_workspace_snapshot"):
        monkeypatch.setattr("deerflow.runtime.runs.worker." + name, AsyncMock(return_value=None))
    monkeypatch.setattr("deerflow.runtime.runs.worker._produced_output_paths", AsyncMock(return_value=[]))

    events = DbRunEventStore(db.sessions)
    manager = RunManager(RunRepository(db.sessions), event_store=events, run_ownership_config=RunOwnershipConfig(heartbeat_enabled=False))
    admission = ChannelExecutionReceipt(version=1, publication_id=publication_id, owner_user_id=link.owner_user_id, thread_id=thread_id, assistant_id="lead_agent", agent_name=None if link.agent_kind == "lead_agent" else link.agent_ref)
    record = await manager.create_or_reject(thread_id, "lead_agent", user_id=link.owner_user_id, kwargs={"output_capture": {"version": 1}, "channel_execution": admission.model_dump(mode="json")})
    human = HumanMessage(input_text, additional_kwargs={"channel_turn_id": publication_id})
    model = FakeMessagesListChatModel(responses=[AIMessage(ANSWER, id=sdk_id)])
    calls = []

    async def node(state, config):
        calls.append(config.get("run_id"))
        return {"messages": [await model.ainvoke(state["messages"], config)]}

    graph = StateGraph(MessagesState)
    graph.add_node("model", node)
    graph.add_edge(START, "model")
    graph.add_edge("model", END)
    compiled = graph.compile()
    bridge = MemoryStreamBridge()
    cleanups = StreamCleanupRegistry(bridge)
    token = set_current_user(SimpleNamespace(id=link.owner_user_id))
    try:
        await run_agent(
            bridge,
            manager,
            record,
            ctx=RunContext(checkpointer=None, event_store=events, schedule_stream_cleanup=cleanups.schedule),
            agent_factory=lambda **kwargs: compiled,
            graph_input={"messages": [human]},
            config={"run_id": uuid4(), "configurable": {"thread_id": thread_id, "agent_name": admission.agent_name}},
        )
        snapshot = await manager.get_output_snapshot(record.run_id, user_id=link.owner_user_id)
        assert snapshot.status.value == "success" and len(calls) == 1
        receipt = validate_receipt(snapshot.kwargs["output_evidence"])
        assert receipt.assistant_message_id == sdk_id and receipt.input_message_id == human.id
        audit = await read_durable_output_source(events, receipt=receipt)
        assert audit.state == "matched" and audit.text == ANSWER
        assert audit.input_message["additional_kwargs"]["channel_turn_id"] == publication_id
        projection = RunOutputProjection(
            version=1, owner_user_id=link.owner_user_id, thread_id=thread_id, run_id=record.run_id, run_status="success", state="complete", channel_execution=admission, output_evidence=receipt, source={"text": audit.text}, reason=None
        )
        assert projection.output_evidence.text_sha256 == canonical_text_digest(ANSWER)
        yield projection
    finally:
        reset_current_user(token)
        await manager.shutdown(timeout=2)
        await cleanups.close()
        await bridge.close()


@pytest.mark.asyncio
async def test_managed_primary_consumes_actual_worker_proof_and_completes_original_group(publication_environment, monkeypatch):
    e = publication_environment
    record = await e.reserve()
    guard = e.guard_factory()
    assert await e.inbox.mark_dispatch_started(record, guard=guard) is True
    # This dormant composition calls the real offline worker directly. It does
    # not stand in for the later installed bus/strict-HTTP dispatch consumer.
    async with captured_projection(e.db, monkeypatch, link=e.link, thread_id=e.thread_id, publication_id=record.publication_id, sdk_id=LONG_SDK_ID, input_text="\n".join(row.sanitized_input["text"] for row in e.origins)) as projection:
        bound = await e.inbox.bind_publication_run(record, run_id=projection.run_id, guard=guard)
        outbox = Outbox(e.sessions, inbox=e.inbox)
        identity = await outbox.plan_publication_reply(bound, output=projection, guard=guard)
        assert await outbox.plan_publication_reply(record, output=projection, guard=guard) == identity
        saved = [row for row in await rows(e.db, TelegramInboundMarkRow) if row.turn_id == record.turn_id]
        assert len(saved) == len(record.origins) == 2
        assert all(row.assistant_message_id == LONG_SDK_ID and row.outbox_id == identity and row.route_complete and row.state == "reply_pending" for row in saved)
        primary = next(row for row in await rows(e.db, TelegramOutboxRow) if row.id == identity)
        assert primary.dedupe_key == "reply:" + record.turn_id
        assert all(part["message_ids"] == [] for part in primary.parts)
