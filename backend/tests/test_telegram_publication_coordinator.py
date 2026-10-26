"""Actual SQL/bus/HTTP-owner handoff, with controlled remote HTTP replies.

This component suite uses the real host authority, SQL custody and retained HTTP
owner. Remote thread/run/output JSON is scripted; no model-creating or durable
remote source provenance is claimed here. Root's full composition covers that.
"""

import asyncio
from copy import deepcopy
from datetime import UTC, datetime
from types import SimpleNamespace

import httpx
import pytest
import pytest_asyncio
from _telegram_repository_test_support import db as db
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker
from telegram import Chat, Message, Update, User
from test_telegram_publication_authority import attempt_environment as attempt_environment

from app.channels.message_bus import InboundMessage, MessageBus
from app.telegram_bots.gateway_client import GatewayClientOwner
from app.telegram_bots.inbox import InboxRepository, InboxRow
from app.telegram_bots.producer import Outbox
from app.telegram_bots.publication import PublicationCoordinator, PublicationRefused
from app.telegram_bots.publication_authority import PublicationAttemptAuthority
from deerflow.persistence.telegram_bots.model import TelegramInboundMarkRow

pytestmark = pytest.mark.asyncio
PLAIN = {"version": 1, "kind": "plain_text", "forwarded": False}


@pytest_asyncio.fixture
async def coordinator_environment(attempt_environment):
    base = attempt_environment
    e = SimpleNamespace(**vars(base), requests=[], responder=None, thread_id="coordinator-original-thread")

    async def respond(request):
        e.requests.append((request.method, request.url.path, dict(request.url.params)))
        if e.responder is not None:
            return await e.responder(request)
        if request.url.path.endswith("/" + e.thread_id):
            return httpx.Response(200, json={"thread_id": e.thread_id, "owner_user_id": e.link.owner_user_id, "agent_instance": None, "metadata": {"channel_agent_name": "lead_agent"}})
        return httpx.Response(404, headers={"X-DeerFlow-Channel-Execution-Version": "1", "X-DeerFlow-Output-Inventory-Version": "1"}, json={})

    e.http = GatewayClientOwner(gateway_url="https://gateway.example", langgraph_url="https://runtime.example/original/api", transport=httpx.MockTransport(respond))
    e.host = PublicationAttemptAuthority(
        links=e.db.links, authority=e.live.authority, client=e.http, channel=lambda identity: e.current[0] if identity == e.link.id else None, create_allowed=lambda: e.creating[0], read_allowed=lambda: e.reading[0]
    )
    e.inbox, e.outbox, e.bus = InboxRepository(e.db.sessions), Outbox(e.db.sessions), MessageBus(inbound_queue_maxsize=4)
    e.coordinator = PublicationCoordinator(inbox=e.inbox, outbox=e.outbox, bus=e.bus, http_owner=e.http, capture_attempt=e.host.capture)

    async def add_origin(*, message_id=701, text="  First input α", profile=PLAIN, sanitized=None):
        incoming = Update(message_id + 1000, message=Message(message_id, datetime.now(UTC), Chat(e.link.chat_id, "private"), from_user=User(e.link.telegram_user_id, "Unit human", False), text=text))
        row, _ = await e.inbox.accept(e.link, incoming, sanitized_input=sanitized if sanitized is not None else {"type": "text", "text": text})
        basis = {"input_profile": deepcopy(profile)} if profile is not None else {}
        return await e.inbox.route(row, kind="turn", target_id=e.link.agent_ref, basis=basis, operation_ids=[])

    e.add_origin = add_origin
    e.origins = (await add_origin(), await add_origin(message_id=702, text="second input\n"))
    try:
        yield e
    finally:
        await e.coordinator.stop()
        e.bus.discard_pending_inbound()
        await e.http.stop()


async def saved_marks(e):
    async with e.db.sessions() as session:
        values = await session.scalars(
            select(TelegramInboundMarkRow).where(TelegramInboundMarkRow.link_id == e.link.id, TelegramInboundMarkRow.message_id.in_([row.message_id for row in e.origins])).order_by(TelegramInboundMarkRow.message_id)
        )
        return tuple(InboxRow.from_row(row) for row in values)


async def test_real_sql_handoff_is_once_with_exact_private_context(coordinator_environment):
    e = coordinator_environment
    record = await e.coordinator.dispatch(e.link, tuple(reversed(e.origins)), thread_id=e.thread_id)
    message = e.bus.get_inbound_nowait()
    e.bus.inbound_task_done()
    assert message.text == "  First input α\n\nsecond input\n"
    assert (message.channel_name, message.owner_user_id, message.connection_id, message.workspace_id, message.topic_id) == ("telegram", e.link.owner_user_id, e.link.connection_id, str(e.link.bot_id), None)
    assert (message.chat_id, message.user_id, message.thread_ts) == (str(e.link.chat_id), str(e.link.telegram_user_id), "701")
    assert message.execution_required is True
    assert message.execution_context.publication_id == record.publication_id
    assert message.execution_context.client.capture_inventory is True and message.execution_context.client.capture_output is True
    assert message.metadata["channel_turn_id"] == record.publication_id and message.metadata["event_id"] == record.event_id
    assert message.metadata["human_message_kwargs"]["channel_turn_id"] == record.publication_id
    assert record.dispatch_started is True
    assert all(row.route_basis["publications"][-1]["publication_execution"]["dispatch_started"] is True for row in await saved_marks(e))
    replay = await e.coordinator.dispatch(e.link, e.origins, thread_id=e.thread_id)
    assert replay.publication_id == record.publication_id and e.bus.inbound_queue.empty()
    assert all(method == "GET" for method, _, _ in e.requests), "Enqueue is not creating HTTP proof"


async def test_concurrent_real_marker_contenders_enqueue_one_item(coordinator_environment):
    e = coordinator_environment
    records = await asyncio.gather(*(e.coordinator.dispatch(e.link, e.origins, thread_id=e.thread_id) for _ in range(3)))
    assert len({row.publication_id for row in records}) == 1 and e.bus.inbound_queue.qsize() == 1
    assert all(row.dispatch_started for row in records)


@pytest.mark.parametrize("closed", [False, True])
async def test_real_bus_capacity_refuses_before_marker(coordinator_environment, closed):
    e = coordinator_environment
    if closed:
        e.bus.close_inbound()
    else:
        for n in range(e.bus.inbound_queue_maxsize):
            item = InboundMessage("other", "chat", "user", str(n))
            reservation = e.bus.reserve_inbound(item)
            reservation.commit(item)
    with pytest.raises(PublicationRefused):
        await e.coordinator.dispatch(e.link, e.origins, thread_id=e.thread_id)
    marks = await saved_marks(e)
    assert all(not row.route_basis["publications"][-1]["publication_execution"]["dispatch_started"] for row in marks)
    assert not any(method == "POST" for method, _, _ in e.requests)


@pytest.mark.parametrize(
    "profile", [None, {}, {"version": True, "kind": "plain_text", "forwarded": False}, {"version": 1, "kind": "plain_text", "forwarded": 0}, {"version": 1, "kind": "plain_text", "forwarded": True}, {**PLAIN, "unknown": True}]
)
async def test_unproved_input_profile_refuses_before_any_custody(coordinator_environment, profile):
    e = coordinator_environment
    origin = await e.add_origin(message_id=703, profile=profile)
    with pytest.raises(PublicationRefused):
        await e.coordinator.dispatch(e.link, (origin,), thread_id=e.thread_id)
    async with e.db.sessions() as session:
        row = await session.get(TelegramInboundMarkRow, (e.link.id, origin.message_id, origin.chat_id))
        assert row.turn_id is None
    assert not e.requests and e.bus.inbound_queue.empty()


@pytest.mark.parametrize(
    "sanitized", [{"type": "text", "text": "safe", "caption": "media caption"}, {"type": "text", "text": "safe", "files": []}, {"type": "text", "text": "safe", "command": "stop"}, {"text": "missing type"}, {"type": "text", "text": "   "}]
)
async def test_unsupported_sanitized_input_is_not_subtracted(coordinator_environment, sanitized):
    e = coordinator_environment
    origin = await e.add_origin(message_id=703, sanitized=sanitized)
    with pytest.raises(PublicationRefused):
        await e.coordinator.dispatch(e.link, (origin,), thread_id=e.thread_id)
    assert not e.requests and e.bus.inbound_queue.empty()


async def test_retirement_after_marker_commit_leaves_unresolved_without_handoff(coordinator_environment, monkeypatch):
    e = coordinator_environment
    actual = e.inbox.mark_dispatch_started

    async def retire(*args, **kwargs):
        result = await actual(*args, **kwargs)
        e.reading[0] = False
        return result

    monkeypatch.setattr(e.inbox, "mark_dispatch_started", retire)
    with pytest.raises(PublicationRefused):
        await e.coordinator.dispatch(e.link, e.origins, thread_id=e.thread_id)
    assert e.bus.inbound_queue.empty()
    assert all(row.route_basis["publications"][-1]["publication_execution"]["dispatch_started"] is True for row in await saved_marks(e))


async def test_logger_failure_after_actual_enqueue_does_not_allow_second_handoff(coordinator_environment, monkeypatch):
    from app.channels import message_bus

    e = coordinator_environment
    actual = message_bus.logger.info

    def fail(message, *args, **kwargs):
        if message.startswith("[Bus] inbound enqueued"):
            raise RuntimeError("private diagnostic")
        return actual(message, *args, **kwargs)

    monkeypatch.setattr(message_bus.logger, "info", fail)
    with pytest.raises(PublicationRefused, match="^Managed publication refused$"):
        await e.coordinator.dispatch(e.link, e.origins, thread_id=e.thread_id)
    assert e.bus.inbound_queue.qsize() == 1
    record = await e.coordinator.dispatch(e.link, e.origins, thread_id=e.thread_id)
    assert record.dispatch_started is True and e.bus.inbound_queue.qsize() == 1


async def test_input_snapshot_is_detached_before_authority_await(coordinator_environment, monkeypatch):
    e = coordinator_environment
    entered, release = asyncio.Event(), asyncio.Event()
    actual = e.host.capture

    async def held(*args, **kwargs):
        entered.set()
        await release.wait()
        return await actual(*args, **kwargs)

    coordinator = PublicationCoordinator(inbox=e.inbox, outbox=e.outbox, bus=e.bus, http_owner=e.http, capture_attempt=held)
    origins = deepcopy(e.origins)
    task = asyncio.create_task(coordinator.dispatch(e.link, origins, thread_id=e.thread_id))
    try:
        await asyncio.wait_for(entered.wait(), 5)
        origins[0].sanitized_input["text"] = "caller changed after snapshot"
        release.set()
        record = await task
        assert e.bus.get_inbound_nowait().text.startswith("  First input α\n\n")
        e.bus.inbound_task_done()
        assert record.dispatch_started is True
    finally:
        release.set()
        await asyncio.gather(task, return_exceptions=True)
        await coordinator.stop()


async def test_shutdown_closes_new_handoff_but_keeps_borrowed_owner_open(coordinator_environment):
    e = coordinator_environment
    e.coordinator.begin_shutdown()
    with pytest.raises(PublicationRefused):
        await e.coordinator.dispatch(e.link, e.origins, thread_id=e.thread_id)
    assert not e.requests
    await e.coordinator.stop()
    await e.coordinator.stop()
    assert e.http.closed is False


@pytest.mark.parametrize("field", ["thread_id", "endpoint_id"])
async def test_declared_attempt_bookmark_must_match_original_scope(coordinator_environment, field):
    e = coordinator_environment

    async def mismatched(*args, **kwargs):
        actual = await e.host.capture(*args, **kwargs)
        values = {"thread_id": e.thread_id, "endpoint_id": e.http.endpoint_id}
        values[field] = "different-thread" if field == "thread_id" else "f" * 64
        # Controlled host bookmark corruption over actual SQL authority/guards.
        # This does not claim a different thread's remote ownership was proved.
        return SimpleNamespace(admission=actual.admission, create_guard=actual.create_guard, read_guard=actual.read_guard, **values)

    coordinator = PublicationCoordinator(inbox=e.inbox, outbox=e.outbox, bus=e.bus, http_owner=e.http, capture_attempt=mismatched)
    try:
        with pytest.raises(PublicationRefused):
            await coordinator.dispatch(e.link, e.origins, thread_id=e.thread_id)
        assert e.bus.inbound_queue.empty() and not e.requests
        assert all(row.turn_id is None for row in await saved_marks(e))
    finally:
        await coordinator.stop()


async def test_wrapped_guard_keeps_actual_pre_writer_permission_refresh(coordinator_environment, monkeypatch):
    from app.telegram_bots import authority

    e = coordinator_environment
    permitted = [True]
    actual_require = authority.require

    async def require(owner, *actions, **kwargs):
        if "runs:create" in actions and not permitted[0]:
            raise authority.CeilingRefused()
        return await actual_require(owner, *actions, **kwargs)

    async def withdraw_after_thread(request):
        permitted[0] = False
        return httpx.Response(200, json={"thread_id": e.thread_id, "owner_user_id": e.link.owner_user_id, "agent_instance": None, "metadata": {"channel_agent_name": "lead_agent"}})

    monkeypatch.setattr(authority, "require", require)
    e.responder = withdraw_after_thread
    with pytest.raises(PublicationRefused):
        await e.coordinator.dispatch(e.link, e.origins, thread_id=e.thread_id)
    assert all(row.turn_id is None for row in await saved_marks(e)), "Withdrawal after earlier creating preflight must precede first custody write"
    assert e.bus.inbound_queue.empty()


async def test_cancelled_committed_marker_unit_remains_owned_until_exit_settles(coordinator_environment, monkeypatch):
    e = coordinator_environment
    entered, release = asyncio.Event(), asyncio.Event()
    marking = [False]

    class HeldExitSession(AsyncSession):
        async def __aexit__(self, *args):
            result = await super().__aexit__(*args)
            if marking[0]:
                entered.set()
                await release.wait()
            return result

    # Actual transaction commit/close happens before the held exit bookkeeping.
    # This is not a native cursor-close or driver-commit survival claim.
    e.inbox.session_factory = async_sessionmaker(e.db.engine, class_=HeldExitSession, expire_on_commit=False)
    actual = e.inbox.mark_dispatch_started

    async def mark(*args, **kwargs):
        marking[0] = True
        try:
            return await actual(*args, **kwargs)
        finally:
            marking[0] = False

    monkeypatch.setattr(e.inbox, "mark_dispatch_started", mark)
    operation = asyncio.create_task(e.coordinator.dispatch(e.link, e.origins, thread_id=e.thread_id))
    stop = None
    try:
        await asyncio.wait_for(entered.wait(), 5)
        operation.cancel()
        done, _ = await asyncio.wait({operation}, timeout=0.05)
        assert not done
        operation.cancel()
        done, _ = await asyncio.wait({operation}, timeout=0.05)
        assert not done
        assert all(row.route_basis["publications"][-1]["publication_execution"]["dispatch_started"] is True for row in await saved_marks(e))
        assert e.bus.inbound_queue.empty()
        stop = asyncio.create_task(e.coordinator.stop())
        done, _ = await asyncio.wait({stop}, timeout=0.05)
        assert not done and e.http.closed is False
        stop.cancel()
        done, _ = await asyncio.wait({stop}, timeout=0.05)
        assert not done
        release.set()
        with pytest.raises(asyncio.CancelledError):
            await operation
        with pytest.raises(asyncio.CancelledError):
            await stop
        await e.coordinator.stop()
        assert e.bus.inbound_queue.empty() and e.http.closed is False
    finally:
        release.set()
        await asyncio.gather(operation, *([stop] if stop is not None else []), return_exceptions=True)
