"""Actual SDK client teardown stays owned through failures and cancellation."""

import asyncio
import secrets
import threading
from unittest.mock import AsyncMock

import pytest
from _telegram_bot_test_support import RecordingRequest
from _telegram_repository_test_support import db as db
from sqlalchemy import select
from test_telegram_supervisor import _supervisor_logging as _supervisor_logging
from test_telegram_supervisor import build, credential, running_one

from app.channels.message_bus import OutboundMessage
from app.telegram_bots.cipher import token_cipher, unwrap_token, wrap_token
from deerflow.persistence.telegram_bots.model import TelegramOutboxRow

pytestmark = pytest.mark.asyncio


async def test_stop_caller_cancellation_retains_actual_client_cleanup(db, monkeypatch):
    e, link, held = await running_one(db, monkeypatch)
    entered, release = asyncio.Event(), asyncio.Event()
    shutdown = e.normal.shutdown

    async def blocked_shutdown():
        entered.set()
        await release.wait()
        await shutdown()

    monkeypatch.setattr(e.normal, "shutdown", blocked_shutdown)
    stopping = asyncio.create_task(e.supervisor.stop())
    try:
        await asyncio.wait_for(entered.wait(), 3)
        stopping.cancel()
        with pytest.raises(asyncio.CancelledError):
            await stopping
        assert not e.normal.closed
        # The existing owner survives its cancelled awaiter; a second stop
        # must join it instead of abandoning or racing a replacement close.
        finishing = asyncio.create_task(e.supervisor.stop())
        await asyncio.sleep(0)
        assert not finishing.done()
        release.set()
        await asyncio.wait_for(finishing, 3)
        assert e.normal.closed and e.updates.closed
        assert e.supervisor.channel(link.id) is None
    finally:
        release.set()
        await asyncio.gather(stopping, return_exceptions=True)
        await e.supervisor.stop()


async def test_sdk_shutdown_error_still_closes_both_exact_requests(db, monkeypatch):
    e, link, held = await running_one(db, monkeypatch)
    other_request, other_updates = RecordingRequest(), RecordingRequest()
    other, other_token = await credential(db, e.env, bot=link.bot_id + 1)
    other_request.enqueue_get_me(bot_id=other.bot_id)
    other_poll = other_updates.enqueue("getUpdates", result=[], release=asyncio.Event())
    e.supervisor.request_factory = lambda **kwargs: other_request if kwargs["connection_pool_size"] == 8 else other_updates
    await e.supervisor.add(other, other_token)
    await asyncio.wait_for(other_poll.started.wait(), 2)
    shutdown = e.normal.shutdown
    attempts = 0

    async def transient_shutdown_failure():
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise RuntimeError("Offline request shutdown failure")
        await shutdown()

    monkeypatch.setattr(e.normal, "shutdown", transient_shutdown_failure)
    try:
        with pytest.raises(RuntimeError, match="telegram-client-cleanup-failed"):
            await asyncio.wait_for(e.supervisor.stop(), 3)
        assert attempts >= 2
        assert e.normal.closed and e.updates.closed
        assert other_request.closed and other_updates.closed
        assert e.supervisor.channel(other.id) is None
        # An SDK cleanup error stays visible and keeps its client owned until
        # a later stop can establish complete cleanup.
        assert e.supervisor.channel(link.id) is not None
        await asyncio.wait_for(e.supervisor.stop(), 3)
        assert e.supervisor.channel(link.id) is None
    finally:
        await e.supervisor.stop()


def sent_message(link, message_id):
    return {"message_id": message_id, "date": 1_893_456_000, "chat": {"id": link.chat_id, "type": "private"}, "text": "Fixed product text"}


async def test_real_fixed_plan_claim_sdk_and_receipt_share_supervisor_sender(db, monkeypatch):
    e, link, held = await running_one(db, monkeypatch, connected=True)
    await db.links.end_pending(link.id, skip_reason="superseded")
    e.normal.enqueue("sendMessage", result=sent_message(link, 901))
    try:
        identity = await e.supervisor.channel(link.id).send_text("hello.privacy", source_id="fixed-supervisor")
        assert [call.endpoint for call in e.normal.calls] == ["getMe"]
        await e.supervisor.sender.tick()
        await e.supervisor.sender.drain()
        async with db.sessions() as session:
            row = await session.get(TelegramOutboxRow, identity)
            assert row.state == "sent" and row.parts[0]["state"] == "sent"
            assert row.parts[0]["message_ids"] == [901]
        assert [call.endpoint for call in e.normal.calls] == ["getMe", "sendMessage"]
    finally:
        await e.supervisor.stop()


async def test_outbound_filters_foreign_channel_bot_and_connection_before_adapter(db, monkeypatch):
    e, link, held = await running_one(db, monkeypatch, connected=True)
    channel = e.supervisor.channel(link.id)
    original = channel.send
    observed = AsyncMock(wraps=original)
    monkeypatch.setattr(channel, "send", observed)
    try:
        for name, bot, connection in [("slack", link.bot_id, link.connection_id), ("telegram", link.bot_id + 1, link.connection_id), ("telegram", str(link.bot_id), link.connection_id), ("telegram", link.bot_id, "foreign-connection")]:
            await e.service.bus.publish_outbound(OutboundMessage(channel_name=name, chat_id=str(link.chat_id), thread_id="untrusted-thread", text="Unsupported model text", metadata={"telegram_bot_id": bot}, connection_id=connection))
        observed.assert_not_awaited()
        # A routing match still has no authoritative model-final reference.
        for connection in [None, link.connection_id]:
            await e.service.bus.publish_outbound(
                OutboundMessage(channel_name="telegram", chat_id=str(link.chat_id), thread_id="untrusted-thread", text="Unsupported model text", metadata={"telegram_bot_id": link.bot_id}, connection_id=connection)
            )
        assert observed.await_count == 2
        assert [call.endpoint for call in e.normal.calls] == ["getMe"]
    finally:
        await e.supervisor.stop()


async def test_late_trusted_fixed_plan_survives_channel_drain_for_next_start(db, monkeypatch):
    e, link, held = await running_one(db, monkeypatch, connected=True)
    await db.links.end_pending(link.id, skip_reason="superseded")
    channel = e.supervisor.channel(link.id)
    observed = AsyncMock(wraps=channel.send)
    monkeypatch.setattr(channel, "send", observed)
    frame = OutboundMessage(channel_name="telegram", chat_id=str(link.chat_id), thread_id="untrusted-thread", text="Unsupported model text", metadata={"telegram_bot_id": link.bot_id}, connection_id=link.connection_id)
    successor = None
    try:
        await e.supervisor.begin_shutdown()
        # This is the real bus listener during the existing manager's drain.
        # Its unsupported model frame creates no fabricated output identity.
        await e.service.bus.publish_outbound(frame)
        observed.assert_awaited_once()
        identity = await channel.send_text("hello.privacy", source_id="late-trusted-fixed")
        async with db.sessions() as session:
            row = await session.get(TelegramOutboxRow, identity)
            assert row.state == "pending" and row.parts[0]["state"] == "pending"
        assert [call.endpoint for call in e.normal.calls] == ["getMe"]
        await e.supervisor.stop()
        await e.service.bus.publish_outbound(frame)
        observed.assert_awaited_once()

        successor = build(db, monkeypatch, environ=e.env)
        successor.normal.enqueue_get_me(bot_id=link.bot_id)
        successor.normal.enqueue("sendMessage", result=sent_message(link, 902))
        polling = successor.updates.enqueue("getUpdates", result=[], release=asyncio.Event())
        await successor.supervisor.check()
        await successor.supervisor.start()
        await asyncio.wait_for(polling.started.wait(), 2)
        await successor.supervisor.sender.tick()
        await successor.supervisor.sender.drain()
        async with db.sessions() as session:
            row = await session.get(TelegramOutboxRow, identity)
            assert row.state == "sent" and row.parts[0]["message_ids"] == [902]
            assert len(list(await session.scalars(select(TelegramOutboxRow).where(TelegramOutboxRow.dedupe_key == "notice:late-trusted-fixed")))) == 1
        assert [call.endpoint for call in successor.normal.calls] == ["getMe", "sendMessage"]
    finally:
        await e.supervisor.stop()
        if successor is not None:
            await successor.supervisor.stop()


@pytest.mark.parametrize("old_state", ["same", "different", "unreadable"])
async def test_token_replacement_clears_pause_only_with_positive_plaintext_proof(db, monkeypatch, old_state):
    e = build(db, monkeypatch)
    await e.supervisor.check()
    await e.supervisor.start()
    link, original_token = await credential(db, e.env)
    if old_state == "unreadable":
        link = await db.links.store_token(link.id, "unreadable-fixture", actor_id=link.owner_user_id)
    link = await db.links.set_health(link.id, "conflict", holds=True)
    assert link.acts_paused_since is not None
    candidate = original_token if old_state == "same" else str(link.bot_id) + ":" + secrets.token_urlsafe(30)
    e.normal.enqueue_get_me(bot_id=link.bot_id)
    poll = e.updates.enqueue("getUpdates", result=[], release=asyncio.Event())
    try:
        result = await e.supervisor.replace_token(link, candidate, actor_id=link.owner_user_id)
        await asyncio.wait_for(poll.started.wait(), 3)
        assert result.token_revision == link.token_revision + 1
        assert result.acts_paused_since == (None if old_state == "different" else link.acts_paused_since)
        stored, revision = await db.links.token(link.id)
        opened = unwrap_token(token_cipher(e.env), link.bot_id, stored)
        assert bool(opened.key == candidate) and revision == result.token_revision
    finally:
        await e.supervisor.stop()


async def test_token_comparison_losing_cas_never_retires_current_successor(db, monkeypatch):
    from app.telegram_bots import supervisor as module

    e, link, held = await running_one(db, monkeypatch)
    stored, revision = await db.links.token(link.id)
    entered, release = threading.Event(), threading.Event()
    original = module.unwrap_token

    def stalled_comparison(cipher, bot_id, encrypted):
        if encrypted == stored:
            entered.set()
            assert release.wait(10)
        return original(cipher, bot_id, encrypted)

    monkeypatch.setattr(module, "unwrap_token", stalled_comparison)
    proposed = str(link.bot_id) + ":" + secrets.token_urlsafe(30)
    replacing = asyncio.create_task(e.supervisor.replace_token(link, proposed, actor_id=link.owner_user_id))
    try:
        assert await asyncio.wait_for(asyncio.to_thread(entered.wait, 3), 4)
        winner_token = str(link.bot_id) + ":" + secrets.token_urlsafe(30)
        winner = await db.links.store_token(link.id, wrap_token(token_cipher(e.env), link.bot_id, winner_token), actor_id=link.owner_user_id)
        new_request, new_updates = RecordingRequest(), RecordingRequest()
        new_request.enqueue_get_me(bot_id=link.bot_id)
        new_poll = new_updates.enqueue("getUpdates", result=[], release=asyncio.Event())
        e.supervisor.request_factory = lambda **kwargs: new_request if kwargs["connection_pool_size"] == 8 else new_updates
        await e.supervisor.reconcile()
        await asyncio.wait_for(new_poll.started.wait(), 3)
        successor = e.supervisor.channel(link.id)
        assert successor.transport_revision == winner.token_revision
        release.set()
        with pytest.raises(ValueError, match="token-changed"):
            await replacing
        assert e.supervisor.channel(link.id) is successor
        assert not new_request.closed and not new_updates.closed
        assert (await db.links.get(link.id)).token_revision == winner.token_revision
    finally:
        release.set()
        await asyncio.gather(replacing, return_exceptions=True)
        await e.supervisor.stop()
