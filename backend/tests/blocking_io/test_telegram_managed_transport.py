"""A cold recipient timezone must not block the SDK sender's owning loop."""

import asyncio
import secrets
import threading
from types import SimpleNamespace
from zoneinfo import ZoneInfo

import pytest
from _telegram_bot_test_support import ManualClock, RecordingRequest
from _telegram_repository_test_support import add_link, bind_link
from _telegram_repository_test_support import db as db
from sqlalchemy import select
from support.detectors.blocking_io_runtime import BlockingError

from app.telegram_bots.adapter import TelegramBotChannel
from app.telegram_bots.producer import DeliveryEligibility, Outbox, OutboxSender
from deerflow.persistence.telegram_bots.model import TelegramOutboxRow


@pytest.mark.asyncio
async def test_cold_recipient_timezone_calculation_is_off_loop_with_actual_sdk(db):
    # Setup's validators are independent of the cold send-time regression.
    await asyncio.to_thread(ZoneInfo, "America/New_York")
    link = await bind_link(db, await add_link(db))
    timezone = "America/Argentina/Ushuaia"
    await asyncio.to_thread(ZoneInfo, timezone)
    await db.settings.update(link.owner_user_id, timezone=timezone)
    async with db.sessions.begin() as session:
        for row in await session.scalars(select(TelegramOutboxRow)):
            row.state = "sent"
    owner_loop, owner_thread, observed_loops, construction_threads = asyncio.get_running_loop(), threading.get_ident(), [], []
    authority_parts = []

    class LoopRecordingRequest(RecordingRequest):
        async def initialize(self):
            observed_loops.append(asyncio.get_running_loop())
            await super().initialize()

        async def shutdown(self):
            observed_loops.append(asyncio.get_running_loop())
            await super().shutdown()

        async def do_request(self, *args, **kwargs):
            observed_loops.append(asyncio.get_running_loop())
            return await super().do_request(*args, **kwargs)

    request, updates, clock = LoopRecordingRequest(), LoopRecordingRequest(), ManualClock()
    request.enqueue_get_me(bot_id=link.bot_id)
    request.enqueue("sendMessage", result={"message_id": 501, "date": 1_893_456_000, "chat": {"id": link.chat_id, "type": "private"}, "text": "Saved product text"})

    async def protect_notice(context):
        return False

    async def allowed(link, row, part):
        observed_loops.append(asyncio.get_running_loop())
        authority_parts.append(part["state"] if part is not None else None)
        assert (await db.links.get(link.id)).binding_generation == row.binding_generation
        return DeliveryEligibility.allow()

    def factory(**kwargs):
        construction_threads.append(threading.get_ident())
        return request if kwargs["connection_pool_size"] == 8 else updates

    services = SimpleNamespace(
        send_allowed=lambda: True,
        config=db.config,
        links=db.links,
        settings=db.settings,
        outbox=Outbox(db.sessions, clock=clock.now),
        tls_context=object(),
        request_factory=factory,
        protect_notice=protect_notice,
        eligibility=allowed,
    )
    token = str(link.bot_id) + ":" + secrets.token_urlsafe(30)
    channel = await asyncio.to_thread(TelegramBotChannel, link=link, token=token, supervisor=services, monotonic=clock.monotonic, sleep=clock.sleep, now=clock.now)
    services.channel = lambda link_id: channel if link_id == link.id else None
    assert construction_threads and all(thread != owner_thread for thread in construction_threads)
    failures = []

    async def send(*, link_id, **scope):
        try:
            return await channel.send_part(**scope)
        except BlockingError as error:
            failures.append(error)
            raise

    sender = OutboxSender(services.outbox, run_id="cold-zone-process", deliver_part=send, eligibility=allowed)
    try:
        await channel.start()
        identity = await channel.send_text("hello.privacy", source_id="cold-timezone")
        await asyncio.to_thread(ZoneInfo.clear_cache)
        await sender.tick()
        await sender.drain()
        if failures:
            raise failures[0]
        async with db.sessions() as session:
            row = await session.get(TelegramOutboxRow, identity)
            assert row.state == "sent" and row.parts[0]["message_ids"] == [501]
        assert request.calls[-1].endpoint == "sendMessage"
        assert request.calls[-1].parameters["disable_notification"] is True
        assert authority_parts == [None, "pending", "sending"]
    finally:
        await sender.stop()
        await channel.stop()
        assert observed_loops and all(loop is owner_loop for loop in observed_loops)
        await asyncio.to_thread(ZoneInfo.clear_cache)
