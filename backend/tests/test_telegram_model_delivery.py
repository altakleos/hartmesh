"""Real SQL and pinned SDK transport; proof DTOs are synthetic protocol input."""

import asyncio
import secrets
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from _telegram_bot_test_support import ManualClock, RecordingRequest
from _telegram_repository_test_support import db as db  # noqa: F401
from sqlalchemy import select
from test_telegram_publication_custody import publication_environment as publication_environment
from test_telegram_publication_primary import _managed_turn, _output
from test_telegram_verified_reply import projection

from app.telegram_bots.adapter import TelegramBotChannel
from app.telegram_bots.model_source import ModelSource
from app.telegram_bots.producer import Outbox, OutboxSender, PrimaryTurnNotice
from deerflow.persistence.telegram_bots.model import TelegramOutboxRow


@pytest.mark.asyncio
@pytest.mark.parametrize("mode", ["valid", "changed-after-prefix", "retired-token", "part-fixed-injection", "claimed-callback", "claimed-media", "claimed-contradiction", "claimed-missing-basis", "legacy-fixed"])
async def test_actual_sender_rereads_each_model_part_and_preserves_prefix(publication_environment, monkeypatch, mode):
    e = publication_environment
    record = await _managed_turn(e)
    text = "x" * 5000 if mode == "changed-after-prefix" else "**synthetic answer**"
    output = projection(record, text=text)
    clock, request, updates = ManualClock(), RecordingRequest(), RecordingRequest()
    request.enqueue_get_me(bot_id=e.link.bot_id)
    for number in range(2):
        request.enqueue("sendMessage", result={"message_id": 501 + number, "date": 1_893_456_000, "chat": {"id": e.link.chat_id, "type": "private"}, "text": "Recorded model text"})
    monkeypatch.setattr("deerflow.config.agents_config.load_agent_config", lambda *args, **kwargs: SimpleNamespace(name=e.link.agent_ref))
    box = Outbox(e.sessions, clock=clock.now)
    if mode == "legacy-fixed":
        identity = await box.plan_publication_notice(record, notice=PrimaryTurnNotice("sealed_failed", _output(record, reason="failed")), guard=e.guard_factory())
        monkeypatch.setattr("app.gateway.authz.resolve_route_permissions", AsyncMock(return_value=[]))
    else:
        identity = await box.plan_verified_publication_reply(record, output=output, guard=e.guard_factory())
    async with e.sessions.begin() as session:
        for row in await session.scalars(select(TelegramOutboxRow)):
            if row.id != identity:
                row.state = "sent"
            elif mode == "part-fixed-injection":
                from copy import deepcopy

                altered = deepcopy(row.parts)
                altered[0]["source_ref"] = {"key": "error.generic", "values": {}}
                row.parts = altered
    reads = []

    async def read(original, current_link):
        reads.append((original, current_link))
        if mode == "retired-token":
            await e.links.store_token(e.link.id, "new-wrapped-fixture", actor_id=e.owner_id)
        return projection(record, text="changed source") if mode == "changed-after-prefix" and len(reads) > 1 else output

    async def protect_notice(context):
        return False

    services = SimpleNamespace(
        send_allowed=lambda: True,
        config=e.db.config,
        links=e.links,
        settings=e.db.settings,
        outbox=box,
        tls_context=object(),
        product_name="HartMesh",
        request_factory=lambda **kwargs: request if kwargs["connection_pool_size"] == 8 else updates,
        protect_notice=protect_notice,
        eligibility=e.live.authority.delivery,
        model_source=ModelSource(read_output=read),
    )
    channel = await asyncio.to_thread(TelegramBotChannel, link=e.link, token=str(e.link.bot_id) + ":" + secrets.token_urlsafe(30), supervisor=services, monotonic=clock.monotonic, sleep=clock.sleep, now=clock.now)
    services.channel = lambda link_id: channel if link_id == e.link.id else None

    async def deliver(*, link_id, **scope):
        if mode in {"claimed-callback", "claimed-media", "claimed-contradiction", "claimed-missing-basis"}:
            from copy import deepcopy

            async with e.sessions.begin() as session:
                row = await session.get(TelegramOutboxRow, identity)
                parts = deepcopy(row.parts)
                if mode == "claimed-callback":
                    parts[0]["callback_group_id"] = "unsupported-injected-group"
                elif mode == "claimed-media":
                    parts[0]["kind"] = "document"
                    parts[0]["source_ref"] = {"path": "/mnt/user-data/outputs/unqualified.txt"}
                elif mode == "claimed-missing-basis":
                    payload = deepcopy(row.payload)
                    payload.pop("reply_basis")
                    row.payload = payload
                else:
                    payload = deepcopy(row.payload)
                    payload["reply_basis"]["version"] = 1
                    payload["reply_basis"]["outcome"] = {"kind": "sealed_failed", "projection": {"version": 1, "state": "unavailable", "reason": "failed", "output_evidence": {"kind": "unavailable", "reason": "failed"}}}
                    row.payload = payload
                row.parts = parts
        return await channel.send_part(**scope)

    sender = OutboxSender(box, run_id="model-test-process", deliver_part=deliver, eligibility=e.live.authority.delivery)
    try:
        await channel.start()
        claimed = await sender.tick()
        await sender.drain()
        if mode != "part-fixed-injection":
            assert claimed == 1
        if mode == "changed-after-prefix":
            # The existing durable one-second part pacing releases its worker.
            clock.advance(2)
            assert await sender.tick() == 1
            await sender.drain()
        async with e.sessions() as session:
            saved = await session.get(TelegramOutboxRow, identity)
        sent = [call for call in request.calls if call.endpoint == "sendMessage"]
        if mode == "legacy-fixed":
            assert len(sent) == 1 and not reads
            assert saved.state == "sent" and saved.parts[0]["source_ref"] == {"key": "error.generic", "values": {}}
        elif mode == "claimed-media":
            assert not sent and not reads and len(saved.parts) == 1 and "unavailable_reason" not in saved.parts[0]
        elif mode in {"claimed-callback", "claimed-contradiction", "claimed-missing-basis"}:
            assert not sent and not reads and saved.state == "failed", "A saved part that does not validate never will, so the person is told"
        elif mode == "part-fixed-injection":
            assert claimed == 0 and not sent and not reads
        elif mode == "valid":
            assert len(sent) == 1 and sent[0].parameters["text"] == "<b>synthetic answer</b>"
            assert saved.state == "sent" and saved.parts[0]["message_ids"] == [501]
            assert len(reads) == 1
        elif mode == "changed-after-prefix":
            assert len(sent) == 1 and len(reads) == 2
            assert saved.parts[0]["state"] == "sent" and saved.parts[0]["message_ids"] == [501]
            assert saved.parts[1]["state"] != "sent" and saved.state == "failed", "The rest cannot be sent, and the person is told"
        else:
            assert len(reads) == 1 and not sent and saved.state != "failed"
    finally:
        await sender.stop()
        await channel.stop()
