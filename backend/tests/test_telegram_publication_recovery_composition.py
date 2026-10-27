"""Actual worker SQL output survives recreation of the recovery consumer.

Real graph/SDK/ASGI/SQL/source/sender; controlled model/config and ordinary task
Paths. This does not claim native-volume or NoUser qualification.
"""

import asyncio

import httpx
import pytest
from _run_output_capture_test_support import capture_environment as capture_environment
from _run_output_capture_test_support import output_store as output_store
from _telegram_repository_test_support import db as db
from sqlalchemy import select
from test_telegram_publication_recovery_custody import recovery_environment as recovery_environment
from test_telegram_supervisor import _supervisor_logging as _supervisor_logging
from test_telegram_supervisor import build
from test_telegram_turn_composition import test_actual_managed_turn_creates_once_and_delivers_only_verified_output as exercise_turn


@pytest.mark.asyncio
@pytest.mark.parametrize("case", ["recovery_text", "recovery_custom_text"])
async def test_fresh_recovery_consumer_plans_real_saved_output_without_second_model(capture_environment, monkeypatch, tmp_path, case):
    await exercise_turn(capture_environment, monkeypatch, tmp_path, case)


@pytest.mark.asyncio
async def test_actual_startup_closes_and_sends_fixed_notice_without_private_or_model_call(recovery_environment, monkeypatch):
    """Real startup, custody, SQL, sender and SDK; target config controlled."""
    from app.telegram_bots.cipher import token_cipher, wrap_token
    from app.telegram_bots.gateway_client import GatewayClientOwner
    from deerflow.persistence.telegram_bots.model import TelegramInboundMarkRow, TelegramOutboxRow

    e = recovery_environment
    boot = build(e.db, monkeypatch)
    host = boot.supervisor
    host.authority = e.live.authority
    private_calls = []

    def no_private_io(request):
        private_calls.append((request.method, request.url.path))
        raise AssertionError("Fixed restart notice reached private/model HTTP")

    async def no_named_ceilings(*args, **kwargs):
        return frozenset()

    monkeypatch.setattr("app.gateway.authz.resolve_route_permissions", no_named_ceilings)
    owner = GatewayClientOwner(gateway_url="http://gateway.test", langgraph_url="http://gateway.test/api", transport=httpx.MockTransport(no_private_io))
    host.thread_client = owner
    e.endpoint_id = owner.endpoint_id
    token = str(e.link.bot_id) + ":" + "synthetic_" * 5
    e.link = await e.links.store_token(e.link.id, wrap_token(token_cipher(boot.env), e.link.bot_id, token), actor_id=e.owner_id)
    record = await e.reserve()
    async with e.sessions.begin() as writer:
        for row in await writer.scalars(select(TelegramOutboxRow)):
            row.state = "sent"  # Welcome delivery is outside this recovery test.
    boot.normal.enqueue_get_me(bot_id=e.link.bot_id)
    boot.updates.enqueue("getUpdates", result=[], release=asyncio.Event())
    try:
        await host.check()
        await host.start()
        assert host.send_allowed() and host._recovery_phase is None
        async with e.sessions() as reader:
            primary = await reader.scalar(select(TelegramOutboxRow).where(TelegramOutboxRow.link_id == e.link.id, TelegramOutboxRow.dedupe_key == "reply:" + record.turn_id))
            saved = await reader.get(TelegramInboundMarkRow, (e.link.id, 701, e.link.chat_id))
        assert primary is not None and primary.state == "pending"
        assert primary.payload["reply_basis"]["version"] == 3
        assert saved.route_complete and saved.run_id is None and saved.assistant_message_id is None
        assert saved.route_basis["publication_recovery"]["kind"] == "restart_before_dispatch"
        boot.normal.enqueue("sendMessage", result={"message_id": 9901, "date": 1_893_456_000, "chat": {"id": e.link.chat_id, "type": "private"}, "text": "restart notice"})
        await host.sender.tick()
        await host.sender.drain()
        sent = [call for call in boot.normal.calls if call.endpoint == "sendMessage"]
        assert len(sent) == 1
        assert sent[0].parameters["text"] == "The service restarted before this was answered. Your message is kept."
        assert sent[0].parameters["reply_parameters"]["message_id"] == 701
        async with e.sessions() as reader:
            completed = await reader.get(TelegramOutboxRow, primary.id)
            saved = await reader.get(TelegramInboundMarkRow, (e.link.id, 701, e.link.chat_id))
        assert completed.state == "sent" and completed.parts[0]["message_ids"] == [9901]
        assert saved.state == "done" and saved.run_id is None and saved.assistant_message_id is None
        assert private_calls == []
    finally:
        await host.stop()
