"""Host UI receipts neither grant public routing nor change model input identity."""

from dataclasses import replace
from datetime import UTC, datetime

import pytest
from _telegram_repository_test_support import db as db
from telegram import Chat, Message, Update, User
from test_telegram_publication_authority import attempt_environment as attempt_environment
from test_telegram_publication_coordinator import coordinator_environment as coordinator_environment

from app.telegram_bots.inbox import _publication_basis, _publication_input


@pytest.mark.asyncio
async def test_public_route_cannot_assert_host_human_input_ui_receipt(coordinator_environment):
    e = coordinator_environment
    incoming = Update(1901, message=Message(901, datetime.now(UTC), Chat(e.link.chat_id, "private"), from_user=User(e.link.telegram_user_id, "Human", False), text="Actual accepted input"))
    row, _ = await e.inbox.accept(e.link, incoming, sanitized_input={"type": "text", "text": incoming.message.text})
    with pytest.raises(ValueError, match="invalid-inbox-route"):
        await e.inbox.route(row, kind="turn", target_id=e.link.agent_ref, basis={"input_profile": {"version": 1, "kind": "plain_text", "forwarded": False}, "human_input_ui": {"held_prompt_id": "a" * 32}}, operation_ids=[])


@pytest.mark.asyncio
async def test_server_ui_receipt_does_not_change_business_digest_or_shared_publication_basis(coordinator_environment):
    e = coordinator_environment
    changed = tuple(replace(row, route_basis={**row.route_basis, "human_input_ui": {"form_id": "a" * 32, "revision": index}}) for index, row in enumerate(e.origins))
    assert _publication_input(changed) == _publication_input(e.origins)
    assert [_publication_basis(row) for row in changed] == [_publication_basis(row) for row in e.origins]


@pytest.mark.asyncio
async def test_initial_route_preserves_actual_server_ui_receipt(coordinator_environment):
    from sqlalchemy import select

    from deerflow.persistence.telegram_bots.model import TelegramInboundMarkRow

    e = coordinator_environment
    incoming = Update(1902, message=Message(902, datetime.now(UTC), Chat(e.link.chat_id, "private"), from_user=User(e.link.telegram_user_id, "Human", False), text="/help"))
    row, _ = await e.inbox.accept(e.link, incoming, sanitized_input={"command": "help"})
    receipt = {"command_closed": "help"}
    async with e.inbox._transaction() as writer:
        mark = await writer.scalar(select(TelegramInboundMarkRow).where(TelegramInboundMarkRow.link_id == row.link_id, TelegramInboundMarkRow.message_id == row.message_id))
        mark.route_basis = {"human_input_ui": receipt}
    routed = await e.inbox.route(row, kind="command", target_id="help", basis={}, operation_ids=[])
    assert routed.route_basis == {"human_input_ui": receipt}
    replay = await e.inbox.route(row, kind="command", target_id="help", basis={}, operation_ids=[])
    assert replay.route_basis == routed.route_basis


@pytest.mark.asyncio
async def test_new_stop_closes_ui_after_actual_admission_and_duplicate_does_not_repeat(db, monkeypatch):
    from types import SimpleNamespace

    from test_telegram_bot_handlers import ingress
    from test_telegram_chat_turns import controlled_execution, message, until

    e = await ingress(db, monkeypatch=monkeypatch, lead=True)
    turns, state, reached = await controlled_execution(e, monkeypatch)
    calls = []

    async def command(channel, update, word, *, captured_link):
        row, created = await e.services.outbox.inbox.accept(captured_link, update, sanitized_input={"command": word})
        assert not created and not row.route_complete
        calls.append((channel, update.update_id, word, captured_link.id))

    try:
        await e.channel.handle_update(message(e, 1910, content="Actual running input"))
        await until(lambda: state.posts == 1)
        e.services.human_inputs = SimpleNamespace(command=command)
        incoming = message(e, 1911, content="/stop")
        await e.channel.handle_update(incoming)
        await until(lambda: state.stop_calls == 1)
        await e.channel.handle_update(incoming)
        assert calls == [(e.channel, incoming.update_id, "stop", e.channel.link.id)]
        assert state.stop_calls == 1
    finally:
        await turns.stop()
