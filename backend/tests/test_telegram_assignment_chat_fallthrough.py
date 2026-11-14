"""An installed assignment probe preserves actual ordinary chat input custody."""

import pytest
from _telegram_bot_test_support import make_update
from _telegram_repository_test_support import db as db
from sqlalchemy import select
from test_telegram_chat_turns import controlled_execution
from test_telegram_work_chat_runtime import installed_work_runtime
from test_telegram_work_preview_recovery import dispatch
from test_telegram_work_source_delivery import work_delivery as work_delivery
from test_work_attention_pages import work_pages as work_pages

from app.telegram_bots.inbox import InboxRow, chat_input_parts
from deerflow.persistence.telegram_bots.model import TelegramInboundMarkRow, TelegramOutboxRow


@pytest.mark.asyncio
async def test_installed_assignment_probe_preserves_ordinary_chat_custody(work_delivery, monkeypatch):
    e = work_delivery
    owner = await installed_work_runtime(e)
    e.services, e.request = owner.host, e.channel.bot.request
    turns, _state, _reached = await controlled_execution(e, monkeypatch)
    words = "Explain the current references"
    update = make_update(6411, account=e.link.chat_id, text=words)
    try:
        e.request.enqueue("sendChatAction", result=True)
        assert await e.channel.handle_update(update)
        async with e.db.sessions() as reader:
            saved = await reader.get(TelegramInboundMarkRow, (e.link.id, 6411, e.link.chat_id))
            assert saved.route_kind == "turn"
            assert saved.sanitized_input == {"type": "text", "text": words}
            assert chat_input_parts(InboxRow.from_row(saved), bot_id=e.link.bot_id) == (words, [])
            assert (await reader.scalars(select(TelegramInboundMarkRow).where(TelegramInboundMarkRow.message_id == 6411))).all() == [saved]
        turn = next(iter(turns._turns.values()))
        assert turn.inbound.text == words and turn.origins[0].message_id == 6411
        assert await e.channel.handle_update(update)
        assert next(iter(turns._turns.values())) is turn
    finally:
        await turns.stop()
        await owner.stop()


@pytest.mark.asyncio
@pytest.mark.parametrize("routed", [False, True])
@pytest.mark.parametrize("entry", ["replay", "recovery"])
async def test_legacy_unpublished_probe_capture_gets_one_resend_notice(work_delivery, monkeypatch, routed, entry):
    e = work_delivery
    owner = await installed_work_runtime(e)
    e.services, e.request = owner.host, e.channel.bot.request
    turns, state, _reached = await controlled_execution(e, monkeypatch)
    words = "Previously captured ordinary question"
    update = make_update(6421, account=e.link.chat_id, text=words)
    origin, _created = await owner.host.outbox.inbox.accept(e.link, update, sanitized_input={"text": words})
    if routed:
        await owner.host.outbox.inbox.route(origin, kind="turn", target_id=e.link.agent_ref, basis={"input_profile": {"version": 1, "kind": "plain_text", "forwarded": False}}, operation_ids=[])
    try:
        if entry == "recovery":
            await turns._close_left_inputs(lambda: True, link_id=e.link.id)
        else:
            assert await e.channel.handle_update(update)
        assert not turns._turns and state.posts == 0
        async with e.db.sessions() as reader:
            saved = await reader.get(TelegramInboundMarkRow, (e.link.id, 6421, e.link.chat_id))
            assert saved.sanitized_input == {"text": words}
            assert saved.turn_id is None and not saved.route_basis.get("publications")
            assert saved.route_complete and len(saved.delivery_ids) == 1
            e.identity = saved.delivery_ids[0]
            notice = await reader.get(TelegramOutboxRow, e.identity)
            assert [part["source_ref"]["key"] for part in notice.parts] == ["assign.resend"]
        await dispatch(e, 6422)
        assert not e.dispatch_errors
        assert await e.channel.handle_update(update)
        async with e.db.sessions() as reader:
            assert (await reader.get(TelegramInboundMarkRow, (e.link.id, 6421, e.link.chat_id))).delivery_ids == [e.identity]
        assert not turns._turns and state.posts == 0
    finally:
        await turns.stop()
        await owner.stop()
