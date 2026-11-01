"""Actual SQL UI transitions over the recorded Telegram SDK (source protocol seeds)."""

import pytest
from _telegram_human_input_test_support import request
from _telegram_repository_test_support import db as db
from sqlalchemy import select
from test_telegram_bot_handlers import message, sent
from test_telegram_callback_commands import drain, press
from test_telegram_human_input_sdk import ui as ui
from test_telegram_supervisor import _supervisor_logging as _supervisor_logging

from deerflow.persistence.telegram_bots.model import TelegramCallbackRow


async def show(e, raw):
    await e.offer(raw)
    e.request.enqueue("sendMessage", result=sent(e, identity=701))
    await drain(e)


async def button(e, kind, *, option_id=None):
    """The one on screen: a tick keeps the keyboard before it live until the list is left."""
    async with e.services.outbox.sessions() as session:
        rows = list(await session.scalars(select(TelegramCallbackRow).where(TelegramCallbackRow.kind == kind, TelegramCallbackRow.consumed_at.is_(None)).order_by(TelegramCallbackRow.created_at)))
        if option_id is not None:
            rows = [row for row in rows if row.payload.get("option_id") == option_id]
        assert rows
        return rows[-1].id


async def click(e, kind, identity, *, target=701, send=False, result_id=None, option_id=None):
    callback = await button(e, kind, option_id=option_id)
    e.request.enqueue("answerCallbackQuery")
    if send:
        e.request.enqueue("editMessageReplyMarkup", result=sent(e, identity=target))
    e.request.enqueue("sendMessage" if send else "editMessageText", result=sent(e, identity=result_id or target))
    assert await e.channel.handle_update(press(e, identity, callback, message_id=target))
    await drain(e)
    if send:
        await drain(e)


@pytest.mark.asyncio
async def test_other_answer_is_saved_once_and_replay_does_not_become_plain(ui):
    e = ui
    await show(e, request())
    await click(e, "clarify_other", 1501, send=True, result_id=702)
    assert "Type your answer" in next(call for call in reversed(e.request.calls) if call.endpoint == "sendMessage").parameters["text"]
    incoming = message(e, 1502, content="A completely different answer")
    e.request.enqueue("editMessageText", result=sent(e, identity=702))
    assert await e.services.human_inputs.handle_text(e.channel, incoming)
    await drain(e)
    first = e.services.turns.accept_staged.await_args.args[1]
    assert first.text == "A completely different answer"
    assert first.human_message_kwargs["human_input_response"]["response_kind"] == "text"
    assert await e.services.human_inputs.handle_text(e.channel, incoming)
    assert e.services.turns.accept_staged.await_args.args[1] == first


@pytest.mark.asyncio
async def test_typed_form_field_review_change_and_send_use_original_request(ui):
    e = ui
    raw = request(version=2, input_mode="form", fields=[{"name": "name", "label": "Your name", "type": "text", "required": True}])
    raw.pop("options")
    await show(e, raw)
    e.request.enqueue("editMessageText", result=sent(e, identity=701))
    assert await e.services.human_inputs.handle_text(e.channel, message(e, 1510, content="Alice"))
    await drain(e)
    assert "Your name: Alice" in e.request.calls[-1].parameters["text"]
    assert e.services.turns.accept_staged.await_count == 0
    await click(e, "form_change", 1511)
    await click(e, "form_field", 1512, send=True, result_id=702)
    e.request.enqueue("editMessageText", result=sent(e, identity=702))
    assert await e.services.human_inputs.handle_text(e.channel, message(e, 1513, content="Bob"))
    await drain(e)
    await click(e, "form_send", 1514, target=702)
    stage = e.services.turns.accept_staged.await_args.args[1]
    assert stage.text == 'Your name: Bob [values: {"name":"Bob"}]'
    assert stage.human_message_kwargs["human_input_response"]["request_id"] == raw["request_id"]


@pytest.mark.asyncio
async def test_multiselect_toggles_actual_options_in_place_then_advances(ui):
    e = ui
    raw = request(version=2, input_mode="form", fields=[{"name": "colors", "label": "Colors", "type": "multi_select", "required": True, "options": [{"id": "red", "label": "Red", "value": "red"}]}])
    raw.pop("options")
    identity = await e.offer(raw)
    first = await e.services.outbox.get(e.link, identity)
    e.request.enqueue("sendMessage", result=sent(e, identity=701))
    await drain(e)
    await click(e, "form_option", 1520)
    actual = e.request.calls[-1]
    assert actual.endpoint == "editMessageText" and actual.parameters["message_id"] == 701
    assert actual.parameters["reply_markup"]["inline_keyboard"][0][0]["text"] == "✓ Red"
    assert not await e.services.human_inputs.step_current(first)
    await click(e, "form_next", 1521)
    assert "Colors: red" in e.request.calls[-1].parameters["text"]
    assert e.services.turns.accept_staged.await_count == 0


@pytest.mark.asyncio
@pytest.mark.parametrize("choice", ["prompt_reopen", "prompt_send_new", "prompt_discard"])
async def test_expired_other_prompt_holds_actual_text_for_explicit_choice(ui, choice):
    from datetime import UTC, datetime, timedelta

    e = ui
    await show(e, request())
    await click(e, "clarify_other", 1530, send=True, result_id=702)
    async with e.services.outbox.sessions.begin() as session:
        prompt = await session.scalar(select(TelegramCallbackRow).where(TelegramCallbackRow.kind == "prompt"))
        prompt.expires_at = datetime.now(UTC) - timedelta(seconds=1)
    incoming = message(e, 1531, content="Held private answer")
    e.request.enqueue("sendMessage", result=sent(e, identity=703))
    assert await e.services.human_inputs.handle_text(e.channel, incoming)
    await drain(e)
    assert "This answer prompt closed" in e.request.calls[-1].parameters["text"]
    assert e.services.turns.accept_staged.await_count == 0
    assert await e.services.human_inputs.handle_text(e.channel, incoming)
    # Neither a replay nor reopening silently submits the held text.
    e.request.enqueue("answerCallbackQuery")
    e.request.enqueue("editMessageReplyMarkup", result=sent(e, identity=703))
    e.request.enqueue("sendMessage", result=sent(e, identity=704))
    callback = await button(e, choice)
    assert await e.channel.handle_update(press(e, 1532, callback, message_id=703))
    await drain(e)
    await drain(e)
    if choice == "prompt_send_new":
        stage = e.services.turns.accept_staged.await_args.args[1]
        assert stage.text == "Held private answer" and stage.human_message_kwargs == {}
        actual = await e.services.callbacks.get(callback)
        assert actual.consumed_at is None and "publication_owner" in actual.payload
        from app.telegram_bots.callbacks import CallbackRefused

        discard = await e.services.callbacks.get(await button(e, "prompt_discard"))
        with pytest.raises(CallbackRefused):
            await e.services.human_inputs.execute_callback(e.channel, discard, update_id=1533)
    else:
        assert e.services.turns.accept_staged.await_count == 0
        if choice == "prompt_reopen":
            assert "Type your answer" in next(call for call in reversed(e.request.calls) if call.endpoint == "sendMessage").parameters["text"]
        else:
            assert "Discarded" in e.request.calls[-1].parameters["text"]


@pytest.mark.asyncio
async def test_plain_publication_cancels_only_current_scope_ui_and_clears_keyboard(ui):
    from app.telegram_bots.inbox import InboxRow
    from deerflow.persistence.telegram_bots.model import TelegramInboundMarkRow

    e = ui
    identity = await e.offer(request())
    e.request.enqueue("sendMessage", result=sent(e, identity=701))
    await drain(e)
    incoming = message(e, 1550, content="Continue without choosing")
    origin, _ = await e.services.outbox.inbox.accept(e.link, incoming, sanitized_input={"type": "text", "text": incoming.message.text})
    origin = await e.services.outbox.inbox.route(origin, kind="turn", target_id=e.link.agent_ref, basis={"input_profile": {"version": 1, "kind": "plain_text", "forwarded": False}}, operation_ids=[])
    await e.services.human_inputs.before_plain_publication(e.channel, e.link, [origin])
    assert not await e.services.human_inputs.step_current(await e.services.outbox.get(e.link, identity))
    e.request.enqueue("editMessageReplyMarkup", result=sent(e, identity=701))
    await drain(e)
    assert e.request.calls[-1].endpoint == "editMessageReplyMarkup"
    assert e.request.calls[-1].parameters["reply_markup"] == {"inline_keyboard": []}
    async with e.services.outbox.sessions() as session:
        saved = InboxRow.from_row(await session.get(TelegramInboundMarkRow, (origin.link_id, origin.message_id, origin.chat_id)))
        assert "submission" not in saved.route_basis and saved.turn_id is None


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ["clarify_option", "network", "form_send"])
async def test_staged_answer_keeps_original_controls_until_actual_enqueue(ui, kind):
    from deerflow.persistence.telegram_bots.model import TelegramActRow

    e = ui
    if kind == "network":
        raw = request(
            source="sandbox_network",
            input_mode="single_choice",
            details={"host": "example.test", "port": 8443, "ttl_seconds": 300},
            options=[{"id": "allow_temporary", "label": "Allow", "value": "allow_temporary"}, {"id": "deny", "label": "Deny", "value": "deny"}],
        )
    elif kind == "form_send":
        raw = request(version=2, input_mode="form", fields=[{"name": "field", "label": "Field", "type": "checkbox", "required": False}])
        raw.pop("options")
    else:
        raw = request()
    await show(e, raw)
    if kind == "form_send":
        await click(e, "form_skip", 1560)
    callback = await button(e, kind, option_id="allow_temporary" if kind == "network" else None)
    e.services.turns.accept_staged.return_value = False
    e.request.enqueue("answerCallbackQuery")
    e.request.enqueue("editMessageText", result=sent(e, identity=701))
    assert await e.channel.handle_update(press(e, 1561, callback, message_id=701))
    await drain(e)
    async with e.services.outbox.sessions() as session:
        actual = await session.get(TelegramCallbackRow, callback)
        assert actual.consumed_at is None and actual.payload["publication_owner"]["state"] == "accepted"
        owner = actual.payload["publication_owner"]
        if kind == "network":
            audit = await session.get(TelegramActRow, callback)
            assert audit.state == "pending" and audit.result is None
    first = e.services.turns.accept_staged.await_args.args[1]
    if kind == "clarify_option":
        from app.telegram_bots.callbacks import CallbackRefused

        other = await e.services.callbacks.get(await button(e, "clarify_other"))
        with pytest.raises(CallbackRefused):
            await e.services.human_inputs.execute_callback(e.channel, other, update_id=1562)
    e.request.enqueue("answerCallbackQuery")
    assert await e.channel.handle_update(press(e, 1562, callback, message_id=701))
    await drain(e)
    assert e.services.turns.accept_staged.await_args.args[1] == first
    async with e.services.outbox.sessions() as session:
        actual = await session.get(TelegramCallbackRow, callback)
        assert actual.consumed_at is None and actual.payload["publication_owner"] == owner


def quoted_message(e, identity, content, target):
    from telegram import Update

    payload = message(e, identity, content=content).to_dict()
    payload["message"]["reply_to_message"] = sent(e, identity=target)
    return Update.de_json(payload, e.channel.bot)


@pytest.mark.asyncio
async def test_actual_reply_to_optional_typed_field_uses_same_acknowledged_prompt(ui):
    e = ui
    raw = request(version=2, input_mode="form", fields=[{"name": "note", "label": "Optional note", "type": "text", "required": False}])
    raw.pop("options")
    await show(e, raw)
    incoming = quoted_message(e, 1570, "A useful note", 701)
    assert await e.services.human_inputs.handle_text(e.channel, incoming)
    async with e.services.outbox.sessions() as session:
        row = await session.scalar(select(TelegramCallbackRow).where(TelegramCallbackRow.kind == "form"))
        assert row.payload["state"] == "review" and row.payload["values"] == {"note": "A useful note"}
    e.request.enqueue("editMessageText", result=sent(e, identity=701))
    await drain(e)
    assert "Optional note: A useful note" in e.request.calls[-1].parameters["text"]


@pytest.mark.asyncio
async def test_reply_to_used_card_is_held_by_actual_routing_tombstone(ui):
    e = ui
    await show(e, request())
    callback = await button(e, "clarify_other")
    row = await e.services.callbacks.get(callback)
    link, _item, _source, guard = await e.services.human_inputs._callback_source(e.channel, row)
    await e.services.callbacks.consume_group(row.group_id, link=link, guard=guard)
    assert await e.services.human_inputs.handle_text(e.channel, quoted_message(e, 1571, "Do not silently publish this", 701))
    assert e.services.turns.accept_staged.await_count == 0
    e.request.enqueue("sendMessage", result=sent(e, identity=703))
    await drain(e)
    assert "This prompt can no longer take an answer" in e.request.calls[-1].parameters["text"]


@pytest.mark.asyncio
async def test_closing_a_scope_cancels_old_controls_when_private_source_is_unreadable(ui, monkeypatch):
    from unittest.mock import AsyncMock

    from app.channels.execution import ChannelExecutionProtocolError

    e = ui
    await show(e, request())
    monkeypatch.setattr(e.services.human_inputs.source, "read_request", AsyncMock(side_effect=ChannelExecutionProtocolError()))
    await e.services.human_inputs.command(e.channel, message(e, 1580, content="/new"), "new")
    async with e.services.outbox.sessions() as session:
        controls = list(await session.scalars(select(TelegramCallbackRow).where(TelegramCallbackRow.kind.in_(["clarify_option", "clarify_other"]))))
        assert controls and all(row.consumed_at is not None for row in controls)


@pytest.mark.asyncio
async def test_navigation_retires_the_complete_form_change_group_including_back(ui):
    e = ui
    raw = request(version=2, input_mode="form", fields=[{"name": "field", "label": "Field", "type": "checkbox", "required": False}])
    raw.pop("options")
    await show(e, raw)
    await click(e, "form_skip", 1590)
    await click(e, "form_change", 1591)
    back = await button(e, "back")
    await e.services.human_inputs.command(e.channel, message(e, 1592, content="/help"), "help")
    actual = await e.services.callbacks.get(back)
    assert actual.consumed_at is not None and actual.closed_reason == "command"


@pytest.mark.asyncio
async def test_malformed_network_request_uses_fixed_error_without_controls_and_logs_no_payload(ui, caplog):
    import logging

    from app.telegram_bots.render import render

    e = ui
    caplog.set_level(logging.WARNING, logger="app.telegram_bots.human_input")
    raw = request(source="sandbox_network", input_mode="single_choice", question="Private question sentinel", details={"host": "private-host-sentinel.example", "port": True, "ttl_seconds": 300})
    identity = await e.offer(raw)
    item = await e.services.outbox.get(e.link, identity)
    assert item.payload["reply_basis"]["keyboard"] == []
    e.request.enqueue("sendMessage", result=sent(e, identity=701))
    await drain(e)
    actual = e.request.calls[-1].parameters
    assert actual["text"] == render("error.generic", "en-US") and "reply_markup" not in actual
    messages = [record.getMessage() for record in caplog.records if record.name == "app.telegram_bots.human_input"]
    assert messages == [f"telegram_bots human_input_invalid source=sandbox_network link={e.link.id}"]
    assert "Private question sentinel" not in caplog.text and "private-host-sentinel.example" not in caplog.text


@pytest.mark.asyncio
@pytest.mark.parametrize("word", ["help", "new"])
async def test_replayed_command_cannot_close_a_later_form_or_prompt(ui, word):
    from app.telegram_bots.threads import ThreadCoordinator

    e = ui
    threads = ThreadCoordinator(links=e.services.links, authority=e.live.authority, client=e.services.thread_client, channel=e.services.channel, send_allowed=e.services.send_allowed, outbox=e.services.outbox, product_name="HartMesh")
    e.services.threads = threads
    try:
        command = message(e, 1600, content="/" + word)
        e.request.enqueue("sendMessage", result=sent(e, identity=701))
        assert await e.channel.handle_update(command)
        await drain(e)
        raw = request(version=2, input_mode="form", fields=[{"name": "field", "label": "New field", "type": "text", "required": True}])
        raw.pop("options")
        await e.offer(raw)
        e.request.enqueue("sendMessage", result=sent(e, identity=702))
        await drain(e)
        async with e.services.outbox.sessions() as session:
            form = await session.scalar(select(TelegramCallbackRow).where(TelegramCallbackRow.kind == "form"))
            identity, original = form.id, dict(form.payload)
        e.channel._edit_hashes.clear()  # Durable admission must own replay after process caches disappear.
        assert await e.channel.handle_update(command)
        await drain(e)
        async with e.services.outbox.sessions() as session:
            form = await session.get(TelegramCallbackRow, identity)
            assert form.payload == original and form.consumed_at is None
            prompt = await session.scalar(select(TelegramCallbackRow).where(TelegramCallbackRow.kind == "prompt", TelegramCallbackRow.consumed_at.is_(None)))
            assert prompt is not None and prompt.payload["form_id"] == identity
    finally:
        await threads.stop()
