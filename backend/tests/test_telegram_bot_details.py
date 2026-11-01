"""The smaller promises a bot makes: who was here last, whose words a forward is, and what a notice offers.

Real pinned Application and real SQL. Only the Bot API transport is recorded, and where a turn is
needed, the Gateway reads.
"""

import asyncio
from datetime import timedelta
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from _telegram_repository_test_support import code_of
from _telegram_repository_test_support import db as db
from sqlalchemy import select
from sqlalchemy.exc import OperationalError
from telegram import Update
from test_telegram_bot_handlers import ingress, message, sent
from test_telegram_callback_commands import drain, press, setup
from test_telegram_chat_turns import controlled_execution, terminal, until
from test_telegram_chat_turns import message as chat_message
from test_telegram_every_press_answers import failed_turn, tap
from test_telegram_supervisor import _supervisor_logging as _supervisor_logging

from app.telegram_bots import adapter as adapter_module
from app.telegram_bots import authority
from app.telegram_bots.adapter import SendContext
from app.telegram_bots.publication import fixed_source
from app.telegram_bots.render import render
from app.telegram_bots.settings import silent_now
from deerflow.persistence.telegram_bots.model import TelegramBotLinkRow, TelegramCallbackRow, TelegramInboundMarkRow, TelegramOutboxRow

HOUR = 3600


async def link_row(db, e):
    async with db.sessions() as session:
        return await session.get(TelegramBotLinkRow, e.link.id)


def last_send(e):
    return next(call for call in reversed(e.request.calls) if call.endpoint == "sendMessage")


def forwarded(e, identity, content):
    payload = message(e, identity, content=content).to_dict()
    payload["message"]["forward_origin"] = {"type": "hidden_user", "date": 1_893_456_000, "sender_user_name": "Somebody"}
    return Update.de_json(payload, e.channel.bot)


@pytest.mark.asyncio
async def test_a_message_and_a_press_each_say_the_person_was_here(db, monkeypatch):
    e = await setup(db, monkeypatch)
    try:
        e.request.enqueue("sendMessage", result=sent(e, identity=501))
        assert await e.channel.handle_update(message(e, 910, content="/settings"))
        await drain(e)
        seen = (await link_row(db, e)).last_update_at
        assert seen is not None, "The web said 'No message yet' for a bot in daily use"
        e.clock.advance(120)
        async with db.sessions() as session:
            chinese = next(row for row in await session.scalars(select(TelegramCallbackRow)) if row.payload.get("value") == "zh-CN")
        e.request.enqueue("answerCallbackQuery", result=True)
        e.request.enqueue("editMessageText", result=sent(e, identity=501))
        assert await e.channel.handle_update(press(e, 912, chinese.id, message_id=501))
        await drain(e)
        assert (await link_row(db, e)).last_update_at > seen
    finally:
        await e.sender.stop()
        await e.channel.stop()


@pytest.mark.asyncio
async def test_a_person_active_at_night_hears_the_answer(db, monkeypatch):
    e = await setup(db, monkeypatch)
    try:
        e.clock.advance(3 * HOUR)  # 22:00 in the person's zone.
        e.request.enqueue("sendMessage", result=sent(e, identity=502))
        assert await e.channel.handle_update(message(e, 913, content="/privacy"))
        await drain(e)
        assert not last_send(e).parameters.get("disable_notification"), "They wrote a moment ago, so quiet hours do not apply"
        assert silent_now(await db.links.get(e.link.id), await db.settings.get(e.live.id), db.config, e.clock.now() + timedelta(minutes=31))
    finally:
        await e.sender.stop()
        await e.channel.stop()


@pytest.mark.asyncio
async def test_being_active_is_written_at_most_once_a_minute(db, monkeypatch):
    e = await setup(db, monkeypatch)
    writes = []
    actual = e.services.links.update_if_current

    async def counted(*args, **kwargs):
        if "last_update_at" in kwargs:
            writes.append(kwargs["last_update_at"])
        return await actual(*args, **kwargs)

    monkeypatch.setattr(e.services.links, "update_if_current", counted)
    try:
        for identity in range(920, 924):
            e.request.enqueue("sendMessage", result=sent(e, identity=identity))
            assert await e.channel.handle_update(message(e, identity, content="/privacy"))
            await drain(e)
        assert len(writes) == 1
    finally:
        await e.sender.stop()
        await e.channel.stop()


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", [OperationalError("synthetic", {}, Exception("synthetic")), OSError("connection lost")])
async def test_a_failed_stamp_does_not_stop_the_message(db, monkeypatch, failure):
    e = await setup(db, monkeypatch)
    actual = e.services.links.update_if_current

    async def failing(*args, **kwargs):
        if "last_update_at" in kwargs:
            raise failure
        return await actual(*args, **kwargs)

    monkeypatch.setattr(e.services.links, "update_if_current", failing)
    try:
        e.request.enqueue("sendMessage", result=sent(e, identity=501))
        assert await e.channel.handle_update(message(e, 930, content="/privacy"))
        await drain(e)
        assert "sendMessage" in [call.endpoint for call in e.request.calls]
    finally:
        await e.sender.stop()
        await e.channel.stop()


@pytest.mark.asyncio
async def test_a_stranger_does_not_count_as_the_person(db, monkeypatch):
    e = await setup(db, monkeypatch)
    try:
        e.request.enqueue("sendMessage", result=sent(e, identity=501, chat=7_000_000_002))
        assert await e.channel.handle_update(message(e, 931, content="Hello", sender=7_000_000_002))
        await asyncio.gather(*tuple(e.channel._ingress_tasks))
        assert (await link_row(db, e)).last_update_at is None
    finally:
        await e.sender.stop()
        await e.channel.stop()


@pytest.mark.asyncio
async def test_a_new_person_does_not_inherit_when_the_last_was_here(db, monkeypatch):
    e = await setup(db, monkeypatch)
    try:
        e.request.enqueue("sendMessage", result=sent(e, identity=501))
        assert await e.channel.handle_update(message(e, 932, content="/privacy"))
        await drain(e)
        assert (await link_row(db, e)).last_update_at is not None
        unbound = await db.links.unbind(await db.links.get(e.link.id), actor_id=e.link.owner_user_id)
        late = await db.links.update_if_current(unbound.id, token_revision=unbound.token_revision, last_update_at=e.clock.now())
        assert late.last_update_at is not None, "A message read just before the disconnect can stamp just after it"
        code = code_of(await db.links.mint_code(late))
        hello = [{"source_ref": {"key": "hello.chat", "values": {"product": "HartMesh", "email": "fixture@example.test"}}}, {"source_ref": {"key": "hello.privacy", "values": {}}}]
        await db.links.bind(late, telegram_user_id=7_000_000_002, chat_id=7_000_000_002, first_name="Next", username=None, code=code, start_identity={"update_id": 933, "message_id": 933, "chat_id": 7_000_000_002}, hello_parts=hello)
        assert (await link_row(db, e)).last_update_at is None
    finally:
        await e.sender.stop()
        await e.channel.stop()


@pytest.mark.asyncio
async def test_a_forwarded_stop_is_material_and_stops_nothing(db, monkeypatch):
    e = await ingress(db, monkeypatch=monkeypatch, lead=True)
    turns, state, reached = await controlled_execution(e, monkeypatch)
    try:
        await e.channel.handle_update(chat_message(e, 1830, content="Running"))
        await asyncio.wait_for(reached.wait(), 5)
        await until(lambda: state.posts == 1)
        first = next(iter(turns._turns.values()))
        await e.channel.handle_update(forwarded(e, 1831, "/stop now please"))
        await asyncio.sleep(0.1)
        assert not first.stop_requested and state.stop_calls == 0
    finally:
        await turns.stop()
        await e.channel.stop()


@pytest.mark.asyncio
async def test_a_skill_named_with_the_bot_runs_and_the_run_gets_the_words_the_person_meant(db, monkeypatch):
    e = await ingress(db, monkeypatch=monkeypatch, lead=True)
    turns, state, reached = await controlled_execution(e, monkeypatch)
    words = []

    def outcome(link, text):
        words.append(text)
        return "run"

    monkeypatch.setattr(adapter_module, "slash_skill_outcome", outcome)
    try:
        await e.channel.handle_update(chat_message(e, 1840, content=f"/weekly-report@{e.link.bot_username} for March"))
        await asyncio.wait_for(reached.wait(), 5)
        assert words == ["/weekly-report for March"]
        async with db.sessions() as session:
            mark = await session.scalar(select(TelegramInboundMarkRow).where(TelegramInboundMarkRow.update_id == 1840))
        assert mark.sanitized_input["text"] == "/weekly-report for March"
    finally:
        await turns.stop()
        await e.channel.stop()


@pytest.mark.asyncio
async def test_a_failed_skill_lookup_promises_nothing_it_cannot_keep(db, monkeypatch):
    e = await setup(db, monkeypatch)
    monkeypatch.setattr(adapter_module, "slash_skill_outcome", lambda link, text: "failed")
    try:
        e.request.enqueue("sendMessage", result=sent(e, identity=501))
        assert await e.channel.handle_update(message(e, 941, content="/weekly-report"))
        await drain(e)
        assert last_send(e).parameters["text"] == "The skill /weekly-report could not be looked up just now. Send it again in a moment."
    finally:
        await e.sender.stop()
        await e.channel.stop()


@pytest.mark.asyncio
async def test_stop_with_messages_waiting_says_so_once(db, monkeypatch):
    e = await ingress(db, monkeypatch=monkeypatch, lead=True)
    turns, state, reached = await controlled_execution(e, monkeypatch)
    plain = turns._observer.request_stop

    async def request_stop(record, link, *, stop_id):
        await plain(record, link, stop_id=stop_id)
        state.terminal = terminal(record, status="interrupted")
        return "requested"

    turns._observer.request_stop = request_stop
    try:
        await e.channel.handle_update(chat_message(e, 2001, content="First"))
        await asyncio.wait_for(reached.wait(), 5)
        await until(lambda: state.posts == 1)
        first = next(iter(turns._turns.values()))
        for identity in (2002, 2003, 2004):
            await e.channel.handle_update(chat_message(e, identity, content=f"Waiting {identity}"))
        await turns.flush_inputs(e.channel, topic=None)
        waiting = turns.queue.waiting_count(e.channel.link, None)
        assert waiting
        await e.channel.handle_update(chat_message(e, 2005, content="/stop"))
        first.wake.set()
        await until(first.done.is_set)
        await asyncio.sleep(0.2)
        async with db.sessions() as session:
            notices = [row.dedupe_key for row in await session.scalars(select(TelegramOutboxRow)) if row.dedupe_key.startswith("notice:chat-cancelled:")]
            open_marks = list(await session.scalars(select(TelegramInboundMarkRow).where(TelegramInboundMarkRow.message_id.in_([2002, 2003, 2004]), TelegramInboundMarkRow.route_complete.is_(False))))
        assert notices == [], "The stop's own text says they were not answered"
        assert open_marks == [], "Each of them still ends"
    finally:
        await turns.stop()
        await e.channel.stop()


@pytest.mark.asyncio
async def test_a_try_again_waiting_when_stop_arrives_keeps_its_own_answer(db, monkeypatch):
    world = await failed_turn(db, monkeypatch, identity=2021)
    e, turns, state = world.e, world.turns, world.state
    plain = turns._observer.request_stop

    async def request_stop(record, link, *, stop_id):
        await plain(record, link, stop_id=stop_id)
        state.terminal = terminal(record, status="interrupted")
        return "requested"

    turns._observer.request_stop = request_stop
    state.terminal = None
    try:
        await e.channel.handle_update(chat_message(e, 2022, content="Running"))
        await until(lambda: state.posts == 2)
        running = next(iter(turns._turns.values()))
        await tap(e, 2023, world.retry_id, message_id=world.retry_message)
        assert turns.queue.waiting_count(e.channel.link, None) == 1
        await e.channel.handle_update(chat_message(e, 2024, content="/stop"))
        running.wake.set()
        await until(running.done.is_set)
        await asyncio.sleep(0.2)
        async with db.sessions() as session:
            notices = [row.dedupe_key for row in await session.scalars(select(TelegramOutboxRow)) if row.dedupe_key.startswith("notice:chat-cancelled:")]
        assert notices == ["notice:chat-cancelled:" + world.retry_id], "The press has its own card to close, and the stop's text does not reach it"
    finally:
        await turns.stop()
        await e.channel.stop()


@pytest.mark.asyncio
async def test_a_model_refusal_says_whom_to_ask_and_keeps_try_again(db, monkeypatch):
    e = await ingress(db, monkeypatch=monkeypatch, lead=True)
    turns, state, reached = await controlled_execution(e, monkeypatch)
    monkeypatch.setattr(authority, "is_administrator", AsyncMock(return_value=False))
    try:
        await e.channel.handle_update(chat_message(e, 2011, content="Anything"))
        await asyncio.wait_for(reached.wait(), 5)
        await until(lambda: state.posts == 1)
        first = next(iter(turns._turns.values()))
        state.terminal = terminal(first.record, status="error", reason="model_refused")
        first.wake.set()
        await until(first.done.is_set)
        async with db.sessions() as session:
            reply = await session.scalar(select(TelegramOutboxRow).where(TelegramOutboxRow.dedupe_key == "reply:" + first.record.turn_id))
            retry = await session.scalar(select(TelegramCallbackRow).where(TelegramCallbackRow.kind == "retry_message"))
        assert reply.parts[0]["source_ref"]["key"] == "error.model_refused"
        assert reply.parts[0]["source_ref"]["values"]["reader_is_administrator"] is False
        assert retry is not None, "It works once the account is fixed"
    finally:
        await turns.stop()
        await e.channel.stop()


@pytest.mark.asyncio
async def test_a_chinese_speaker_gets_chinese_alerts_from_the_first_press_after_a_restart(db, monkeypatch):
    e = await setup(db, monkeypatch)
    try:
        await db.links.save_settings(e.live.id, {"locale": "zh-CN"}, language_changed=False)
        await e.channel.stop()
        e.channel._callback_locale = None
        e.request.enqueue_get_me(bot_id=e.link.bot_id)
        await e.channel.start()
        assert e.channel._callback_locale == (e.link.owner_user_id, e.link.binding_generation, "zh-CN")
    finally:
        await e.sender.stop()
        await e.channel.stop()


@pytest.mark.asyncio
async def test_someone_on_a_bot_not_yet_connected_is_told_how_to_connect_it(db, monkeypatch):
    e = await ingress(db, connected=False, monkeypatch=monkeypatch, lead=True)
    try:
        e.request.enqueue("sendMessage", result=sent(e, identity=950, chat=7_000_000_001))
        assert await e.channel.handle_update(message(e, 1850, content="/start", sender=7_000_000_001))
        await asyncio.gather(*tuple(e.channel._ingress_tasks))
        assert last_send(e).parameters["text"].startswith("This bot is not connected yet.")
    finally:
        await e.channel.stop()


@pytest.mark.asyncio
async def test_a_notice_carries_its_button(db, monkeypatch):
    e = await setup(db, monkeypatch)
    try:
        context = SendContext(e.link.id, e.link.chat_id, e.link.binding_generation, e.channel.transport_revision, "optional")
        e.request.enqueue("sendMessage", result=sent(e, identity=960))
        assert await e.channel.send_notice("link.conflict", context=context, button=("label.open_telegram", "https://example.test/settings"), product="HartMesh") == 960
        keyboard = last_send(e).parameters["reply_markup"]["inline_keyboard"]
        assert keyboard[0][0]["text"] == "Open Telegram settings" and keyboard[0][0]["url"] == "https://example.test/settings"
    finally:
        await e.sender.stop()
        await e.channel.stop()


@pytest.mark.asyncio
async def test_a_notice_survives_telegram_refusing_its_address(db, monkeypatch):
    e = await setup(db, monkeypatch)
    try:
        context = SendContext(e.link.id, e.link.chat_id, e.link.binding_generation, e.channel.transport_revision, "optional")
        e.request.enqueue("sendMessage", status=400, body={"ok": False, "error_code": 400, "description": "Bad Request: inline keyboard button URL 'http://localhost/settings' is invalid"})
        e.request.enqueue("sendMessage", result=sent(e, identity=961))
        assert await e.channel.send_notice("link.conflict", context=context, button=("label.open_telegram", "http://localhost/settings"), product="HartMesh") == 961
        assert last_send(e).parameters.get("reply_markup") is None
    finally:
        await e.sender.stop()
        await e.channel.stop()


@pytest.mark.asyncio
@pytest.mark.parametrize("administrator, ask", [(True, "your service provider"), (False, "an administrator")])
async def test_full_files_and_a_refused_model_ask_the_right_person(monkeypatch, administrator, ask):
    monkeypatch.setattr(authority, "is_administrator", AsyncMock(return_value=administrator))
    host = SimpleNamespace(product_name="HartMesh")
    link = SimpleNamespace(owner_user_id="owner")
    for key in ("chat.files_full", "new.files_full", "error.model_refused"):
        source = await fixed_source(host, link, key)
        assert ask in render(source["key"], "en-US", **source["values"])


def test_the_person_is_told_the_transcribers_full_name():
    from app.telegram_bots.voice import PROVIDER_NAMES

    assert PROVIDER_NAMES["gemini"] == "Google Gemini"
