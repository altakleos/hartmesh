"""Actual pinned SDK commands and presses share guarded SQL delivery and actions."""

import asyncio

import pytest
from _telegram_repository_test_support import db as db
from sqlalchemy import select
from telegram import Update
from test_telegram_bot_handlers import ingress, message, sent
from test_telegram_supervisor import _supervisor_logging as _supervisor_logging
from test_telegram_threads import environment as thread_environment

from app.telegram_bots.acts import Acts
from app.telegram_bots.callbacks import CallbackHandler, CallbackRepository, Prompts
from app.telegram_bots.commands import CallbackCommands
from app.telegram_bots.producer import OutboxSender
from deerflow.persistence.telegram_bots.model import TelegramActRow, TelegramCallbackRow, TelegramInboundMarkRow, TelegramOutboxRow


async def setup(db, monkeypatch, *, with_threads=False, off_loop=False):
    e = await thread_environment(db, monkeypatch, off_loop=off_loop) if with_threads else await ingress(db, monkeypatch=monkeypatch, lead=True, off_loop=off_loop)
    if with_threads:
        e.services.threads = e.threads
    e.services.callbacks = CallbackRepository(db.sessions, config=db.config)
    e.services.prompts = Prompts(e.services.callbacks)
    e.services.acts = Acts(db.sessions, settings=db.settings, callbacks=e.services.callbacks, outbox=e.services.outbox)
    e.services.commands = CallbackCommands(e.services)

    e.services.eligibility = e.live.authority.delivery
    e.sender = OutboxSender(e.services.outbox, run_id="command-keyboards", deliver_part=lambda **kw: e.channel.send_part(**{k: v for k, v in kw.items() if k != "link_id"}), eligibility=e.services.eligibility)
    async with db.sessions.begin() as session:
        for row in await session.scalars(select(TelegramOutboxRow)):
            row.state, row.skip_reason = "skipped", "superseded"
    return e


def press(e, identity, callback, *, message_id=501):
    return Update.de_json(
        {
            "update_id": identity,
            "callback_query": {
                "id": "query-" + str(identity),
                "from": {"id": e.link.telegram_user_id, "first_name": "Fixture", "is_bot": False},
                "chat_instance": "fixture-chat",
                "data": "h1:" + callback,
                "message": {"message_id": message_id, "date": 1_893_456_000, "chat": {"id": e.link.chat_id, "type": "private"}, "text": "Fixed settings"},
            },
        },
        e.channel.bot,
    )


async def drain(e):
    await asyncio.gather(*tuple(e.channel._ingress_tasks))
    e.clock.advance(2)
    await e.sender.tick()
    await e.sender.drain()


@pytest.mark.asyncio
async def test_settings_command_press_changes_one_setting_and_edits_with_new_controls(db, monkeypatch, *, off_loop=False):
    e = await setup(db, monkeypatch, off_loop=off_loop)
    try:
        e.request.enqueue("sendMessage", result=sent(e, identity=501))
        assert await e.channel.handle_update(message(e, 910, content="/settings"))
        await drain(e)
        first = next(call for call in e.request.calls if call.endpoint == "sendMessage")
        assert "Language of this chat's fixed texts: English" in first.parameters["text"]
        async with db.sessions() as session:
            chinese = next(row for row in await session.scalars(select(TelegramCallbackRow)) if row.payload.get("value") == "zh-CN")
            callback_id = chinese.id
        e.request.enqueue("answerCallbackQuery", result=True)
        e.request.enqueue("editMessageText", result=sent(e, identity=501))
        assert await e.channel.handle_update(press(e, 911, callback_id))
        await drain(e)
        assert (await db.settings.get(e.live.id)).locale == "zh-CN"
        endpoints = [call.endpoint for call in e.request.calls]
        assert endpoints.index("answerCallbackQuery") < endpoints.index("editMessageText")
        edits = [call for call in e.request.calls if call.endpoint == "editMessageText"]
        assert len(edits) == 1 and edits[0].parameters["message_id"] == 501
        assert "中文" in edits[0].parameters["text"]
        assert len(edits[0].parameters["reply_markup"]["inline_keyboard"][0]) == 2
        async with db.sessions() as session:
            act = await session.get(TelegramActRow, callback_id)
            assert act.state == "done" and act.body == {"field": "locale", "old": "en-US", "new": "zh-CN"}
            previous = await session.get(TelegramCallbackRow, callback_id)
            assert previous.consumed_at is not None
        # Redelivering the gesture cannot repeat its setting mutation/edit.
        e.request.enqueue("answerCallbackQuery", result=True)
        assert await e.channel.handle_update(press(e, 912, callback_id))
        await drain(e)
        assert len([call for call in e.request.calls if call.endpoint == "editMessageText"]) == 1
    finally:
        await e.sender.stop()
        await e.channel.stop()


@pytest.mark.asyncio
@pytest.mark.parametrize("kind,key,creates", [("new_confirm", "New conversation started.", 1), ("new_keep", "Kept. This conversation goes on.", 0)])
async def test_new_command_choice_uses_actual_sdk_gateway_and_original_input(db, monkeypatch, kind, key, creates, *, off_loop=False):
    e = await setup(db, monkeypatch, with_threads=True, off_loop=off_loop)
    try:
        e.request.enqueue("sendMessage", result=sent(e, identity=501))
        assert await e.channel.handle_update(message(e, 920, content="/new"))
        await drain(e)
        assert not e.http_calls, "A confirmation offer must not create a conversation"
        async with db.sessions() as session:
            row = await session.scalar(select(TelegramCallbackRow).where(TelegramCallbackRow.kind == kind))
            callback_id = row.id
            mark = await session.get(TelegramInboundMarkRow, (e.link.id, 920, e.link.chat_id))
            creation_id = mark.route_basis["thread_preparation"]["creation_id"]
            assert not mark.route_complete
        e.request.enqueue("answerCallbackQuery", result=True)
        e.request.enqueue("editMessageText", result=sent(e, identity=501))
        assert await e.channel.handle_update(press(e, 921, callback_id))
        await drain(e)
        assert sum(request.method == "POST" for request in e.http_calls) == creates
        assert (await db.connections.get_thread_id(e.link.connection_id, str(e.link.chat_id), None)) == (creation_id if creates else None)
        edit = next(call for call in e.request.calls if call.endpoint == "editMessageText")
        assert edit.parameters["message_id"] == 501 and edit.parameters["text"] == key
        assert edit.parameters["reply_markup"] == {"inline_keyboard": []}
        async with db.sessions() as session:
            mark = await session.get(TelegramInboundMarkRow, (e.link.id, 920, e.link.chat_id))
            assert mark.route_complete and mark.route_basis["callback_choice"]["callback_id"] == callback_id
        e.request.enqueue("answerCallbackQuery", result=True)
        assert await e.channel.handle_update(press(e, 922, callback_id))
        await drain(e)
        assert sum(request.method == "POST" for request in e.http_calls) == creates
        assert len([call for call in e.request.calls if call.endpoint == "editMessageText"]) == 1
    finally:
        await e.sender.stop()
        await e.threads.stop()
        await e.http_client.stop()
        await e.channel.stop()


@pytest.mark.asyncio
async def test_unimplemented_fixed_command_has_its_required_name_and_delivers(db, monkeypatch):
    e = await setup(db, monkeypatch)
    try:
        e.request.enqueue("sendMessage", result=sent(e, identity=501))
        assert await e.channel.handle_update(message(e, 930, content="/stop"))
        await drain(e)
        sends = [call for call in e.request.calls if call.endpoint == "sendMessage"]
        assert len(sends) == 1 and sends[0].parameters["text"].startswith("/stop is not a command")
    finally:
        await e.sender.stop()
        await e.channel.stop()


@pytest.mark.asyncio
@pytest.mark.parametrize("word", ["agent", "memory"])
async def test_existing_fixed_navigation_also_clears_only_its_prompt_scope(db, monkeypatch, word):
    e = await setup(db, monkeypatch)
    try:
        await e.services.prompts.open(e.link, "clarify_other", {"thread_id": "thread", "request_id": "general"}, chat_id=e.link.chat_id, message_thread_id=None, message_id=600)
        other = await e.services.prompts.open(e.link, "clarify_other", {"thread_id": "thread", "request_id": "topic"}, chat_id=e.link.chat_id, message_thread_id=99, message_id=601)
        await e.services.prompts.close(e.link, e.link.chat_id, None, reason="expired")
        assert await e.services.prompts.current(e.link, e.link.chat_id, None) is not None
        e.request.enqueue("sendMessage", result=sent(e, identity=501))
        assert await e.channel.handle_update(message(e, 931, content="/" + word))
        assert await e.services.prompts.current(e.link, e.link.chat_id, None) is None
        assert (await e.services.prompts.current(e.link, e.link.chat_id, 99)).id == other
        await drain(e)
        assert len([call for call in e.request.calls if call.endpoint == "sendMessage"]) == 1
    finally:
        await e.sender.stop()
        await e.channel.stop()


@pytest.mark.asyncio
async def test_callback_ack_can_reach_actual_sdk_without_a_second_metadata_read(db, monkeypatch):
    e = await setup(db, monkeypatch)
    reads = []

    async def no_second_read(*args, **kwargs):
        reads.append(True)
        pytest.fail("A fixed callback acknowledgement cannot wait for another metadata read")

    try:
        monkeypatch.setattr(e.services.settings, "get", no_second_read)
        monkeypatch.setattr(e.channel, "refresh_link", no_second_read)
        e.request.enqueue("answerCallbackQuery", result=True)
        await e.channel.answer_callback_ack("query-deadline", "stale.not_waiting", captured_link=e.channel.link, deadline=asyncio.get_running_loop().time() + 0.5)
        calls = [call for call in e.request.calls if call.endpoint == "answerCallbackQuery"]
        assert len(calls) == 1 and calls[0].parameters["callback_query_id"] == "query-deadline"
        assert calls[0].parameters["show_alert"] is True and not reads
    finally:
        await e.sender.stop()
        await e.channel.stop()


@pytest.mark.asyncio
async def test_slow_precheck_still_attempts_one_actual_ack_and_admits_no_later_read(db, monkeypatch):
    e = await setup(db, monkeypatch)
    entered, release = asyncio.Event(), asyncio.Event()
    fresh_reads, effects = [], []
    captured = e.channel.link

    async def held_link(_captured):
        entered.set()
        await release.wait()
        return captured

    async def never_read(*args, **kwargs):
        fresh_reads.append(True)
        pytest.fail("Expired press admitted a new metadata read")

    async def execute(*args):
        effects.append(True)

    task = None
    try:
        monkeypatch.setattr(e.services.callbacks, "current_link", held_link)
        monkeypatch.setattr(e.services.callbacks, "get", never_read)
        monkeypatch.setattr(e.services.settings, "get", never_read)
        monkeypatch.setattr(e.channel, "refresh_link", never_read)
        frame = e.request.enqueue("answerCallbackQuery", result=True)
        handler = CallbackHandler(e.services.callbacks, execute=execute)
        task = asyncio.create_task(handler.press(e.channel, press(e, 918, "a" * 32).callback_query, captured_link=captured))
        await asyncio.wait_for(entered.wait(), 3)
        await asyncio.wait_for(frame.started.wait(), 3)
        assert not task.done(), "Started precheck cleanup must remain owned"
        release.set()
        await asyncio.wait_for(task, 3)
        assert not fresh_reads and not effects
        assert len([call for call in e.request.calls if call.endpoint == "answerCallbackQuery"]) == 1
    finally:
        release.set()
        if task is not None:
            await asyncio.gather(task, return_exceptions=True)
        await e.sender.stop()
        await e.channel.stop()
