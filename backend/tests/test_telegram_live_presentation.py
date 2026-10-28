"""Disposable drafts use actual SDK serialization and original live custody."""

import importlib

import pytest
from _telegram_live_test_support import own_view
from _telegram_repository_test_support import db as db
from sqlalchemy import select
from test_telegram_bot_handlers import ingress, sent
from test_telegram_callback_commands import drain, setup
from test_telegram_supervisor import _supervisor_logging as _supervisor_logging

from deerflow.persistence.telegram_bots.model import TelegramOutboxRow


@pytest.mark.asyncio
async def test_draft_coalesces_activity_and_reappears_after_a_chat_send(db, monkeypatch):
    e = await ingress(db, monkeypatch=monkeypatch, lead=True)
    module = importlib.import_module("app.telegram_bots.streaming")
    view = module.LivePresentation(channel=e.channel, link=e.channel.link, draft_id=1101, topic=None, is_current=lambda: True, observer=None)
    try:
        e.request.enqueue("sendMessageDraft")
        await view.start()
        first = e.request.calls[-1]
        assert first.endpoint == "sendMessageDraft" and first.parameters["draft_id"] == 1101
        assert first.parameters["text"] == "" and first.parameters["can_stop"] is True and first.parameters["keep_on_stop"] is True
        await view.activity("activity.read", {"file": "first.txt"})
        await view.activity("activity.read", {"file": "R&D.txt"})
        await view.flush()
        assert len([call for call in e.request.calls if call.endpoint == "sendMessageDraft"]) == 1
        e.clock.advance(1)
        e.request.enqueue("sendMessageDraft")
        await view.flush()
        assert "R&amp;D.txt" in e.request.calls[-1].parameters["text"]
        assert "first.txt" not in e.request.calls[-1].parameters["text"]
        view.message_sent(chat_id=e.link.chat_id, topic=None)
        e.clock.advance(1)
        e.request.enqueue("sendMessageDraft")
        await view.flush()
        assert len([call for call in e.request.calls if call.endpoint == "sendMessageDraft"]) == 3
        view.message_sent(chat_id=e.link.chat_id + 1, topic=None)
        e.clock.advance(1)
        await view.flush()
        assert len([call for call in e.request.calls if call.endpoint == "sendMessageDraft"]) == 3
        e.clock.advance(19)
        e.request.enqueue("sendMessageDraft")
        await view.flush()
        assert len([call for call in e.request.calls if call.endpoint == "sendMessageDraft"]) == 4
    finally:
        await view.close()
        await e.channel.stop()


@pytest.mark.asyncio
async def test_retired_draft_and_unrelated_stop_cannot_use_later_binding(db, monkeypatch):
    e = await ingress(db, monkeypatch=monkeypatch, lead=True)
    module = importlib.import_module("app.telegram_bots.streaming")
    current = [True]
    view = module.LivePresentation(channel=e.channel, link=e.channel.link, draft_id=1111, topic=None, is_current=lambda: current[0], observer=None)
    try:
        e.request.enqueue("sendMessageDraft")
        await view.start()
        assert view.matches_stop(chat_id=e.link.chat_id, draft_id=1111)
        assert not view.matches_stop(chat_id=e.link.chat_id + 1, draft_id=1111)
        assert not view.matches_stop(chat_id=e.link.chat_id, draft_id=1112)
        current[0] = False
        e.clock.advance(20)
        await view.activity("activity.other", {})
        await view.flush()
        assert len([call for call in e.request.calls if call.endpoint == "sendMessageDraft"]) == 1
        assert not view.matches_stop(chat_id=e.link.chat_id, draft_id=1111)
    finally:
        await view.close()
        await e.channel.stop()


@pytest.mark.asyncio
async def test_protection_forces_edit_from_first_send_and_retains_stored_stop(db, monkeypatch):
    e = await setup(db, monkeypatch)
    module = importlib.import_module("app.telegram_bots.streaming")

    async def protected(_context):
        return True

    e.services.protect_notice = protected
    view = module.LivePresentation(channel=e.channel, link=e.channel.link, draft_id=1121, topic=None, is_current=lambda: True, observer=None)
    own_view(view)
    try:
        e.request.enqueue("sendMessage", result=sent(e, identity=601))
        await view.start()
        assert view.mode == "edit"
        await drain(e)
        initial = next(call for call in e.request.calls if call.endpoint == "sendMessage")
        assert initial.parameters["protect_content"] is True
        markup = initial.parameters["reply_markup"]
        assert markup["inline_keyboard"][0][0]["callback_data"].startswith("h1:")
        assert not any(call.endpoint == "sendMessageDraft" for call in e.request.calls)
        await view.activity("activity.read", {"file": "safe.txt"})
        e.request.enqueue("editMessageText", result=sent(e, identity=601))
        await view.flush()
        edited = e.request.calls[-1]
        assert edited.endpoint == "editMessageText" and edited.parameters["message_id"] == 601
        assert edited.parameters["reply_markup"] == markup
        assert "safe.txt" in edited.parameters["text"]
        e.request.enqueue("deleteMessage")
        await view.abandon()
        assert e.request.calls[-1].endpoint == "deleteMessage"
    finally:
        await view.close()
        await e.sender.stop()
        await e.channel.stop()


@pytest.mark.asyncio
async def test_placeholder_without_live_owner_is_held_and_can_be_retired_before_final(db, monkeypatch):
    e = await setup(db, monkeypatch)
    module = importlib.import_module("app.telegram_bots.streaming")
    e.services.config = e.services.config.model_copy(update={"streaming": "edit"})
    view = module.LivePresentation(channel=e.channel, link=e.channel.link, draft_id=1131, topic=None, is_current=lambda: True, observer=None)
    try:
        await view.start()
        await drain(e)
        assert not any(call.endpoint == "sendMessage" for call in e.request.calls)
        async with db.sessions() as session:
            row = await session.get(TelegramOutboxRow, view._anchor_outbox)
            assert row.state == "skipped" and row.skip_reason == "superseded"
        assert await view.prepare_final()
        async with db.sessions() as session:
            row = await session.get(TelegramOutboxRow, view._anchor_outbox)
            assert row.state == "skipped" and row.skip_reason == "superseded"
            assert not list(await session.scalars(select(TelegramOutboxRow).where(TelegramOutboxRow.state == "sending")))
    finally:
        await view.close()
        await e.sender.stop()
        await e.channel.stop()


@pytest.mark.asyncio
async def test_pause_during_limiter_wait_prevents_a_late_thinking_send(db, monkeypatch):
    e = await setup(db, monkeypatch)
    module = importlib.import_module("app.telegram_bots.streaming")
    e.services.config = e.services.config.model_copy(update={"streaming": "edit"})
    view = module.LivePresentation(channel=e.channel, link=e.channel.link, draft_id=1141, topic=None, is_current=lambda: True, observer=None)
    own_view(view)
    try:
        await view.start()
        e.channel.bot.rate_limiter._short_hold_until = e.clock.monotonic() + 1
        await e.sender.tick()
        await e.clock.wait_for_sleep()
        await view.pause()
        e.clock.advance(2)
        await e.sender.drain()
        assert not any(call.endpoint == "sendMessage" for call in e.request.calls)
        async with db.sessions() as session:
            row = await session.get(TelegramOutboxRow, view._anchor_outbox)
            assert row.state == "skipped" and row.skip_reason == "superseded"
    finally:
        e.clock.advance(5)
        await view.close()
        await e.sender.stop()
        await e.channel.stop()


@pytest.mark.asyncio
async def test_refused_stop_can_resume_same_draft_without_another_model_task(db, monkeypatch):
    e = await ingress(db, monkeypatch=monkeypatch, lead=True)
    module = importlib.import_module("app.telegram_bots.streaming")
    view = module.LivePresentation(channel=e.channel, link=e.channel.link, draft_id=1181, topic=None, is_current=lambda: True, observer=None)
    try:
        e.request.enqueue("sendMessageDraft")
        await view.start()
        await view.pause()
        e.clock.advance(1)
        await view.flush()
        assert len([call for call in e.request.calls if call.endpoint == "sendMessageDraft"]) == 1
        assert await view.resume()
        e.request.enqueue("sendMessageDraft")
        await view.flush()
        drafts = [call for call in e.request.calls if call.endpoint == "sendMessageDraft"]
        assert len(drafts) == 2 and all(call.parameters["draft_id"] == 1181 for call in drafts)
    finally:
        await view.close()
        await e.channel.stop()
