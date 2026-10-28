"""Retirement during the final awaited guard cannot admit a late SDK send."""

import asyncio

import pytest
from _telegram_live_test_support import own_view
from _telegram_repository_test_support import db as db
from test_telegram_bot_handlers import ingress, sent
from test_telegram_callback_commands import setup
from test_telegram_supervisor import _supervisor_logging as _supervisor_logging

from app.telegram_bots.streaming import LivePresentation


@pytest.mark.asyncio
async def test_draft_retirement_during_final_scoped_read_blocks_actual_sdk(db, monkeypatch):
    e = await ingress(db, monkeypatch=monkeypatch, lead=True)
    view = LivePresentation(channel=e.channel, link=e.channel.link, draft_id=1161, topic=None, is_current=lambda: True, observer=None)
    entered, release = asyncio.Event(), asyncio.Event()
    actual, calls = e.channel._input_current, []

    async def held(link):
        result = await actual(link)
        calls.append(link)
        if len(calls) == 4:  # final refresh inside the real limiter callback
            entered.set()
            await release.wait()
        return result

    monkeypatch.setattr(e.channel, "_input_current", held)
    task = None
    try:
        e.request.enqueue("sendMessageDraft")
        task = asyncio.create_task(view.start())
        await asyncio.wait_for(entered.wait(), 3)
        await view.close()
        release.set()
        await task
        assert not any(call.endpoint == "sendMessageDraft" for call in e.request.calls)
    finally:
        release.set()
        if task is not None:
            await asyncio.gather(task, return_exceptions=True)
        await view.close()
        await e.channel.stop()


@pytest.mark.asyncio
async def test_placeholder_retirement_during_final_keyboard_read_blocks_actual_sdk(db, monkeypatch):
    e = await setup(db, monkeypatch)
    e.services.config = e.services.config.model_copy(update={"streaming": "edit"})
    view = LivePresentation(channel=e.channel, link=e.channel.link, draft_id=1171, topic=None, is_current=lambda: True, observer=None)
    own_view(view)
    entered, release = asyncio.Event(), asyncio.Event()
    actual, calls = e.services.callbacks.read_keyboard, []

    async def held(captured):
        result = await actual(captured)
        calls.append(captured)
        if len(calls) == 3:  # final keyboard read inside the actual limiter
            entered.set()
            await release.wait()
        return result

    monkeypatch.setattr(e.services.callbacks, "read_keyboard", held)
    try:
        await view.start()
        e.request.enqueue("sendMessage", result=sent(e, identity=671))
        await e.sender.tick()
        await asyncio.wait_for(entered.wait(), 3)
        await view.close()
        release.set()
        await e.sender.drain()
        assert not any(call.endpoint == "sendMessage" for call in e.request.calls)
    finally:
        release.set()
        await view.close()
        await e.sender.stop()
        await e.channel.stop()
