"""Actual profile work under the strict gate: drawing stays off the Gateway loop."""

import asyncio
from zoneinfo import ZoneInfo

import pytest
from _telegram_repository_test_support import db as db
from test_telegram_bot_handlers import ingress
from test_telegram_supervisor import _supervisor_logging as _supervisor_logging

from app.telegram_bots.profile import ProfileSync

pytestmark = pytest.mark.asyncio


async def test_picture_and_fonts_are_drawn_and_loaded_off_the_event_loop(db, monkeypatch):
    await asyncio.to_thread(ZoneInfo, "America/New_York")
    e = await ingress(db, monkeypatch=monkeypatch, off_loop=True)
    e.services.channels = lambda: (e.channel,)
    e.request.enqueue_get_me(bot_id=e.link.bot_id)
    for endpoint in ("setMyName", "setMyShortDescription", "setMyDescription", "setMyCommands", "setChatMenuButton", "setMyProfilePhoto"):
        e.request.enqueue(endpoint, result=True)
    try:
        assert await ProfileSync(e.services).push(e.link)
        assert any(call.endpoint == "setMyProfilePhoto" for call in e.request.calls)
    finally:
        await e.channel.stop()
