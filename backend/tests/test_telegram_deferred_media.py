"""Deferred media must not wedge the managed polling admission cursor."""

from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from _telegram_repository_test_support import db as db
from telegram import Update
from test_telegram_bot_handlers import message, sent
from test_telegram_callback_commands import drain, setup
from test_telegram_supervisor import _supervisor_logging as _supervisor_logging


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ["voice"])
async def test_deferred_media_is_classified_without_download_or_model_admission(db, monkeypatch, kind):
    e = await setup(db, monkeypatch)
    e.services.turns = SimpleNamespace(accept=AsyncMock(return_value=False), retire=AsyncMock(), message_sent=lambda *args, **kwargs: None)
    payload = message(e, 1191, content="Material").to_dict()
    payload["message"].pop("text")
    media = {"file_id": "offline-file", "file_unique_id": "offline-unique"}
    if kind == "photo":
        media.update(width=10, height=10)
        payload["message"][kind] = [media]
    else:
        if kind == "voice":
            media["duration"] = 1
        payload["message"][kind] = media
    update = Update.de_json(payload, e.channel.bot)
    try:
        assert await e.channel.handle_update(update), "A deferred type must not block all following Telegram updates"
        e.request.enqueue("sendMessage", result=sent(e, identity=691))
        await drain(e)
        e.services.turns.accept.assert_not_awaited()
        assert [call.endpoint for call in e.request.calls] == ["getMe", "sendMessage"]
        assert "not passed on" in e.request.calls[-1].parameters["text"].lower()
    finally:
        await e.sender.stop()
        await e.channel.stop()
