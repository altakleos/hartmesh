"""A real callback task keeps the accepted provider update identity."""

from unittest.mock import AsyncMock

import pytest
from _telegram_live_test_support import own_view
from _telegram_repository_test_support import db as db
from sqlalchemy import select
from test_telegram_bot_handlers import sent
from test_telegram_callback_commands import drain, press, setup
from test_telegram_supervisor import _supervisor_logging as _supervisor_logging

from app.telegram_bots.streaming import LivePresentation
from deerflow.persistence.telegram_bots.model import TelegramCallbackRow


@pytest.mark.asyncio
async def test_actual_stop_callback_retains_update_identity_across_ingress_task(db, monkeypatch):
    e = await setup(db, monkeypatch)

    async def protected(_context):
        return True

    e.services.protect_notice = protected
    view = LivePresentation(channel=e.channel, link=e.channel.link, draft_id=1151, topic=None, is_current=lambda: True, observer=None)
    execute = AsyncMock()
    own_view(view, execute_callback=execute)
    try:
        e.request.enqueue("sendMessage", result=sent(e, identity=651))
        await view.start()
        await drain(e)
        async with db.sessions() as session:
            row = await session.scalar(select(TelegramCallbackRow).where(TelegramCallbackRow.kind == "stop_answer"))
            callback_id = row.id
        e.request.enqueue("answerCallbackQuery")
        assert await e.channel.handle_update(press(e, 1152, callback_id, message_id=651))
        await drain(e)
        execute.assert_awaited_once()
        args, kwargs = execute.await_args
        assert args[0] is e.channel and args[1].id == callback_id
        assert kwargs == {"update_id": 1152, "query_id": "query-1152"}
        assert await view.matches_callback(args[1])
    finally:
        await view.close()
        await e.sender.stop()
        await e.channel.stop()
