"""Actual SDK callback ingress routes saved file controls to the turn controller.

The executor is observed here; source qualification and delivery-only SQL retry
are covered separately by the file-source tests.
"""

import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from _telegram_repository_test_support import db as db
from sqlalchemy import select
from test_telegram_bot_handlers import sent
from test_telegram_callback_commands import press, setup
from test_telegram_supervisor import _supervisor_logging as _supervisor_logging

from deerflow.persistence.telegram_bots.model import TelegramCallbackRow


@pytest.mark.asyncio
async def test_actual_file_retry_callback_reaches_turn_controller_with_original_update(db, monkeypatch):
    e = await setup(db, monkeypatch)
    try:
        owner = await e.live.authority.prepare_live_owner(e.link.owner_user_id)
        guard = db.links.publication_guard(e.link, check_live_owner=owner, check_attempt_current=lambda: e.channel._callback_current(e.link))
        await e.services.outbox.plan_product_keyboard(
            e.link,
            source_id="file-retry-routing-protocol",
            parts=[{"kind": "text", "source_ref": {"key": "file.out_failed", "values": {"file": "result.txt"}}}],
            keyboard=[[{"label": "label.send_again", "kind": "file_retry", "target_kind": "thread", "target_id": "original-thread", "payload": {"thread_id": "original-thread", "path": "/mnt/user-data/outputs/result.txt"}}]],
            callbacks=e.services.callbacks,
            guard=guard,
        )
        e.request.enqueue("sendMessage", result=sent(e, identity=501))
        await e.sender.tick()
        await e.sender.drain()
        async with db.sessions() as reader:
            row = await reader.scalar(select(TelegramCallbackRow).where(TelegramCallbackRow.kind == "file_retry"))
            identity = row.id
        execute = AsyncMock()
        e.services.turns = SimpleNamespace(execute_callback=execute)
        e.request.enqueue("answerCallbackQuery")
        assert await e.channel.handle_update(press(e, 1761, identity))
        await asyncio.gather(*tuple(e.channel._ingress_tasks))
        execute.assert_awaited_once()
        args, kwargs = execute.await_args
        assert args[0] is e.channel and args[1].id == identity
        assert kwargs == {"update_id": 1761, "query_id": "query-1761"}
    finally:
        e.services.turns = None
        await e.sender.stop()
        await e.channel.stop()
