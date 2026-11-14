"""Damaged callback topic cannot erase retained original input custody."""

from datetime import timedelta

import pytest
from test_telegram_request_continuation_retention import db as db
from test_telegram_request_continuation_retention import work_delivery as work_delivery
from test_telegram_request_continuation_retention import work_pages as work_pages
from test_telegram_voice_continuations import pending_voice_child
from test_telegram_work_outcome_delivery import clear_outcome_cleanup
from test_telegram_work_preview_recovery import dispatch

from app.telegram_bots.work_request_continuation_retention import request_dependencies
from deerflow.persistence.telegram_bots.model import TelegramCallbackRow, TelegramInboundMarkRow, TelegramOutboxRow


@pytest.mark.asyncio
async def test_damaged_callback_topic_retains_original_anchor_input(work_delivery, monkeypatch):
    e = work_delivery
    _acts, parent, _original = await pending_voice_child(e, monkeypatch)
    source = parent.continuation["request_activation"]
    identity = source["input"]
    original_key = (identity["link_id"], identity["message_id"], identity["chat_id"])
    e.outcome_identity = source["outcome_id"]
    await clear_outcome_cleanup(e)
    await dispatch(e, 1182)
    assert not e.dispatch_errors, e.dispatch_errors
    async with e.db.sessions() as reader:
        assert (await reader.get(TelegramOutboxRow, e.outcome_identity)).state == "sent"
        assert (await reader.get(TelegramInboundMarkRow, original_key)).route_complete
    async with e.db.sessions.begin() as writer:
        callback = await writer.get(TelegramCallbackRow, parent.id)
        assert callback.message_thread_id is None
        callback.message_thread_id = 9941
    async with e.db.sessions() as reader:
        dependencies = await request_dependencies(reader, e.clock() + timedelta(days=401), max_age_days=180)
    assert parent.id in dependencies.acts
    assert original_key in dependencies.inputs
    assert source["origin"]["anchor_outbox_id"] in dependencies.deliveries
    assert source["outcome_id"] in dependencies.deliveries
