from datetime import timedelta

import pytest
from _telegram_repository_test_support import db as db
from test_telegram_management_edit_retention import prepare_family
from test_telegram_work_source_delivery import work_delivery as work_delivery
from test_work_attention_pages import work_pages as work_pages

from app.telegram_bots.retention import sweep_rows
from deerflow.persistence.telegram_bots.model import TelegramActRow, TelegramInboundMarkRow


@pytest.mark.asyncio
async def test_immutable_replacement_input_locator_pins_a_mark_with_damaged_route_kind(work_delivery, monkeypatch):
    e = work_delivery
    cards, parent_id, edit, newest, incoming, prompt_parent, prompt_id = await prepare_family(e, monkeypatch, "pending")
    key = (incoming.link_id, incoming.message_id, incoming.chat_id)
    async with e.db.sessions.begin() as writer:
        old = await writer.get(TelegramActRow, edit.target_id)
        assert old.result["input"]["message_id"] == incoming.message_id
        mark = await writer.get(TelegramInboundMarkRow, key)
        assert mark.state == "done" and mark.route_complete
        mark.route_kind = "assignment_draft"
    later = e.clock.now() + timedelta(days=401)
    await cards.callbacks.sweep(later)
    await sweep_rows(e.db.sessions, later, callback_max_age_days=180)
    async with e.db.sessions() as reader:
        assert await reader.get(TelegramActRow, newest) is not None
        assert await reader.get(TelegramInboundMarkRow, key) is not None
