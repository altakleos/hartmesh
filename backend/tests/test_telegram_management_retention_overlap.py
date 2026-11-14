from copy import deepcopy
from datetime import timedelta

import pytest
from _storage_spaces_test_support import operation
from _telegram_repository_test_support import db as db
from test_telegram_work_interaction_delivery import guard_for, selected
from test_telegram_work_management_delivery import open_card, prepare_management
from test_telegram_work_management_navigation import publish_edit
from test_telegram_work_source_delivery import work_delivery as work_delivery
from test_work_attention_pages import work_pages as work_pages

from app.telegram_bots.retention import sweep_rows
from deerflow.persistence.telegram_bots.model import TelegramActRow, TelegramCallbackRow, TelegramOutboxRow


@pytest.mark.asyncio
async def test_conflicting_parent_claims_keep_entire_native_and_ui_family(work_delivery):
    e = work_delivery
    plans, button = await prepare_management(e, "work_withdraw")
    await open_card(e, plans, button)
    control = await selected(e, "work_withdraw_confirm")
    act = await plans.confirm(e.link, control.id, chat_id=e.link.chat_id, message_id=control.message_id, message_thread_id=None, guard=await guard_for(e))
    assert (await e.acts.check(act.id)).state == "done"
    await plans.observe(e.link, e.identity)
    await publish_edit(e)
    other = operation()
    async with e.db.sessions.begin() as writer:
        parent = await writer.get(TelegramOutboxRow, e.identity)
        values = {prop.key: deepcopy(getattr(parent, prop.key)) for prop in parent.__mapper__.column_attrs}
        values.update(id=other, dedupe_key="conflicting-parent:" + other)
        writer.add(TelegramOutboxRow(**values))
    later = e.clock.now() + timedelta(days=401)
    await plans.callbacks.sweep(later)
    await sweep_rows(e.db.sessions, later, callback_max_age_days=180)
    async with e.db.sessions() as reader:
        assert await reader.get(TelegramActRow, act.id) is not None
        assert await reader.get(TelegramOutboxRow, e.identity) is not None
        assert await reader.get(TelegramOutboxRow, other) is not None
        assert await reader.get(TelegramCallbackRow, control.id) is not None
