"""Raw malformed retention locators may hold only their real custody scope."""

from copy import deepcopy
from datetime import timedelta

import pytest
from test_telegram_request_continuation_retention import db as db
from test_telegram_request_continuation_retention import work_delivery as work_delivery
from test_telegram_request_continuation_retention import work_pages as work_pages
from test_telegram_voice_continuations import pending_voice_child

from app.telegram_bots.work_request_continuation_retention import request_dependencies
from deerflow.persistence.telegram_bots.model import TelegramActRow, TelegramInboundMarkRow


@pytest.mark.asyncio
@pytest.mark.parametrize("scope", ["foreign_topic", "foreign_generation"])
async def test_damaged_input_locator_does_not_pin_foreign_custody(work_delivery, monkeypatch, scope):
    e = work_delivery
    _acts, parent, _original = await pending_voice_child(e, monkeypatch)
    identity = parent.continuation["request_activation"]["input"]
    original_key = (identity["link_id"], identity["message_id"], identity["chat_id"])
    async with e.db.sessions.begin() as writer:
        mark = await writer.get(TelegramInboundMarkRow, original_key)
        values = {column.name: deepcopy(getattr(mark, column.name)) for column in TelegramInboundMarkRow.__table__.columns}
        values["message_id"] += 50
        values["update_id"] += 50
        if scope == "foreign_topic":
            values["message_thread_id"] = 9941
        else:
            values["binding_generation"] += 1
        writer.add(TelegramInboundMarkRow(**values))
        foreign_key = (values["link_id"], values["message_id"], values["chat_id"])
        row = await writer.get(TelegramActRow, parent.id)
        value = deepcopy(row.continuation)
        value["request_activation"]["input"] = {**identity, "message_id": values["message_id"], "update_id": values["update_id"]}
        row.continuation = value
    async with e.db.sessions() as reader:
        dependencies = await request_dependencies(reader, e.clock() + timedelta(days=401), max_age_days=180)
    assert parent.id in dependencies.acts
    assert original_key in dependencies.inputs
    assert foreign_key not in dependencies.inputs
