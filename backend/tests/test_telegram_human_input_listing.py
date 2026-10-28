"""Seeded SQL listing controls, without claiming qualified request authority."""

from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest
from _telegram_repository_test_support import add_link, bind_link
from _telegram_repository_test_support import db as db
from sqlalchemy import event

from app.telegram_bots.producer import Outbox
from deerflow.persistence.telegram_bots.model import TelegramCallbackRow, TelegramOutboxRow


@pytest.mark.asyncio
async def test_open_request_listing_filters_and_bounds_before_materializing(db):
    link = await bind_link(db, await add_link(db))
    expected, now = [], datetime.now(UTC)
    async with db.sessions.begin() as writer:
        for number in range(68):
            identity = uuid4().hex
            version = 5 if number < 33 or number > 65 else 4
            if number < 33:
                expected.append(identity)
            writer.add(
                TelegramOutboxRow(
                    id=identity,
                    link_id=link.id,
                    owner_user_id=link.owner_user_id,
                    binding_generation=link.binding_generation,
                    chat_id=link.chat_id,
                    message_thread_id=None,
                    dedupe_key="listing:" + str(number),
                    class_="reply",
                    state="pending",
                    not_before=now,
                    created_at=now + timedelta(seconds=number),
                    payload={"reply_basis": {"version": version}},
                    parts=[],
                )
            )
            writer.add(
                TelegramCallbackRow(
                    id=uuid4().hex,
                    group_id=uuid4().hex,
                    link_id=link.id,
                    binding_generation=link.binding_generation,
                    chat_id=link.chat_id + (1 if number == 66 else 0),
                    message_thread_id=None,
                    kind="clarify_option",
                    payload={"anchor_outbox_id": identity},
                    created_at=now,
                    consumed_at=now if number == 67 else None,
                )
            )
    statements = []

    def observed(_connection, _cursor, statement, _params, _context, _many):
        statements.append(statement)

    event.listen(db.engine.sync_engine, "before_cursor_execute", observed)
    try:
        found = await Outbox(db.sessions).list_human_inputs(link, topic=None)
    finally:
        event.remove(db.engine.sync_engine, "before_cursor_execute", observed)
    assert [item.id for item in found] == list(reversed(expected))[0:32]
    assert len(statements) == 1 and "LIMIT" in statements[0].upper() and "EXISTS" in statements[0].upper()
