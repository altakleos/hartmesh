"""Raw class labels cannot replace digest source authority."""

import pytest
from _telegram_repository_test_support import db as db
from sqlalchemy import select
from test_telegram_bot_adapter import message
from test_telegram_work_source_delivery import work_delivery as work_delivery
from test_work_attention_pages import work_pages as work_pages

from deerflow.persistence.telegram_bots.model import TelegramOutboxRow


@pytest.mark.asyncio
@pytest.mark.parametrize("key", ["digest.header", "summary.item.question", "concealed.digest_news"])
async def test_generic_reply_cannot_impersonate_a_digest(work_delivery, key):
    e = work_delivery
    async with e.db.sessions.begin() as writer:
        for row in await writer.scalars(select(TelegramOutboxRow)):
            row.state = "sent"
    await e.sender.outbox.plan_product(e.link, source_id="unqualified-digest", parts=[{"kind": "text", "source_ref": {"key": key, "values": {"n": 3, "objective": "Fabricated assignment"}}}])
    message(e.channel.bot.request, identity=5101)
    assert await e.sender.tick() == 1
    await e.sender.drain()
    assert not [call for call in e.channel.bot.request.calls if call.endpoint == "sendMessage"]
