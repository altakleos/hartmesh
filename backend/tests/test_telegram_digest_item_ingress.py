"""Reusable digest item taps create deduplicated full previews via real ingress."""

import pytest
from _telegram_repository_test_support import db as db
from sqlalchemy import select
from test_telegram_digest_items import navigation
from test_telegram_work_callback_dispatch import install_dispatch, press_work
from test_telegram_work_control_admission import admissions
from test_telegram_work_source_delivery import work_delivery as work_delivery
from test_work_attention_pages import work_pages as work_pages

from deerflow.persistence.telegram_bots.model import TelegramActRow, TelegramCallbackRow, TelegramOutboxRow


@pytest.mark.asyncio
@pytest.mark.parametrize("protected", [False, True])
async def test_digest_item_ingress_creates_one_preview_per_query_without_work_action(work_delivery, protected):
    from app.telegram_bots.work_digest_items import DigestItems
    from app.telegram_bots.work_outcome_delivery import WorkOutcomePlans

    e = work_delivery
    digests, _anchor, callback_id, _target = await navigation(e, protected)
    items = DigestItems(digests=digests, requests=e.plans)
    install_dispatch(e, WorkOutcomePlans(e.plans, admissions(e).acts))
    e.channel._services.work_digest_items = items
    async with e.db.sessions() as reader:
        callback = await reader.get(TelegramCallbackRow, callback_id)
    for update_id in (7201, 7201, 7202):
        await press_work(e, callback, message_id=5801, update_id=update_id)
    async with e.db.sessions() as reader:
        previews = list(await reader.scalars(select(TelegramOutboxRow).where(TelegramOutboxRow.dedupe_key.like("notice:work-navigation:%"))))
        assert len(previews) == 2
        assert all(row.state == "pending" and row.class_ == "reply" for row in previews)
        assert (await reader.get(TelegramCallbackRow, callback_id)).consumed_at is None
        assert not list(await reader.scalars(select(TelegramActRow)))
