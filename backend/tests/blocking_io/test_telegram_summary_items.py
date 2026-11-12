"""Summary navigation uses nonblocking current source and callback reads."""

import pytest
from _telegram_repository_test_support import db as db
from test_telegram_summary_items import test_summary_item_opens_current_question_without_notification_dependency as opening
from test_telegram_work_source_delivery import work_delivery as work_delivery
from test_work_attention_pages import work_pages as work_pages


@pytest.mark.asyncio
async def test_protected_summary_item(work_delivery):
    await opening(work_delivery, True)
