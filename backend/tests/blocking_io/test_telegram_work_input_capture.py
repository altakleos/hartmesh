"""Text capture borrows one guarded asynchronous writer."""

import pytest
from _telegram_repository_test_support import db as db
from test_telegram_work_input_capture import test_changed_review_retains_accepted_text_and_original_seen_without_an_act as capture
from test_telegram_work_source_delivery import work_delivery as work_delivery
from test_work_attention_pages import work_pages as work_pages


@pytest.mark.asyncio
async def test_work_input_capture(work_delivery):
    await capture(work_delivery, "comment")
