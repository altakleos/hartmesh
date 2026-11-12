"""The periodic summary reader performs no blocking I/O."""

import pytest
from _telegram_repository_test_support import db as db
from test_telegram_summary_scheduler import test_summary_scheduler_ticks_and_restarts_without_new_attempts as tick
from test_telegram_work_source_delivery import work_delivery as work_delivery
from test_work_attention_pages import work_pages as work_pages


@pytest.mark.asyncio
async def test_summary_scheduler(work_delivery):
    await tick(work_delivery)
