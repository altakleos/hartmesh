"""Complete paginated Attention uses asynchronous readers through every page."""

import asyncio
from zoneinfo import ZoneInfo

import pytest
import pytest_asyncio
from _telegram_repository_test_support import db as db
from test_work_attention_pages import test_complete_attention_ranks_old_actionable_records_before_new_closed_history as complete_attention
from test_work_attention_pages import work_pages as work_pages


@pytest_asyncio.fixture(autouse=True)
async def fixture_timezone():
    await asyncio.to_thread(ZoneInfo, "America/New_York")


@pytest.mark.asyncio
async def test_complete_attention_pages_keep_the_loop_clear(work_pages):
    await complete_attention(work_pages)
