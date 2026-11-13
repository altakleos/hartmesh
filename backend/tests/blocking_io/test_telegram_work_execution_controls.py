"""Original-attempt Stop admission and dispatch keep the event loop clear."""

import asyncio
from zoneinfo import ZoneInfo

import pytest
import pytest_asyncio
from _telegram_repository_test_support import db as db
from test_telegram_work_source_delivery import work_delivery as work_delivery
from test_telegram_work_stop_controls import test_one_stop_press_stages_once_then_cancels_only_original_run as original_stop
from test_work_attention_pages import work_pages as work_pages


@pytest_asyncio.fixture(autouse=True)
async def fixture_timezone():
    await asyncio.to_thread(ZoneInfo, "America/New_York")


@pytest.mark.asyncio
async def test_stop_admission_and_exact_dispatch_keep_the_loop_clear(work_delivery):
    await original_stop(work_delivery)
