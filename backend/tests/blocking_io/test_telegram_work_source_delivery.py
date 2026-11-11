"""Real Work SDK authorization performs no blocking I/O on the Gateway loop."""

import asyncio
from zoneinfo import ZoneInfo

import pytest
import pytest_asyncio
from _telegram_repository_test_support import db as db
from test_telegram_work_source_delivery import test_frozen_work_preview_reaches_sdk_only_with_its_source_reader as delivery
from test_telegram_work_source_delivery import work_delivery as work_delivery
from test_work_attention_pages import work_pages as work_pages


@pytest_asyncio.fixture(autouse=True)
async def fixture_timezone():
    await asyncio.to_thread(ZoneInfo, "America/New_York")


@pytest.mark.asyncio
async def test_work_source_sdk_boundary_keeps_the_loop_clear(work_delivery):
    await delivery(work_delivery)
