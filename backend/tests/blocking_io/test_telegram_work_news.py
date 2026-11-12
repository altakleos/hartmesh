"""Colleague source reads and source refusal remain nonblocking."""

import asyncio
from zoneinfo import ZoneInfo

import pytest
import pytest_asyncio
from _telegram_repository_test_support import db as db
from test_telegram_work_news_sources import test_generic_product_reply_cannot_impersonate_colleague_news as refuse
from test_telegram_work_news_sources import test_news_source_uses_colleague_and_canonical_assignment as read
from test_telegram_work_source_delivery import work_delivery as work_delivery
from test_work_attention_pages import work_pages as work_pages


@pytest_asyncio.fixture(autouse=True)
async def fixture_timezone():
    await asyncio.to_thread(ZoneInfo, "America/New_York")
    await asyncio.to_thread(ZoneInfo, "UTC")


@pytest.mark.asyncio
async def test_colleague_source_read(work_delivery):
    await read(work_delivery, "edit")


@pytest.mark.asyncio
async def test_unqualified_news_refused(work_delivery):
    await refuse(work_delivery, "work.changed")
