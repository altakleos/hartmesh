"""Full news previews and their original launch proof keep the event loop clear."""

import asyncio
from zoneinfo import ZoneInfo

import pytest
import pytest_asyncio
from _telegram_repository_test_support import db as db
from test_telegram_news_carrier_families import start_news
from test_telegram_work_carriers import test_news_resume_qualifies_its_actual_event_at_each_entry as resume_entry
from test_telegram_work_source_delivery import work_delivery as work_delivery
from test_work_attention_pages import work_pages as work_pages


@pytest_asyncio.fixture(autouse=True)
async def fixture_timezone():
    await asyncio.to_thread(ZoneInfo, "America/New_York")
    await asyncio.to_thread(ZoneInfo, "UTC")


@pytest.mark.asyncio
async def test_proposed_assignment_launch_proof(work_delivery, monkeypatch):
    await start_news(work_delivery, monkeypatch, "work.proposed")


@pytest.mark.asyncio
async def test_late_news_source_change(work_delivery, monkeypatch):
    await resume_entry(work_delivery, monkeypatch, "during-native")
