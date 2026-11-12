"""Combined source reads preserve the event-loop I/O boundary."""

import asyncio
from zoneinfo import ZoneInfo

import pytest
import pytest_asyncio
from _telegram_repository_test_support import db as db
from test_telegram_digest_projection import test_digest_preserves_aliased_news_actor_and_request_models as mixed
from test_telegram_work_source_delivery import work_delivery as work_delivery
from test_work_attention_pages import work_pages as work_pages


@pytest_asyncio.fixture(autouse=True)
async def fixture_timezone():
    await asyncio.to_thread(ZoneInfo, "America/New_York")
    await asyncio.to_thread(ZoneInfo, "UTC")


@pytest.mark.asyncio
async def test_digest_combined_source(work_delivery, monkeypatch):
    await mixed(work_delivery, monkeypatch, True)
