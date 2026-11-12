"""Full digest previews preserve nonblocking source and callback authority."""

import asyncio
from zoneinfo import ZoneInfo

import pytest
import pytest_asyncio
from _telegram_repository_test_support import db as db
from test_telegram_digest_items import test_digest_item_opens_full_question_and_keeps_navigation_reusable as opens
from test_telegram_digest_items import test_digest_preview_final_source_read_refuses_changed_origin as fences
from test_telegram_work_source_delivery import work_delivery as work_delivery
from test_work_attention_pages import work_pages as work_pages


@pytest_asyncio.fixture(autouse=True)
async def fixture_timezone():
    await asyncio.to_thread(ZoneInfo, "America/New_York")
    await asyncio.to_thread(ZoneInfo, "UTC")


@pytest.mark.asyncio
async def test_full_digest_preview(work_delivery):
    await opens(work_delivery, True)


@pytest.mark.asyncio
async def test_digest_preview_source_fence(work_delivery, monkeypatch):
    await fences(work_delivery, monkeypatch, "callback")
