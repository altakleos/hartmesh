"""Capacity HTTP/source qualification and consent transfer remain asynchronous."""

import asyncio
from zoneinfo import ZoneInfo

import pytest
import pytest_asyncio
from _telegram_repository_test_support import db as db
from test_telegram_capacity_delivery import test_capacity_caption_uses_actual_visible_task_and_missing_time as busy_caption
from test_telegram_capacity_offers import test_free_place_offer_has_full_preview_and_fresh_atomic_start as free_place
from test_telegram_work_source_delivery import work_delivery as work_delivery
from test_work_attention_pages import work_pages as work_pages


@pytest_asyncio.fixture(autouse=True)
async def fixture_timezone():
    await asyncio.to_thread(ZoneInfo, "America/New_York")


@pytest.mark.asyncio
async def test_current_capacity_caption_keeps_the_loop_clear(work_delivery, monkeypatch):
    await busy_caption(work_delivery, monkeypatch)


@pytest.mark.asyncio
async def test_free_place_consent_keeps_the_loop_clear(work_delivery, monkeypatch):
    await free_place(work_delivery, monkeypatch)
