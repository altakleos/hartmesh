"""Navigation preview repair keeps provider-final authority nonblocking."""

import asyncio
from zoneinfo import ZoneInfo

import pytest
import pytest_asyncio
from _telegram_repository_test_support import db as db
from test_telegram_navigation_recovery import test_navigation_preview_refresh_preserves_full_source_and_original_gesture as refresh
from test_telegram_navigation_recovery import test_navigation_refresh_rechecks_new_group_after_provider as fence
from test_telegram_work_source_delivery import work_delivery as work_delivery
from test_work_attention_pages import work_pages as work_pages


@pytest_asyncio.fixture(autouse=True)
async def fixture_timezone():
    await asyncio.to_thread(ZoneInfo, "America/New_York")
    await asyncio.to_thread(ZoneInfo, "UTC")


@pytest.mark.asyncio
async def test_navigation_repair(work_delivery):
    await refresh(work_delivery, "locale")


@pytest.mark.asyncio
async def test_navigation_repair_final_group(work_delivery, monkeypatch):
    await fence(work_delivery, monkeypatch, "target")
