"""Reusable Work navigation keeps its final origin authority nonblocking."""

import asyncio
from zoneinfo import ZoneInfo

import pytest
import pytest_asyncio
from _telegram_repository_test_support import db as db
from test_telegram_work_navigation_origin import test_navigation_origin_final_provider_refuses_changed_anchor as fence
from test_telegram_work_navigation_origin import test_navigation_origin_is_current_nonconsuming_and_per_query as read
from test_telegram_work_source_delivery import work_delivery as work_delivery
from test_work_attention_pages import work_pages as work_pages


@pytest_asyncio.fixture(autouse=True)
async def fixture_timezone():
    await asyncio.to_thread(ZoneInfo, "America/New_York")
    await asyncio.to_thread(ZoneInfo, "UTC")


@pytest.mark.asyncio
async def test_reusable_navigation(work_delivery):
    await read(work_delivery, True)


@pytest.mark.asyncio
async def test_navigation_final_origin(work_delivery, monkeypatch):
    await fence(work_delivery, monkeypatch, True, "callback")
