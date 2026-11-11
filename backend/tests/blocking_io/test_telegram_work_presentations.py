"""Work presentation planning owns async SQL without blocking the Gateway loop."""

import asyncio
from zoneinfo import ZoneInfo

import pytest
import pytest_asyncio
from _telegram_repository_test_support import db as db
from test_telegram_work_presentations import test_durable_request_plan_is_atomic_and_reuses_original_controls as durable_plan
from test_work_attention_pages import work_pages as work_pages


@pytest_asyncio.fixture(autouse=True)
async def fixture_timezone():
    await asyncio.to_thread(ZoneInfo, "America/New_York")


@pytest.mark.asyncio
async def test_work_presentation_plan_keeps_the_loop_clear(work_pages, monkeypatch):
    await durable_plan(work_pages, monkeypatch)
