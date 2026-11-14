"""Actual composite outcome and capacity SDK paths keep blocking work off-loop."""

import asyncio
from zoneinfo import ZoneInfo

import pytest
import pytest_asyncio
from _telegram_repository_test_support import db as db
from support.detectors.blocking_io_runtime import detect_blocking_io_strict
from test_telegram_composite_execution_outcomes import test_composite_capacity_offer_uses_parent_consent_and_new_full_start as exercise_capacity
from test_telegram_composite_execution_outcomes import test_composite_unknown_notice_uses_actual_original_consent as exercise_notice
from test_telegram_work_source_delivery import work_delivery as work_delivery
from test_work_attention_pages import work_pages as work_pages

from app.gateway import deps  # noqa: F401 -- Warm Gateway dependencies before the asynchronous bot lifespan.


@pytest_asyncio.fixture(autouse=True)
async def timezones():
    await asyncio.to_thread(ZoneInfo, "America/New_York")
    await asyncio.to_thread(ZoneInfo, "UTC")


@pytest.mark.asyncio
async def test_composite_notice_does_not_block(work_delivery, monkeypatch):
    with detect_blocking_io_strict():
        await exercise_notice(work_delivery, monkeypatch)


@pytest.mark.asyncio
async def test_composite_capacity_reveal_does_not_block(work_delivery, monkeypatch):
    with detect_blocking_io_strict():
        await exercise_capacity(work_delivery, monkeypatch, "protected")
