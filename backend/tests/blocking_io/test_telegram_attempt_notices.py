"""Terminal notice source joins and protected previews stay nonblocking."""

import asyncio
from zoneinfo import ZoneInfo

import pytest
import pytest_asyncio
from _telegram_repository_test_support import db as db
from test_telegram_attempt_notices import test_attempt_notice_delivers_from_actual_terminal_work_event as delivery
from test_telegram_attempt_notices import test_attempt_notice_final_source_and_grant_snapshot as final_source
from test_telegram_attempt_notices import test_protected_interruption_show_uses_acknowledged_teaser as protected_show
from test_telegram_work_source_delivery import work_delivery as work_delivery
from test_work_attention_pages import work_pages as work_pages


@pytest_asyncio.fixture(autouse=True)
async def fixture_timezone():
    await asyncio.to_thread(ZoneInfo, "America/New_York")
    await asyncio.to_thread(ZoneInfo, "UTC")


@pytest.mark.asyncio
async def test_terminal_notice_delivery(work_delivery):
    await delivery(work_delivery, "attempt.failed", "Failed:")


@pytest.mark.asyncio
async def test_contained_notice_delivery(work_delivery):
    await delivery(work_delivery, "interrupted", "Interrupted:")


@pytest.mark.asyncio
async def test_terminal_notice_protected_show(work_delivery):
    await protected_show(work_delivery)


@pytest.mark.asyncio
async def test_terminal_notice_final_source(work_delivery, monkeypatch):
    await final_source(work_delivery, monkeypatch, "reason")
