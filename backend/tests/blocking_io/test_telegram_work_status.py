"""Status source and SDK delivery keep cold timezone I/O off the event loop."""

import asyncio
from zoneinfo import ZoneInfo

import pytest
import pytest_asyncio
from _telegram_repository_test_support import db as db
from test_telegram_work_source_delivery import work_delivery as work_delivery
from test_telegram_work_status import test_status_reads_exact_attempt_and_current_progress as read_status
from test_telegram_work_status_delivery import test_protected_status_show_reads_current_progress_without_starting_work as show_status
from test_telegram_work_status_delivery import test_status_final_sdk_check_revalidates_source_and_authority as final_status
from test_telegram_work_status_delivery import test_status_first_delivery_is_silent_and_has_no_execution_controls as send_status
from test_telegram_work_status_delivery import test_status_progress_before_first_send_refreshes_the_saved_plan as refresh_status
from test_work_attention_pages import work_pages as work_pages


@pytest_asyncio.fixture(autouse=True)
async def fixture_timezone():
    await asyncio.to_thread(ZoneInfo, "America/New_York")
    await asyncio.to_thread(ZoneInfo, "UTC")


@pytest.mark.asyncio
async def test_status_source(work_delivery):
    ZoneInfo.clear_cache()
    await read_status(work_delivery)


@pytest.mark.asyncio
async def test_status_delivery(work_delivery):
    ZoneInfo.clear_cache()
    await send_status(work_delivery, False)


@pytest.mark.asyncio
async def test_status_show(work_delivery):
    await show_status(work_delivery)


@pytest.mark.asyncio
async def test_status_refresh(work_delivery):
    await refresh_status(work_delivery)


@pytest.mark.asyncio
async def test_status_final_authority(work_delivery, monkeypatch):
    await final_status(work_delivery, monkeypatch, "grant")
