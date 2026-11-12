"""Status refresh and exact-target recovery retain ownership on the event loop."""

import asyncio
from zoneinfo import ZoneInfo

import pytest
import pytest_asyncio
from _telegram_repository_test_support import db as db
from test_telegram_status_updates import test_missing_status_target_repairs_by_acknowledged_complete_replacement as repair_status
from test_telegram_status_updates import test_status_edits_coalesce_current_progress_and_obey_thirty_second_floor as edit_status
from test_telegram_status_updates import test_status_late_locale_change_waits_for_refresh_instead_of_retiring as refresh_status
from test_telegram_status_updates import test_status_update_final_authority_refuses_changed_source as final_status
from test_telegram_work_source_delivery import work_delivery as work_delivery
from test_work_attention_pages import work_pages as work_pages


@pytest_asyncio.fixture(autouse=True)
async def fixture_timezone():
    await asyncio.to_thread(ZoneInfo, "America/New_York")
    await asyncio.to_thread(ZoneInfo, "UTC")


@pytest.mark.asyncio
async def test_status_edit(work_delivery):
    ZoneInfo.clear_cache()
    await edit_status(work_delivery)


@pytest.mark.asyncio
async def test_status_display_refresh(work_delivery, monkeypatch):
    await refresh_status(work_delivery, monkeypatch)


@pytest.mark.asyncio
async def test_status_replacement(work_delivery):
    await repair_status(work_delivery)


@pytest.mark.asyncio
async def test_status_final_authority(work_delivery, monkeypatch):
    await final_status(work_delivery, monkeypatch, "grant")
