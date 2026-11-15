"""Summary Work navigation retains its actual SQL and SDK lifetime."""

import asyncio
from zoneinfo import ZoneInfo

import pytest
import pytest_asyncio
from _telegram_repository_test_support import db as db
from test_telegram_summary_work_controls import test_overflow_snapshot_allocation_keeps_the_original_press_fences as allocation
from test_telegram_summary_work_controls import test_summary_start_items_and_overflow_open_current_native_views as navigation
from test_telegram_summary_work_controls import test_summary_work_view_final_sdk_fence_keeps_actual_source_and_access as fences
from test_telegram_work_source_delivery import work_delivery as work_delivery
from test_work_attention_pages import work_pages as work_pages


@pytest_asyncio.fixture(autouse=True)
async def fixture_timezone():
    # The shared ingress fixture seeds its owner's chosen timezone before the
    # summary installs UTC. Keep that fixture-only cold lookup off-loop.
    await asyncio.to_thread(ZoneInfo, "America/New_York")
    await asyncio.to_thread(ZoneInfo, "UTC")


@pytest.mark.asyncio
@pytest.mark.parametrize("protected", [False, True])
async def test_summary_work_navigation(work_delivery, protected):
    await navigation(work_delivery, protected)


@pytest.mark.asyncio
async def test_overflow_retired_during_collection(work_delivery, monkeypatch):
    await allocation(work_delivery, monkeypatch, "retire")


@pytest.mark.asyncio
@pytest.mark.parametrize("change", ["callback", "show"])
async def test_summary_work_final_source_fence(work_delivery, monkeypatch, change):
    await fences(work_delivery, monkeypatch, change)
