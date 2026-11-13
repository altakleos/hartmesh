"""Stopping summaries preserves actual SQLite/SDK ownership under strict I/O."""

import asyncio
from zoneinfo import ZoneInfo

import pytest
import pytest_asyncio
from _telegram_repository_test_support import db as db
from test_telegram_summary_preferences import test_summary_stop_final_provider_prevents_stale_edit as fence
from test_telegram_summary_preferences import test_summary_stop_keeps_body_and_other_buttons as stop
from test_telegram_supervisor import _supervisor_logging as _supervisor_logging
from test_telegram_work_source_delivery import work_delivery as work_delivery
from test_work_attention_pages import work_pages as work_pages


@pytest.mark.asyncio
async def test_summary_stop(work_delivery):
    await stop(work_delivery, True)


@pytest.mark.asyncio
async def test_summary_stop_final_grant(work_delivery, monkeypatch):
    await fence(work_delivery, monkeypatch, "grant")


@pytest_asyncio.fixture(autouse=True)
async def _zones():
    await asyncio.to_thread(ZoneInfo, "America/New_York")
