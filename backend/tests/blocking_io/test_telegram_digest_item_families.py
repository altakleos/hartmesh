"""Family previews retain file and navigation authority without blocking I/O."""

import asyncio
from zoneinfo import ZoneInfo

import pytest
import pytest_asyncio
from _telegram_repository_test_support import db as db
from test_telegram_digest_item_families import test_digest_review_preview_delivers_exact_files_before_review_controls as review
from test_telegram_digest_item_families import test_preview_file_read_rechecks_navigation_in_final_provider_snapshot as file_fence
from test_telegram_work_file_exports import work_exports as work_exports
from test_telegram_work_source_delivery import work_delivery as work_delivery
from test_work_attention_pages import work_pages as work_pages


@pytest_asyncio.fixture(autouse=True)
async def fixture_timezone():
    await asyncio.to_thread(ZoneInfo, "America/New_York")
    await asyncio.to_thread(ZoneInfo, "UTC")


@pytest.mark.asyncio
@pytest.mark.parametrize("work_exports", ["review_event"], indirect=True)
async def test_protected_review_preview(work_exports):
    await review(work_exports, True)


@pytest.mark.asyncio
@pytest.mark.parametrize("work_exports", ["done_event"], indirect=True)
async def test_result_file_navigation_fence(work_exports, monkeypatch):
    await file_fence(work_exports, monkeypatch, "callback")
