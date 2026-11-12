"""Digest recovery uses current source admission without blocking I/O."""

import asyncio
from zoneinfo import ZoneInfo

import pytest
import pytest_asyncio
from _telegram_repository_test_support import db as db
from test_telegram_digest_view_repair import test_digest_missing_target_repair_sends_one_new_protected_card as repair
from test_telegram_digest_view_repair import test_digest_repair_final_provider_rechecks_new_callback_group as fence
from test_telegram_digest_view_repair import test_digest_show_retains_multiple_refreshes_before_one_sdk_edit as refresh
from test_telegram_work_source_delivery import work_delivery as work_delivery
from test_work_attention_pages import work_pages as work_pages


@pytest_asyncio.fixture(autouse=True)
async def fixture_timezone():
    await asyncio.to_thread(ZoneInfo, "America/New_York")
    await asyncio.to_thread(ZoneInfo, "UTC")


@pytest.mark.asyncio
async def test_digest_missing_target(work_delivery):
    await repair(work_delivery)


@pytest.mark.asyncio
async def test_digest_repeated_refresh(work_delivery):
    await refresh(work_delivery)


@pytest.mark.asyncio
async def test_digest_repair_final_group(work_delivery, monkeypatch):
    await fence(work_delivery, monkeypatch)
