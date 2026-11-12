"""Non-review result source admission, native bytes and Show stay off-loop."""

import asyncio
from zoneinfo import ZoneInfo

import pytest
import pytest_asyncio
from _telegram_repository_test_support import db as db
from test_telegram_result_notice_delivery import test_protected_result_notice_requires_actual_show_and_cleanup_before_files as reveal
from test_telegram_result_notice_delivery import test_result_notice_delivers_files_before_complete_card_without_review_controls as deliver
from test_telegram_result_notice_recovery import test_explicit_repair_preserves_uploaded_file_and_replaces_only_card as repair
from test_telegram_result_notice_recovery import test_partial_result_refresh_keeps_real_file_ack_and_updates_card as refresh
from test_telegram_work_file_exports import work_exports as work_exports
from test_telegram_work_source_delivery import work_delivery as work_delivery
from test_work_attention_pages import work_pages as work_pages


@pytest_asyncio.fixture(autouse=True)
async def fixture_timezone():
    await asyncio.to_thread(ZoneInfo, "America/New_York")
    await asyncio.to_thread(ZoneInfo, "UTC")


@pytest.mark.asyncio
@pytest.mark.parametrize("work_exports", ["done_event", "waiting_event"], indirect=True)
async def test_result_notice_delivery(work_exports):
    await deliver(work_exports)


@pytest.mark.asyncio
@pytest.mark.parametrize("work_exports", ["done_event"], indirect=True)
async def test_result_notice_show(work_exports):
    await reveal(work_exports, False)


@pytest.mark.asyncio
@pytest.mark.parametrize("work_exports", ["done_event"], indirect=True)
async def test_result_notice_refresh(work_exports):
    await refresh(work_exports, "locale")


@pytest.mark.asyncio
@pytest.mark.parametrize("work_exports", ["done_event"], indirect=True)
async def test_result_notice_repair(work_exports, monkeypatch):
    await repair(work_exports, monkeypatch, None)
