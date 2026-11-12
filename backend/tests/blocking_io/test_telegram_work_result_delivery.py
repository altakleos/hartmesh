"""Result media keeps native reads, SDK setup and final admission off-loop."""

import asyncio
from zoneinfo import ZoneInfo

import pytest
import pytest_asyncio
from _telegram_repository_test_support import db as db
from test_telegram_work_file_exports import work_exports as work_exports
from test_telegram_work_result_delivery import test_duplicate_paths_in_distinct_spaces_send_in_order_with_durable_photo_fallback as album
from test_telegram_work_result_delivery import test_result_file_upload_precedes_card_and_arms_controls_after_whole_ack as document
from test_telegram_work_result_recovery import test_show_clears_teaser_before_protected_files_and_final_controls as reveal
from test_telegram_work_source_delivery import work_delivery as work_delivery
from test_work_attention_pages import work_pages as work_pages


@pytest_asyncio.fixture(autouse=True)
async def fixture_timezone():
    await asyncio.to_thread(ZoneInfo, "America/New_York")
    await asyncio.to_thread(ZoneInfo, "UTC")


@pytest.mark.asyncio
@pytest.mark.parametrize("work_exports", ["review"], indirect=True)
async def test_result_document(work_exports, monkeypatch):
    await document(work_exports, monkeypatch)


@pytest.mark.asyncio
@pytest.mark.parametrize("work_exports", ["review_album"], indirect=True)
async def test_result_album_fallback(work_exports):
    await album(work_exports, True)


@pytest.mark.asyncio
@pytest.mark.parametrize("work_exports", ["review"], indirect=True)
async def test_result_reveal(work_exports):
    await reveal(work_exports, "ok")
