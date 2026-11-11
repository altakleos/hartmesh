"""Owned preview recovery and raw SDK repair keep the Gateway loop clear."""

import asyncio
from zoneinfo import ZoneInfo

import pytest
import pytest_asyncio
from _telegram_repository_test_support import db as db
from test_telegram_work_preview_recovery import test_protected_reveal_and_missing_anchor_preserve_current_source as reveal_case
from test_telegram_work_preview_recovery import test_reveal_reads_new_comments_and_withholds_controls_until_all_chunks_ack as multipart_case
from test_telegram_work_source_delivery import work_delivery as work_delivery
from test_work_attention_pages import work_pages as work_pages


@pytest_asyncio.fixture(autouse=True)
async def fixture_timezone():
    await asyncio.to_thread(ZoneInfo, "America/New_York")


@pytest.mark.asyncio
async def test_protected_reveal_and_repair_owns_nonblocking_delivery(work_delivery):
    ZoneInfo.clear_cache()
    await reveal_case(work_delivery, True)


@pytest.mark.asyncio
async def test_multipart_reveal_cleanup_and_all_chunks_keep_the_loop_clear(work_delivery):
    ZoneInfo.clear_cache()
    await multipart_case(work_delivery, "ok")
