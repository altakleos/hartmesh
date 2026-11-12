"""Full saved-comment followups preserve asynchronous delivery ownership."""

import asyncio
from zoneinfo import ZoneInfo

import pytest
import pytest_asyncio
from _telegram_repository_test_support import db as db
from test_telegram_work_comment_cards import test_comment_card_rechecks_referenced_space_before_sdk as reference_check
from test_telegram_work_comment_cards import test_saved_comment_shows_complete_current_request_before_fresh_controls as comment
from test_telegram_work_source_delivery import work_delivery as work_delivery
from test_work_attention_pages import work_pages as work_pages


@pytest_asyncio.fixture(autouse=True)
async def fixture_timezone():
    await asyncio.to_thread(ZoneInfo, "America/New_York")


@pytest.mark.asyncio
async def test_review_comment_and_next_preview(work_delivery):
    await comment(work_delivery, "review")


@pytest.mark.asyncio
async def test_final_space_withdrawal(work_delivery, monkeypatch):
    await reference_check(work_delivery, monkeypatch)
