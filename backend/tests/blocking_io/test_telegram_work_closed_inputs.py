"""Closed Work input recovery keeps SDK and database work off the event loop."""

import asyncio
from zoneinfo import ZoneInfo

import pytest
import pytest_asyncio
from _telegram_repository_test_support import db as db
from test_telegram_work_closed_inputs import test_closed_work_input_requires_the_saved_explicit_choice as choose
from test_telegram_work_closed_inputs import test_spent_review_card_reopens_comment_without_submitting_changes as reopen
from test_telegram_work_source_delivery import work_delivery as work_delivery
from test_work_attention_pages import work_pages as work_pages


@pytest_asyncio.fixture(autouse=True)
async def fixture_timezone():
    await asyncio.to_thread(ZoneInfo, "America/New_York")


@pytest.mark.asyncio
async def test_new_message_choice(work_delivery):
    await choose(work_delivery, "prompt_send_new")


@pytest.mark.asyncio
async def test_reopen_spent_review(work_delivery):
    await reopen(work_delivery)
