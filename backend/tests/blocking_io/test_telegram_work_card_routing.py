"""Actual SDK Work-card text routing uses only asynchronous persistence."""

import asyncio
from zoneinfo import ZoneInfo

import pytest
import pytest_asyncio
from _telegram_repository_test_support import db as db
from test_telegram_work_card_routing import test_actual_quoted_card_text_saves_one_answer_or_comment as reply
from test_telegram_work_card_routing import test_reply_to_first_part_of_work_prompt_uses_that_prompt as prompt_reply
from test_telegram_work_source_delivery import work_delivery as work_delivery
from test_work_attention_pages import work_pages as work_pages


@pytest_asyncio.fixture(autouse=True)
async def fixture_timezone():
    await asyncio.to_thread(ZoneInfo, "America/New_York")


@pytest.mark.asyncio
async def test_review_comment(work_delivery):
    await reply(work_delivery, "review")


@pytest.mark.asyncio
async def test_multipart_prompt(work_delivery):
    await prompt_reply(work_delivery)
