"""Actual untied text, source authorization and response admission stay off-loop."""

import asyncio
from zoneinfo import ZoneInfo

import pytest
import pytest_asyncio
from _telegram_repository_test_support import db as db
from test_telegram_work_source_delivery import work_delivery as work_delivery
from test_telegram_work_untied_inputs import test_save_untied_text_only_after_actual_choice as save_answer
from test_work_attention_pages import work_pages as work_pages


@pytest_asyncio.fixture(autouse=True)
async def fixture_timezone():
    await asyncio.to_thread(ZoneInfo, "America/New_York")


@pytest.mark.asyncio
async def test_untied_answer_requires_choice(work_delivery):
    await save_answer(work_delivery, "information")
