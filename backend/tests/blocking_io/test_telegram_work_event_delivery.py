"""Question materialization and final SDK checks keep I/O off the loop."""

import asyncio
from zoneinfo import ZoneInfo

import pytest
import pytest_asyncio
from _telegram_repository_test_support import db as db
from test_telegram_work_event_delivery import test_protected_event_is_concealed_until_actual_show as protected
from test_telegram_work_event_delivery import test_question_intent_materializes_once_and_arms_only_after_complete_sdk_ack as question
from test_telegram_work_source_delivery import work_delivery as work_delivery
from test_work_attention_pages import work_pages as work_pages


@pytest_asyncio.fixture(autouse=True)
async def fixture_timezone():
    await asyncio.to_thread(ZoneInfo, "America/New_York")
    await asyncio.to_thread(ZoneInfo, "UTC")


@pytest.mark.asyncio
async def test_question_materialization(work_delivery):
    await question(work_delivery, "information")


@pytest.mark.asyncio
async def test_protected_question_show(work_delivery):
    await protected(work_delivery)
