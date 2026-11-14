"""Actual spoken drafting and whole-card SDK consent stay nonblocking."""

import asyncio
from zoneinfo import ZoneInfo

import pytest
import pytest_asyncio
from _telegram_repository_test_support import db as db
from support.detectors.blocking_io_runtime import detect_blocking_io_strict
from test_telegram_assignment_voice_inputs import test_spoken_draft_controls_wait_for_heard_and_full_card_sdk_ack as exercise_card
from test_telegram_assignment_voice_inputs import test_spoken_manual_criteria_keep_both_transcripts_without_a_second_draft_call as exercise_criteria
from test_telegram_work_source_delivery import work_delivery as work_delivery
from test_work_attention_pages import work_pages as work_pages

from app.gateway import deps  # noqa: F401 -- Prepare Gateway dependencies before the strict asynchronous lifespan.


@pytest_asyncio.fixture(autouse=True)
async def timezones():
    await asyncio.to_thread(ZoneInfo, "America/New_York")
    await asyncio.to_thread(ZoneInfo, "UTC")


@pytest.mark.asyncio
async def test_voice_assignment_sdk_consent_does_not_block(work_delivery, monkeypatch):
    with detect_blocking_io_strict():
        await exercise_card(work_delivery, monkeypatch)


@pytest.mark.asyncio
async def test_voice_manual_criteria_do_not_block(work_delivery, monkeypatch):
    with detect_blocking_io_strict():
        await exercise_criteria(work_delivery, monkeypatch)
