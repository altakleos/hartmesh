"""Actual typed/voice composite SDK and receipt delivery paths stay nonblocking."""

import asyncio
from zoneinfo import ZoneInfo

import pytest
import pytest_asyncio
from _telegram_repository_test_support import db as db
from support.detectors.blocking_io_runtime import detect_blocking_io_strict
from test_telegram_request_execution_outcomes import test_request_child_unknown_notice_uses_original_full_confirmation as exercise_notice
from test_telegram_typed_continuations import test_actual_typed_decision_retains_input_until_its_combined_child_finishes as exercise_typed
from test_telegram_voice_continuations import test_primary_sdk_ack_keeps_voice_child_custody_through_restart as exercise_voice_ack
from test_telegram_work_source_delivery import work_delivery as work_delivery
from test_work_attention_pages import work_pages as work_pages

from app.gateway import deps  # noqa: F401 -- Warm Gateway dependencies before the strict asynchronous lifespan.


@pytest_asyncio.fixture(autouse=True)
async def timezones():
    await asyncio.to_thread(ZoneInfo, "America/New_York")
    await asyncio.to_thread(ZoneInfo, "UTC")


@pytest.mark.asyncio
async def test_typed_request_confirmation_does_not_block(work_delivery, monkeypatch):
    with detect_blocking_io_strict():
        await exercise_typed(work_delivery, monkeypatch)


@pytest.mark.asyncio
async def test_voice_primary_ack_and_restart_do_not_block(work_delivery, monkeypatch):
    with detect_blocking_io_strict():
        await exercise_voice_ack(work_delivery, monkeypatch)


@pytest.mark.asyncio
async def test_voice_child_notice_does_not_block(work_delivery, monkeypatch):
    with detect_blocking_io_strict():
        await exercise_notice(work_delivery, monkeypatch, "voice")
