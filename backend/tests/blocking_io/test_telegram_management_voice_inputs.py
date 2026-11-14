"""Spoken saved-edit ingress and SDK confirmation stay nonblocking."""

import asyncio
from zoneinfo import ZoneInfo

import pytest
import pytest_asyncio
from _telegram_repository_test_support import db as db
from support.detectors.blocking_io_runtime import detect_blocking_io_strict
from test_telegram_management_voice_inputs import test_default_spoken_correction_stays_private_and_needs_fresh_save as exercise_save
from test_telegram_management_voice_inputs import test_oversized_spoken_correction_delivers_heard_and_the_limit as exercise_limit
from test_telegram_management_voice_inputs import test_spoken_correction_keeps_captured_editor_across_provider_wait as exercise_closed
from test_telegram_work_source_delivery import work_delivery as work_delivery
from test_work_attention_pages import work_pages as work_pages

from app.gateway import deps  # noqa: F401 -- Initialize Gateway dependencies before the strict lifespan.


@pytest_asyncio.fixture(autouse=True)
async def timezones():
    await asyncio.to_thread(ZoneInfo, "America/New_York")
    await asyncio.to_thread(ZoneInfo, "UTC")


@pytest.mark.asyncio
async def test_spoken_management_preview_and_save_do_not_block(work_delivery, monkeypatch):
    with detect_blocking_io_strict():
        await exercise_save(work_delivery, monkeypatch, 6180)


@pytest.mark.asyncio
async def test_spoken_management_retirement_does_not_block(work_delivery, monkeypatch):
    with detect_blocking_io_strict():
        await exercise_closed(work_delivery, monkeypatch, "closed")


@pytest.mark.asyncio
async def test_spoken_management_legacy_echo_order_does_not_block(work_delivery, monkeypatch):
    with detect_blocking_io_strict():
        await exercise_limit(work_delivery, monkeypatch, True)
