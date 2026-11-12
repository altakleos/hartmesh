"""Actual voice completion, consent delivery and Respond avoid blocking I/O."""

import asyncio
from zoneinfo import ZoneInfo

import pytest
import pytest_asyncio
from _telegram_repository_test_support import db as db
from test_telegram_work_source_delivery import work_delivery as work_delivery
from test_telegram_work_voice_inputs import test_voice_answer_waits_for_actual_save_and_replays_without_resubmission as save_voice
from test_work_attention_pages import work_pages as work_pages


@pytest_asyncio.fixture(autouse=True)
async def fixture_timezone():
    await asyncio.to_thread(ZoneInfo, "America/New_York")


@pytest.mark.asyncio
async def test_voice_answer_requires_explicit_save(work_delivery, monkeypatch):
    await save_voice(work_delivery, monkeypatch)
