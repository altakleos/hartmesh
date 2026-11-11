"""Direct Work-card input capture and admission stay off blocking I/O."""

import asyncio
from zoneinfo import ZoneInfo

import pytest
import pytest_asyncio
from _telegram_repository_test_support import db as db
from test_telegram_work_card_inputs import test_card_input_final_writer_rejects_late_drift as drift
from test_telegram_work_card_inputs import test_card_reply_captures_before_admission_and_saves_one_response as reply
from test_telegram_work_source_delivery import work_delivery as work_delivery
from test_work_attention_pages import work_pages as work_pages


@pytest_asyncio.fixture(autouse=True)
async def fixture_timezone():
    await asyncio.to_thread(ZoneInfo, "America/New_York")


@pytest.mark.asyncio
async def test_review_comment(work_delivery):
    await reply(work_delivery, "review")


@pytest.mark.asyncio
async def test_coherent_input_drift(work_delivery, monkeypatch):
    await drift(work_delivery, monkeypatch, "coherent_input")
