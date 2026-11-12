"""Receipt/source decoding and current SQL authority stay nonblocking."""

import asyncio
from zoneinfo import ZoneInfo

import pytest
import pytest_asyncio
from _telegram_repository_test_support import db as db
from test_telegram_attempt_output import test_final_snapshot_never_sends_an_excerpt_after_source_loss as final_source
from test_telegram_attempt_output import test_no_result_delivers_exact_receipted_output as delivery
from test_telegram_work_source_delivery import work_delivery as work_delivery
from test_work_attention_pages import work_pages as work_pages


@pytest_asyncio.fixture(autouse=True)
async def fixture_timezone():
    await asyncio.to_thread(ZoneInfo, "America/New_York")
    await asyncio.to_thread(ZoneInfo, "UTC")


@pytest.mark.asyncio
async def test_exact_excerpt_delivery(work_delivery):
    await delivery(work_delivery)


@pytest.mark.asyncio
async def test_final_excerpt_source(work_delivery, monkeypatch):
    await final_source(work_delivery, monkeypatch, "source")
