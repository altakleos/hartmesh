"""Own-action delivery, cancel and explicit recovery remain asynchronous."""

import asyncio
from zoneinfo import ZoneInfo

import pytest
import pytest_asyncio
from _telegram_repository_test_support import db as db
from test_telegram_work_execution_outcomes import test_cancel_wait_edits_its_reply_and_never_launches as cancel_reply
from test_telegram_work_execution_outcomes import test_refused_runless_original_can_be_recovered_only_by_explicit_same_action as original_retry
from test_telegram_work_source_delivery import work_delivery as work_delivery
from test_work_attention_pages import work_pages as work_pages


@pytest_asyncio.fixture(autouse=True)
async def fixture_timezone():
    await asyncio.to_thread(ZoneInfo, "America/New_York")


@pytest.mark.asyncio
async def test_execution_reply_cancel_keeps_the_loop_clear(work_delivery, monkeypatch):
    await cancel_reply(work_delivery, monkeypatch)


@pytest.mark.asyncio
async def test_original_runless_retry_keeps_the_loop_clear(work_delivery, monkeypatch):
    await original_retry(work_delivery, monkeypatch)
