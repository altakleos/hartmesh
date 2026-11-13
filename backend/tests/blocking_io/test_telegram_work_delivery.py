"""Status publication and authenticated capacity reads keep storage async."""

import asyncio
from zoneinfo import ZoneInfo

import pytest
import pytest_asyncio
from _telegram_repository_test_support import db as db
from test_telegram_work_actions import actions as actions
from test_telegram_work_capacity import test_work_capacity_is_read_over_http_as_the_person as capacity
from test_telegram_work_chat_status import test_chat_status_title_reaches_sdk_without_concealment as status
from test_telegram_work_chat_status import test_failed_seed_rolls_back_done_and_recovers_receipt_without_launch as lost
from test_telegram_work_source_delivery import work_delivery as work_delivery
from test_telegram_work_start import startable as startable
from test_work_attention_pages import work_pages as work_pages

from app.gateway import deps  # noqa: F401 -- Gateway imports dependencies before the async bot lifespan.


@pytest_asyncio.fixture(autouse=True)
async def fixture_timezone():
    await asyncio.to_thread(ZoneInfo, "America/New_York")


@pytest.mark.asyncio
async def test_capacity_route_read_does_not_block(startable):
    await capacity(startable)


@pytest.mark.asyncio
async def test_chat_status_sdk_delivery_does_not_block(work_delivery, monkeypatch):
    await status(work_delivery, monkeypatch, True)


@pytest.mark.asyncio
async def test_status_seed_receipt_recovery_does_not_block(work_delivery, monkeypatch):
    await lost(work_delivery, monkeypatch, ValueError)
