"""Work history and delivery share async SQL through rollback and cancellation."""

import asyncio
from zoneinfo import ZoneInfo

import pytest
import pytest_asyncio
from _telegram_repository_test_support import db as db
from _telegram_work_test_support import work_environment as _work_environment  # noqa: F401 -- Prepare Gateway imports before the async fixture.
from test_telegram_work_actions import actions as actions
from test_telegram_work_event_reader import test_cancellation_drains_admitted_projection_and_commit as cancellation
from test_telegram_work_event_reader import test_delivery_and_cursor_roll_back_together_then_recover as rollback


@pytest_asyncio.fixture(autouse=True)
async def fixture_timezone():
    await asyncio.to_thread(ZoneInfo, "America/New_York")
    await asyncio.to_thread(ZoneInfo, "UTC")


@pytest.mark.asyncio
async def test_reader_rollback(actions):
    await rollback(actions)


@pytest.mark.asyncio
async def test_reader_cancellation(actions):
    await cancellation(actions)
