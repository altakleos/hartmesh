"""Saved-work preview, command and retention paths stay asynchronous."""

import asyncio
from zoneinfo import ZoneInfo

import pytest
import pytest_asyncio
from _telegram_repository_test_support import db as db
from test_telegram_work_management_delivery import test_withdraw_first_tap_never_mutates_and_second_tap_owns_one_command as withdraw
from test_telegram_work_management_navigation import test_discard_closes_only_the_private_edit_copy as discard
from test_telegram_work_source_delivery import work_delivery as work_delivery
from test_work_attention_pages import work_pages as work_pages

from app.gateway import deps  # noqa: F401 -- import before async Gateway lifetime.


@pytest_asyncio.fixture(autouse=True)
async def fixture_timezone():
    await asyncio.to_thread(ZoneInfo, "America/New_York")


@pytest.mark.asyncio
async def test_saved_withdraw_ui_keeps_the_loop_clear(work_delivery):
    await withdraw(work_delivery)


@pytest.mark.asyncio
async def test_private_discard_ui_keeps_the_loop_clear(work_delivery):
    await discard(work_delivery)
