"""Opaque Work list reads retain nonblocking provider and SQL boundaries."""

import asyncio
from zoneinfo import ZoneInfo

import pytest
import pytest_asyncio
from _telegram_repository_test_support import db as db
from test_telegram_work_list import test_snapshot_fixes_work_membership_but_reads_new_current_question as current_question
from test_telegram_work_list import test_work_page_rechecks_current_assignment_and_inspect as current_work
from test_work_attention_pages import work_pages as work_pages


@pytest_asyncio.fixture(autouse=True)
async def fixture_timezone():
    await asyncio.to_thread(ZoneInfo, "America/New_York")


@pytest.mark.asyncio
async def test_inventory_refresh_keeps_the_loop_clear(work_pages):
    await current_question(work_pages)


@pytest.mark.asyncio
async def test_inventory_withdrawal_keeps_the_loop_clear(work_pages):
    await current_work(work_pages)
