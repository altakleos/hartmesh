"""Saved Work commands and physical receipt reads keep the event loop clear."""

import asyncio
from zoneinfo import ZoneInfo

import pytest
import pytest_asyncio
from _telegram_repository_test_support import db as db
from test_telegram_work_actions import actions as actions
from test_telegram_work_edit_drafts import test_edit_applies_only_its_stored_draft_and_records_one_command as edit
from test_telegram_work_management_actions import test_withdraw_replays_one_exact_command_and_closes_its_request as withdraw
from test_telegram_work_management_retention import test_unadmitted_edit_expires_without_mutating_saved_work as expire

from app.gateway import deps  # noqa: F401 -- Gateway imports dependencies before its async lifespan.


@pytest_asyncio.fixture(autouse=True)
async def fixture_timezone():
    await asyncio.to_thread(ZoneInfo, "America/New_York")


@pytest.mark.asyncio
async def test_saved_edit_keeps_the_loop_clear(actions):
    await edit(actions)


@pytest.mark.asyncio
async def test_withdraw_keeps_the_loop_clear(actions):
    await withdraw(actions)


@pytest.mark.asyncio
async def test_edit_history_keeps_the_loop_clear(actions):
    await expire(actions)
