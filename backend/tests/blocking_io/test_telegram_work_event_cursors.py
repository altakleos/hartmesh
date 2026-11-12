"""Baseline startup and binding remain entirely on the async SQL boundary."""

import asyncio
from zoneinfo import ZoneInfo

import pytest
import pytest_asyncio
from _telegram_repository_test_support import db as db
from _telegram_work_test_support import work_environment as _work_environment  # noqa: F401 -- Gateway dependency imports precede async fixture startup.
from test_telegram_work_actions import actions as actions
from test_telegram_work_event_cursors import test_initial_baseline_is_atomic_and_restart_keeps_its_revision as restart
from test_telegram_work_event_cursors import test_new_colleague_binding_never_resets_an_existing_cursor as binding


@pytest_asyncio.fixture(autouse=True)
async def fixture_timezone():
    await asyncio.to_thread(ZoneInfo, "America/New_York")
    await asyncio.to_thread(ZoneInfo, "UTC")


@pytest.mark.asyncio
async def test_baseline_restart(actions):
    await restart(actions)


@pytest.mark.asyncio
async def test_baseline_binding_rollback(actions):
    await binding(actions, True)
