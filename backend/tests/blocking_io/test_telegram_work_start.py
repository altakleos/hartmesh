"""Explicit activation journal recovery retains asynchronous storage I/O."""

import asyncio
from zoneinfo import ZoneInfo

import pytest
import pytest_asyncio
from _telegram_repository_test_support import db as db
from test_telegram_work_actions import actions as actions
from test_telegram_work_start import startable as startable
from test_telegram_work_start import test_cancel_unknown_start_never_launches_a_runless_reservation as cancel
from test_telegram_work_start import test_lost_activation_response_recovers_by_read_only_exact_receipt as lost
from test_telegram_work_start import test_runless_reservation_retries_same_body_and_conversation as runless

from app.gateway import deps  # noqa: F401 -- Gateway dependencies load before the async bot lifespan.


@pytest_asyncio.fixture(autouse=True)
async def fixture_timezone():
    await asyncio.to_thread(ZoneInfo, "America/New_York")


@pytest.mark.asyncio
async def test_activation_response_loss_does_not_block(startable, monkeypatch):
    await lost(startable, monkeypatch)


@pytest.mark.asyncio
async def test_runless_activation_recovery_does_not_block(startable, monkeypatch):
    await runless(startable, monkeypatch)


@pytest.mark.asyncio
async def test_cancelled_activation_observation_does_not_block(startable, monkeypatch):
    await cancel(startable, monkeypatch)
