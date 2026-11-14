"""Composite service ownership and actual SDK consent keep blocking work off-loop."""

import asyncio
from zoneinfo import ZoneInfo

import pytest
import pytest_asyncio
from _telegram_repository_test_support import db as db
from support.detectors.blocking_io_runtime import detect_blocking_io_strict
from test_telegram_work_actions import actions as actions
from test_telegram_work_composite_delivery import test_confirmed_start_keeps_actual_original_card_custody as exercise_card
from test_telegram_work_continuations import test_cancel_during_child_admission_preserves_uncertainty_until_return as exercise_cancel
from test_telegram_work_continuations import test_ready_delegate_recovers_one_canonical_activation_child as exercise_child
from test_telegram_work_source_delivery import work_delivery as work_delivery
from test_telegram_work_start import startable as startable
from test_work_attention_pages import work_pages as work_pages

from app.gateway import deps  # noqa: F401 -- Gateway dependencies load before the asynchronous bot lifespan.


@pytest_asyncio.fixture(autouse=True)
async def timezones():
    await asyncio.to_thread(ZoneInfo, "America/New_York")
    await asyncio.to_thread(ZoneInfo, "UTC")


@pytest.mark.asyncio
async def test_canonical_composite_child_does_not_block(startable, monkeypatch):
    with detect_blocking_io_strict():
        await exercise_child(startable, monkeypatch)


@pytest.mark.asyncio
async def test_actual_sdk_composite_consent_does_not_block(work_delivery, monkeypatch):
    with detect_blocking_io_strict():
        await exercise_card(work_delivery, monkeypatch, None)


@pytest.mark.asyncio
async def test_inflight_composite_cancel_does_not_block(startable, monkeypatch):
    with detect_blocking_io_strict():
        await exercise_cancel(startable, monkeypatch)
