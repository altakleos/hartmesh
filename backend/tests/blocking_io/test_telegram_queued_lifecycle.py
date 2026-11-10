"""Unavailable input closure retains cold authority and writer I/O ownership."""

import asyncio
from zoneinfo import ZoneInfo

import pytest
import pytest_asyncio
from _telegram_repository_test_support import db as db
from test_telegram_instance_admission import admission as admission
from test_telegram_instance_publication import publication as publication
from test_telegram_queued_instance_lifecycle import test_callback_owned_retry_closes_only_without_its_own_publication as exercise_callback
from test_telegram_queued_instance_lifecycle import test_closure_owns_final_writer_and_rechecks_original_lifetime as exercise_cancel
from test_telegram_queued_instance_lifecycle import test_queued_input_closes_before_storage_or_execution as exercise_close


@pytest_asyncio.fixture(autouse=True)
async def timezone_precondition():
    await asyncio.to_thread(ZoneInfo, "America/New_York")


@pytest.mark.asyncio
async def test_unavailable_closure_owns_io(admission, monkeypatch):
    await exercise_close(admission, monkeypatch, "use", "access.agent_off", "agent_unavailable")


@pytest.mark.asyncio
async def test_callback_closure_owns_io(admission):
    await exercise_callback(admission, False)


@pytest.mark.asyncio
async def test_cancelled_closure_owns_io(admission):
    await exercise_cancel(admission, "cancel")


@pytest.mark.asyncio
async def test_claimed_wait_retirement_owns_io(admission):
    from test_telegram_queued_instance_lifecycle import test_closed_input_retires_wait_delivery_before_raw_send_after_restore

    await test_closed_input_retires_wait_delivery_before_raw_send_after_restore(admission, True)
