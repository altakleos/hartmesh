"""Retry controls retain SDK, journal and byte ownership off the event loop."""

import asyncio
from zoneinfo import ZoneInfo

import pytest
import pytest_asyncio
from _telegram_repository_test_support import db as db
from test_telegram_instance_file_admission import file_admission as file_admission
from test_telegram_instance_file_retry_delivery import test_actual_control_retries_after_original_route_closed_without_model_or_reattachment as exercise_retry
from test_telegram_instance_file_retry_delivery import test_lost_retry_confirmation_plans_uncertainty_without_reapplying as exercise_lost
from test_telegram_instance_publication import publication as publication


@pytest_asyncio.fixture(autouse=True)
async def timezone_precondition():
    await asyncio.to_thread(ZoneInfo, "America/New_York")


@pytest.mark.asyncio
async def test_collected_source_retry_owns_its_io(file_admission, monkeypatch):
    await exercise_retry(file_admission, monkeypatch, True)


@pytest.mark.asyncio
async def test_unknown_confirmation_recovery_is_metadata_only(file_admission, monkeypatch):
    await exercise_lost(file_admission, monkeypatch)
