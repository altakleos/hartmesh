"""Startup file receipt recovery owns native and SQL work off the event loop."""

import asyncio
from zoneinfo import ZoneInfo

import pytest
import pytest_asyncio
from _telegram_repository_test_support import db as db
from test_telegram_file_recovery import test_completed_captionless_file_recovers_its_question_without_reapplying as exercise_saved
from test_telegram_file_recovery import test_completed_retry_without_outcome_recovers_after_input_and_control_collection as exercise_retry
from test_telegram_file_recovery import test_startup_phase_ending_inside_question_writer_rolls_back_closure as exercise_phase
from test_telegram_instance_file_admission import file_admission as file_admission
from test_telegram_instance_publication import publication as publication


@pytest_asyncio.fixture(autouse=True)
async def timezone_precondition():
    await asyncio.to_thread(ZoneInfo, "America/New_York")


@pytest.mark.asyncio
async def test_saved_file_recovery_owns_io(file_admission, monkeypatch):
    await exercise_saved(file_admission, monkeypatch)


@pytest.mark.asyncio
async def test_retry_recovery_owns_io(file_admission, monkeypatch):
    await exercise_retry(file_admission, monkeypatch)


@pytest.mark.asyncio
async def test_recovery_writer_keeps_original_phase(file_admission, monkeypatch):
    await exercise_phase(file_admission, monkeypatch)
