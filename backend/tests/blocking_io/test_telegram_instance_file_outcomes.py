"""File outcome planning owns SQL and provider/config reads off-loop."""

import asyncio
from zoneinfo import ZoneInfo

import pytest
import pytest_asyncio
from _telegram_repository_test_support import db as db
from test_telegram_instance_file_admission import file_admission as file_admission
from test_telegram_instance_file_outcomes import test_final_outcome_writer_rolls_back_reply_and_retry_when_use_changes as exercise_writer
from test_telegram_instance_file_outcomes import test_full_files_reply_uses_current_manage_permission as exercise_full
from test_telegram_instance_file_outcomes import test_journal_refusal_selects_fixed_notice_and_exact_retry_target as exercise_retry
from test_telegram_instance_publication import publication as publication


@pytest_asyncio.fixture(autouse=True)
async def timezone_precondition():
    await asyncio.to_thread(ZoneInfo, "America/New_York")


@pytest.mark.asyncio
async def test_full_files_reader_values_own_io(file_admission, monkeypatch):
    await exercise_full(file_admission, monkeypatch)


@pytest.mark.asyncio
async def test_file_retry_plan_owns_its_writer(file_admission, monkeypatch):
    await exercise_retry(file_admission, monkeypatch, "name-collision", "file.not_saved", True)


@pytest.mark.asyncio
async def test_outcome_final_writer_keeps_receipt_guard(file_admission, monkeypatch):
    await exercise_writer(file_admission, monkeypatch)
