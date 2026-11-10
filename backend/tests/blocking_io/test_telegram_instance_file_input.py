"""Incoming file routing retains owned I/O through attachment publication."""

import asyncio
from zoneinfo import ZoneInfo

import pytest
import pytest_asyncio
from _telegram_repository_test_support import db as db
from test_telegram_instance_file_admission import file_admission as file_admission
from test_telegram_instance_file_input import test_captioned_input_reaches_actual_manager_without_legacy_upload as exercise_publish
from test_telegram_instance_file_input import test_captionless_input_saves_directly_asks_once_and_starts_nothing as exercise_save
from test_telegram_instance_file_input import test_question_writer_rechecks_use_before_closing_the_input as exercise_writer
from test_telegram_instance_publication import publication as publication


@pytest_asyncio.fixture(autouse=True)
async def timezone_precondition():
    await asyncio.to_thread(ZoneInfo, "America/New_York")


@pytest.mark.asyncio
async def test_captionless_save_owns_io_without_execution(file_admission, monkeypatch):
    await exercise_save(file_admission, monkeypatch)


@pytest.mark.asyncio
async def test_saved_receipt_reaches_actual_manager_off_loop(file_admission, monkeypatch, tmp_path):
    await exercise_publish(file_admission, monkeypatch, tmp_path, 4)


@pytest.mark.asyncio
async def test_question_commit_keeps_final_authority(file_admission, monkeypatch):
    await exercise_writer(file_admission, monkeypatch)
