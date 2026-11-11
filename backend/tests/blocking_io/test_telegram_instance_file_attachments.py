"""Receipt projections and pending attachment writers retain their I/O owners."""

import asyncio
from zoneinfo import ZoneInfo

import pytest
import pytest_asyncio
from _telegram_repository_test_support import db as db
from test_telegram_instance_file_admission import file_admission as file_admission
from test_telegram_instance_file_attachments import test_borrowed_pending_writer_uses_prepared_reader_and_rolls_back as exercise_borrowed
from test_telegram_instance_file_attachments import test_composer_projects_completed_receipt_without_download_or_write as exercise_composer
from test_telegram_instance_file_attachments import test_pending_instance_file_keeps_actual_path_and_attaches_without_another_write as exercise_pending
from test_telegram_instance_publication import publication as publication


@pytest_asyncio.fixture(autouse=True)
async def timezone_precondition():
    await asyncio.to_thread(ZoneInfo, "America/New_York")


@pytest.mark.asyncio
async def test_composer_receipt_owns_io(file_admission, monkeypatch):
    await exercise_composer(file_admission, monkeypatch)


@pytest.mark.asyncio
async def test_pending_receipt_handoff_owns_io(file_admission, monkeypatch):
    await exercise_pending(file_admission, monkeypatch)


@pytest.mark.asyncio
async def test_borrowed_writer_keeps_reader_preparation_outside(file_admission, monkeypatch):
    await exercise_borrowed(file_admission, monkeypatch)
