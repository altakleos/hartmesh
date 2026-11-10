"""File journal, callbacks and owned native buffers obey the loop's I/O contract."""

import asyncio
from zoneinfo import ZoneInfo

import pytest
import pytest_asyncio
from _telegram_repository_test_support import db as db
from test_telegram_instance_file_admission import file_admission as file_admission
from test_telegram_instance_file_puts import test_explicit_press_gets_child_id_and_replay_never_downloads_again as exercise_retry
from test_telegram_instance_file_puts import test_file_journal_precedes_download_and_receipt_replays_without_bytes as exercise_put
from test_telegram_instance_file_puts import test_repeated_cancellation_drains_native_write_and_lease as exercise_cancel
from test_telegram_instance_file_puts import test_settled_file_metadata_expires_but_uncertain_or_malformed_journal_stays as exercise_sweep
from test_telegram_instance_publication import publication as publication


@pytest_asyncio.fixture(autouse=True)
async def timezone_precondition():
    await asyncio.to_thread(ZoneInfo, "America/New_York")


@pytest.mark.asyncio
async def test_file_journal_and_actual_home_write(file_admission, monkeypatch):
    await exercise_put(file_admission, monkeypatch)


@pytest.mark.asyncio
async def test_explicit_retry_and_receipt_replay(file_admission, monkeypatch):
    await exercise_retry(file_admission, monkeypatch)


@pytest.mark.asyncio
async def test_cancelled_journal_write_retains_buffer(file_admission, monkeypatch):
    await exercise_cancel(file_admission, monkeypatch)


@pytest.mark.asyncio
async def test_settled_journal_sweep_owns_io(file_admission, monkeypatch):
    await exercise_sweep(file_admission, monkeypatch, "done", False, True, True)
