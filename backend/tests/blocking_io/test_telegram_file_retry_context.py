"""Original file context survives receipt collection without event-loop I/O."""

import asyncio
from zoneinfo import ZoneInfo

import pytest
import pytest_asyncio
from _telegram_repository_test_support import db as db
from test_telegram_file_retry_context import test_final_writer_rolls_back_completion_and_pending_context_together as exercise_writer
from test_telegram_file_retry_context import test_retry_after_inbox_collection_attaches_once_without_another_model_turn as exercise_retry
from test_telegram_instance_file_admission import file_admission as file_admission
from test_telegram_instance_publication import publication as publication


@pytest_asyncio.fixture(autouse=True)
async def timezone_precondition():
    await asyncio.to_thread(ZoneInfo, "America/New_York")


@pytest.mark.asyncio
async def test_retry_context_owns_sql_and_native_file_io(file_admission, monkeypatch):
    await exercise_retry(file_admission, monkeypatch)


@pytest.mark.asyncio
async def test_atomic_handoff_rechecks_final_authority(file_admission, monkeypatch):
    await exercise_writer(file_admission, monkeypatch)
