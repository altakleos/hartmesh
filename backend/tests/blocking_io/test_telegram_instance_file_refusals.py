"""File refusal planning owns cold authorization and SQL off the event loop."""

import asyncio
from zoneinfo import ZoneInfo

import pytest
import pytest_asyncio
from _telegram_repository_test_support import db as db
from test_telegram_instance_file_admission import file_admission as file_admission
from test_telegram_instance_file_refusals import test_readiness_outcome_rechecks_authority_in_its_final_writer as exercise_writer
from test_telegram_instance_file_refusals import test_readiness_refusal_closes_entire_input_without_journal_or_download as exercise_refusal
from test_telegram_instance_publication import publication as publication


@pytest_asyncio.fixture(autouse=True)
async def timezone_precondition():
    await asyncio.to_thread(ZoneInfo, "America/New_York")


@pytest.mark.asyncio
async def test_execution_refusal_owns_io(file_admission, monkeypatch):
    await exercise_refusal(file_admission, monkeypatch, "Read the file")


@pytest.mark.asyncio
async def test_refusal_writer_keeps_final_authority(file_admission, monkeypatch):
    await exercise_writer(file_admission, monkeypatch, "grant")
