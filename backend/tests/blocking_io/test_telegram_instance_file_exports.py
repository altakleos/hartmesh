"""Native Home exports and cancellation retain their dedicated I/O ownership."""

import asyncio
from zoneinfo import ZoneInfo

import pytest
import pytest_asyncio
from _telegram_repository_test_support import db as db
from test_telegram_instance_file_admission import file_admission as file_admission
from test_telegram_instance_file_exports import exports as exports
from test_telegram_instance_file_exports import test_exports_as_the_human_without_execution_or_workspace as exercise_export
from test_telegram_instance_file_exports import test_repeated_cancellation_drains_native_export as exercise_cancel
from test_telegram_instance_publication import publication as publication


@pytest_asyncio.fixture(autouse=True)
async def timezone_precondition():
    await asyncio.to_thread(ZoneInfo, "America/New_York")


@pytest.mark.asyncio
async def test_export_owns_native_io(exports):
    await exercise_export(exports)


@pytest.mark.asyncio
async def test_cancelled_export_drains_native_io(exports, monkeypatch):
    await exercise_cancel(exports, monkeypatch)
