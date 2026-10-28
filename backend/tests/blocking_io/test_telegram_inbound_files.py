"""Actual capped download, native upload and worker input under strict I/O."""

import asyncio
from zoneinfo import ZoneInfo

import pytest
import pytest_asyncio
from _run_output_capture_test_support import capture_environment as capture_environment
from _run_output_capture_test_support import output_store as output_store
from test_telegram_supervisor import _supervisor_logging as _supervisor_logging
from test_telegram_turn_composition import test_actual_managed_turn_creates_once_and_delivers_only_verified_output as exercise_turn


@pytest_asyncio.fixture(autouse=True)
async def timezone_fixture_precondition():
    await asyncio.to_thread(ZoneInfo, "UTC")
    await asyncio.to_thread(ZoneInfo, "America/New_York")


@pytest.mark.asyncio
async def test_managed_file_download_write_and_worker_input_own_their_io(capture_environment, monkeypatch, tmp_path):
    await exercise_turn(capture_environment, monkeypatch, tmp_path, "text", managed_ingress=True, inbound_file=True)
