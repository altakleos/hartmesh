"""Actual child/cancellation receipts remain observation-only while inactive."""

import pytest
from _config_singleton_guard import restore_config_singletons  # noqa: F401 -- autouse fixture
from _run_output_capture_test_support import capture_environment as capture_environment
from _run_output_capture_test_support import output_store as output_store
from test_telegram_retry_lifecycle import test_inactive_save_recovers_its_actual_no_due_child as no_due
from test_telegram_retry_lifecycle import test_inactive_stop_receipt_never_dispatches as stopped
from test_telegram_supervisor import _supervisor_logging as _supervisor_logging


@pytest.mark.asyncio
async def test_actual_no_due_receipt(capture_environment, monkeypatch, tmp_path):
    await no_due(capture_environment, monkeypatch, tmp_path)


@pytest.mark.asyncio
async def test_actual_stop_receipt(capture_environment, monkeypatch, tmp_path):
    await stopped(capture_environment, monkeypatch, tmp_path)
