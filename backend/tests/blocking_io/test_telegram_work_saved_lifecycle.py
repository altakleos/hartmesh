"""Saved Start refusals use the actual worker, SQL and recorded SDK off-loop."""

import pytest
from _config_singleton_guard import restore_config_singletons  # noqa: F401 -- autouse fixture
from _run_output_capture_test_support import capture_environment as capture_environment
from _run_output_capture_test_support import output_store as output_store
from test_telegram_supervisor import _supervisor_logging as _supervisor_logging
from test_telegram_work_saved_lifecycle import SAVED_CASES
from test_telegram_work_saved_lifecycle import test_saved_start_reports_current_lifecycle as saved_start


@pytest.mark.asyncio
@pytest.mark.parametrize("status,key,change", SAVED_CASES)
async def test_current_saved_start_refusal(capture_environment, monkeypatch, tmp_path, status, key, change):
    await saved_start(capture_environment, monkeypatch, tmp_path, status, key, change)
