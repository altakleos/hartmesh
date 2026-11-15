"""Default Work owners and the actual Gateway worker retain strict I/O ownership."""

import pytest
from _config_singleton_guard import restore_config_singletons  # noqa: F401 -- autouse fixture
from _run_output_capture_test_support import capture_environment as capture_environment
from _run_output_capture_test_support import output_store as output_store
from test_telegram_supervisor import _supervisor_logging as _supervisor_logging
from test_telegram_work_worker import test_default_supervisor_joins_actual_proposal_consent_worker_report_and_sdk as exercise


@pytest.mark.asyncio
async def test_default_work_owners_worker_and_sdk_keep_io_owned(capture_environment, monkeypatch, tmp_path):
    await exercise(capture_environment, monkeypatch, tmp_path)
