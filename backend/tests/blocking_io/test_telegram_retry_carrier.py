"""Missing Retry origins cannot become ordinary fixed messages."""

import pytest
from _config_singleton_guard import restore_config_singletons  # noqa: F401 -- autouse fixture
from _run_output_capture_test_support import capture_environment as capture_environment
from _run_output_capture_test_support import output_store as output_store
from test_telegram_execution_retry_refusals import test_current_execution_retry_preserves_actual_native as execution
from test_telegram_supervisor import _supervisor_logging as _supervisor_logging


@pytest.mark.asyncio
async def test_missing_retry_origin_keeps_source_authority(capture_environment, monkeypatch, tmp_path):
    await execution(capture_environment, monkeypatch, tmp_path, "stop", "inspect", carrier=True)
