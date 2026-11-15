"""Foreign and own Stop denials preserve real native reservations off-loop."""

import pytest
from _config_singleton_guard import restore_config_singletons  # noqa: F401 -- autouse fixture
from _run_output_capture_test_support import capture_environment as capture_environment
from _run_output_capture_test_support import output_store as output_store
from test_telegram_stop_refusal_ownership import STOP_CASES
from test_telegram_stop_refusal_ownership import test_stop_reports_foreign_requester_after_manage_loss as stop_denial
from test_telegram_supervisor import _supervisor_logging as _supervisor_logging


@pytest.mark.asyncio
@pytest.mark.parametrize("change", STOP_CASES)
async def test_stop_permission_denial(capture_environment, monkeypatch, tmp_path, change):
    await stop_denial(capture_environment, monkeypatch, tmp_path, change)
