"""A refetched Retry cannot settle another actual command's known receipt."""

import pytest
from _config_singleton_guard import restore_config_singletons  # noqa: F401 -- autouse fixture
from _run_output_capture_test_support import capture_environment as capture_environment
from _run_output_capture_test_support import output_store as output_store
from test_telegram_management_retry_refusals import test_current_management_retry_denial_preserves_unknown_owner as management
from test_telegram_supervisor import _supervisor_logging as _supervisor_logging


@pytest.mark.asyncio
async def test_known_receipt_keeps_its_original_card(capture_environment, monkeypatch, tmp_path):
    await management(capture_environment, monkeypatch, tmp_path, "cancel", invalid="retarget")
