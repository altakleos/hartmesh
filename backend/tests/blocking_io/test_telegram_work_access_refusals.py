"""Current fixed callback denials keep default composition off the event loop."""

import pytest
from _config_singleton_guard import restore_config_singletons  # noqa: F401 -- autouse fixture
from _run_output_capture_test_support import capture_environment as capture_environment
from _run_output_capture_test_support import output_store as output_store
from test_telegram_supervisor import _supervisor_logging as _supervisor_logging
from test_telegram_work_access_refusals import ACCESS_CASES
from test_telegram_work_access_refusals import test_actual_start_press_explains_lost_inspect as access_press


@pytest.mark.asyncio
@pytest.mark.parametrize("entry,change", ACCESS_CASES)
async def test_callback_access_refusal(capture_environment, monkeypatch, tmp_path, entry, change):
    await access_press(capture_environment, monkeypatch, tmp_path, entry, change)
