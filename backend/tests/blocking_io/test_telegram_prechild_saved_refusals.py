"""Default Save/current Start advice stays off the event loop."""

import pytest
from _config_singleton_guard import restore_config_singletons  # noqa: F401 -- autouse fixture
from _run_output_capture_test_support import capture_environment as capture_environment
from _run_output_capture_test_support import output_store as output_store
from test_telegram_prechild_composite_refusals import SAVE_CHANGES
from test_telegram_prechild_composite_refusals import test_committed_save_explains_unadmitted_start as saved_primary
from test_telegram_supervisor import _supervisor_logging as _supervisor_logging


@pytest.mark.asyncio
@pytest.mark.parametrize("change", SAVE_CHANGES)
async def test_current_save_start_refusal(capture_environment, monkeypatch, tmp_path, change):
    await saved_primary(capture_environment, monkeypatch, tmp_path, change)
