"""Late current-state changes veto actual receipt-owned composite advice."""

import pytest
from _config_singleton_guard import restore_config_singletons  # noqa: F401 -- autouse fixture
from _run_output_capture_test_support import capture_environment as capture_environment
from _run_output_capture_test_support import output_store as output_store
from test_telegram_prechild_composite_refusals import test_committed_save_explains_unadmitted_start as saved_primary
from test_telegram_supervisor import _supervisor_logging as _supervisor_logging


@pytest.mark.asyncio
@pytest.mark.parametrize("race", ["work-enabled", "manage", "base", "readiness", "readiness-authority"])
async def test_current_change_after_final_settings_read(capture_environment, monkeypatch, tmp_path, race):
    await saved_primary(capture_environment, monkeypatch, tmp_path, "readiness-paused" if race.startswith("readiness") else "work-off", race=race)
