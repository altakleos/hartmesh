"""Default callback lifecycle refusal keeps Gateway loop ownership."""

import pytest
from _config_singleton_guard import restore_config_singletons  # noqa: F401 -- autouse fixture
from _run_output_capture_test_support import capture_environment as capture_environment
from _run_output_capture_test_support import output_store as output_store
from test_telegram_supervisor import _supervisor_logging as _supervisor_logging
from test_telegram_work_action_refusals import test_actual_start_press_tells_current_lifecycle_without_admission as lifecycle_press


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "status,permissions,key,entry",
    [
        ("suspended", 7, "agent.paused", "work_start"),
        ("archived", 7, "agent.retired", "work_start"),
        ("suspended", 2, "access.agent_off", "work_start"),
        ("owned", 7, None, "work_start"),
        ("suspended", 7, "agent.paused", "assign_save"),
        ("suspended", 7, "agent.paused", "assign_start"),
    ],
)
async def test_lifecycle_press(capture_environment, monkeypatch, tmp_path, status, permissions, key, entry):
    await lifecycle_press(capture_environment, monkeypatch, tmp_path, status, permissions, key, entry)
