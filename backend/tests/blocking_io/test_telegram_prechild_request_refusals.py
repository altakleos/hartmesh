"""Real default Decision/Voice controls and primary receipts pass strict I/O."""

import pytest
from _config_singleton_guard import restore_config_singletons  # noqa: F401 -- autouse fixture
from _run_output_capture_test_support import capture_environment as capture_environment
from _run_output_capture_test_support import output_store as output_store
from test_telegram_default_composite_controls import test_default_decision_offers_explicit_resume as default_decision
from test_telegram_default_composite_controls import test_default_voice_offers_explicit_resume as default_voice
from test_telegram_prechild_request_refusals import test_request_primary_survives_unadmitted_start as request_primary
from test_telegram_supervisor import _supervisor_logging as _supervisor_logging


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ["decision", "voice"])
@pytest.mark.parametrize("change", ["capability", "inspect", "paused"])
async def test_current_request_start_refusal(capture_environment, monkeypatch, tmp_path, kind, change):
    await request_primary(capture_environment, monkeypatch, tmp_path, kind, change)


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ["decision", "voice"])
async def test_default_combined_control(capture_environment, monkeypatch, tmp_path, kind):
    await (default_decision if kind == "decision" else default_voice)(capture_environment, monkeypatch, tmp_path)
