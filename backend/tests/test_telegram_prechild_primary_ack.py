"""Native primary truth stays separate from actual primary display receipts."""

import pytest
from _config_singleton_guard import restore_config_singletons  # noqa: F401 -- autouse fixture
from _run_output_capture_test_support import capture_environment as capture_environment
from _run_output_capture_test_support import output_store as output_store
from test_telegram_prechild_composite_refusals import test_committed_save_explains_unadmitted_start as saved_primary
from test_telegram_prechild_request_refusals import test_request_primary_survives_unadmitted_start as request_primary
from test_telegram_supervisor import _supervisor_logging as _supervisor_logging


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ["save", "decision", "voice"])
async def test_fixed_advice_requires_actual_primary_ack(capture_environment, monkeypatch, tmp_path, kind):
    if kind == "save":
        await saved_primary(capture_environment, monkeypatch, tmp_path, "inspect", race="primary-ack")
    else:
        await request_primary(capture_environment, monkeypatch, tmp_path, kind, "capability", damage_ack=True)


@pytest.mark.asyncio
async def test_pending_primary_defers_then_releases_advice(capture_environment, monkeypatch, tmp_path):
    await request_primary(capture_environment, monkeypatch, tmp_path, "decision", "capability", pending_primary=True)
