"""Actual child custody and late delivery races retain strict SQL ownership."""

import asyncio
from zoneinfo import ZoneInfo

import pytest
import pytest_asyncio
from _config_singleton_guard import restore_config_singletons  # noqa: F401 -- autouse fixture
from _run_output_capture_test_support import capture_environment as capture_environment
from _run_output_capture_test_support import output_store as output_store
from test_telegram_composite_execution_outcomes import db as db
from test_telegram_composite_execution_outcomes import work_delivery as work_delivery
from test_telegram_composite_execution_outcomes import work_pages as work_pages
from test_telegram_prechild_custody_fences import test_unplanned_refusal_retains_real_child_custody as child_custody
from test_telegram_prechild_delivery_races import test_current_change_after_final_settings_read as late_change
from test_telegram_prechild_primary_ack import test_fixed_advice_requires_actual_primary_ack as primary_ack
from test_telegram_prechild_primary_ack import test_pending_primary_defers_then_releases_advice as pending_primary
from test_telegram_supervisor import _supervisor_logging as _supervisor_logging


@pytest_asyncio.fixture(autouse=True)
async def _prepare_timezone():
    # The real SQL fixture validates this configured zone during its setup.
    await asyncio.to_thread(ZoneInfo, "America/New_York")


@pytest.mark.asyncio
@pytest.mark.parametrize("owner", ["direct", "reverse", "native"])
async def test_retained_child_custody(work_delivery, monkeypatch, owner):
    await child_custody(work_delivery, monkeypatch, owner)


@pytest.mark.asyncio
@pytest.mark.parametrize("race", ["work-enabled", "manage", "base", "readiness", "readiness-authority"])
async def test_late_current_change(capture_environment, monkeypatch, tmp_path, race):
    await late_change(capture_environment, monkeypatch, tmp_path, race)


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ["save", "decision", "voice"])
async def test_actual_primary_ack(capture_environment, monkeypatch, tmp_path, kind):
    await primary_ack(capture_environment, monkeypatch, tmp_path, kind)


@pytest.mark.asyncio
async def test_wait_for_actual_primary(capture_environment, monkeypatch, tmp_path):
    await pending_primary(capture_environment, monkeypatch, tmp_path)
