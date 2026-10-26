"""Recovery consumer over actual worker/SQL/source/sender under Blockbuster."""

import asyncio
from zoneinfo import ZoneInfo

import pytest
import pytest_asyncio
from _run_output_capture_test_support import capture_environment as capture_environment
from _run_output_capture_test_support import output_store as output_store
from _telegram_repository_test_support import db as db
from test_telegram_publication_recovery_composition import test_actual_startup_closes_and_sends_fixed_notice_without_private_or_model_call as exercise_restart
from test_telegram_publication_recovery_custody import recovery_environment as recovery_environment
from test_telegram_supervisor import _supervisor_logging as _supervisor_logging
from test_telegram_turn_composition import test_actual_managed_turn_creates_once_and_delivers_only_verified_output as exercise_turn


@pytest_asyncio.fixture(autouse=True)
async def timezone_fixture_precondition():
    await asyncio.to_thread(ZoneInfo, "UTC")
    await asyncio.to_thread(ZoneInfo, "America/New_York")


@pytest.mark.asyncio
@pytest.mark.parametrize("case", ["recovery_text", "recovery_custom_text"])
async def test_recreated_consumer_owns_actual_output_and_delivery_io(capture_environment, monkeypatch, tmp_path, case):
    await exercise_turn(capture_environment, monkeypatch, tmp_path, case)


@pytest.mark.asyncio
async def test_startup_fixed_notice_owns_actual_custody_and_delivery_io(recovery_environment, monkeypatch):
    await exercise_restart(recovery_environment, monkeypatch)
