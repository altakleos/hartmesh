"""Actual host/SDK/output/source/Telegram path under whole-item I/O checks."""

import asyncio
from types import SimpleNamespace
from zoneinfo import ZoneInfo

import pytest
import pytest_asyncio
from _run_output_capture_test_support import capture_environment as capture_environment
from _run_output_capture_test_support import output_store as output_store
from _telegram_ingress_test_support import install_live_owner
from _telegram_repository_test_support import db as db
from test_chat_output_view import exercise_unsupported_provider
from test_telegram_supervisor import _supervisor_logging as _supervisor_logging
from test_telegram_turn_composition import test_actual_managed_turn_creates_once_and_delivers_only_verified_output as exercise_turn


@pytest_asyncio.fixture(autouse=True)
async def timezone_fixture_precondition():
    await asyncio.to_thread(ZoneInfo, "UTC")
    await asyncio.to_thread(ZoneInfo, "America/New_York")


@pytest.mark.asyncio
@pytest.mark.parametrize("case", ["text", "produced_file", "no_user_text", "no_user_custom_text", "no_user_uncertain"])
async def test_output_view_and_primary_delivery_keep_io_owned(capture_environment, monkeypatch, tmp_path, case):
    await exercise_turn(capture_environment, monkeypatch, tmp_path, case)


@pytest.mark.asyncio
async def test_unsupported_provider_refuses_without_cold_native_imports(db, monkeypatch):
    live = await install_live_owner(db, monkeypatch)
    user = await live.users.get_user_by_id(live.id)
    app = SimpleNamespace(state=SimpleNamespace(storage_spaces_enabled=True, storage_spaces=None, agent_execution=SimpleNamespace(storage_reason="preparing")))
    await exercise_unsupported_provider(app, user, monkeypatch)
