"""Retained command custody and real SDK settlement remain off the event loop."""

import asyncio
from zoneinfo import ZoneInfo

import pytest
from _config_singleton_guard import restore_config_singletons  # noqa: F401 -- autouse fixture
from _run_output_capture_test_support import capture_environment as capture_environment
from _run_output_capture_test_support import output_store as output_store
from _telegram_repository_test_support import db as db
from test_telegram_inbox_unanswered_replacement import test_sealed_reply_replacement_preserves_delivery_custody as replacement
from test_telegram_retained_management_ownership import MANAGEMENT_OWNERS
from test_telegram_retained_management_ownership import test_retained_management_preserves_real_owners as ownership
from test_telegram_retained_management_refusals import test_management_prompt_dependency_precedes_delivery as dependency
from test_telegram_retained_management_refusals import test_retained_management_reports_current_denial as management
from test_telegram_retained_stop_all import test_stop_all_waits_for_each_actual_refusal_delivery as stop_all
from test_telegram_retained_stop_refusals import STOP_CASES
from test_telegram_retained_stop_refusals import test_saved_stop_reports_current_permission as stop
from test_telegram_supervisor import _supervisor_logging as _supervisor_logging


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ["edit", "cancel"])
@pytest.mark.parametrize("loss", ["manage", "inspect", "paused", "capability"])
@pytest.mark.parametrize("admitted", [True, False])
async def test_retained_management_refusal(capture_environment, monkeypatch, tmp_path, kind, loss, admitted):
    await management(capture_environment, monkeypatch, tmp_path, kind, loss, admitted)


@pytest.mark.asyncio
@pytest.mark.parametrize("entry,loss", STOP_CASES)
async def test_retained_stop_refusal(capture_environment, monkeypatch, tmp_path, entry, loss):
    await stop(capture_environment, monkeypatch, tmp_path, entry, loss)


@pytest.mark.asyncio
@pytest.mark.parametrize("change", MANAGEMENT_OWNERS)
async def test_retained_management_owners(capture_environment, monkeypatch, tmp_path, change):
    await ownership(capture_environment, monkeypatch, tmp_path, change)


@pytest.mark.asyncio
async def test_management_prompt_dependency(capture_environment, monkeypatch, tmp_path):
    await dependency(capture_environment, monkeypatch, tmp_path)


@pytest.mark.asyncio
@pytest.mark.parametrize("change", ["clean", "claim", "partial", "ambiguous", "historical", "reverse", "turn", "topic", "publication", "replacement-claimed", "dispatch-conflict", "ack-null", "dispatch-empty"])
async def test_unanswered_reply_custody(db, change):
    # Repository fixture preparation, as in other strict SQL-only cases.
    await asyncio.to_thread(ZoneInfo, "America/New_York")
    await replacement(db, change)


@pytest.mark.asyncio
async def test_stop_all_refusal_delivery(capture_environment, monkeypatch, tmp_path):
    await stop_all(capture_environment, monkeypatch, tmp_path)
