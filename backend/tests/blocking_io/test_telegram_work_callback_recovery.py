"""Callback recovery uses the same strict I/O boundary as original controls."""

import asyncio
from zoneinfo import ZoneInfo

import pytest
import pytest_asyncio
from _telegram_repository_test_support import db as db
from test_telegram_work_callback_recovery import test_actual_changed_confirmation_shows_complete_current_review as changed
from test_telegram_work_callback_recovery import test_actual_general_routing_refreshes_a_withdrawn_recipient as routing
from test_telegram_work_source_delivery import work_delivery as work_delivery
from test_work_attention_pages import work_pages as work_pages


@pytest_asyncio.fixture(autouse=True)
async def fixture_timezone():
    await asyncio.to_thread(ZoneInfo, "America/New_York")


@pytest.mark.asyncio
async def test_current_review_callback(work_delivery, monkeypatch):
    await changed(work_delivery, monkeypatch, False)


@pytest.mark.asyncio
async def test_routing_choice_recovery(work_delivery, monkeypatch):
    await routing(work_delivery, monkeypatch, True, False)


@pytest.mark.asyncio
async def test_superseded_outcome_final_authority(work_delivery, monkeypatch):
    from test_telegram_work_superseded_outcomes import test_queued_conflict_yields_to_fresh_preview_before_sdk

    await test_queued_conflict_yields_to_fresh_preview_before_sdk(work_delivery, monkeypatch, "final_authority")
