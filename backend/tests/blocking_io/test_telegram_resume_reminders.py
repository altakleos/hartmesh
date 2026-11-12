"""Saved-answer reminder provenance and SDK decisions stay on the async path."""

import asyncio
from zoneinfo import ZoneInfo

import pytest
import pytest_asyncio
from _telegram_repository_test_support import db as db
from test_telegram_resume_reminders import test_protected_reminder_show_stays_silent_even_when_summary_becomes_enabled as protected_show
from test_telegram_resume_reminders import test_reminder_final_settings_fence_observes_summary_enable as settings_drift
from test_telegram_resume_reminders import test_reminder_skips_after_actual_work_transition_at_final_boundary as resumed
from test_telegram_resume_reminders import test_reminder_waits_an_hour_then_sends_silently_with_only_a_web_link as delivery
from test_telegram_work_source_delivery import work_delivery as work_delivery
from test_work_attention_pages import work_pages as work_pages


@pytest_asyncio.fixture(autouse=True)
async def fixture_timezone():
    await asyncio.to_thread(ZoneInfo, "America/New_York")
    await asyncio.to_thread(ZoneInfo, "UTC")


@pytest.mark.asyncio
async def test_reminder_delivery(work_delivery):
    ZoneInfo.clear_cache()
    await delivery(work_delivery, 20)


@pytest.mark.asyncio
async def test_reminder_resumed_at_final_boundary(work_delivery, monkeypatch):
    await resumed(work_delivery, monkeypatch, "resume")


@pytest.mark.asyncio
async def test_reminder_protected_show(work_delivery):
    await protected_show(work_delivery)


@pytest.mark.asyncio
async def test_reminder_settings_drift(work_delivery, monkeypatch):
    await settings_drift(work_delivery, monkeypatch)
