"""Notification budget admission keeps cold calendar reads off the event loop."""

import asyncio
from zoneinfo import ZoneInfo

import pytest
from _telegram_repository_test_support import db as db
from test_telegram_notification_policy import test_daily_budget_is_shared_across_bots_and_survives_sender_restart as daily_budget

from app.telegram_bots import notifications


@pytest.mark.asyncio
async def test_calendar_read_during_real_budget_admission(db, monkeypatch, tmp_path):
    await asyncio.to_thread(ZoneInfo, "America/New_York")
    original = notifications.local_now
    calls = []

    def cold_calendar(settings, now):
        tmp_path.stat()
        calls.append(True)
        return original(settings, now)

    monkeypatch.setattr(notifications, "local_now", cold_calendar)
    await daily_budget(db)
    assert calls
