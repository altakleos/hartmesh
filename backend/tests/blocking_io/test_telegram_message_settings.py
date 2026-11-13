"""Chat preference updates keep source reads and SQL off the event loop."""

import asyncio
from zoneinfo import ZoneInfo

import pytest
from _telegram_repository_test_support import db as db
from test_telegram_message_settings_commands import test_message_settings_navigation_and_choice_use_one_atomic_receipt as choice
from test_telegram_supervisor import _supervisor_logging as _supervisor_logging


@pytest.mark.asyncio
async def test_message_settings_choice(db, monkeypatch):
    await asyncio.to_thread(ZoneInfo, "America/New_York")
    await choice(db, monkeypatch, "summary", "07:00", {"summary_enabled": True, "summary_minute": 420}, True, off_loop=True)
