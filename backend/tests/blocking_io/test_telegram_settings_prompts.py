"""Custom quiet hours retain the real SQLite/SDK path under strict I/O checks."""

import asyncio
from zoneinfo import ZoneInfo

import pytest
from _telegram_repository_test_support import db as db
from test_telegram_settings_prompts import test_custom_quiet_hours_apply_once_after_prompt_ack as apply_hours
from test_telegram_supervisor import _supervisor_logging as _supervisor_logging


@pytest.mark.asyncio
async def test_custom_quiet_hours(db, monkeypatch):
    await asyncio.to_thread(ZoneInfo, "America/New_York")
    await apply_hours(db, monkeypatch, off_loop=True)
