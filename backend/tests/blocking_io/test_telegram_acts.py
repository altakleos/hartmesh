"""Actual settings transactions with cold timezone validation under strict I/O."""

import asyncio
from zoneinfo import ZoneInfo

import pytest
from _telegram_repository_test_support import db as db


@pytest.mark.asyncio
async def test_cold_locale_update_keeps_timezone_validation_off_loop(db):
    await asyncio.to_thread(ZoneInfo, "America/New_York")
    await db.settings.ensure("strict-settings-owner", timezone="America/New_York", locale="en-US")
    await asyncio.to_thread(ZoneInfo.clear_cache)
    result = await db.settings.update("strict-settings-owner", locale="zh-CN")
    assert result.locale == "zh-CN" and result.timezone == "America/New_York"
