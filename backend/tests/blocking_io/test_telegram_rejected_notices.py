"""Rejected credential notices keep metadata and calendar preparation off-loop."""

import asyncio
from zoneinfo import ZoneInfo

import pytest
from _telegram_repository_test_support import db as db
from test_telegram_rejected_notices import test_rejected_notice_prefers_chat_and_deduplicates_revision as notice
from test_telegram_supervisor import _supervisor_logging as _supervisor_logging


@pytest.mark.asyncio
async def test_rejected_notice(db, monkeypatch):
    await asyncio.to_thread(ZoneInfo, "America/New_York")
    await notice(db, monkeypatch, False, off_loop=True)
