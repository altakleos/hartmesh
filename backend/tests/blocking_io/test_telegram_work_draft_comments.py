"""A confirmation-card comment and retained-draft refresh stay asynchronous."""

import asyncio
from zoneinfo import ZoneInfo

import pytest
import pytest_asyncio
from _telegram_repository_test_support import db as db
from test_telegram_work_draft_comments import test_reply_to_confirmation_saves_comment_and_keeps_unsubmitted_draft as reply
from test_telegram_work_source_delivery import work_delivery as work_delivery
from test_work_attention_pages import work_pages as work_pages


@pytest_asyncio.fixture(autouse=True)
async def fixture_timezone():
    await asyncio.to_thread(ZoneInfo, "America/New_York")


@pytest.mark.asyncio
async def test_review_comment_preserves_confirmation(work_delivery, monkeypatch):
    await reply(work_delivery, monkeypatch, "review")
