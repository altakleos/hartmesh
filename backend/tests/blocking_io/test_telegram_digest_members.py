"""Digest member authority remains off the event loop."""

import asyncio
from zoneinfo import ZoneInfo

import pytest
import pytest_asyncio
from _telegram_repository_test_support import db as db
from test_telegram_digest_members import test_digest_manager_question_qualifies_routing_roster as routing
from test_telegram_digest_members import test_question_digest_source_is_rechecked_after_provider as question
from test_telegram_work_source_delivery import work_delivery as work_delivery
from test_work_attention_pages import work_pages as work_pages


@pytest_asyncio.fixture(autouse=True)
async def fixture_timezone():
    await asyncio.to_thread(ZoneInfo, "America/New_York")
    await asyncio.to_thread(ZoneInfo, "UTC")


@pytest.mark.asyncio
async def test_digest_question(work_delivery, monkeypatch):
    await question(work_delivery, monkeypatch, "event")


@pytest.mark.asyncio
async def test_digest_routing(work_delivery, monkeypatch):
    await routing(work_delivery, monkeypatch)
