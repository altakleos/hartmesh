"""Manager event materialization, delivery and Show stay asynchronous."""

import asyncio
from zoneinfo import ZoneInfo

import pytest
import pytest_asyncio
from _telegram_repository_test_support import db as db
from test_telegram_work_routing_event_delivery import test_manager_gets_original_routing_reason_and_current_choices as manager
from test_telegram_work_routing_event_delivery import test_protected_manager_notice_reveals_exact_explanation_and_roster as reveal
from test_telegram_work_source_delivery import work_delivery as work_delivery
from test_work_attention_pages import work_pages as work_pages


@pytest_asyncio.fixture(autouse=True)
async def fixture_timezone():
    await asyncio.to_thread(ZoneInfo, "America/New_York")
    await asyncio.to_thread(ZoneInfo, "UTC")


@pytest.mark.asyncio
async def test_manager_question(work_delivery, monkeypatch):
    await manager(work_delivery, monkeypatch, "cannot_provide", True)


@pytest.mark.asyncio
async def test_manager_show(work_delivery, monkeypatch):
    await reveal(work_delivery, monkeypatch, False, True)
