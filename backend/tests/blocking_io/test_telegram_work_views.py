"""Work source publication and intent admission keep provider/SQL work async."""

import asyncio
from zoneinfo import ZoneInfo

import pytest
import pytest_asyncio
from _telegram_repository_test_support import db as db
from test_telegram_work_source_delivery import work_delivery as work_delivery
from test_telegram_work_start_controls import test_start_retired_during_conversation_preparation_rolls_back_journal as retired_start
from test_telegram_work_start_controls import test_unresolved_start_retains_original_control_and_full_ack as retain_start
from test_telegram_work_view_delivery import test_command_list_and_reusable_details_have_durable_distinct_gestures as explicit_views
from test_work_attention_pages import work_pages as work_pages


@pytest_asyncio.fixture(autouse=True)
async def fixture_timezone():
    await asyncio.to_thread(ZoneInfo, "America/New_York")


@pytest.mark.asyncio
async def test_full_work_views_keep_the_loop_clear(work_delivery):
    await explicit_views(work_delivery)


@pytest.mark.asyncio
async def test_late_start_retirement_keeps_the_loop_clear(work_delivery, monkeypatch):
    await retired_start(work_delivery, monkeypatch)


@pytest.mark.asyncio
async def test_unknown_start_retention_keeps_the_loop_clear(work_delivery, monkeypatch):
    await retain_start(work_delivery, monkeypatch, journal="missing")
