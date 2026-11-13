"""Private previews, receipt recovery and evidence retention keep I/O async."""

import asyncio
from zoneinfo import ZoneInfo

import pytest
import pytest_asyncio
from _telegram_repository_test_support import db as db
from test_telegram_assignment_card_delivery import test_draft_preview_is_a_durable_reply_and_creates_no_work as preview
from test_telegram_assignment_card_recovery import test_observer_recovers_known_no_due_child_and_parent_atomically as recover
from test_telegram_assignment_retention import test_uncertain_save_keeps_original_input_full_ack_and_journals as retain
from test_telegram_work_source_delivery import work_delivery as work_delivery
from test_work_attention_pages import work_pages as work_pages


@pytest_asyncio.fixture(autouse=True)
async def fixture_timezone():
    await asyncio.to_thread(ZoneInfo, "America/New_York")


@pytest.mark.asyncio
async def test_assignment_preview_keeps_the_loop_clear(work_delivery):
    await preview(work_delivery)


@pytest.mark.asyncio
async def test_known_child_receipt_recovery_keeps_the_loop_clear(work_delivery, monkeypatch):
    await recover(work_delivery, monkeypatch)


@pytest.mark.asyncio
async def test_assignment_retention_keeps_the_loop_clear(work_delivery):
    await retain(work_delivery)
