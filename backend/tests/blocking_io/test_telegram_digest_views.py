"""Digest Show source/receipt authority stays nonblocking through SDK edits."""

import asyncio
from zoneinfo import ZoneInfo

import pytest
import pytest_asyncio
from _telegram_repository_test_support import db as db
from test_telegram_digest_views import test_digest_show_edits_original_card_and_keeps_original_batch_ack as show
from test_telegram_digest_views import test_digest_show_final_provider_rejects_changed_origin_or_presentation as fence
from test_telegram_digest_views import test_pending_digest_show_keeps_consumed_origin_after_callback_sweep as sweep
from test_telegram_work_source_delivery import work_delivery as work_delivery
from test_work_attention_pages import work_pages as work_pages


@pytest_asyncio.fixture(autouse=True)
async def fixture_timezone():
    await asyncio.to_thread(ZoneInfo, "America/New_York")
    await asyncio.to_thread(ZoneInfo, "UTC")


@pytest.mark.asyncio
async def test_digest_show_sdk(work_delivery):
    await show(work_delivery)


@pytest.mark.asyncio
async def test_digest_show_final_receipt(work_delivery, monkeypatch):
    await fence(work_delivery, monkeypatch, "receipt")


@pytest.mark.asyncio
async def test_digest_show_sweep(work_delivery):
    await sweep(work_delivery)
