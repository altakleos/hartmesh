"""Colleague source reads and source refusal remain nonblocking."""

import asyncio
from zoneinfo import ZoneInfo

import pytest
import pytest_asyncio
from _telegram_repository_test_support import db as db
from test_telegram_work_news_delivery import test_current_colleague_notice_reaches_sdk_without_work_controls as deliver
from test_telegram_work_news_delivery import test_protected_colleague_show_edits_actual_teaser_without_starting_work as reveal
from test_telegram_work_news_recovery import test_missing_news_anchor_repair_keeps_show_proof_and_refuses_live_claim as repair
from test_telegram_work_news_recovery import test_news_refresh_before_send_uses_current_presentation as refresh
from test_telegram_work_news_sources import test_generic_product_reply_cannot_impersonate_colleague_news as refuse
from test_telegram_work_news_sources import test_news_source_uses_colleague_and_canonical_assignment as read
from test_telegram_work_source_delivery import work_delivery as work_delivery
from test_work_attention_pages import work_pages as work_pages


@pytest_asyncio.fixture(autouse=True)
async def fixture_timezone():
    await asyncio.to_thread(ZoneInfo, "America/New_York")
    await asyncio.to_thread(ZoneInfo, "UTC")


@pytest.mark.asyncio
async def test_colleague_source_read(work_delivery):
    await read(work_delivery, "edit")


@pytest.mark.asyncio
async def test_unqualified_news_refused(work_delivery):
    await refuse(work_delivery, "work.changed")


@pytest.mark.asyncio
async def test_colleague_sdk_delivery(work_delivery):
    await deliver(work_delivery, "edit")


@pytest.mark.asyncio
async def test_colleague_protected_show(work_delivery):
    await reveal(work_delivery, "ok")


@pytest.mark.asyncio
async def test_colleague_news_refresh(work_delivery):
    await refresh(work_delivery, "protection")


@pytest.mark.asyncio
async def test_colleague_news_repair(work_delivery):
    await repair(work_delivery, None)
