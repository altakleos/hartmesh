"""Digest SDK dispatch and final compound authority keep blocking I/O off-loop."""

import asyncio
from zoneinfo import ZoneInfo

import pytest
import pytest_asyncio
from _telegram_repository_test_support import db as db
from test_telegram_digest_delivery import test_ack_cannot_settle_replaced_batch_manifest as conflict
from test_telegram_digest_delivery import test_digest_final_provider_read_refuses_any_changed_member_or_authority as fence
from test_telegram_digest_delivery import test_digest_sends_one_qualified_navigation_and_acks_members_together as send
from test_telegram_work_source_delivery import work_delivery as work_delivery
from test_work_attention_pages import work_pages as work_pages


@pytest_asyncio.fixture(autouse=True)
async def fixture_timezone():
    await asyncio.to_thread(ZoneInfo, "America/New_York")
    await asyncio.to_thread(ZoneInfo, "UTC")


@pytest.mark.asyncio
async def test_digest_sdk(work_delivery):
    ZoneInfo.clear_cache()
    await send(work_delivery, False)


@pytest.mark.asyncio
async def test_digest_final_claim(work_delivery, monkeypatch):
    await fence(work_delivery, monkeypatch, "member_claim")


@pytest.mark.asyncio
async def test_digest_conflicting_ack(work_delivery):
    await conflict(work_delivery)
