"""Family recovery and digest ingress stay nonblocking at authority boundaries."""

import asyncio
from zoneinfo import ZoneInfo

import pytest
import pytest_asyncio
from _telegram_repository_test_support import db as db
from test_telegram_digest_family_recovery import test_family_refresh_rechecks_origin_after_replacement as fence
from test_telegram_digest_family_recovery import test_preview_refresh_preserves_already_acknowledged_media as media
from test_telegram_digest_item_ingress import test_digest_item_ingress_creates_one_preview_per_query_without_work_action as ingress
from test_telegram_work_file_exports import work_exports as work_exports
from test_telegram_work_source_delivery import work_delivery as work_delivery
from test_work_attention_pages import work_pages as work_pages


@pytest_asyncio.fixture(autouse=True)
async def fixture_timezone():
    await asyncio.to_thread(ZoneInfo, "America/New_York")
    await asyncio.to_thread(ZoneInfo, "UTC")


@pytest.mark.asyncio
@pytest.mark.parametrize("work_exports", ["review_event"], indirect=True)
async def test_review_media_recovery(work_exports, monkeypatch):
    await media(work_exports, monkeypatch, "review", False)


@pytest.mark.asyncio
@pytest.mark.parametrize("work_exports", ["done_event"], indirect=True)
async def test_notice_recovery_fence(work_exports, monkeypatch):
    await fence(work_exports, monkeypatch, "news")


@pytest.mark.asyncio
async def test_digest_ingress(work_delivery):
    await ingress(work_delivery, True)
