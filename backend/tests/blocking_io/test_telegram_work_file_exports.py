"""Work outcome exports preserve asynchronous service and native I/O boundaries."""

import asyncio
from zoneinfo import ZoneInfo

import pytest
import pytest_asyncio
from _telegram_repository_test_support import db as db
from test_telegram_work_file_exports import test_canonical_result_file_exports_as_recipient_without_workspace as export
from test_telegram_work_file_exports import test_real_delivery_claim_shares_the_final_file_snapshot as claim
from test_telegram_work_file_exports import work_exports as work_exports
from test_telegram_work_source_delivery import work_delivery as work_delivery
from test_work_attention_pages import work_pages as work_pages


@pytest_asyncio.fixture(autouse=True)
async def fixture_timezone():
    await asyncio.to_thread(ZoneInfo, "America/New_York")
    await asyncio.to_thread(ZoneInfo, "UTC")


@pytest.mark.asyncio
async def test_work_result_export(work_exports):
    await export(work_exports)


@pytest.mark.asyncio
async def test_work_result_claim(work_exports, monkeypatch):
    await claim(work_exports, monkeypatch)
