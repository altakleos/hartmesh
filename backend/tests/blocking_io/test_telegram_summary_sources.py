"""Summary source and provider fences perform no blocking I/O."""

import pytest
from _telegram_repository_test_support import db as db
from test_telegram_summary_sources import test_summary_final_provider_read_rejects_changed_complete_selection as fences
from test_telegram_summary_sources import test_summary_source_reads_existing_requests_without_creating_deliveries as reads
from test_telegram_work_source_delivery import work_delivery as work_delivery
from test_work_attention_pages import work_pages as work_pages


@pytest.mark.asyncio
async def test_summary_read(work_delivery):
    await reads(work_delivery)


@pytest.mark.asyncio
async def test_summary_source_fence(work_delivery, monkeypatch):
    await fences(work_delivery, monkeypatch, "title")
