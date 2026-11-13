"""Protected summary Show keeps source and callback admission nonblocking."""

import pytest
from _telegram_repository_test_support import db as db
from test_telegram_summary_views import test_summary_show_edits_protected_teaser_with_full_current_navigation as show
from test_telegram_summary_views import test_summary_show_final_provider_snapshot_prevents_stale_edit as fences
from test_telegram_work_source_delivery import work_delivery as work_delivery
from test_work_attention_pages import work_pages as work_pages


@pytest.mark.asyncio
async def test_summary_show(work_delivery):
    await show(work_delivery, True)


@pytest.mark.asyncio
async def test_summary_show_final_claim(work_delivery, monkeypatch):
    await fences(work_delivery, monkeypatch, "claim")
