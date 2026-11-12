"""Summary refresh and held repair preserve nonblocking final admission."""

import pytest
from _telegram_repository_test_support import db as db
from test_telegram_summary_recovery import test_queued_summary_refresh_preserves_daily_identity_and_original_control_age as refresh
from test_telegram_summary_recovery import test_summary_held_repair_fences_final_failure_disposition as held
from test_telegram_work_source_delivery import work_delivery as work_delivery
from test_work_attention_pages import work_pages as work_pages


@pytest.mark.asyncio
async def test_summary_refresh(work_delivery):
    await refresh(work_delivery, "protection")


@pytest.mark.asyncio
async def test_summary_held_repair(work_delivery, monkeypatch):
    await held(work_delivery, monkeypatch)
