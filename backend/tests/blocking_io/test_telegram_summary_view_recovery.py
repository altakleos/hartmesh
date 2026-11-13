"""Summary Show recovery keeps SQL and source qualification nonblocking."""

import pytest
from _telegram_repository_test_support import db as db
from test_telegram_summary_view_recovery import test_deleted_summary_show_target_repairs_as_a_new_protected_message as replacement
from test_telegram_summary_view_recovery import test_pending_summary_show_refreshes_current_source_with_original_age as refresh
from test_telegram_work_source_delivery import work_delivery as work_delivery
from test_work_attention_pages import work_pages as work_pages


@pytest.mark.asyncio
async def test_summary_show_refresh(work_delivery):
    await refresh(work_delivery, "title")


@pytest.mark.asyncio
async def test_summary_show_replacement(work_delivery):
    await replacement(work_delivery)
