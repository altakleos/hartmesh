"""Complete interaction rendering under the strict event-loop detector."""

import pytest
from _telegram_repository_test_support import db as db
from test_telegram_work_interactions import test_stale_changes_note_is_retained_only_as_a_fresh_confirmation as stale_changes
from test_work_attention_pages import work_pages as work_pages


@pytest.mark.asyncio
async def test_saved_changes_preview(work_pages):
    await stale_changes(work_pages)
