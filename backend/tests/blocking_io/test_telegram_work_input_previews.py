"""Input-owned full previews and confirmation stay on asynchronous SQL paths."""

import pytest
from _telegram_repository_test_support import db as db
from test_telegram_work_input_previews import test_stale_note_is_kept_then_confirmed_against_complete_new_review as changes
from test_telegram_work_source_delivery import work_delivery as work_delivery
from test_work_attention_pages import work_pages as work_pages


@pytest.mark.asyncio
async def test_kept_note_preview_and_confirmation(work_delivery):
    await changes(work_delivery)
