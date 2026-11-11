"""Typed Work submission and its receipt worker borrow asynchronous SQL."""

import pytest
from _telegram_repository_test_support import db as db
from test_telegram_work_input_admission import test_unchanged_changes_note_admits_once_and_worker_applies_original_cas as changes
from test_telegram_work_source_delivery import work_delivery as work_delivery
from test_work_attention_pages import work_pages as work_pages


@pytest.mark.asyncio
async def test_captured_changes_submission(work_delivery):
    await changes(work_delivery)
