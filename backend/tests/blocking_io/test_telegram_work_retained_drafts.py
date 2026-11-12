"""Retained draft navigation borrows the existing asynchronous writer."""

import pytest
from _telegram_repository_test_support import db as db
from test_telegram_work_retained_drafts import test_draft_navigation_settles_old_input_without_submitting as navigate
from test_telegram_work_retained_drafts import test_review_note_controls_expire_from_original_input_even_after_refresh as expiry
from test_telegram_work_source_delivery import work_delivery as work_delivery
from test_work_attention_pages import work_pages as work_pages


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ["changes_open", "dismiss"])
async def test_review_note_navigation(work_delivery, monkeypatch, kind):
    await navigate(work_delivery, monkeypatch, kind)


@pytest.mark.asyncio
async def test_retained_note_expiry(work_delivery, monkeypatch):
    await expiry(work_delivery, monkeypatch)
