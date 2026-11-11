"""Input-owned preview recovery keeps asynchronous writer ownership."""

import pytest
from _telegram_repository_test_support import db as db
from test_telegram_work_callback_recovery import test_known_refusal_keeps_typed_decision_and_input_lineage as decision
from test_telegram_work_input_refresh import test_changed_retained_note_press_refreshes_full_review_without_submitting as changes
from test_telegram_work_source_delivery import work_delivery as work_delivery
from test_work_attention_pages import work_pages as work_pages


@pytest.mark.asyncio
async def test_changed_retained_note(work_delivery, monkeypatch):
    await changes(work_delivery, monkeypatch, "changes_confirm")


@pytest.mark.asyncio
async def test_refused_typed_decision(work_delivery, monkeypatch):
    await decision(work_delivery, monkeypatch)
