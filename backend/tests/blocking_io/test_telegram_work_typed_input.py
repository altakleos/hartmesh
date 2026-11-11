"""Typed Work input and recovered receipts retain strict asynchronous I/O."""

import pytest
from _telegram_repository_test_support import db as db
from test_telegram_work_source_delivery import work_delivery as work_delivery
from test_telegram_work_typed_input import test_actual_typed_changes_are_saved_once_or_retained_for_current_review as changes
from test_telegram_work_typed_input import test_actual_typed_decision_waits_for_complete_preview_and_confirmation as decision
from test_telegram_work_typed_input import test_reopened_input_owner_settles_late_receipt_without_resubmitting as recover
from test_work_attention_pages import work_pages as work_pages


@pytest.mark.asyncio
async def test_prompt_changes_input(work_delivery):
    await changes(work_delivery, False)


@pytest.mark.asyncio
async def test_typed_decision_note(work_delivery, monkeypatch):
    await decision(work_delivery, monkeypatch, True, False)


@pytest.mark.asyncio
async def test_input_receipt_recovery(work_delivery, monkeypatch):
    await recover(work_delivery, monkeypatch)
