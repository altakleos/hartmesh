"""Closed review recovery retains guarded asynchronous ownership."""

import asyncio
from zoneinfo import ZoneInfo

import pytest
import pytest_asyncio
from _telegram_repository_test_support import db as db
from test_telegram_work_retired_reviews import test_retired_review_callback_retires_old_controls_and_keeps_note as callback
from test_telegram_work_retired_reviews import test_text_arriving_after_review_closure_is_kept_without_submission as typed
from test_telegram_work_source_delivery import work_delivery as work_delivery
from test_work_attention_pages import work_pages as work_pages


@pytest_asyncio.fixture(autouse=True)
async def fixture_timezone():
    await asyncio.to_thread(ZoneInfo, "America/New_York")


@pytest.mark.asyncio
async def test_replacement_review(work_delivery, monkeypatch):
    await callback(work_delivery, monkeypatch, True)


@pytest.mark.asyncio
async def test_closed_review_note(work_delivery, monkeypatch):
    await typed(work_delivery, monkeypatch, False)


@pytest.mark.asyncio
async def test_orphaned_null_confirmation(work_delivery, monkeypatch):
    from test_telegram_work_retired_reviews import test_resumed_input_cannot_reopen_a_missing_confirmation_journal

    await test_resumed_input_cannot_reopen_a_missing_confirmation_journal(work_delivery, monkeypatch, None)


@pytest.mark.asyncio
async def test_original_input_admission_marker(work_delivery):
    from test_telegram_work_input_admission import test_missing_marked_input_journal_cannot_be_admitted_again

    await test_missing_marked_input_journal_cannot_be_admitted_again(work_delivery)
