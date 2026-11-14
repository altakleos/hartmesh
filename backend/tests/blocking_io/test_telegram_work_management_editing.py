"""Typed saved-work replacement and recovery keep the event loop clear."""

import asyncio
from zoneinfo import ZoneInfo

import pytest
import pytest_asyncio
from _telegram_repository_test_support import db as db
from test_telegram_work_management_editing import test_interrupted_extraction_recovers_without_another_model_call as recover
from test_telegram_work_management_editing import test_saved_edit_correction_extracts_once_and_saves_only_after_fresh_full_preview as edit
from test_telegram_work_source_delivery import work_delivery as work_delivery
from test_work_attention_pages import work_pages as work_pages

from app.gateway import deps  # noqa: F401 -- resolve before the async Gateway lifetime.


@pytest_asyncio.fixture(autouse=True)
async def fixture_timezone():
    await asyncio.to_thread(ZoneInfo, "America/New_York")


@pytest.mark.asyncio
async def test_typed_saved_edit_keeps_the_loop_clear(work_delivery, monkeypatch):
    await edit(work_delivery, monkeypatch, False)


@pytest.mark.asyncio
async def test_unknown_model_recovery_keeps_the_loop_clear(work_delivery, monkeypatch):
    await recover(work_delivery, monkeypatch)
