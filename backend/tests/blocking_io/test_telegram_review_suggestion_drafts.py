"""Reviewer result, canonical source, read-only press and refresh stay nonblocking."""

import asyncio
from zoneinfo import ZoneInfo

import pytest
import pytest_asyncio
from _telegram_repository_test_support import db as db
from test_telegram_review_suggestion_drafts import test_actual_review_result_suggestion_drafts_from_its_final_sdk_control as drafting
from test_telegram_review_suggestion_fences import test_late_review_change_cannot_commit_a_suggestion_draft_after_its_fresh_check as final_admission
from test_telegram_review_suggestion_fences import test_review_suggestion_final_sdk_qualifies_its_canonical_report_after_file_ack as canonical
from test_telegram_review_suggestion_legacy import test_legacy_review_show_and_locale_refresh_keep_the_unflagged_keyboard as legacy
from test_telegram_review_suggestion_show import test_review_suggestion_keeps_its_real_show_and_cleanup_through_drafting as show
from test_telegram_work_file_exports import work_exports as work_exports
from test_telegram_work_source_delivery import work_delivery as work_delivery
from test_work_attention_pages import work_pages as work_pages


@pytest_asyncio.fixture(autouse=True)
async def fixture_timezone():
    await asyncio.to_thread(ZoneInfo, "America/New_York")
    await asyncio.to_thread(ZoneInfo, "UTC")


@pytest.mark.asyncio
@pytest.mark.parametrize("work_exports", ["review_suggestion_event"], indirect=True)
@pytest.mark.parametrize("permissions", [7, 3, 1])
async def test_private_draft(work_exports, monkeypatch, permissions):
    await drafting(work_exports, monkeypatch, permissions)


@pytest.mark.asyncio
@pytest.mark.parametrize("work_exports", ["review_suggestion_event"], indirect=True)
@pytest.mark.parametrize("damage", ["resolved", "rerouted", "revision"])
async def test_last_guard(work_exports, monkeypatch, damage):
    await final_admission(work_exports, monkeypatch, damage)


@pytest.mark.asyncio
@pytest.mark.parametrize("work_exports", ["review_suggestion_event"], indirect=True)
@pytest.mark.parametrize("damage", ["actor", "attempt"])
async def test_canonical_report(work_exports, monkeypatch, damage):
    await canonical(work_exports, monkeypatch, damage)


@pytest.mark.asyncio
@pytest.mark.parametrize("work_exports", ["review_suggestion_event"], indirect=True)
@pytest.mark.parametrize("damage", ["none", "show", "cleanup"])
async def test_show(work_exports, monkeypatch, damage):
    await show(work_exports, monkeypatch, damage)


@pytest.mark.asyncio
@pytest.mark.parametrize("work_exports", ["review_suggestion_event"], indirect=True)
async def test_legacy_refresh(work_exports, monkeypatch):
    await legacy(work_exports, monkeypatch)
