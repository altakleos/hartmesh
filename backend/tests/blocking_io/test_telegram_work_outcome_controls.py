"""Native outcome source, SDK and execution guards keep one drained lifetime."""

import asyncio
from zoneinfo import ZoneInfo

import pytest
import pytest_asyncio
from _telegram_repository_test_support import db as db
from test_telegram_work_outcome_controls import test_cannot_help_change_opens_a_fresh_full_management_preview as change
from test_telegram_work_outcome_controls import test_decision_and_review_changes_keep_their_canonical_event_family as families
from test_telegram_work_outcome_controls import test_legacy_no_result_keyboard_decodes_and_delivers_unchanged as legacy
from test_telegram_work_outcome_controls import test_live_outcome_control_retains_its_original_settled_act as retention
from test_telegram_work_outcome_controls import test_multipart_outcome_waits_for_its_last_actual_ack as multipart
from test_telegram_work_outcome_controls import test_no_result_offers_current_manager_change_without_mutating_work as no_result
from test_telegram_work_outcome_controls import test_outcome_final_sdk_fence_qualifies_primary_detail_and_control as final_sdk
from test_telegram_work_outcome_controls import test_resume_from_saved_answer_retains_its_original_primary_receipt as resume
from test_telegram_work_outcome_controls import test_retry_suffixed_outcome_keeps_receipts_retention_and_real_control as replacement
from test_telegram_work_source_delivery import work_delivery as work_delivery
from test_work_attention_pages import work_pages as work_pages


@pytest_asyncio.fixture(autouse=True)
async def fixture_timezone():
    await asyncio.to_thread(ZoneInfo, "America/New_York")
    await asyncio.to_thread(ZoneInfo, "UTC")


@pytest.mark.asyncio
async def test_change_from_saved_reason(work_delivery):
    await change(work_delivery)


@pytest.mark.asyncio
@pytest.mark.parametrize("damage", ["none", "event", "all-source"])
async def test_original_answer_through_native_entry(work_delivery, monkeypatch, damage):
    await resume(work_delivery, monkeypatch, damage)


@pytest.mark.asyncio
@pytest.mark.parametrize("damage", ["primary", "detail"])
async def test_outcome_source_at_final_sdk(work_delivery, monkeypatch, damage):
    await final_sdk(work_delivery, monkeypatch, damage)


@pytest.mark.asyncio
async def test_complete_outcome_ack(work_delivery):
    await multipart(work_delivery)


@pytest.mark.asyncio
@pytest.mark.parametrize("pending", [False, True, "missing", "control", "control-marker", "control-child"])
async def test_outcome_primary_retention(work_delivery, pending):
    await retention(work_delivery, pending)


@pytest.mark.asyncio
async def test_manager_change_after_no_result(work_delivery):
    await no_result(work_delivery, "plain")


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ["decide", "changes_requested"])
async def test_canonical_families(work_delivery, kind):
    await families(work_delivery, kind)


@pytest.mark.asyncio
async def test_replacement(work_delivery):
    await replacement(work_delivery)


@pytest.mark.asyncio
async def test_legacy_keyboard(work_delivery):
    await legacy(work_delivery)
