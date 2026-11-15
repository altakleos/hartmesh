"""Result presses, real replies and final delivery retain one nonblocking lifetime."""

import asyncio
from zoneinfo import ZoneInfo

import pytest
import pytest_asyncio
from _telegram_repository_test_support import db as db
from test_telegram_result_suggestion_drafts import test_installed_result_press_drafts_once_without_saving_work_or_inventing_inbox as press
from test_telegram_result_suggestion_drafts import test_suggestion_private_draft_keeps_real_manual_criteria_and_edit_inputs as typing
from test_telegram_result_suggestion_fences import test_lost_suggestion_marker_during_extraction_never_repairs_or_repeats_admission as marker
from test_telegram_result_suggestion_fences import test_suggestion_callback_message_cannot_replace_the_actual_sdk_receipt as receipt
from test_telegram_result_suggestion_fences import test_suggestion_final_sdk_fence_refuses_lost_original_proof as delivery
from test_telegram_result_suggestion_limits import test_real_suggestion_over_drafting_limit_gets_one_manual_assign_reply as limits
from test_telegram_result_suggestion_recovery import test_replacement_suggestion_owner_offers_manual_fallback_without_repeating_unknown_extraction as recovery
from test_telegram_result_suggestion_retention import test_suggestion_retention_keeps_owned_anchor_when_source_json_is_damaged as damaged
from test_telegram_result_suggestion_retention import test_suggestion_source_group_retention_never_follows_foreign_locators as retention
from test_telegram_result_suggestion_show import test_suggestion_press_requires_its_actual_protected_show_receipt as show
from test_telegram_result_suggestion_start import test_suggestion_start_retains_original_receipts_through_gateway_admission as gateway
from test_telegram_work_file_exports import work_exports as work_exports
from test_telegram_work_source_delivery import work_delivery as work_delivery
from test_work_attention_pages import work_pages as work_pages


@pytest_asyncio.fixture(autouse=True)
async def fixture_timezone():
    await asyncio.to_thread(ZoneInfo, "America/New_York")
    await asyncio.to_thread(ZoneInfo, "UTC")


@pytest.mark.asyncio
@pytest.mark.parametrize("work_exports", ["done_suggestion_event"], indirect=True)
async def test_press_save_and_retention(work_exports, monkeypatch):
    await press(work_exports, monkeypatch)


@pytest.mark.asyncio
@pytest.mark.parametrize("work_exports", ["done_suggestion_event"], indirect=True)
@pytest.mark.parametrize("mode", ["fallback", "edit"])
async def test_real_replies(work_exports, monkeypatch, mode):
    await typing(work_exports, monkeypatch, mode)


@pytest.mark.asyncio
@pytest.mark.parametrize("work_exports", ["done_suggestion_event"], indirect=True)
@pytest.mark.parametrize("damage", ["event", "grant"])
async def test_provider_final_source(work_exports, monkeypatch, damage):
    await delivery(work_exports, monkeypatch, damage)


@pytest.mark.asyncio
@pytest.mark.parametrize("work_exports", ["done_suggestion_event"], indirect=True)
@pytest.mark.parametrize("fallback", [False, True])
async def test_marker_loss(work_exports, monkeypatch, fallback):
    await marker(work_exports, monkeypatch, fallback)


@pytest.mark.asyncio
@pytest.mark.parametrize("work_exports", ["done_suggestion_event"], indirect=True)
@pytest.mark.parametrize("damage", ["none", "show", "cleanup", "cleanup-shape"])
async def test_protected_show(work_exports, monkeypatch, damage):
    await show(work_exports, monkeypatch, damage)


@pytest.mark.asyncio
@pytest.mark.parametrize("work_exports", ["done_long_suggestion_event", "done_suggestion_event"], indirect=True)
async def test_drafting_limit(work_exports, monkeypatch):
    await limits(work_exports, monkeypatch)


@pytest.mark.asyncio
@pytest.mark.parametrize("work_exports", ["done_suggestion_event"], indirect=True)
@pytest.mark.parametrize("foreign", ["binding", "topic", "anchor"])
async def test_retention_scope(work_exports, monkeypatch, foreign):
    await retention(work_exports, monkeypatch, foreign)


@pytest.mark.asyncio
@pytest.mark.parametrize("work_exports", ["done_suggestion_event"], indirect=True)
async def test_actual_sdk_receipt(work_exports, monkeypatch):
    await receipt(work_exports, monkeypatch)


@pytest.mark.asyncio
@pytest.mark.parametrize("work_exports", ["done_suggestion_event"], indirect=True)
@pytest.mark.parametrize("shape", ["payload", "parts", "member"])
async def test_damaged_json(work_exports, monkeypatch, shape):
    await damaged(work_exports, monkeypatch, shape)


@pytest.mark.asyncio
@pytest.mark.parametrize("work_exports", ["done_suggestion_event"], indirect=True)
@pytest.mark.parametrize("damage", ["none", "marker"])
async def test_interrupted_request(work_exports, monkeypatch, damage):
    await recovery(work_exports, monkeypatch, damage)


@pytest.mark.asyncio
@pytest.mark.parametrize("work_exports", ["done_suggestion_event"], indirect=True)
@pytest.mark.parametrize("damage", ["none", "event"])
async def test_gateway_start(work_exports, monkeypatch, damage):
    await gateway(work_exports, monkeypatch, damage)
