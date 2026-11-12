"""Colleague source reads and source refusal remain nonblocking."""

import asyncio
from zoneinfo import ZoneInfo

import pytest
import pytest_asyncio
from _telegram_repository_test_support import db as db
from test_telegram_work_answer_news import test_answer_news_delivers_from_saved_information_receipt as answered
from test_telegram_work_answer_news import test_answer_receipt_withdrawn_at_final_sdk_fence_is_not_sent as answer_withdrawal
from test_telegram_work_news_delivery import test_current_colleague_notice_reaches_sdk_without_work_controls as deliver
from test_telegram_work_news_delivery import test_protected_colleague_show_edits_actual_teaser_without_starting_work as reveal
from test_telegram_work_news_recovery import test_missing_news_anchor_repair_keeps_show_proof_and_refuses_live_claim as repair
from test_telegram_work_news_recovery import test_news_refresh_before_send_uses_current_presentation as refresh
from test_telegram_work_news_sources import test_generic_product_reply_cannot_impersonate_colleague_news as refuse
from test_telegram_work_news_sources import test_news_source_uses_colleague_and_canonical_assignment as read
from test_telegram_work_proposal_news import test_not_now_clears_only_the_acknowledged_keyboard_without_changing_work as dismiss_proposal
from test_telegram_work_proposal_news import test_proposal_delivers_complete_details_web_link_and_dismiss as proposal
from test_telegram_work_proposal_news import test_proposal_final_sdk_snapshot_rechecks_source_and_complete_preview as proposal_drift
from test_telegram_work_source_delivery import work_delivery as work_delivery
from test_telegram_work_stop_news import test_protected_stopped_news_show_uses_current_local_clock as stopped_show
from test_telegram_work_stop_news import test_stop_notice_reformats_when_midnight_passes_during_final_preparation as midnight
from test_telegram_work_stop_news import test_stopped_news_delivers_exact_colleague_cause as stopped
from test_telegram_work_stop_news import test_stopped_notice_final_clock_and_cause_are_current as stop_clock
from test_work_attention_pages import work_pages as work_pages


@pytest_asyncio.fixture(autouse=True)
async def fixture_timezone():
    await asyncio.to_thread(ZoneInfo, "America/New_York")
    await asyncio.to_thread(ZoneInfo, "UTC")


@pytest.mark.asyncio
async def test_colleague_source_read(work_delivery):
    await read(work_delivery, "edit")


@pytest.mark.asyncio
async def test_unqualified_news_refused(work_delivery):
    await refuse(work_delivery, "work.changed")


@pytest.mark.asyncio
async def test_colleague_sdk_delivery(work_delivery):
    await deliver(work_delivery, "edit")


@pytest.mark.asyncio
async def test_colleague_protected_show(work_delivery):
    await reveal(work_delivery, "ok")


@pytest.mark.asyncio
async def test_colleague_news_refresh(work_delivery):
    await refresh(work_delivery, "protection")


@pytest.mark.asyncio
async def test_colleague_news_repair(work_delivery):
    await repair(work_delivery, None)


@pytest.mark.asyncio
async def test_information_answer_delivery(work_delivery):
    await answered(work_delivery)


@pytest.mark.asyncio
async def test_information_answer_final_withdrawal(work_delivery, monkeypatch):
    await answer_withdrawal(work_delivery, monkeypatch)


@pytest.mark.asyncio
async def test_colleague_stop_delivery(work_delivery):
    ZoneInfo.clear_cache()
    await stopped(work_delivery, "stop")


@pytest.mark.asyncio
async def test_colleague_stop_clock_refresh(work_delivery, monkeypatch):
    ZoneInfo.clear_cache()
    await stop_clock(work_delivery, monkeypatch, "timezone", "stop")


@pytest.mark.asyncio
async def test_colleague_stop_midnight(work_delivery, monkeypatch):
    await midnight(work_delivery, monkeypatch)


@pytest.mark.asyncio
async def test_colleague_stop_show_clock(work_delivery):
    ZoneInfo.clear_cache()
    await stopped_show(work_delivery)


@pytest.mark.asyncio
async def test_derived_proposal_delivery(work_delivery):
    await proposal(work_delivery)


@pytest.mark.asyncio
async def test_protected_proposal_dismissal(work_delivery):
    await dismiss_proposal(work_delivery, True)


@pytest.mark.asyncio
async def test_proposal_final_preview_drift(work_delivery, monkeypatch):
    await proposal_drift(work_delivery, monkeypatch, "criteria")
