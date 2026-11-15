"""Actual manual review replies preserve Gateway loop ownership."""

import asyncio
from zoneinfo import ZoneInfo

import pytest
import pytest_asyncio
from _telegram_repository_test_support import db as db
from test_telegram_start_review_reply import test_actual_saved_start_refusal_opens_the_current_full_review as journaled
from test_telegram_start_review_reply import test_actual_start_delivers_full_current_review_as_reply_with_notifications_off as delivered
from test_telegram_start_review_reply import test_aged_pending_review_keeps_actual_saved_admission_family as aged
from test_telegram_start_review_reply import test_aged_request_review_handoff_retains_actual_request_confirmation as request_aged
from test_telegram_start_review_reply import test_context_recovered_start_uses_its_actual_refused_child_review as contextual
from test_telegram_start_review_reply import test_context_review_last_sdk_fence_requires_both_actual_journals as context_fenced
from test_telegram_start_review_reply import test_ineligible_context_review_keeps_qualified_execution_notice as fallback
from test_telegram_start_review_reply import test_ineligible_saved_start_commits_no_review_owner as ineligible
from test_telegram_start_review_reply import test_last_review_reply_fence_rechecks_actual_owners as fenced
from test_telegram_start_review_reply import test_live_review_reply_retains_its_real_start_and_cleanup_through_sweeps as retained
from test_telegram_start_review_reply import test_original_source_loss_rolls_back_first_review_publication as creation
from test_telegram_start_review_reply import test_request_start_refusal_delivers_actual_current_full_review as request_delivered
from test_telegram_start_review_reply import test_review_handoff_accepts_once_through_actual_acknowledged_sdk_controls as accepted
from test_telegram_start_review_reply import test_review_reply_exports_actual_files_before_arming_controls as files
from test_telegram_start_review_reply import test_saved_refusal_review_rechecks_source_and_native_absence as journal_fenced
from test_telegram_start_review_reply import test_saved_review_retention_rejects_another_actual_work_ack as foreign_retention
from test_telegram_start_review_reply import test_staged_review_reply_is_materialized_by_a_reopened_runtime as restarted
from test_telegram_work_source_delivery import work_delivery as work_delivery
from test_work_attention_pages import work_pages as work_pages


@pytest_asyncio.fixture(autouse=True)
async def fixture_timezone():
    await asyncio.to_thread(ZoneInfo, "America/New_York")
    await asyncio.to_thread(ZoneInfo, "UTC")


@pytest.mark.asyncio
@pytest.mark.parametrize("topic", [None, 77])
async def test_full_reply(work_delivery, monkeypatch, topic):
    await delivered(work_delivery, monkeypatch, topic)


@pytest.mark.asyncio
async def test_reopened_reply(work_delivery, monkeypatch):
    await restarted(work_delivery, monkeypatch)


@pytest.mark.asyncio
@pytest.mark.parametrize("change", ["gesture", "role", "age"])
async def test_creation_proof(work_delivery, monkeypatch, change):
    await creation(work_delivery, monkeypatch, change)


@pytest.mark.asyncio
@pytest.mark.parametrize("missing_controls", [False, True])
async def test_retained_proof(work_delivery, monkeypatch, missing_controls):
    await retained(work_delivery, monkeypatch, missing_controls)


@pytest.mark.asyncio
@pytest.mark.parametrize("change", ["inspect", "manager", "recipient", "cleanup", "operation", "witness_message", "witness_expiry"])
async def test_final_proof(work_delivery, monkeypatch, change):
    await fenced(work_delivery, monkeypatch, change)


@pytest.mark.asyncio
@pytest.mark.parametrize("composite", [False, True])
@pytest.mark.parametrize("reopened", [False, True])
async def test_journal_reply(work_delivery, monkeypatch, composite, reopened):
    await journaled(work_delivery, monkeypatch, composite, reopened)


@pytest.mark.asyncio
@pytest.mark.parametrize("topic", [None, 77])
@pytest.mark.parametrize("repair", [False, True])
async def test_file_ack_and_repair(work_delivery, monkeypatch, tmp_path, topic, repair):
    await files(work_delivery, monkeypatch, tmp_path, topic, repair)


@pytest.mark.asyncio
async def test_review_acceptance(work_delivery, monkeypatch, tmp_path):
    await accepted(work_delivery, monkeypatch, tmp_path)


@pytest.mark.asyncio
@pytest.mark.parametrize("composite,change", [(False, "inspect"), (False, "operation"), (False, "journal"), (True, "manager"), (True, "recipient"), (True, "origin"), (True, "witness_expiry")])
async def test_journal_final_fence(work_delivery, monkeypatch, composite, change):
    await journal_fenced(work_delivery, monkeypatch, composite, change)


@pytest.mark.asyncio
@pytest.mark.parametrize("change", ["expired", "late_expired", "role"])
async def test_journal_creation_eligibility(work_delivery, monkeypatch, change):
    await ineligible(work_delivery, monkeypatch, change)


@pytest.mark.asyncio
@pytest.mark.parametrize("composite", [False, True])
@pytest.mark.parametrize("missing_intent", [False, True])
async def test_journal_aged_proof(work_delivery, monkeypatch, composite, missing_intent):
    await aged(work_delivery, monkeypatch, composite, missing_intent)


@pytest.mark.asyncio
@pytest.mark.parametrize("missing_intent", [False, True])
@pytest.mark.parametrize("foreign_kind", ["direct", "composite"])
async def test_foreign_retention(work_delivery, monkeypatch, missing_intent, foreign_kind):
    await foreign_retention(work_delivery, monkeypatch, missing_intent, foreign_kind)


@pytest.mark.asyncio
@pytest.mark.parametrize("composite", [False, True])
@pytest.mark.parametrize("later_replay", [None, "before", "after"])
async def test_context_journal_reply(work_delivery, monkeypatch, composite, later_replay):
    await contextual(work_delivery, monkeypatch, composite, later_replay)


@pytest.mark.asyncio
@pytest.mark.parametrize("composite,damage", [(False, "child"), (True, "root"), (False, "native")])
async def test_context_final_proof(work_delivery, monkeypatch, composite, damage):
    await context_fenced(work_delivery, monkeypatch, composite, damage)


@pytest.mark.asyncio
@pytest.mark.parametrize("damage", [None, "origin"])
async def test_qualified_fallback(work_delivery, monkeypatch, damage):
    await fallback(work_delivery, monkeypatch, damage)


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ["decide", "respond"])
async def test_request_journal_aged_proof(work_delivery, monkeypatch, kind):
    await request_aged(work_delivery, monkeypatch, kind)


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ["decide", "respond"])
async def test_request_journal_sdk(work_delivery, monkeypatch, kind):
    await request_delivered(work_delivery, monkeypatch, kind)
