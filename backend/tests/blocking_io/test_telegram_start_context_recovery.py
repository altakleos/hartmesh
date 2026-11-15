"""Actual bounded Start family and SDK source checks retain strict I/O owners."""

import asyncio
from zoneinfo import ZoneInfo

import pytest
import pytest_asyncio
from _telegram_repository_test_support import db as db
from test_telegram_start_context_delivery import test_actual_original_source_reaches_the_context_child_final_sql as source
from test_telegram_start_context_delivery import test_context_child_recovers_only_its_own_real_runless_promise as runless
from test_telegram_start_context_lifecycle import test_aged_conclusively_cancelled_context_releases_its_original_control as closed
from test_telegram_start_context_lifecycle import test_aged_sweeps_keep_context_ancestry_even_when_relation_markers_are_lost as retained
from test_telegram_start_context_lifecycle import test_capacity_offer_transfers_the_actual_waiting_context_child as capacity
from test_telegram_start_context_lifecycle import test_native_child_evidence_pins_ancestry_after_its_journal_is_deleted as orphan
from test_telegram_start_context_lifecycle import test_original_cancel_control_fences_the_waiting_context_child as cancel
from test_telegram_start_context_lifecycle import test_transferred_context_capacity_offer_can_expire_after_its_actual_native_start as closed_offer
from test_telegram_start_context_native_boundary import test_late_other_instance_operation_is_refused_at_actual_child_native_entry as late_foreign
from test_telegram_start_context_ownership import test_actual_conversation_creation_is_idempotent_and_cancel_fences_late_staging as preparation
from test_telegram_start_context_ownership import test_actual_foreign_work_child_operation_holds_the_planned_family as foreign
from test_telegram_start_context_ownership import test_conversation_refusal_cannot_replace_the_original_real_runless_promise as original_promise
from test_telegram_start_context_ownership import test_parent_recovers_the_saved_child_receipt_after_progress_settlement_and_a_successor as history
from test_telegram_start_context_ownership import test_staged_context_cannot_reconstruct_a_missing_journal_or_renew_time as damaged
from test_telegram_start_context_recovery import test_actual_conversation_refusal_uses_one_distinct_child_and_keeps_original_cas as conversation
from test_telegram_start_context_recovery import test_actual_generation_race_gets_one_fresh_execution_with_the_same_body as execution
from test_telegram_start_context_recovery import test_execution_refresh_cannot_commit_after_its_last_owner_wait as owner_wait
from test_telegram_start_context_retry import test_actual_retry_qualifies_its_effective_child_and_original_deadline as retry
from test_telegram_work_actions import actions as actions
from test_telegram_work_source_delivery import work_delivery as work_delivery
from test_telegram_work_start import startable as startable
from test_work_attention_pages import work_pages as work_pages


@pytest_asyncio.fixture(autouse=True)
async def fixture_timezone():
    await asyncio.to_thread(ZoneInfo, "America/New_York")


@pytest.mark.asyncio
@pytest.mark.parametrize("repeated", [False, True])
async def test_execution_context(startable, monkeypatch, repeated):
    await execution(startable, monkeypatch, repeated)


@pytest.mark.asyncio
@pytest.mark.parametrize("retire", ["owner", "deadline"])
async def test_final_owner(startable, monkeypatch, retire):
    await owner_wait(startable, monkeypatch, retire)


@pytest.mark.asyncio
@pytest.mark.parametrize("reason", ["conversation-changed", "space-changed", "conversation-mismatch"])
@pytest.mark.parametrize("repeated", [False, True])
async def test_conversation_context(startable, monkeypatch, reason, repeated):
    await conversation(startable, monkeypatch, reason, repeated)


@pytest.mark.asyncio
@pytest.mark.parametrize("interrupt", ["response-loss", "cancel"])
async def test_resource_preparation(startable, monkeypatch, interrupt):
    await preparation(startable, monkeypatch, interrupt)


@pytest.mark.asyncio
@pytest.mark.parametrize("damage", ["missing-child", "paired-time-drift"])
async def test_damaged_family(startable, monkeypatch, damage):
    await damaged(startable, monkeypatch, damage)


@pytest.mark.asyncio
async def test_canonical_receipt_history(startable, monkeypatch):
    await history(startable, monkeypatch)


@pytest.mark.asyncio
async def test_original_promise(startable, monkeypatch):
    await original_promise(startable, monkeypatch)


@pytest.mark.asyncio
async def test_foreign_operation(startable, monkeypatch):
    await foreign(startable, monkeypatch)


@pytest.mark.asyncio
async def test_late_foreign_operation(startable, monkeypatch, tmp_path):
    await late_foreign(startable, monkeypatch, tmp_path)


@pytest.mark.asyncio
@pytest.mark.parametrize("origin", ["news", "composite"])
@pytest.mark.parametrize("damage", [None, "gesture", "child-marker"])
async def test_original_sdk_source(work_delivery, monkeypatch, origin, damage):
    await source(work_delivery, monkeypatch, origin, damage)


@pytest.mark.asyncio
@pytest.mark.parametrize("origin", ["news", "composite"])
async def test_original_runless_promise(work_delivery, monkeypatch, origin):
    await runless(work_delivery, monkeypatch, origin)


@pytest.mark.asyncio
async def test_waiting_capacity_handoff(work_delivery, monkeypatch):
    await capacity(work_delivery, monkeypatch)


@pytest.mark.asyncio
async def test_original_cancel(work_delivery, monkeypatch):
    await cancel(work_delivery, monkeypatch)


@pytest.mark.asyncio
@pytest.mark.parametrize("damage", [False, True])
async def test_retained_ancestry(work_delivery, monkeypatch, damage):
    await retained(work_delivery, monkeypatch, damage)


@pytest.mark.asyncio
async def test_closed_cancel_ancestry(work_delivery, monkeypatch):
    await closed(work_delivery, monkeypatch)


@pytest.mark.asyncio
async def test_closed_capacity_offer(work_delivery, monkeypatch):
    await closed_offer(work_delivery, monkeypatch)


@pytest.mark.asyncio
@pytest.mark.parametrize("mode", ["uncertain", "refused", "stale", "late-stale", "expired-promise", "expired-unknown", "expired-old-control"])
async def test_acknowledged_retry(work_delivery, monkeypatch, mode):
    await retry(work_delivery, monkeypatch, mode)


@pytest.mark.asyncio
@pytest.mark.parametrize("remove_root_relation", [False, True])
async def test_native_orphan_ancestry(work_delivery, monkeypatch, remove_root_relation):
    await orphan(work_delivery, monkeypatch, remove_root_relation)
