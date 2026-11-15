"""Read-only Start refresh and its final SDK source fencing under strict I/O."""

import asyncio
from zoneinfo import ZoneInfo

import pytest
import pytest_asyncio
from _telegram_repository_test_support import db as db
from test_telegram_work_source_delivery import work_delivery as work_delivery
from test_telegram_work_start_refresh import test_actual_stale_start_shows_current_assignment_and_needs_a_new_sdk_press as refreshed
from test_telegram_work_start_refresh import test_refresh_final_sdk_read_rechecks_original_start_proof as final_proof
from test_telegram_work_start_refresh import test_stale_start_rejects_actual_same_operation_evidence_bound_to_other_work as foreign
from test_telegram_work_start_refresh import test_stale_start_with_an_owned_reservation_cannot_create_a_fresh_preview as owned
from test_telegram_work_start_refresh_families import test_refreshed_start_keeps_each_real_carrier_proof as carrier
from test_telegram_work_start_refresh_origins import test_new_start_rechecks_news_origin_after_refreshed_preview_ack as news
from test_telegram_work_start_refresh_origins import test_repeated_actual_refreshes_keep_group_proofs_compact_through_native_entry as repeated
from test_telegram_work_start_refresh_origins import test_stale_capacity_offer_refresh_preserves_its_waiting_owner_transfer as capacity
from test_telegram_work_start_refresh_retention import test_actual_foreign_cleanup_receipt_cannot_unlock_a_refreshed_start as cleanup_scope
from test_telegram_work_start_refresh_retention import test_live_refresh_keeps_aged_original_journals_and_inputs_through_real_sweeps as aged
from test_telegram_work_start_refresh_retention import test_live_refreshed_start_retains_its_actual_acknowledged_cleanup as cleanup_hold
from test_telegram_work_start_refresh_retention import test_missing_public_source_control_holds_its_aged_body_without_a_new_effect as missing_source
from test_telegram_work_start_refresh_retention import test_owned_runless_admission_retains_its_historical_refresh_ancestry as historical
from test_telegram_work_start_refresh_retention import test_pin_only_discovery_uses_the_real_gesture_and_contains_bad_parent_data as pin_only
from test_work_attention_pages import work_pages as work_pages


@pytest_asyncio.fixture(autouse=True)
async def fixture_timezone():
    await asyncio.to_thread(ZoneInfo, "America/New_York")


@pytest.mark.asyncio
@pytest.mark.parametrize("legacy", [False, "row", "stored"])
async def test_current_preview(work_delivery, monkeypatch, legacy):
    await refreshed(work_delivery, monkeypatch, legacy)


@pytest.mark.asyncio
@pytest.mark.parametrize("damage", ["reservation", "ack"])
async def test_final_source(work_delivery, monkeypatch, damage):
    await final_proof(work_delivery, monkeypatch, damage)


@pytest.mark.asyncio
@pytest.mark.parametrize("journal", ["missing", "malformed"])
async def test_owned_launch(work_delivery, monkeypatch, journal):
    await owned(work_delivery, monkeypatch, journal)


@pytest.mark.asyncio
@pytest.mark.parametrize("evidence", ["event", "attempt"])
async def test_conflicting_operation(work_delivery, monkeypatch, evidence):
    await foreign(work_delivery, monkeypatch, evidence)


@pytest.mark.asyncio
@pytest.mark.parametrize("family", ["assignment", "outcome", "execution", "management-back", "status"])
async def test_carrier_origin(work_delivery, monkeypatch, family):
    await carrier(work_delivery, monkeypatch, family, False)


@pytest.mark.asyncio
@pytest.mark.parametrize("missing", [False, True])
async def test_news_native_source(work_delivery, monkeypatch, missing):
    await news(work_delivery, monkeypatch, missing)


@pytest.mark.asyncio
async def test_capacity_transfer(work_delivery, monkeypatch):
    await capacity(work_delivery, monkeypatch)


@pytest.mark.asyncio
async def test_repeated_refresh(work_delivery, monkeypatch):
    await repeated(work_delivery, monkeypatch)


@pytest.mark.asyncio
async def test_cleanup_retention(work_delivery, monkeypatch):
    await cleanup_hold(work_delivery, monkeypatch)


@pytest.mark.asyncio
@pytest.mark.parametrize("damage", ["foreign-message", "topic"])
async def test_cleanup_scope(work_delivery, monkeypatch, damage):
    await cleanup_scope(work_delivery, monkeypatch, damage)


@pytest.mark.asyncio
async def test_saved_management_retention(work_delivery, monkeypatch):
    await aged(work_delivery, monkeypatch, "management-saved")


@pytest.mark.asyncio
@pytest.mark.parametrize("family,damage", [("view", "descriptor"), ("view", "generation"), ("outcome", "generation"), ("execution", "generation"), ("management-saved", "generation")])
async def test_historical_native_promise(work_delivery, monkeypatch, family, damage):
    await historical(work_delivery, monkeypatch, family, damage)


@pytest.mark.asyncio
@pytest.mark.parametrize("family", ["assignment", "outcome"])
async def test_missing_source_body(work_delivery, monkeypatch, family):
    await missing_source(work_delivery, monkeypatch, family)


@pytest.mark.asyncio
@pytest.mark.parametrize("damage", ["alternate-parent", "foreign-group", "malformed-assignment"])
async def test_pin_only_ancestry(work_delivery, monkeypatch, damage):
    await pin_only(work_delivery, monkeypatch, damage)
