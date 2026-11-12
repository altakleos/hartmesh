"""Result media keeps native reads, SDK setup and final admission off-loop."""

import asyncio
from zoneinfo import ZoneInfo

import pytest
import pytest_asyncio
from _telegram_repository_test_support import db as db
from test_telegram_work_file_exports import work_exports as work_exports
from test_telegram_work_result_delivery import test_duplicate_paths_in_distinct_spaces_send_in_order_with_durable_photo_fallback as album
from test_telegram_work_result_delivery import test_result_file_upload_precedes_card_and_arms_controls_after_whole_ack as document
from test_telegram_work_result_links import test_warning_url_uses_exact_outcome_attempt_and_durable_plain_fallback as warning_link
from test_telegram_work_result_recovery import test_partial_album_refresh_survives_restart_and_keeps_unsent_hash as recover_album
from test_telegram_work_result_recovery import test_protection_upgrade_requires_new_show_without_relabeling_old_file as recover_protection
from test_telegram_work_result_recovery import test_show_clears_teaser_before_protected_files_and_final_controls as reveal
from test_telegram_work_result_repair import test_explicit_repair_preserves_uploaded_file_and_replaces_only_card as repair
from test_telegram_work_review_event_delivery import test_settled_review_event_sends_files_then_arms_review_controls as event_review
from test_telegram_work_source_delivery import work_delivery as work_delivery
from test_work_attention_pages import work_pages as work_pages


@pytest_asyncio.fixture(autouse=True)
async def fixture_timezone():
    await asyncio.to_thread(ZoneInfo, "America/New_York")
    await asyncio.to_thread(ZoneInfo, "UTC")


@pytest.mark.asyncio
@pytest.mark.parametrize("work_exports", ["review"], indirect=True)
async def test_result_document(work_exports, monkeypatch):
    await document(work_exports, monkeypatch)


@pytest.mark.asyncio
@pytest.mark.parametrize("work_exports", ["review_album"], indirect=True)
async def test_result_album_fallback(work_exports):
    await album(work_exports, True)


@pytest.mark.asyncio
@pytest.mark.parametrize("work_exports", ["review"], indirect=True)
async def test_result_reveal(work_exports):
    await reveal(work_exports, "ok")


@pytest.mark.asyncio
@pytest.mark.parametrize("work_exports", ["review_album"], indirect=True)
async def test_result_partial_album_recovery(work_exports):
    await recover_album(work_exports, "read")


@pytest.mark.asyncio
@pytest.mark.parametrize("work_exports", ["review"], indirect=True)
async def test_result_protection_recovery(work_exports):
    await recover_protection(work_exports)


@pytest.mark.asyncio
@pytest.mark.parametrize("work_exports", ["review"], indirect=True)
async def test_result_warning_link(work_exports, monkeypatch):
    await warning_link(work_exports, monkeypatch, "url_fallback")


@pytest.mark.asyncio
@pytest.mark.parametrize("work_exports", ["review"], indirect=True)
async def test_result_explicit_repair(work_exports, monkeypatch):
    await repair(work_exports, monkeypatch, None)


@pytest.mark.asyncio
@pytest.mark.parametrize("work_exports", ["review_event"], indirect=True)
async def test_result_event_materialization(work_exports):
    await event_review(work_exports, True)
