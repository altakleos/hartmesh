"""Installed notification and saved-input paths stay off the event loop."""

import asyncio
from zoneinfo import ZoneInfo

import pytest
import pytest_asyncio
from _telegram_repository_test_support import db as db
from test_telegram_notification_runtime import test_installed_runtime_baselines_history_then_delivers_new_question as delivers
from test_telegram_notification_runtime import test_runtime_delivery_retry_rebuilds_missing_digest_view as repairs
from test_telegram_notification_runtime import test_runtime_recovers_captured_note_with_original_operation as recovers
from test_telegram_notification_supervisor import test_default_supervisor_seeds_before_releasing_updates as starts
from test_telegram_supervisor import _supervisor_logging as _supervisor_logging
from test_telegram_work_source_delivery import work_delivery as work_delivery
from test_work_attention_pages import work_pages as work_pages


@pytest_asyncio.fixture(autouse=True)
async def _zones():
    # The source fixture deliberately uses this quiet-hours zone.
    await asyncio.to_thread(ZoneInfo, "America/New_York")


@pytest.mark.asyncio
async def test_notification_delivery(work_delivery):
    await delivers(work_delivery)


@pytest.mark.asyncio
async def test_notification_retry(work_delivery):
    await repairs(work_delivery)


@pytest.mark.asyncio
async def test_notification_saved_input(work_delivery):
    await recovers(work_delivery)


@pytest.mark.asyncio
async def test_notification_startup(work_pages, monkeypatch):
    await starts(work_pages, monkeypatch)


@pytest.mark.asyncio
async def test_notification_file_outcome_retry(work_delivery, monkeypatch, tmp_path):
    from test_telegram_notification_runtime import test_file_outcome_retry_preserves_evidence_and_closes_warning

    await test_file_outcome_retry_preserves_evidence_and_closes_warning(work_delivery, monkeypatch, tmp_path)


@pytest.mark.asyncio
async def test_notification_shutdown_failure_drains():
    from test_telegram_notification_supervisor import test_notification_shutdown_drains_after_failure

    await test_notification_shutdown_drains_after_failure("worker", True)
