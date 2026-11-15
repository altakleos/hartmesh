"""Status controls use actual SDK/SQL owners and an offline activation launcher."""

import asyncio
from zoneinfo import ZoneInfo

import pytest
import pytest_asyncio
from _telegram_repository_test_support import db as db
from test_telegram_status_controls import test_status_progress_edit_replaces_stop_only_after_its_complete_ack as edit
from test_telegram_status_controls import test_status_replacement_clears_only_the_previous_actual_keyboard as cleanup
from test_telegram_status_controls import test_status_stop_stages_only_its_acknowledged_original_attempt as stop
from test_telegram_status_controls import test_stopped_status_resume_keeps_original_stop_proof as resume
from test_telegram_work_source_delivery import work_delivery as work_delivery
from test_work_attention_pages import work_pages as work_pages

from app.gateway import deps  # noqa: F401 -- initialize before the strict async lifetime.


@pytest_asyncio.fixture(autouse=True)
async def fixture_timezone():
    await asyncio.to_thread(ZoneInfo, "America/New_York")
    await asyncio.to_thread(ZoneInfo, "UTC")


@pytest.mark.asyncio
async def test_status_edit_and_ack_replay_keep_the_loop_clear(work_delivery):
    await edit(work_delivery)


@pytest.mark.asyncio
async def test_status_replacement_cleanup_keeps_the_loop_clear(work_delivery):
    await cleanup(work_delivery)


@pytest.mark.asyncio
async def test_status_stop_admission_keeps_the_loop_clear(work_delivery):
    await stop(work_delivery, True)


@pytest.mark.asyncio
async def test_status_resume_admission_keeps_the_loop_clear(work_delivery, monkeypatch):
    await resume(work_delivery, monkeypatch, "none")
