"""Closed assignment recovery uses real owners without blocking the loop."""

import asyncio
from zoneinfo import ZoneInfo

import pytest
import pytest_asyncio
from _telegram_repository_test_support import db as db
from test_telegram_closed_assignment_prompts import test_damaged_reopen_retains_owned_history_behind_unfinished_actual_reply as retain
from test_telegram_closed_assignment_prompts import test_legacy_acknowledged_transcript_is_not_echoed_again_before_choices as voice
from test_telegram_closed_assignment_prompts import test_reopen_assignment_prompt_opens_fresh_question_without_using_held_words as reopen
from test_telegram_closed_assignment_prompts import test_reopened_prompt_final_sdk_refuses_lost_original_proof as proof
from test_telegram_work_source_delivery import work_delivery as work_delivery
from test_work_attention_pages import work_pages as work_pages

from app.gateway import deps  # noqa: F401 -- resolve before the async Gateway lifetime.


@pytest_asyncio.fixture(autouse=True)
async def fixture_timezone():
    await asyncio.to_thread(ZoneInfo, "America/New_York")


@pytest.mark.asyncio
async def test_reopened_assignment_keeps_the_loop_clear(work_delivery, monkeypatch):
    await reopen(work_delivery, monkeypatch)


@pytest.mark.asyncio
async def test_closed_voice_recovery_keeps_the_loop_clear(work_delivery, monkeypatch):
    await voice(work_delivery, monkeypatch, True, "prompt_reopen")


@pytest.mark.asyncio
async def test_reopen_final_source_check_keeps_the_loop_clear(work_delivery, monkeypatch):
    await proof(work_delivery, monkeypatch, True, "cleanup")


@pytest.mark.asyncio
async def test_damaged_recovery_retention_keeps_the_loop_clear(work_delivery, monkeypatch):
    await retain(work_delivery, monkeypatch, "descriptor", False)
