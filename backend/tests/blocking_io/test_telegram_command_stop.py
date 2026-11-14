"""Typed Stop routing, original cancellation and recovery keep the loop clear."""

import asyncio
from zoneinfo import ZoneInfo

import pytest
import pytest_asyncio
from _telegram_repository_test_support import db as db
from test_telegram_command_stop import test_default_stop_command_targets_the_only_running_assignment as stop_one
from test_telegram_command_stop import test_multiple_work_targets_require_a_real_acknowledged_choice as stop_choices
from test_telegram_work_source_delivery import work_delivery as work_delivery
from test_work_attention_pages import work_pages as work_pages


@pytest_asyncio.fixture(autouse=True)
async def fixture_timezone():
    await asyncio.to_thread(ZoneInfo, "America/New_York")


@pytest.mark.asyncio
async def test_command_stop_original_cancellation_keeps_the_loop_clear(work_delivery):
    await stop_one(work_delivery, False)


@pytest.mark.asyncio
@pytest.mark.parametrize("choice", ["all", "first_source_failure", "late_target", "retire_sent", "retire_claim"])
async def test_command_stop_choices_recovery_and_expiry_keep_the_loop_clear(work_delivery, monkeypatch, choice):
    await stop_choices(work_delivery, monkeypatch, choice)
