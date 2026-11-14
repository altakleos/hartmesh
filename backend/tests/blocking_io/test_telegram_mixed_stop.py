"""Mixed Stop shares the actual strict adapter/SQL/recorded-HTTP contracts."""

import asyncio
from zoneinfo import ZoneInfo

import pytest
import pytest_asyncio
from _telegram_repository_test_support import db as db
from test_telegram_mixed_stop import test_mixed_stop_asks_before_touching_original_answer_or_assignment as mixed
from test_telegram_mixed_stop_cold import test_cold_selected_stop_only_cancels_its_actual_original_publication as cold
from test_telegram_work_source_delivery import work_delivery as work_delivery
from test_work_attention_pages import work_pages as work_pages


@pytest_asyncio.fixture(autouse=True)
async def fixture_timezone():
    await asyncio.to_thread(ZoneInfo, "America/New_York")


@pytest.mark.asyncio
@pytest.mark.parametrize("choice", ["all", "preparing_all", "cold_preparing_all"])
async def test_mixed_selection_and_completion_keep_the_loop_clear(work_delivery, monkeypatch, choice):
    await mixed(work_delivery, monkeypatch, choice)


@pytest.mark.asyncio
async def test_cold_native_cancel_receipt_keeps_the_loop_clear(work_delivery, monkeypatch):
    await cold(work_delivery, monkeypatch, True)
