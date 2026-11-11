"""Typed input navigation keeps asynchronous writer ownership."""

import asyncio
from zoneinfo import ZoneInfo

import pytest
import pytest_asyncio
from _telegram_repository_test_support import db as db
from test_telegram_work_input_navigation import test_changed_decision_navigation_retains_text_for_another_explicit_choice as refresh
from test_telegram_work_input_navigation import test_navigation_final_input_drift_rolls_back_every_transition as drift
from test_telegram_work_input_navigation import test_typed_decision_navigation_settles_input_and_keeps_new_view as navigate
from test_telegram_work_source_delivery import work_delivery as work_delivery
from test_work_attention_pages import work_pages as work_pages


@pytest_asyncio.fixture(autouse=True)
async def fixture_timezone():
    await asyncio.to_thread(ZoneInfo, "America/New_York")


@pytest.mark.asyncio
async def test_add_note(work_delivery, monkeypatch):
    await navigate(work_delivery, monkeypatch, "decide_note")


@pytest.mark.asyncio
async def test_discard_fence(work_delivery, monkeypatch):
    await drift(work_delivery, monkeypatch, "dismiss")


@pytest.mark.asyncio
async def test_changed_decision(work_delivery, monkeypatch):
    await refresh(work_delivery, monkeypatch, "back", True)
