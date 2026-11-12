"""Conclusive confirmation recovery performs no blocking I/O."""

import asyncio
from zoneinfo import ZoneInfo

import pytest
import pytest_asyncio
from _telegram_repository_test_support import db as db
from test_telegram_work_confirmed_review_recovery import test_conclusive_old_confirmation_recovers_without_repeating_its_command as confirmed
from test_telegram_work_confirmed_review_recovery import test_direct_note_closure_after_admission_recovers_original_input as typed
from test_telegram_work_confirmed_review_recovery import test_known_non_effect_confirmation_checks_original_operation_after_closure as refused
from test_telegram_work_source_delivery import work_delivery as work_delivery
from test_work_attention_pages import work_pages as work_pages


@pytest_asyncio.fixture(autouse=True)
async def fixture_timezone():
    await asyncio.to_thread(ZoneInfo, "America/New_York")


@pytest.mark.asyncio
async def test_confirmed_replacement(work_delivery, monkeypatch):
    await confirmed(work_delivery, monkeypatch, True)


@pytest.mark.asyncio
async def test_admitted_text_closure(work_delivery, monkeypatch):
    await typed(work_delivery, monkeypatch, False)


@pytest.mark.asyncio
async def test_non_effect_closure(work_delivery, monkeypatch):
    await refused(work_delivery, monkeypatch)
