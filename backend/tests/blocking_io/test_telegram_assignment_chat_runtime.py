"""Strict loop coverage for installed proposal and Just answer owners."""

import asyncio
from zoneinfo import ZoneInfo

import pytest
import pytest_asyncio
from test_telegram_assignment_chat_runtime import test_installed_chat_adopts_proposal_and_just_answer_queues_only_the_choice as answer
from test_telegram_assignment_chat_runtime import test_live_observer_adopts_only_verified_proposal_with_installed_owner as live
from test_telegram_assignment_proposal_cards import db as db
from test_telegram_assignment_proposal_cards import proposal_cards as proposal_cards
from test_telegram_assignment_proposal_cards import work_delivery as work_delivery
from test_telegram_assignment_proposal_cards import work_pages as work_pages


@pytest_asyncio.fixture(autouse=True)
async def fixture_timezone():
    await asyncio.to_thread(ZoneInfo, "America/New_York")


@pytest.mark.asyncio
async def test_just_answer_and_current_card_marks_keep_loop_clear(proposal_cards, monkeypatch):
    await answer(proposal_cards, monkeypatch, 3)


@pytest.mark.asyncio
async def test_live_proposal_source_and_adoption_keep_loop_clear(proposal_cards):
    await live(proposal_cards, "live")
