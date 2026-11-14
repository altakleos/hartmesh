"""Actual SQL, retained source and SDK proposal flows under strict I/O checks."""

import asyncio
from zoneinfo import ZoneInfo

import pytest
import pytest_asyncio
from _telegram_repository_test_support import db as db
from support.detectors.blocking_io_runtime import detect_blocking_io_strict
from test_telegram_assignment_proposal_cards import proposal_cards as proposal_cards
from test_telegram_assignment_proposal_cards import test_actual_card_ack_owns_controls_and_finishes_original_delivery as exercise_delivery
from test_telegram_assignment_proposal_cards import test_just_answer_freezes_one_new_message_in_the_original_conversation as exercise_answer
from test_telegram_work_source_delivery import work_delivery as work_delivery
from test_work_attention_pages import work_pages as work_pages


@pytest_asyncio.fixture(autouse=True)
async def prepare_timezones():
    await asyncio.to_thread(ZoneInfo, "America/New_York")
    await asyncio.to_thread(ZoneInfo, "UTC")


@pytest.mark.asyncio
@pytest.mark.parametrize("operation", ["card", "answer"])
async def test_proposal_flow_keeps_blocking_work_owned(proposal_cards, operation):
    with detect_blocking_io_strict():
        await (exercise_delivery(proposal_cards) if operation == "card" else exercise_answer(proposal_cards))
