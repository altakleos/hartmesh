"""Canonical event mapping never leaves the async SQL boundary."""

import asyncio
from zoneinfo import ZoneInfo

import pytest
import pytest_asyncio
from _telegram_repository_test_support import db as db
from _telegram_work_test_support import work_environment as _work_environment  # noqa: F401 -- Gateway imports precede async fixture startup.
from test_telegram_work_actions import actions as actions
from test_telegram_work_event_intents import test_actual_activation_and_review_settlement_resolve_exact_attempt as settlement
from test_telegram_work_event_intents import test_unmaterialized_intent_cannot_be_claimed_or_acknowledged as unrendered
from test_telegram_work_event_intents import test_web_answer_uses_exact_receipt_and_edits_the_actors_question as response
from test_work_execution import activation as _activation  # noqa: F401 -- Prepare host fixture dependencies before the runtime gate.


@pytest_asyncio.fixture(autouse=True)
async def fixture_timezone():
    await asyncio.to_thread(ZoneInfo, "America/New_York")
    await asyncio.to_thread(ZoneInfo, "UTC")


@pytest.mark.asyncio
async def test_event_response(actions):
    await response(actions)


@pytest.mark.asyncio
async def test_event_settlement(actions):
    await settlement(actions)


@pytest.mark.asyncio
async def test_event_waits_for_materialization(actions):
    await unrendered(actions)
