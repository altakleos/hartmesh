"""Manager-card SQL/source reads and SDK presentation stay on owned boundaries."""

import asyncio
from zoneinfo import ZoneInfo

import pytest
import pytest_asyncio
from _telegram_repository_test_support import db as db
from test_telegram_instance_network_cards import network as network
from test_telegram_instance_network_cards import test_current_manager_answer_keeps_existing_act_and_resume_protocol as exercise_answer
from test_telegram_instance_network_cards import test_manager_withdrawal_inside_sdk_limiter_prevents_raw_card as exercise_limiter
from test_telegram_instance_publication import publication as publication


@pytest_asyncio.fixture(autouse=True)
async def timezone_precondition():
    await asyncio.to_thread(ZoneInfo, "America/New_York")


@pytest.mark.asyncio
async def test_manager_answer(network):
    await exercise_answer(network)


@pytest.mark.asyncio
async def test_late_manager_withdrawal(network):
    await exercise_limiter(network)
