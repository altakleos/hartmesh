"""Actual browser instance setup and its borrowed writer under strict I/O."""

import asyncio
from zoneinfo import ZoneInfo

import pytest
import pytest_asyncio
from _telegram_repository_test_support import db as db
from test_telegram_instance_setup import setup_api as setup_api
from test_telegram_instance_setup import test_current_use_connects_company_own_and_colleague_assistants as connect_current
from test_telegram_instance_setup import test_execution_refusal_precedes_token_and_carries_the_reason as refused_execution
from test_telegram_instance_setup import test_target_is_rechecked_in_the_final_capacity_writer as writer_recheck
from test_telegram_policy import policy_api as policy_api
from test_telegram_router import api as api


@pytest_asyncio.fixture(autouse=True)
async def seeded_timezone():
    # Fixture preparation writes settings; production setup is exercised cold.
    await asyncio.to_thread(ZoneInfo, "UTC")


@pytest.mark.asyncio
async def test_instance_setup_keeps_the_loop_clear(setup_api):
    ZoneInfo.clear_cache()
    await connect_current(setup_api, "company", None, "active")


@pytest.mark.asyncio
async def test_instance_refusal_keeps_the_loop_clear(setup_api):
    await refused_execution(setup_api, "operator-hold")


@pytest.mark.asyncio
async def test_instance_setup_rechecks_its_own_writer(setup_api, monkeypatch):
    await writer_recheck(setup_api, monkeypatch, "use")
