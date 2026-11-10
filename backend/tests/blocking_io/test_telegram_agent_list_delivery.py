"""Bound discovery, SQL list publication and actual SDK dispatch under strict I/O."""

import asyncio
from zoneinfo import ZoneInfo

import pytest
import pytest_asyncio
from _telegram_repository_test_support import db as db
from test_telegram_agent_list_delivery import (
    delivery as delivery,
)
from test_telegram_agent_list_delivery import (
    test_list_replay_sends_one_owner_bound_list_with_open_link as test_list_replay_sends_one_owner_bound_list_with_open_link,
)
from test_telegram_agent_list_delivery import (
    test_lost_use_never_reaches_the_raw_sdk as test_lost_use_never_reaches_the_raw_sdk,
)
from test_telegram_agent_list_source import (
    lists as list_environment,  # noqa: F401 -- pytest resolves the renamed fixture by argument.
)
from test_telegram_agent_list_source import (
    test_cancellation_at_final_owner_read_retains_complete_list_unit as test_cancellation_at_final_owner_read_retains_complete_list_unit,
)
from test_telegram_instance_presentation import presentation as presentation

from app.gateway.routers import agent_instances as agent_instances

pytestmark = pytest.mark.asyncio


@pytest_asyncio.fixture(autouse=True)
async def seeded_timezone():
    await asyncio.to_thread(ZoneInfo, "America/New_York")


@pytest_asyncio.fixture
async def lists(list_environment):  # noqa: F811 -- pytest injects the imported fixture under its registered name.
    ZoneInfo.clear_cache()
    return list_environment
