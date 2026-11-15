"""Assignment-family refusal checks keep real SQL and SDK work async."""

import asyncio
from zoneinfo import ZoneInfo

import pytest
import pytest_asyncio
from _telegram_repository_test_support import db as db
from test_telegram_work_action_refusal_owners import test_lifecycle_does_not_bypass_assignment_family as family_owner
from test_telegram_work_source_delivery import work_delivery as work_delivery
from test_work_attention_pages import work_pages as work_pages


@pytest_asyncio.fixture(autouse=True)
async def fixture_timezone():
    await asyncio.to_thread(ZoneInfo, "America/New_York")


@pytest.mark.asyncio
@pytest.mark.parametrize("damage", ["selected", "all-controls", "missing-journal"])
async def test_assignment_family_owner(work_delivery, monkeypatch, damage):
    await family_owner(work_delivery, monkeypatch, damage)
