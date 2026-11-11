"""The real Work journal, borrowed writer and receipt recovery keep I/O off-loop."""

import asyncio
from zoneinfo import ZoneInfo

import pytest
import pytest_asyncio
from _telegram_repository_test_support import db as db
from test_telegram_work_actions import actions as actions
from test_telegram_work_actions import test_lost_response_recovers_exact_receipt_without_another_mutation as recover
from test_telegram_work_actions import test_response_is_attributed_once_and_never_starts_work as respond


@pytest_asyncio.fixture(autouse=True)
async def fixture_timezone():
    await asyncio.to_thread(ZoneInfo, "America/New_York")


@pytest.mark.asyncio
async def test_journal_and_domain_writer_use_async_sql(actions):
    await respond(actions)


@pytest.mark.asyncio
async def test_receipt_recovery_uses_async_sql(actions, monkeypatch):
    await recover(actions, monkeypatch)
