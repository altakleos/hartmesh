"""Stored drafting, manual capture and delegation keep SQL and providers off-loop."""

import asyncio
from zoneinfo import ZoneInfo

import _telegram_work_test_support  # noqa: F401 -- import the fixture's Gateway dependencies during collection
import pytest
import pytest_asyncio
from _telegram_repository_test_support import db as db
from test_telegram_assignment_actions import test_unknown_delegate_response_recovers_only_the_exact_receipt as recover
from test_telegram_assignment_requests import test_input_drafts_once_and_never_opens_thread_run_or_work as request
from test_telegram_assignment_requests import test_manual_criteria_and_prompt_custody_share_the_draft_writer as criteria
from test_telegram_work_actions import actions as actions


@pytest_asyncio.fixture(autouse=True)
async def fixture_timezone():
    await asyncio.to_thread(ZoneInfo, "America/New_York")


@pytest.mark.asyncio
async def test_stored_request_uses_async_io(actions, monkeypatch):
    await request(actions, monkeypatch)


@pytest.mark.asyncio
async def test_manual_criteria_uses_async_io(actions, monkeypatch):
    await criteria(actions, monkeypatch)


@pytest.mark.asyncio
async def test_exact_delegate_recovery_uses_async_io(actions, monkeypatch):
    await recover(actions, monkeypatch)
