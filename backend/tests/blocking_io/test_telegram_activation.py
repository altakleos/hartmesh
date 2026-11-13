"""Stop journals, conflict recovery and lifetime fences retain async SQL I/O."""

import asyncio
from zoneinfo import ZoneInfo

import pytest
import pytest_asyncio
from _telegram_repository_test_support import db as db
from test_telegram_activation import test_command_receipt_does_not_lose_stop_dispatch as lost_receipt
from test_telegram_activation import test_retention_keeps_unresolved_stop_and_expires_valid_settlement as retention
from test_telegram_activation import test_stop_conflict_child_receipt_recovers_before_parent_update as lost_child
from test_telegram_activation import test_worker_shutdown_during_host_read_preserves_original_stop as retired
from test_telegram_work_actions import actions as actions

from app.gateway import deps  # noqa: F401 -- Gateway imports dependencies before its async lifespan and bot fixtures.


@pytest_asyncio.fixture(autouse=True)
async def fixture_timezone():
    # Match the existing strict Work action fixture. This seeds preferences;
    # cold provider/config and runtime timezone lookups have separate tests.
    await asyncio.to_thread(ZoneInfo, "America/New_York")


@pytest.mark.asyncio
async def test_stop_receipt_and_followup_do_not_block(actions, monkeypatch):
    await lost_receipt(actions, monkeypatch)


@pytest.mark.asyncio
async def test_stop_retry_child_recovery_does_not_block(actions, monkeypatch):
    await lost_child(actions, monkeypatch)


@pytest.mark.asyncio
async def test_stop_worker_retirement_and_recovery_do_not_block(actions):
    await retired(actions)


@pytest.mark.asyncio
async def test_stop_journal_retention_does_not_block(actions):
    await retention(actions, "settled")
