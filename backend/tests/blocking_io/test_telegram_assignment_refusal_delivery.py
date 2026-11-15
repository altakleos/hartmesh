"""Policy-disabled save replies retain their final source fence under strict I/O."""

import asyncio
from zoneinfo import ZoneInfo

import pytest
import pytest_asyncio
from _telegram_repository_test_support import db as db
from test_telegram_assignment_refusal_delivery import test_refusal_last_sdk_fence_rechecks_grants_and_actual_receipt as fenced
from test_telegram_assignment_refusal_delivery import test_saved_work_off_refusal_is_delivered_and_can_retry_same_operation as disabled
from test_telegram_assignment_refusal_delivery import test_work_off_wording_is_retired_when_current_policy_is_enabled as enabled
from test_telegram_work_source_delivery import work_delivery as work_delivery
from test_work_attention_pages import work_pages as work_pages


@pytest_asyncio.fixture(autouse=True)
async def fixture_timezone():
    await asyncio.to_thread(ZoneInfo, "America/New_York")


@pytest.mark.asyncio
@pytest.mark.parametrize("manager", [False, True])
async def test_disabled_work(work_delivery, manager):
    await disabled(work_delivery, manager)


@pytest.mark.asyncio
async def test_enabled_work(work_delivery):
    await enabled(work_delivery)


@pytest.mark.asyncio
@pytest.mark.parametrize("change", ["manager", "inspect", "receipt"])
async def test_current_source(work_delivery, monkeypatch, change):
    await fenced(work_delivery, monkeypatch, change)
