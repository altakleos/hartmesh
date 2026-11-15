"""Actual management denial and opening custody stay off the event loop."""

import asyncio
from zoneinfo import ZoneInfo

import pytest
import pytest_asyncio
from _telegram_repository_test_support import db as db
from test_telegram_management_refusals import MANAGEMENT_KINDS, MANAGEMENT_LOSSES
from test_telegram_management_refusals import test_management_denial_preserves_each_real_opening_owner as preserved_owner
from test_telegram_management_refusals import test_management_press_reports_lost_manage as permission_denial
from test_telegram_work_source_delivery import work_delivery as work_delivery
from test_work_attention_pages import work_pages as work_pages


@pytest_asyncio.fixture(autouse=True)
async def fixture_timezone():
    await asyncio.to_thread(ZoneInfo, "America/New_York")


@pytest.mark.asyncio
@pytest.mark.parametrize("kind,key", MANAGEMENT_KINDS)
@pytest.mark.parametrize("loss", MANAGEMENT_LOSSES)
async def test_management_permission_denial(work_delivery, monkeypatch, kind, key, loss):
    await permission_denial(work_delivery, monkeypatch, kind, key, loss)


@pytest.mark.asyncio
@pytest.mark.parametrize("surviving", ["opening", "gesture", "draft"])
async def test_existing_management_owner(work_delivery, surviving):
    await preserved_owner(work_delivery, surviving)
