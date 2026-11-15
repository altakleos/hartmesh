"""Current native readiness refusals keep existing SQL and SDK ownership async."""

import asyncio
from zoneinfo import ZoneInfo

import pytest
import pytest_asyncio
from _telegram_repository_test_support import db as db
from test_telegram_work_execution_outcomes import test_denied_start_names_the_current_supervisor as denied_supervisor
from test_telegram_work_readiness_refusals import test_saved_refusal_uses_current_read_only_execution_probe as current_readiness
from test_telegram_work_source_delivery import work_delivery as work_delivery
from test_work_attention_pages import work_pages as work_pages


@pytest_asyncio.fixture(autouse=True)
async def fixture_timezone():
    await asyncio.to_thread(ZoneInfo, "America/New_York")


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "reason,key", [("agents-none", "unavailable.off"), ("operator-hold", "unavailable.paused"), ("host-replaced", "unavailable.moved"), ("space-unverified", "unavailable.files_unchecked"), (None, "unavailable.fix"), ("ready", "tap.failed")]
)
async def test_current_readiness_refusal(work_delivery, monkeypatch, reason, key):
    await current_readiness(work_delivery, monkeypatch, reason, key)


@pytest.mark.asyncio
async def test_saved_route_ceiling_denial(work_delivery, monkeypatch):
    await denied_supervisor(work_delivery, monkeypatch)
