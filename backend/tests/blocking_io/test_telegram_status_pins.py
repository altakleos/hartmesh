"""Pin lifecycle and service cleanup keep source reads off the event loop."""

import asyncio
from zoneinfo import ZoneInfo

import pytest
import pytest_asyncio
from _telegram_repository_test_support import db as db
from test_telegram_status_pins import test_status_pin_final_fence_rejects_provider_time_changes as final_pin
from test_telegram_status_pins import test_status_pin_service_message_is_deleted_only_for_an_owned_pin as service_pin
from test_telegram_status_pins import test_status_pin_uses_exact_ack_and_is_silent_and_idempotent as first_pin
from test_telegram_status_pins import test_status_terminal_unpins_only_its_own_message as unpin
from test_telegram_work_source_delivery import work_delivery as work_delivery
from test_work_attention_pages import work_pages as work_pages


@pytest_asyncio.fixture(autouse=True)
async def fixture_timezone():
    await asyncio.to_thread(ZoneInfo, "America/New_York")
    await asyncio.to_thread(ZoneInfo, "UTC")


@pytest.mark.asyncio
async def test_status_pin(work_delivery):
    ZoneInfo.clear_cache()
    await first_pin(work_delivery)


@pytest.mark.asyncio
async def test_status_unpin(work_delivery):
    await unpin(work_delivery)


@pytest.mark.asyncio
async def test_status_service_cleanup(work_delivery):
    await service_pin(work_delivery, True)


@pytest.mark.asyncio
async def test_status_final_authority(work_delivery, monkeypatch):
    await final_pin(work_delivery, monkeypatch, "grant")
