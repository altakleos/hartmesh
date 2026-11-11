"""Native export, shared budget and SDK serialization stay off the Gateway loop."""

import asyncio
from zoneinfo import ZoneInfo

import pytest
import pytest_asyncio
from _telegram_repository_test_support import db as db
from test_telegram_instance_file_admission import file_admission as file_admission
from test_telegram_instance_file_exports import exports as exports
from test_telegram_instance_output_delivery import delivery as delivery
from test_telegram_instance_output_delivery import test_cancelled_sender_retains_entire_batch_until_native_read_settles as exercise_cancel
from test_telegram_instance_output_delivery import test_claimed_sender_exports_as_human_and_acks_without_rereading as exercise_send
from test_telegram_instance_publication import publication as publication


@pytest_asyncio.fixture(autouse=True)
async def timezone_precondition():
    await asyncio.to_thread(ZoneInfo, "America/New_York")


@pytest.mark.asyncio
async def test_export_to_sdk_owns_io(delivery):
    await exercise_send(delivery)


@pytest.mark.asyncio
async def test_cancelled_sender_drains_native_export(delivery, monkeypatch):
    await exercise_cancel(delivery, monkeypatch)
