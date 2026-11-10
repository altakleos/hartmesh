"""Managed file transfer keeps SDK and native ownership off the event loop."""

import asyncio
from zoneinfo import ZoneInfo

import pytest
import pytest_asyncio
from _telegram_repository_test_support import db as db
from test_telegram_instance_file_admission import file_admission as file_admission
from test_telegram_instance_file_delivery import test_cancelled_adapter_operation_drains_write_and_releases_budget as exercise_cancel
from test_telegram_instance_file_delivery import test_protected_save_uses_protected_hint_and_never_a_draft as exercise_protected
from test_telegram_instance_file_delivery import test_sdk_file_is_saved_directly_without_resident_or_conversation as exercise_sdk
from test_telegram_instance_publication import publication as publication


@pytest_asyncio.fixture(autouse=True)
async def timezone_precondition():
    await asyncio.to_thread(ZoneInfo, "America/New_York")


@pytest.mark.asyncio
async def test_sdk_download_and_home_write(file_admission, monkeypatch):
    await exercise_sdk(file_admission, monkeypatch)


@pytest.mark.asyncio
async def test_protected_save_hint(file_admission, monkeypatch):
    await exercise_protected(file_admission, monkeypatch)


@pytest.mark.asyncio
async def test_adapter_cancellation_keeps_owned_buffer(file_admission, monkeypatch):
    await exercise_cancel(file_admission, monkeypatch)
