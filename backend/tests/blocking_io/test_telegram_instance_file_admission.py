"""The direct Telegram Home write keeps native buffer work off the Gateway loop."""

import asyncio
from zoneinfo import ZoneInfo

import pytest
import pytest_asyncio
from _telegram_repository_test_support import db as db
from test_telegram_instance_file_admission import file_admission as file_admission
from test_telegram_instance_file_admission import test_repeated_cancellation_retains_buffer_until_direct_write_settles as exercise_cancel
from test_telegram_instance_file_admission import test_use_only_put_is_direct_attributed_and_replayable_without_workspace as exercise_put
from test_telegram_instance_publication import publication as publication


@pytest_asyncio.fixture(autouse=True)
async def timezone_precondition():
    await asyncio.to_thread(ZoneInfo, "America/New_York")


@pytest.mark.asyncio
async def test_direct_file_put_and_replay_own_io(file_admission):
    await exercise_put(file_admission)


@pytest.mark.asyncio
async def test_cancelled_file_put_retains_native_work(file_admission, monkeypatch):
    await exercise_cancel(file_admission, monkeypatch)
