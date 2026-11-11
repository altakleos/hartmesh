"""Instance attachment publication keeps SQL and SDK reads off the event loop."""

import asyncio
from zoneinfo import ZoneInfo

import pytest
import pytest_asyncio
from _telegram_repository_test_support import db as db
from test_telegram_instance_file_admission import file_admission as file_admission
from test_telegram_instance_file_publication import test_actual_manager_sends_composer_files_in_human_message_without_upload as exercise_manager
from test_telegram_instance_file_publication import test_final_dispatch_writer_rechecks_composed_receipt as exercise_writer
from test_telegram_instance_file_publication import test_queued_instance_file_cannot_rebind_path_before_run_post as exercise_post
from test_telegram_instance_publication import publication as publication


@pytest_asyncio.fixture(autouse=True)
async def timezone_precondition():
    await asyncio.to_thread(ZoneInfo, "America/New_York")


@pytest.mark.asyncio
async def test_instance_attachment_through_manager_and_sdk(file_admission, monkeypatch, tmp_path):
    await exercise_manager(file_admission, monkeypatch, tmp_path)


@pytest.mark.asyncio
async def test_final_dispatch_writer_holds_original_home(file_admission, monkeypatch):
    await exercise_writer(file_admission, monkeypatch, "home")


@pytest.mark.asyncio
async def test_actual_run_post_retains_original_home(file_admission, monkeypatch, tmp_path):
    await exercise_post(file_admission, monkeypatch, tmp_path, "home_id", "http")
