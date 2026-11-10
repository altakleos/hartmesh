"""Instance structured-error recovery and Inspect-only delivery under strict I/O."""

import asyncio
from zoneinfo import ZoneInfo

import pytest
import pytest_asyncio
from _telegram_repository_test_support import db as db
from test_telegram_instance_error_frames import test_recovery_reads_original_error_stream_and_plans_once_without_dispatch as exercise_recovery
from test_telegram_instance_publication import publication as publication
from test_telegram_instance_run_outcomes import test_manager_full_files_sdk_keeps_files_and_retry_controls as exercise_files


@pytest_asyncio.fixture(autouse=True)
async def timezone_precondition():
    await asyncio.to_thread(ZoneInfo, "America/New_York")


@pytest.mark.asyncio
@pytest.mark.parametrize("name,reason,key", [("ResidentInterrupted", "storage-recovery", "refused.interrupted_recovery"), ("Refusal", "managers-only", "net.blocked_ask")])
async def test_instance_error_recovery_owns_its_io(publication, name, reason, key):
    await exercise_recovery(publication, name, reason, key)


@pytest.mark.asyncio
async def test_recovery_files_delivery_owns_its_io(publication):
    await exercise_files(publication, recovery=True, url_rejected=False, withdraw=None, remaining=3, outcome="recovery")


@pytest.mark.asyncio
async def test_repeated_start_guidance_owns_its_writer_io(publication):
    from test_telegram_start_failure_history import test_second_failure_guidance_is_durable_and_replay_does_not_recount

    await test_second_failure_guidance_is_durable_and_replay_does_not_recount(publication, "success")
