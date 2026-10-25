"""Actual managed SQL, worker source and primary planning under strict I/O checks."""

import asyncio
from zoneinfo import ZoneInfo

import pytest
import pytest_asyncio
from _telegram_repository_test_support import db as db
from test_telegram_publication_composition import test_managed_primary_consumes_actual_worker_proof_and_completes_original_group as _exercise_managed_publication
from test_telegram_publication_custody import publication_environment as publication_environment


@pytest_asyncio.fixture(autouse=True)
async def timezone_fixture_precondition():
    # The repository fixture creates a New York owner. Load its timezone on a
    # worker before fixture validation, as the existing strict Telegram tests do.
    await asyncio.to_thread(ZoneInfo, "America/New_York")


@pytest.mark.asyncio
async def test_real_managed_publication_and_worker_source_stay_nonblocking(publication_environment, monkeypatch):
    await _exercise_managed_publication(publication_environment, monkeypatch)
