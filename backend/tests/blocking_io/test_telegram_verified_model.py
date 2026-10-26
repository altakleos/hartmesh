"""Whole pinned transport protocol; source DTOs are synthetic, not AI proof."""

import asyncio
from zoneinfo import ZoneInfo

import pytest
import pytest_asyncio
from _telegram_repository_test_support import db as db  # noqa: F401
from test_telegram_model_delivery import test_actual_sender_rereads_each_model_part_and_preserves_prefix as run_sender
from test_telegram_publication_custody import publication_environment as publication_environment


@pytest_asyncio.fixture(autouse=True)
async def timezone_fixture_precondition():
    # Same real repository fixture precondition as the existing strict managed
    # publication case; send-time timezone work remains in the actual adapter.
    await asyncio.to_thread(ZoneInfo, "America/New_York")


@pytest.mark.asyncio
@pytest.mark.parametrize("mode", ["valid", "changed-after-prefix", "retired-token"])
async def test_verified_model_sender_owns_cold_codecs_and_current_reads(publication_environment, monkeypatch, mode):
    await run_sender(publication_environment, monkeypatch, mode)
