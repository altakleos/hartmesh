"""Explicit routing refresh retains receipt and retention boundaries under strict I/O."""

import pytest
from _telegram_repository_test_support import db as db
from test_telegram_work_followup_refresh import test_queued_refresh_source_drift_repairs_the_same_explicit_intent as repair
from test_telegram_work_followup_refresh import test_refresh_retains_consumed_source_and_completed_input_through_collection as retention
from test_telegram_work_source_delivery import work_delivery as work_delivery
from test_work_attention_pages import work_pages as work_pages


@pytest.mark.asyncio
async def test_routing_refresh_repair(work_delivery, monkeypatch):
    await repair(work_delivery, monkeypatch)


@pytest.mark.asyncio
async def test_routing_refresh_retention(work_delivery, monkeypatch):
    await retention(work_delivery, monkeypatch)
