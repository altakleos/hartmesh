"""Routing results retain real receipt and SDK boundaries under strict I/O."""

import pytest
from _telegram_repository_test_support import db as db
from test_telegram_work_routing_outcomes import test_completed_route_edits_original_selection_and_records_ack as deliver
from test_telegram_work_routing_outcomes import test_missing_routing_result_anchor_replans_receipt_without_another_command as repair
from test_telegram_work_source_delivery import work_delivery as work_delivery
from test_work_attention_pages import work_pages as work_pages


@pytest.mark.asyncio
async def test_routing_result_edit(work_delivery, monkeypatch):
    await deliver(work_delivery, monkeypatch)


@pytest.mark.asyncio
async def test_routing_result_repair(work_delivery, monkeypatch):
    await repair(work_delivery, monkeypatch)
