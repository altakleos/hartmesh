"""Daily persistence and final summary admission stay off the event loop."""

import pytest
from _telegram_repository_test_support import db as db
from test_telegram_summary_delivery import test_daily_summary_is_deduplicated_and_delivered_silently as sends
from test_telegram_summary_delivery import test_summary_final_provider_fence_prevents_stale_sdk_dispatch as fences
from test_telegram_work_source_delivery import work_delivery as work_delivery
from test_work_attention_pages import work_pages as work_pages


@pytest.mark.asyncio
async def test_summary_delivery(work_delivery):
    await sends(work_delivery)


@pytest.mark.asyncio
async def test_summary_delivery_final_claim(work_delivery, monkeypatch):
    await fences(work_delivery, monkeypatch, "claim")


@pytest.mark.asyncio
async def test_summary_ack_preserves_actual_wire_group(work_delivery):
    from test_telegram_summary_delivery import test_summary_ack_cannot_activate_replaced_wire_group

    await test_summary_ack_cannot_activate_replaced_wire_group(work_delivery)
