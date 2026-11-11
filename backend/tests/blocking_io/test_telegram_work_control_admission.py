"""Acknowledged Work control admission and receipt execution use async I/O."""

import pytest
from _telegram_repository_test_support import db as db
from test_telegram_work_control_admission import test_second_tap_stages_exact_acceptance_then_worker_applies_once as acceptance
from test_telegram_work_source_delivery import work_delivery as work_delivery
from test_work_attention_pages import work_pages as work_pages


@pytest.mark.asyncio
async def test_confirmed_control_and_receipt_worker(work_delivery):
    await acceptance(work_delivery)
