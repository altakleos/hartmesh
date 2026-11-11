"""Receipt observation uses actual domain reads and guarded asynchronous writers."""

import pytest
from _telegram_repository_test_support import db as db
from test_telegram_work_receipt_recovery import test_observer_reconciles_then_plans_without_executing_actions as observe
from test_telegram_work_source_delivery import work_delivery as work_delivery
from test_work_attention_pages import work_pages as work_pages


@pytest.mark.asyncio
async def test_receipt_observation(work_delivery, monkeypatch):
    await observe(work_delivery, monkeypatch)
