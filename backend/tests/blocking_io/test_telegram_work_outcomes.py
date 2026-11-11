"""Canonical Work outcome reads retain asynchronous source and authority checks."""

import pytest
from _telegram_repository_test_support import db as db
from test_telegram_work_outcome_reads import test_completed_acceptance_reads_exact_receipt_without_repeating_it as completed
from test_telegram_work_source_delivery import work_delivery as work_delivery
from test_work_attention_pages import work_pages as work_pages


@pytest.mark.asyncio
async def test_outcome_receipt_without_command(work_delivery, monkeypatch):
    await completed(work_delivery, monkeypatch)
