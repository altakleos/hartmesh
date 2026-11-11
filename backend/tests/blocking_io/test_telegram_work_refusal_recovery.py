"""Refused confirmation refresh and subsequent action stay off blocking I/O."""

import pytest
from _telegram_repository_test_support import db as db
from test_telegram_work_refusal_recovery import test_changed_refused_confirmation_gets_full_preview_and_new_operation as recovery
from test_telegram_work_source_delivery import work_delivery as work_delivery
from test_work_attention_pages import work_pages as work_pages


@pytest.mark.asyncio
async def test_refusal_preview_and_reconfirmation(work_delivery, monkeypatch):
    await recovery(work_delivery, monkeypatch)
