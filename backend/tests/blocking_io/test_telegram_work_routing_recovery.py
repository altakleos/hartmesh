"""Routing recovery keeps real SQL and SDK work off the event loop."""

import pytest
from _telegram_repository_test_support import db as db
from test_telegram_work_routing_recovery import test_missing_routing_anchor_repairs_without_repeating_its_edit as repair
from test_telegram_work_routing_recovery import test_routing_show_replaces_only_its_own_protected_preview as reveal
from test_telegram_work_source_delivery import work_delivery as work_delivery
from test_work_attention_pages import work_pages as work_pages


@pytest.mark.asyncio
async def test_routing_reveal(work_delivery, monkeypatch):
    await reveal(work_delivery, monkeypatch)


@pytest.mark.asyncio
async def test_routing_repair(work_delivery, monkeypatch):
    await repair(work_delivery, monkeypatch)
