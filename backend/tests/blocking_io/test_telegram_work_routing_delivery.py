"""Source-owned routing exercises real SQL and SDK without event-loop I/O."""

import pytest
from _telegram_repository_test_support import db as db
from test_telegram_work_routing_delivery import test_final_routing_snapshot_retires_changed_facts as retire
from test_telegram_work_routing_delivery import test_routing_delivery_uses_saved_recipients_and_arms_only_after_ack as deliver
from test_telegram_work_source_delivery import work_delivery as work_delivery
from test_work_attention_pages import work_pages as work_pages


@pytest.mark.asyncio
async def test_routing_delivery(work_delivery, monkeypatch):
    await deliver(work_delivery, monkeypatch)


@pytest.mark.asyncio
async def test_routing_recipient_revocation(work_delivery, monkeypatch):
    await retire(work_delivery, monkeypatch, "disabled")


@pytest.mark.asyncio
@pytest.mark.parametrize("count", [0, 8])
async def test_routing_navigation(work_delivery, monkeypatch, count):
    from test_telegram_work_routing_delivery import test_routing_navigation_never_loads_configuration_in_rendering

    await test_routing_navigation_never_loads_configuration_in_rendering(work_delivery, monkeypatch, count)
