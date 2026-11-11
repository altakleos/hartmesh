"""Receipt-owned routing retains SQL and SDK fences under strict I/O."""

import pytest
from _telegram_repository_test_support import db as db
from test_telegram_work_routing_followups import test_followup_route_replays_and_edits_only_its_selection as replay
from test_telegram_work_routing_followups import test_saved_wrong_person_reply_is_the_acknowledged_routing_question as deliver
from test_telegram_work_source_delivery import work_delivery as work_delivery
from test_work_attention_pages import work_pages as work_pages


@pytest.mark.asyncio
async def test_receipt_owned_routing(work_delivery, monkeypatch):
    await deliver(work_delivery, monkeypatch)


@pytest.mark.asyncio
async def test_receipt_owned_routing_replay(work_delivery, monkeypatch):
    await replay(work_delivery, monkeypatch)
