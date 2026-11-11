"""Routing admission borrows real SQL writers without event-loop I/O."""

import pytest
from _telegram_repository_test_support import db as db
from test_telegram_work_routing_admission import test_final_borrowed_writer_fence_rolls_back_reservation as final_fence
from test_telegram_work_routing_admission import test_missing_reserved_journal_never_becomes_a_new_operation as missing_journal
from test_telegram_work_routing_admission import test_route_choice_reserves_current_cas_and_replays_exact_body_during_pause as route_once
from test_telegram_work_source_delivery import work_delivery as work_delivery
from test_work_attention_pages import work_pages as work_pages


@pytest.mark.asyncio
async def test_route_admission(work_delivery, monkeypatch):
    await route_once(work_delivery, monkeypatch)


@pytest.mark.asyncio
async def test_final_recipient_fence(work_delivery, monkeypatch):
    await final_fence(work_delivery, monkeypatch, "disabled")


@pytest.mark.asyncio
async def test_reserved_journal(work_delivery, monkeypatch):
    await missing_journal(work_delivery, monkeypatch)
