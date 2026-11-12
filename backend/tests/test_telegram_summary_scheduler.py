"""The dormant scheduler plans due instance summaries without executing Work."""

import pytest
from _telegram_repository_test_support import db as db
from sqlalchemy import func, select
from test_telegram_summary_delivery import prepare
from test_telegram_work_source_delivery import work_delivery as work_delivery
from test_work_attention_pages import work_pages as work_pages

from deerflow.persistence.agent_instances.work import AgentWorkAttemptRow
from deerflow.persistence.telegram_bots.model import TelegramOutboxRow


@pytest.mark.asyncio
async def test_summary_scheduler_ticks_and_restarts_without_new_attempts(work_delivery):
    from app.telegram_bots.summary_scheduler import SummaryScheduler

    e = work_delivery
    plans = await prepare(e)
    calls = len(e.channel.bot.request.calls)
    async with e.db.sessions() as reader:
        before = await reader.scalar(select(func.count()).select_from(AgentWorkAttemptRow))
    assert await SummaryScheduler(plans).tick() == 1
    assert await SummaryScheduler(plans).tick() == 1
    async with e.db.sessions() as reader:
        assert await reader.scalar(select(func.count()).select_from(TelegramOutboxRow).where(TelegramOutboxRow.class_ == "summary")) == 1
        assert await reader.scalar(select(func.count()).select_from(AgentWorkAttemptRow)) == before
    assert len(e.channel.bot.request.calls) == calls


@pytest.mark.asyncio
async def test_summary_scheduler_returns_before_storage_during_hold(work_delivery, monkeypatch):
    from app.telegram_bots.summary_scheduler import SummaryScheduler

    plans = await prepare(work_delivery)
    plans.available = lambda: False

    def forbidden():
        raise AssertionError("storage entered during hold")

    monkeypatch.setattr(plans.outbox, "sessions", forbidden)
    assert await SummaryScheduler(plans).tick() == 0


@pytest.mark.asyncio
async def test_summary_scheduler_continues_after_one_source_is_denied(work_delivery, monkeypatch):
    from dataclasses import replace

    from app.telegram_bots.producer import ProductSourceRetired
    from app.telegram_bots.summary_scheduler import SummaryScheduler

    plans = await prepare(work_delivery)
    scheduler = SummaryScheduler(plans)
    called = []

    async def page(after):
        return [work_delivery.link, replace(work_delivery.link, id="f" * 32)] if not after else []

    async def plan(link):
        called.append(link.id)
        if len(called) == 1:
            raise ProductSourceRetired()
        return "delivery"

    monkeypatch.setattr(scheduler, "_page", page)
    monkeypatch.setattr(plans, "plan", plan)
    assert await scheduler.tick() == 1
    assert len(called) == 2


@pytest.mark.asyncio
async def test_summary_scheduler_does_not_publish_after_hold_begins_during_source_read(work_delivery, monkeypatch):
    from app.telegram_bots.summary_scheduler import SummaryScheduler

    e = work_delivery
    plans = await prepare(e)
    available = [True]
    plans.available = lambda: available[0]
    read = plans.reader.read

    async def held(*args, **kwargs):
        source = await read(*args, **kwargs)
        available[0] = False
        return source

    monkeypatch.setattr(plans.reader, "read", held)
    assert await SummaryScheduler(plans).tick() == 0
    async with e.db.sessions() as reader:
        assert await reader.scalar(select(func.count()).select_from(TelegramOutboxRow).where(TelegramOutboxRow.class_ == "summary")) == 0
