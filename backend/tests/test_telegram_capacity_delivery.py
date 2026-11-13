"""Busy captions use only owner-filtered HTTP facts at actual SDK boundaries."""

import pytest
from _telegram_repository_test_support import db as db
from test_telegram_capacity_offers import waiting
from test_telegram_capacity_reads import foreign_capacity
from test_telegram_work_preview_recovery import dispatch
from test_telegram_work_source_delivery import work_delivery as work_delivery
from test_work_attention_pages import work_pages as work_pages


async def prepare_busy(e, monkeypatch):
    from app.telegram_bots.work_execution_delivery import WorkExecutionPlans
    from app.telegram_bots.work_execution_sources import WorkExecutionReader
    from app.telegram_bots.work_news_delivery import WorkNewsSources

    _offers, act, _ = await waiting(e, monkeypatch)
    capacity, foreign_id, work_id = await foreign_capacity(e)
    reader = WorkExecutionReader(work=e.human.work, authority=e.live.authority, links=e.db.links, capacity=capacity, clock=e.clock.now)
    plans = WorkExecutionPlans(reader=reader, outbox=e.sender.outbox, callbacks=e.channel._services.callbacks, acts=e.acts)
    e.channel._services.work_execution_sources = WorkNewsSources(plans)
    e.identity = await plans.observe(e.link, act.id)
    return plans, act, foreign_id, work_id


@pytest.mark.asyncio
async def test_capacity_caption_uses_actual_visible_task_and_missing_time(work_delivery, monkeypatch):
    e = work_delivery
    plans, act, _foreign_id, _work_id = await prepare_busy(e, monkeypatch)
    await dispatch(e, 4070)
    assert not e.dispatch_errors, e.dispatch_errors
    text = [c.parameters["text"] for c in e.channel.bot.request.calls if c.endpoint == "sendMessage"][-1]
    assert "Other reviewer" in text and "Review the other guide" in text and "since" not in text
    assert await plans.observe(e.link, act.id) == e.identity and not e.launches


@pytest.mark.asyncio
@pytest.mark.parametrize("change", ["grant", "objective", "person"])
async def test_capacity_caption_never_sends_foreign_facts_changed_at_final_wait(work_delivery, monkeypatch, change):
    from deerflow.persistence.agent_instances.model import AgentInstanceGrantRow
    from deerflow.persistence.agent_instances.work import AgentWorkRow
    from deerflow.persistence.user.model import UserRow

    e = work_delivery
    _plans, _act, foreign_id, work_id = await prepare_busy(e, monkeypatch)
    sources = e.channel._services.work_execution_sources
    real, changed = sources.settings.get, []

    async def retire(*args, **kwargs):
        result = await real(*args, **kwargs)
        if not changed:
            changed.append(True)
            async with e.db.sessions.begin() as writer:
                if change == "grant":
                    (await writer.get(AgentInstanceGrantRow, (foreign_id, e.link.owner_user_id))).permissions = 1
                elif change == "objective":
                    (await writer.get(AgentWorkRow, work_id)).objective = "Changed after capture"
                else:
                    (await writer.get(UserRow, e.link.owner_user_id)).email = "changed-person@example.com"
        return result

    monkeypatch.setattr(sources.settings, "get", retire)
    before = len(e.channel.bot.request.calls)
    await dispatch(e, 4080)
    assert not [c for c in e.channel.bot.request.calls[before:] if c.endpoint == "sendMessage"] and changed and not e.launches
