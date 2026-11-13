"""Execution replies retain their acknowledged original Telegram topic."""

from unittest.mock import AsyncMock

import pytest
from _telegram_repository_test_support import db as db
from sqlalchemy import select
from test_telegram_work_execution_outcomes import busy_notice
from test_telegram_work_interaction_delivery import guard_for, selected
from test_telegram_work_preview_recovery import dispatch
from test_telegram_work_source_delivery import work_delivery as work_delivery
from test_work_attention_pages import work_pages as work_pages

from app.telegram_bots.acts import Acts
from app.telegram_bots.work_execution_delivery import WorkExecutionPlans
from app.telegram_bots.work_execution_sources import WorkExecutionReader
from app.telegram_bots.work_news_delivery import WorkNewsSources
from app.telegram_bots.work_start_controls import WorkStartAdmissions
from deerflow.agent_instances.contract import Refusal
from deerflow.persistence.telegram_bots.model import TelegramCallbackRow, TelegramOutboxRow


@pytest.mark.asyncio
async def test_own_execution_reply_keeps_the_real_original_ack_topic(work_delivery, monkeypatch):
    import test_telegram_work_view_delivery as views
    from test_telegram_work_start import prepare_startable

    e = work_delivery
    topic = 719
    e.acts = Acts(e.db.sessions, settings=e.db.settings, callbacks=e.channel._services.callbacks, outbox=e.sender.outbox, work=e.human.work, authority=e.live.authority, links=e.db.links)
    await prepare_startable(e, monkeypatch)
    make_update = views.make_update
    monkeypatch.setattr(views, "make_update", lambda *args, **kwargs: make_update(*args, **{**kwargs, "topic": topic}))
    plans, _ = await views.prepare_view(e)
    await dispatch(e, 3840)
    async with e.db.sessions() as reader:
        details = await reader.scalar(select(TelegramCallbackRow).where(TelegramCallbackRow.kind == "work_details", TelegramCallbackRow.target_id == e.work["id"]))
    assert details is not None
    e.identity = await plans.open(e.link, details.id, query_id="topic-start", chat_id=e.link.chat_id, message_id=3840, message_thread_id=topic, guard=await guard_for(e))
    await dispatch(e, 3841)
    start = await selected(e, "work_start")
    act = await WorkStartAdmissions(plans, e.acts).stage(e.link, start.id, chat_id=e.link.chat_id, message_id=3841, message_thread_id=topic, guard=await guard_for(e))
    monkeypatch.setattr("app.gateway.work_execution.activate_work", AsyncMock(side_effect=Refusal("capacity", reason="busy")))
    assert (await e.acts.check(act.id)).state == "waiting"
    outcomes = WorkExecutionPlans(reader=WorkExecutionReader(work=e.human.work, authority=e.live.authority, links=e.db.links, clock=e.clock.now), outbox=e.sender.outbox, callbacks=e.channel._services.callbacks, acts=e.acts)
    e.channel._services.work_execution_sources = WorkNewsSources(outcomes)
    e.identity = await outcomes.observe(e.link, act.id)
    async with e.db.sessions() as reader:
        row = await reader.get(TelegramOutboxRow, e.identity)
        assert row.message_thread_id == topic
    before = len(e.channel.bot.request.calls)
    await dispatch(e, 3842)
    calls = [call.parameters for call in e.channel.bot.request.calls[before:] if call.endpoint == "sendMessage"]
    assert calls and all(call["message_thread_id"] == topic for call in calls)


@pytest.mark.asyncio
async def test_late_original_ack_topic_change_does_not_retarget_saved_reply(work_delivery, monkeypatch):
    e = work_delivery
    _plans, act = await busy_notice(e, monkeypatch)
    async with e.db.sessions.begin() as writer:
        callback = await writer.get(TelegramCallbackRow, act.id)
        parent = await writer.get(TelegramOutboxRow, callback.payload["anchor_outbox_id"])
        parent.message_thread_id = 722
        for control in await writer.scalars(select(TelegramCallbackRow).where(TelegramCallbackRow.group_id == callback.group_id)):
            control.message_thread_id = 722
    before = len(e.channel.bot.request.calls)
    await dispatch(e, 3843)
    assert not [call for call in e.channel.bot.request.calls[before:] if call.endpoint == "sendMessage"]
