"""Explicit Work views reach the actual recorded SDK with their source proofs."""

import pytest
from _telegram_bot_test_support import ManualClock, make_update
from _telegram_repository_test_support import db as db
from sqlalchemy import select
from test_telegram_work_interaction_delivery import guard_for, selected
from test_telegram_work_preview_recovery import dispatch
from test_telegram_work_source_delivery import work_delivery as work_delivery
from test_work_attention_pages import work_pages as work_pages

from deerflow.persistence.telegram_bots.model import TelegramOutboxRow


async def prepare_view(e):
    from app.telegram_bots.work_news_delivery import WorkNewsSources
    from app.telegram_bots.work_view_delivery import WorkViewPlans
    from app.telegram_bots.work_view_sources import WorkViewReader

    e.clock = ManualClock(e.sender.outbox.clock())
    e.sender.outbox.clock = e.clock.now
    e.channel._now = e.clock.now
    e.channel._services.callbacks.clock = e.clock.now
    plans = WorkViewPlans(reader=WorkViewReader(work=e.human.work, authority=e.live.authority, links=e.db.links, clock=e.sender.outbox.clock), outbox=e.sender.outbox, callbacks=e.channel._services.callbacks)
    e.channel._services.work_view_sources = WorkNewsSources(plans)
    async with e.db.sessions.begin() as writer:
        (await writer.get(TelegramOutboxRow, e.identity)).state = "skipped"
    update = make_update(3401, account=e.link.chat_id, text="/work")
    inbox, _ = await e.sender.outbox.inbox.accept(e.link, update, sanitized_input={"command": "work"})
    e.identity = await plans.command(e.link, inbox, guard=await guard_for(e))
    return plans, inbox


@pytest.mark.asyncio
async def test_command_list_and_reusable_details_have_durable_distinct_gestures(work_delivery):
    e = work_delivery
    plans, inbox = await prepare_view(e)
    identity = e.identity
    assert await plans.command(e.link, inbox, guard=await guard_for(e)) == identity
    e.identity = identity
    await dispatch(e, 3402)
    assert not e.dispatch_errors, e.dispatch_errors
    button = await selected(e, "work_details")
    first = await plans.open(e.link, button.id, query_id="one", chat_id=e.link.chat_id, message_id=3402, message_thread_id=None, guard=await guard_for(e))
    assert await plans.open(e.link, button.id, query_id="one", chat_id=e.link.chat_id, message_id=3402, message_thread_id=None, guard=await guard_for(e)) == first
    second = await plans.open(e.link, button.id, query_id="two", chat_id=e.link.chat_id, message_id=3402, message_thread_id=None, guard=await guard_for(e))
    assert second != first
    await dispatch(e, 3403)
    await dispatch(e, 3404)
    assert not e.dispatch_errors, e.dispatch_errors
    messages = [call.parameters["text"] for call in e.channel.bot.request.calls if call.endpoint == "sendMessage"]
    async with e.db.sessions() as reader:
        states = [
            (row.id, row.state, row.failed_reason, row.skip_reason, row.payload["presentation_version"], row.not_before) for row in await reader.scalars(select(TelegramOutboxRow).where(TelegramOutboxRow.id.in_([identity, first, second])))
        ]
    assert len(messages) == 3 and e.work["objective"] in messages[-1] and e.work["success_criteria"] in messages[-1], states


@pytest.mark.asyncio
async def test_explicit_work_list_is_full_and_protected_with_notifications_off(work_delivery):
    from deerflow.persistence.telegram_bots.model import TelegramInstancePolicyRow, TelegramPersonSettingsRow

    e = work_delivery
    async with e.db.sessions.begin() as writer:
        writer.add(TelegramInstancePolicyRow(instance_id=e.instance.id, policy="protected", updated_by=e.link.owner_user_id, updated_at=e.sender.outbox.clock()))
        (await writer.get(TelegramPersonSettingsRow, e.link.owner_user_id)).notify = "none"
    await prepare_view(e)
    await dispatch(e, 3411)
    assert not e.dispatch_errors, e.dispatch_errors
    calls = [call.parameters for call in e.channel.bot.request.calls if call.endpoint == "sendMessage"]
    assert len(calls) == 1 and e.work["objective"] in calls[0]["text"] and calls[0]["protect_content"] is True


@pytest.mark.asyncio
@pytest.mark.parametrize("change", ["inbox", "snapshot", "inspect"])
async def test_final_sdk_work_view_check_rejects_changed_origin_and_authority(work_delivery, monkeypatch, change):
    from deerflow.persistence.agent_instances.model import AgentInstanceGrantRow
    from deerflow.persistence.telegram_bots.model import TelegramCallbackRow, TelegramInboundMarkRow

    e = work_delivery
    plans, inbox = await prepare_view(e)
    sources = e.channel._services.work_view_sources
    real = sources.settings.get
    changed = []

    async def retire(*args, **kwargs):
        settings = await real(*args, **kwargs)
        if not changed:
            changed.append(True)
            async with e.db.sessions.begin() as writer:
                if change == "inbox":
                    (await writer.get(TelegramInboundMarkRow, (inbox.link_id, inbox.message_id, inbox.chat_id))).sanitized_input = {"command": "help"}
                elif change == "snapshot":
                    parent = await writer.get(TelegramOutboxRow, e.identity)
                    token = parent.payload["work_view_intent"]["cursor"]
                    snapshot = await writer.get(TelegramCallbackRow, token)
                    snapshot.payload = {**snapshot.payload, "work_ids": []}
                else:
                    (await writer.get(AgentInstanceGrantRow, (e.instance.id, e.link.owner_user_id))).permissions = 1
        return settings

    monkeypatch.setattr(sources.settings, "get", retire)
    await dispatch(e, 3421)
    assert changed and not [call for call in e.channel.bot.request.calls if call.endpoint == "sendMessage"]


@pytest.mark.asyncio
async def test_admitted_navigation_replay_and_delivery_keep_its_archived_full_ack(work_delivery):
    from app.telegram_bots.producer import _item
    from app.telegram_bots.work_result_navigation import NavigationOrigin

    e = work_delivery
    plans, _ = await prepare_view(e)
    await dispatch(e, 3431)
    button = await selected(e, "work_details")
    args = dict(query_id="historical-query", chat_id=e.link.chat_id, message_id=3431, message_thread_id=None)
    identity = await plans.open(e.link, button.id, **args, guard=await guard_for(e))
    async with e.db.sessions() as reader:
        item = _item(await reader.get(TelegramOutboxRow, e.identity))
    source = await plans.reader.read(e.link, item)
    settings = await plans.settings.get(e.link.owner_user_id)
    sources = e.channel._services.work_view_sources
    async with e.sender.outbox._writer(e.link.id) as (writer, _):
        row = await writer.get(TelegramOutboxRow, e.identity, with_for_update=True)
        await sources.recovery._replace(writer, e.link, row, source, settings, NavigationOrigin())
    # Current controls are retired. The saved gesture remains a receipt of the
    # original complete acknowledged presentation; a replay is no new gesture.
    assert await plans.open(e.link, button.id, **args, guard=await guard_for(e)) == identity
    async with e.db.sessions() as reader:
        child = _item(await reader.get(TelegramOutboxRow, identity))
    assert (await plans.reader.read(e.link, child)).work["detail"]["record"]["id"] == e.work["id"]


@pytest.mark.asyncio
async def test_complete_maximum_detail_precedes_its_armed_controls(work_delivery):
    from app.telegram_bots.callbacks import CallbackRefused
    from app.telegram_bots.split import visible_text
    from deerflow.persistence.agent_instances.work import AgentWorkRow
    from deerflow.persistence.telegram_bots.model import TelegramCallbackRow

    e = work_delivery
    objective, criteria = "<>&Ω" * 1024, "✓<>&" * 1024
    async with e.db.sessions.begin() as writer:
        work = await writer.get(AgentWorkRow, e.work["id"])
        work.objective, work.success_criteria = objective, criteria
    plans, _ = await prepare_view(e)
    await dispatch(e, 3441)
    button = await selected(e, "work_details")
    identity = await plans.open(e.link, button.id, query_id="complete-detail", chat_id=e.link.chat_id, message_id=3441, message_thread_id=None, guard=await guard_for(e))
    async with e.db.sessions() as reader:
        row = await reader.get(TelegramOutboxRow, identity)
        parts = row.parts
        control = await reader.scalar(select(TelegramCallbackRow).where(TelegramCallbackRow.group_id == parts[-1]["callback_group_id"]))
    assert len(parts) > 1 and objective in "".join(visible_text(part["html"]) for part in parts)
    assert criteria in "".join(visible_text(part["html"]) for part in parts)
    await dispatch(e, 3442)
    guard = await guard_for(e)
    with pytest.raises(CallbackRefused):
        async with plans.callbacks.writer(e.link, guard) as writer:
            await plans.callbacks.lock_current(e.link, control.id, chat_id=e.link.chat_id, message_id=3442, message_thread_id=None, guard=guard, session=writer)
    for index in range(1, len(parts)):
        await dispatch(e, 3442 + index)
    assert not e.dispatch_errors, e.dispatch_errors
    guard = await guard_for(e)
    async with plans.callbacks.writer(e.link, guard) as writer:
        group = await plans.callbacks.lock_current(e.link, control.id, chat_id=e.link.chat_id, message_id=3441 + len(parts), message_thread_id=None, guard=guard, session=writer)
        assert group.selected.message_id == 3441 + len(parts)
