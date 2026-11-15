"""Summary navigation owns actual acknowledged buttons and starts no work."""

from uuid import uuid4

import pytest
from _telegram_bot_test_support import ManualClock
from _telegram_repository_test_support import db as db
from sqlalchemy import select
from test_telegram_bot_adapter import message
from test_telegram_summary_delivery import prepare
from test_telegram_work_interaction_delivery import guard_for
from test_telegram_work_source_delivery import work_delivery as work_delivery
from test_work_attention_pages import work_pages as work_pages

from deerflow.agent_instances.work_contract import DelegateWork
from deerflow.persistence.telegram_bots.model import TelegramActRow, TelegramCallbackRow, TelegramOutboxRow


async def summary(e, protected=False):
    from app.telegram_bots.work_news_delivery import WorkNewsSources
    from app.telegram_bots.work_view_delivery import WorkViewPlans
    from app.telegram_bots.work_view_sources import WorkViewReader

    created = []
    for index in range(6):
        created.append(await e.human.work.delegate(actor=e.actor, instance_id=e.instance.id, request=DelegateWork(operation_id=uuid4().hex, objective=f"Check reference {index}", success_criteria="Describe verified evidence")))
    clock = ManualClock(e.sender.outbox.clock())
    e.sender.outbox.clock = clock.now
    e.channel._now = clock.now
    e.channel._services.callbacks.clock = clock.now
    plans = await prepare(e)
    if protected:
        from test_telegram_digest_item_families import protect

        await protect(e)
    identity = await plans.plan(e.link)
    message(e.channel.bot.request, identity=7801)
    assert await e.sender.tick() == 1
    await e.sender.drain()
    if protected:
        from app.telegram_bots.summary_views import SummaryViews, SummaryViewSources

        shown = SummaryViews(plans)
        e.channel._services.summary_view_sources = SummaryViewSources(shown)
        async with e.db.sessions() as reader:
            teaser = await reader.get(TelegramOutboxRow, identity)
            reveal = await reader.scalar(select(TelegramCallbackRow).where(TelegramCallbackRow.group_id == teaser.parts[-1]["callback_group_id"], TelegramCallbackRow.kind == "reveal"))
        identity = await shown.reveal(e.link, reveal.id, chat_id=e.link.chat_id, message_id=7801, message_thread_id=None, guard=await guard_for(e))
        clock.advance(10)
        message(e.channel.bot.request, identity=7801, endpoint="editMessageText")
        assert await e.sender.tick() == 1
        await e.sender.drain()
    async with e.db.sessions() as reader:
        parent = await reader.get(TelegramOutboxRow, identity)
        assert parent.state == "sent", e.dispatch_errors
        controls = list(await reader.scalars(select(TelegramCallbackRow).where(TelegramCallbackRow.group_id == parent.parts[-1]["callback_group_id"])))
        details = [control for control in controls if control.kind == "work_details"]
        assert len(details) == 4
        overflow = next(control for control in controls if control.kind == "work_list_open")
        assert len(controls) == 7 and all(control.message_id == 7801 for control in controls)
    views = WorkViewPlans(reader=WorkViewReader(work=e.human.work, authority=e.live.authority, links=e.db.links, clock=e.sender.outbox.clock), outbox=e.sender.outbox, callbacks=plans.callbacks)
    e.channel._services.work_view_sources = WorkNewsSources(views)
    return created, clock, plans, identity, details, overflow, views


@pytest.mark.asyncio
@pytest.mark.parametrize("protected", [False, True])
async def test_summary_start_items_and_overflow_open_current_native_views(work_delivery, protected):
    e = work_delivery
    created, clock, _plans, _identity, details, overflow, views = await summary(e, protected)
    for index, control in enumerate([details[0], overflow]):
        args = dict(query_id=f"summary-view-{index}", chat_id=e.link.chat_id, message_id=7801, message_thread_id=None)
        target = await views.open(e.link, control.id, **args, guard=await guard_for(e))
        assert await views.open(e.link, control.id, **args, guard=await guard_for(e)) == target
        clock.advance(10)
        message(e.channel.bot.request, identity=7802 + index)
        assert await e.sender.tick() == 1
        await e.sender.drain()
        async with e.db.sessions() as reader:
            row = await reader.get(TelegramOutboxRow, target)
            assert row.state == "sent", (e.dispatch_errors, row.parts, row.failed_reason, row.skip_reason, row.not_before)
            assert row.payload["work_view_intent"]["key"] == ("view.detail" if index == 0 else "view.list")
            if index == 0:
                assert row.work_id == control.payload["work_id"]
                assert "Describe verified evidence" in row.parts[0]["visible_text"]
            else:
                assert row.payload["work_news_basis"]["source"]["work"]["page"]["frame"]["page"] == 1
            assert not list(await reader.scalars(select(TelegramActRow)))
    for record in created:
        assert (await e.human.work.get(actor=e.actor, instance_id=e.instance.id, work_id=record["id"]))["attempt"] is None


@pytest.mark.asyncio
async def test_interrupted_summary_navigation_repairs_after_original_group_retirement(work_delivery, monkeypatch):
    e = work_delivery
    _created, clock, plans, identity, details, _overflow, views = await summary(e)
    button = details[0]
    args = dict(query_id="interrupted-summary", chat_id=e.link.chat_id, message_id=7801, message_thread_id=None)
    materialize = views.materialize

    async def interrupt(*_args, **_kwargs):
        raise RuntimeError("interrupted after saved gesture")

    monkeypatch.setattr(views, "materialize", interrupt)
    with pytest.raises(RuntimeError, match="after saved gesture"):
        await views.open(e.link, button.id, **args, guard=await guard_for(e))
    monkeypatch.setattr(views, "materialize", materialize)
    async with plans.callbacks.writer(e.link, await guard_for(e)) as writer:
        group = await plans.callbacks.lock_current(e.link, button.id, chat_id=e.link.chat_id, message_id=7801, message_thread_id=None, guard=await guard_for(e), session=writer)
        await plans.callbacks.consume_group(group, reason="superseded", session=writer)
    target = await views.open(e.link, button.id, **args, guard=await guard_for(e))
    for sdk_id in (7811,):
        clock.advance(10)
        message(e.channel.bot.request, identity=sdk_id)
        assert await e.sender.tick() == 1
        await e.sender.drain()
    async with e.db.sessions() as reader:
        assert (await reader.get(TelegramOutboxRow, target)).state == "sent", e.dispatch_errors
        assert (await reader.get(TelegramCallbackRow, button.id)).closed_reason == "superseded"
        assert not list(await reader.scalars(select(TelegramActRow)))


@pytest.mark.asyncio
@pytest.mark.parametrize("change", ["retire", "spent", "expired", "topic"])
async def test_overflow_snapshot_allocation_keeps_the_original_press_fences(work_delivery, monkeypatch, change):
    from dataclasses import replace
    from datetime import timedelta

    from app.telegram_bots.callbacks import CallbackRefused

    e = work_delivery
    _created, clock, _plans, _identity, _details, button, views = await summary(e)
    retired = []
    guard = replace(await guard_for(e), _attempt=lambda: not retired)
    collect = views.reader.collect
    ran = []

    async def mutate(*args, **kwargs):
        value = await collect(*args, **kwargs)
        ran.append(True)
        if change == "retire":
            retired.append(True)
        else:
            async with e.db.sessions.begin() as writer:
                control = await writer.get(TelegramCallbackRow, button.id)
                if change == "spent":
                    control.consumed_at, control.closed_reason = clock.now(), "superseded"
                elif change == "expired":
                    control.expires_at = clock.now() - timedelta(seconds=1)
                else:
                    control.message_thread_id = 99
        return value

    monkeypatch.setattr(views.reader, "collect", mutate)
    with pytest.raises((CallbackRefused, ValueError)):
        await views.open(e.link, button.id, query_id="withdraw-during-collection", chat_id=e.link.chat_id, message_id=7801, message_thread_id=None, guard=guard)
    assert ran
    async with e.db.sessions() as reader:
        assert not list(await reader.scalars(select(TelegramCallbackRow).where(TelegramCallbackRow.kind.in_(("work_list_snapshot", "work_view_gesture")))))
        assert not list(await reader.scalars(select(TelegramOutboxRow).where(TelegramOutboxRow.dedupe_key.like("notice:work-view:callback:%"))))


@pytest.mark.asyncio
@pytest.mark.parametrize("change", ["callback", "show", "ack", "inspect"])
async def test_summary_work_view_final_sdk_fence_keeps_actual_source_and_access(work_delivery, monkeypatch, change):
    from copy import deepcopy

    from app.gateway import authz
    from deerflow.persistence.agent_instances.model import AgentInstanceGrantRow

    e = work_delivery
    _created, clock, _plans, identity, details, _overflow, views = await summary(e, change in {"show", "ack"})
    button = details[0]
    await views.open(e.link, button.id, query_id="source-fence", chat_id=e.link.chat_id, message_id=7801, message_thread_id=None, guard=await guard_for(e))
    sources = e.channel._services.work_view_sources
    authorize, resolve = sources.authorize, authz.resolve_route_permissions
    armed, changed = [], []

    async def final(*args, **kwargs):
        armed.append(True)
        return await authorize(*args, **kwargs)

    async def provider(*args, **kwargs):
        result = await resolve(*args, **kwargs)
        if armed and not changed:
            async with e.db.sessions.begin() as writer:
                if change == "callback":
                    await writer.delete(await writer.get(TelegramCallbackRow, button.id))
                elif change == "inspect":
                    (await writer.get(AgentInstanceGrantRow, (e.link.agent_ref, e.link.owner_user_id))).permissions = 1
                else:
                    anchor = await writer.get(TelegramOutboxRow, identity)
                    proof = anchor.payload["summary_view_basis"]
                    if change == "show":
                        await writer.delete(await writer.get(TelegramCallbackRow, proof["callback_id"]))
                    else:
                        root = await writer.get(TelegramOutboxRow, proof["parent_id"])
                        parts = deepcopy(root.parts)
                        parts[-1]["message_ids"] = []
                        root.parts = parts
            changed.append(True)
        return result

    monkeypatch.setattr(sources, "authorize", final)
    monkeypatch.setattr(authz, "resolve_route_permissions", provider)
    before = len([call for call in e.channel.bot.request.calls if call.endpoint in {"sendMessage", "editMessageText"}])
    clock.advance(10)
    assert await e.sender.tick() == 1
    await e.sender.drain()
    assert armed and changed
    assert len([call for call in e.channel.bot.request.calls if call.endpoint in {"sendMessage", "editMessageText"}]) == before
    async with e.db.sessions() as reader:
        assert not list(await reader.scalars(select(TelegramActRow)))
