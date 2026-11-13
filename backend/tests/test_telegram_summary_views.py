"""Explicit Show qualifies a protected summary without resending its daily row."""

import pytest
from _telegram_bot_test_support import ManualClock
from _telegram_repository_test_support import db as db
from sqlalchemy import select
from test_telegram_bot_adapter import message
from test_telegram_digest_item_families import protect
from test_telegram_summary_delivery import prepare
from test_telegram_work_source_delivery import work_delivery as work_delivery
from test_work_attention_pages import work_pages as work_pages

from deerflow.persistence.telegram_bots.model import TelegramCallbackRow, TelegramOutboxRow, TelegramPersonSettingsRow


async def teaser(e):
    plans = await prepare(e)
    clock = ManualClock(plans.outbox.clock())
    plans.outbox.clock = clock.now
    e.channel._now = clock.now
    await protect(e)
    identity = await plans.plan(e.link)
    message(e.channel.bot.request, identity=7501)
    assert await e.sender.tick() == 1
    await e.sender.drain()
    async with e.db.sessions() as reader:
        row = await reader.get(TelegramOutboxRow, identity)
        assert row.state == "sent", e.dispatch_errors
        control = await reader.scalar(select(TelegramCallbackRow).where(TelegramCallbackRow.group_id == row.parts[-1]["callback_group_id"]))
        callback = control.id
    return plans, clock, identity, callback


@pytest.mark.asyncio
@pytest.mark.parametrize("summary_enabled", [True, False])
async def test_summary_show_edits_protected_teaser_with_full_current_navigation(work_delivery, summary_enabled):
    from app.telegram_bots.summary_views import SummaryViews, SummaryViewSources

    e = work_delivery
    plans, clock, parent, callback = await teaser(e)
    if not summary_enabled:
        async with e.db.sessions.begin() as writer:
            (await writer.get(TelegramPersonSettingsRow, e.link.owner_user_id)).summary_enabled = False
    views = SummaryViews(plans)
    e.channel._services.summary_view_sources = SummaryViewSources(views)
    live = await e.live.authority.prepare_live_owner(e.link.owner_user_id)
    guard = e.db.links.publication_guard(e.link, check_live_owner=live, check_attempt_current=lambda: True)
    identity = await views.reveal(e.link, callback, chat_id=e.link.chat_id, message_id=7501, message_thread_id=None, guard=guard)
    assert await views.reveal(e.link, callback, chat_id=e.link.chat_id, message_id=7501, message_thread_id=None, guard=guard) == identity
    e.channel.bot.request.enqueue("editMessageText", result={"message_id": 7501, "date": 1893456000, "chat": {"id": e.link.chat_id, "type": "private"}, "text": "Full summary"})
    clock.advance(2)
    assert await e.sender.tick() == 1
    await e.sender.drain()
    async with e.db.sessions() as reader:
        original = await reader.get(TelegramOutboxRow, parent)
        row = await reader.get(TelegramOutboxRow, identity)
        assert original.state == "sent" and original.payload["summary_basis"]["concealed"] is True
        assert row.state == "sent" and row.class_ == "reply", e.dispatch_errors
        assert row.parts[-1]["protect_content"] is True
        controls = list(await reader.scalars(select(TelegramCallbackRow).where(TelegramCallbackRow.group_id == row.parts[-1]["callback_group_id"])))
        assert {control.kind for control in controls} == {"digest_item", "summary_off"}
        assert all(control.created_at == original.created_at and control.message_id == 7501 for control in controls)
    call = next(call for call in e.channel.bot.request.calls if call.endpoint == "editMessageText")
    assert e.work["objective"] in call.parameters["text"]


async def pending_view(e):
    from app.telegram_bots.summary_views import SummaryViews, SummaryViewSources

    plans, clock, parent, callback = await teaser(e)
    views = SummaryViews(plans)
    e.channel._services.summary_view_sources = SummaryViewSources(views)
    live = await e.live.authority.prepare_live_owner(e.link.owner_user_id)
    guard = e.db.links.publication_guard(e.link, check_live_owner=live, check_attempt_current=lambda: True)
    identity = await views.reveal(e.link, callback, chat_id=e.link.chat_id, message_id=7501, message_thread_id=None, guard=guard)
    return views, clock, parent, callback, identity


@pytest.mark.asyncio
async def test_pending_summary_show_retains_expired_origin_without_rearming_it(work_delivery):
    from datetime import timedelta

    from app.telegram_bots.callbacks import CallbackRefused
    from app.telegram_bots.retention import _sweep_outbox

    e = work_delivery
    views, clock, parent, callback, identity = await pending_view(e)
    clock.advance(timedelta(days=views.callbacks.config.callbacks.max_age_days + 1).total_seconds())
    await views.callbacks.sweep(clock.now())
    await _sweep_outbox(e.db.sessions, clock.now(), views.callbacks.config.callbacks.max_age_days)
    async with e.db.sessions() as reader:
        assert await reader.get(TelegramOutboxRow, identity) is not None
        assert await reader.get(TelegramOutboxRow, parent) is not None
        control = await reader.get(TelegramCallbackRow, callback)
        assert control is not None and control.closed_reason == "revealed"
        with pytest.raises(CallbackRefused):
            views.callbacks._usable(control)


@pytest.mark.asyncio
@pytest.mark.parametrize("change", ["parent", "callback", "claim", "grant", "source"])
async def test_summary_show_final_provider_snapshot_prevents_stale_edit(work_delivery, monkeypatch, change):
    from app.gateway import authz
    from deerflow.persistence.agent_instances.model import AgentInstanceGrantRow
    from deerflow.persistence.agent_instances.work import AgentWorkRow

    e = work_delivery
    views, clock, parent, callback, identity = await pending_view(e)
    sources = e.channel._services.summary_view_sources
    authorize, resolve = sources.authorize, authz.resolve_route_permissions
    armed, changed = [], []

    async def final(*args, **kwargs):
        armed.append(True)
        return await authorize(*args, **kwargs)

    async def provider(*args, **kwargs):
        permissions = await resolve(*args, **kwargs)
        if armed and not changed:
            async with e.db.sessions.begin() as writer:
                if change == "parent":
                    (await writer.get(TelegramOutboxRow, parent)).message_id = 9999
                elif change == "callback":
                    (await writer.get(TelegramCallbackRow, callback)).closed_reason = "superseded"
                elif change == "claim":
                    (await writer.get(TelegramOutboxRow, identity)).claim_id = "foreign"
                elif change == "grant":
                    (await writer.get(AgentInstanceGrantRow, (e.link.agent_ref, e.link.owner_user_id))).permissions = 1
                else:
                    (await writer.get(AgentWorkRow, e.work["id"])).objective = "Changed during Show admission"
            changed.append(True)
        return permissions

    monkeypatch.setattr(sources, "authorize", final)
    monkeypatch.setattr(authz, "resolve_route_permissions", provider)
    before = len(e.channel.bot.request.calls)
    clock.advance(2)
    assert await e.sender.tick() == 1
    await e.sender.drain()
    assert changed and len(e.channel.bot.request.calls) == before


@pytest.mark.asyncio
async def test_summary_show_ack_does_not_arm_a_control_changed_during_sdk_response(work_delivery):
    import asyncio

    e = work_delivery
    _views, clock, _parent, _callback, identity = await pending_view(e)
    release = asyncio.Event()
    call = e.channel.bot.request.enqueue("editMessageText", release=release, result={"message_id": 7501, "date": 1893456000, "chat": {"id": e.link.chat_id, "type": "private"}, "text": "Full summary"})
    clock.advance(2)
    assert await e.sender.tick() == 1
    try:
        await asyncio.wait_for(call.started.wait(), 5)
        async with e.db.sessions.begin() as writer:
            row = await writer.get(TelegramOutboxRow, identity)
            control = await writer.scalar(select(TelegramCallbackRow).where(TelegramCallbackRow.group_id == row.parts[0]["callback_group_id"], TelegramCallbackRow.kind == "digest_item"))
            control.target_id = "f" * 32
    finally:
        release.set()
    await e.sender.drain()
    async with e.db.sessions() as reader:
        row = await reader.get(TelegramOutboxRow, identity)
        assert row.state == "failed" and row.failed_reason == "invalid-summary-view-receipt"
        controls = list(await reader.scalars(select(TelegramCallbackRow).where(TelegramCallbackRow.group_id == row.parts[0]["callback_group_id"])))
        assert all(control.message_id is None and control.payload["presentation_pending"] is True for control in controls)


@pytest.mark.asyncio
async def test_summary_show_reaches_its_owner_through_callback_ingress(work_delivery):
    from test_telegram_work_callback_dispatch import install_dispatch, press_work
    from test_telegram_work_control_admission import admissions

    from app.telegram_bots.summary_views import SummaryViews, SummaryViewSources
    from app.telegram_bots.work_outcome_delivery import WorkOutcomePlans
    from deerflow.persistence.telegram_bots.model import TelegramActRow

    e = work_delivery
    plans, _clock, _parent, callback_id = await teaser(e)
    views = SummaryViews(plans)
    install_dispatch(e, WorkOutcomePlans(e.plans, admissions(e).acts))
    e.channel._services.summary_views = views
    e.channel._services.summary_view_sources = SummaryViewSources(views)
    async with e.db.sessions() as reader:
        callback = await reader.get(TelegramCallbackRow, callback_id)
    await press_work(e, callback, message_id=7501, update_id=7601)
    await press_work(e, callback, message_id=7501, update_id=7601)
    async with e.db.sessions() as reader:
        rows = list(await reader.scalars(select(TelegramOutboxRow).where(TelegramOutboxRow.dedupe_key.like("notice:summary-show:%"))))
        assert len(rows) == 1 and rows[0].state == "pending"
        assert not list(await reader.scalars(select(TelegramActRow)))


@pytest.mark.asyncio
@pytest.mark.parametrize("damage", ["delete", "closed_reason"])
async def test_summary_view_ack_retains_exact_consumed_show_proof(work_delivery, damage):
    import asyncio

    e = work_delivery
    views, clock, parent, callback, identity = await pending_view(e)
    release = asyncio.Event()
    call = e.channel.bot.request.enqueue("editMessageText", release=release, result={"message_id": 7501, "date": 1893456000, "chat": {"id": e.link.chat_id, "type": "private"}, "text": "Full summary"})
    clock.advance(2)
    await e.sender.tick()
    try:
        await asyncio.wait_for(call.started.wait(), 5)
        async with e.db.sessions.begin() as writer:
            proof = await writer.get(TelegramCallbackRow, callback)
            if damage == "delete":
                await writer.delete(proof)
            else:
                proof.closed_reason = "superseded"
    finally:
        release.set()
    await e.sender.drain()
    async with e.db.sessions() as reader:
        row = await reader.get(TelegramOutboxRow, identity)
        assert row.state == "failed" and row.failed_reason == "invalid-summary-view-receipt", (row.state, row.failed_reason)
        controls = list(await reader.scalars(select(TelegramCallbackRow).where(TelegramCallbackRow.group_id == row.parts[0]["callback_group_id"])))
        assert controls and all(control.message_id is None and control.payload["presentation_pending"] is True for control in controls)
