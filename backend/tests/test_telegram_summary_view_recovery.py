"""Explicit summary Show survives current-source changes and deleted targets."""

import asyncio

import pytest
from _telegram_repository_test_support import db as db
from sqlalchemy import select
from test_telegram_bot_adapter import message
from test_telegram_summary_views import pending_view
from test_telegram_work_source_delivery import work_delivery as work_delivery
from test_work_attention_pages import work_pages as work_pages

from deerflow.persistence.telegram_bots.model import TelegramCallbackRow, TelegramOutboxRow, TelegramPersonSettingsRow


@pytest.mark.asyncio
@pytest.mark.parametrize("change", ["title", "locale"])
async def test_pending_summary_show_refreshes_current_source_with_original_age(work_delivery, change):
    from deerflow.persistence.agent_instances.work import AgentWorkRow

    e = work_delivery
    _views, clock, parent, _callback, identity = await pending_view(e)
    async with e.db.sessions.begin() as writer:
        if change == "title":
            (await writer.get(AgentWorkRow, e.work["id"])).objective = "Current Show title"
        else:
            (await writer.get(TelegramPersonSettingsRow, e.link.owner_user_id)).locale = "zh-CN"
    before = len(e.channel.bot.request.calls)
    clock.advance(2)
    assert await e.sender.tick() == 1
    await e.sender.drain()
    assert len(e.channel.bot.request.calls) == before
    async with e.db.sessions() as reader:
        row = await reader.get(TelegramOutboxRow, identity)
        original = await reader.get(TelegramOutboxRow, parent)
        assert row.state == "pending" and row.payload["presentation_version"] == 1, e.dispatch_errors
        controls = list(await reader.scalars(select(TelegramCallbackRow).where(TelegramCallbackRow.group_id == row.parts[-1]["callback_group_id"])))
        assert controls and all(control.created_at == original.created_at for control in controls)
    e.channel.bot.request.enqueue("editMessageText", result={"message_id": 7501, "date": 1893456000, "chat": {"id": e.link.chat_id, "type": "private"}, "text": "Fresh summary"})
    clock.advance(2)
    assert await e.sender.tick() == 1
    await e.sender.drain()
    async with e.db.sessions() as reader:
        assert (await reader.get(TelegramOutboxRow, identity)).state == "sent", e.dispatch_errors


@pytest.mark.asyncio
async def test_deleted_summary_show_target_repairs_as_a_new_protected_message(work_delivery):
    e = work_delivery
    views, clock, parent, _callback, identity = await pending_view(e)
    e.channel.bot.request.enqueue("editMessageText", status=400, body={"ok": False, "error_code": 400, "description": "Bad Request: message to edit not found"})
    clock.advance(2)
    assert await e.sender.tick() == 1
    await e.sender.drain()
    async with e.db.sessions() as reader:
        row = await reader.get(TelegramOutboxRow, identity)
        assert row.state == "failed" and row.failed_reason == "summary-view-refresh-required", e.dispatch_errors
        assert row.payload["presentation_version"] == 0
    live = await e.live.authority.prepare_live_owner(e.link.owner_user_id)
    guard = e.db.links.publication_guard(e.link, check_live_owner=live, check_attempt_current=lambda: True)
    assert await views.repair(e.link, identity, guard=guard) == identity
    message(e.channel.bot.request, identity=7701)
    clock.advance(2)
    assert await e.sender.tick() == 1
    await e.sender.drain()
    async with e.db.sessions() as reader:
        row = await reader.get(TelegramOutboxRow, identity)
        original = await reader.get(TelegramOutboxRow, parent)
        assert row.state == "sent" and row.parts[-1]["kind"] == "text", e.dispatch_errors
        assert row.parts[-1]["protect_content"] is True and row.parts[-1]["message_ids"] == [7701]
        assert original.message_id == 7501 and original.state == "sent"


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ["view", "daily"])
async def test_summary_admitted_sdk_drains_before_shutdown(work_delivery, kind):
    from test_telegram_summary_delivery import prepare

    e = work_delivery
    if kind == "view":
        _views, clock, _parent, _callback, _identity = await pending_view(e)
        clock.advance(2)
    else:
        plans = await prepare(e)
        await plans.plan(e.link)
    release = asyncio.Event()
    call = e.channel.bot.request.enqueue("editMessageText" if kind == "view" else "sendMessage", release=release, result={"message_id": 7501, "date": 1893456000, "chat": {"id": e.link.chat_id, "type": "private"}, "text": "Summary"})
    assert await e.sender.tick() == 1
    await asyncio.wait_for(call.started.wait(), 5)
    stopping = asyncio.create_task(e.sender.stop())
    try:
        await asyncio.sleep(0.05)
        assert not stopping.done() and not call.cancelled.is_set(), "admitted summary SDK was cancelled instead of draining"
    finally:
        release.set()
        await asyncio.wait_for(stopping, 5)
    assert not call.cancelled.is_set()


@pytest.mark.asyncio
@pytest.mark.parametrize("change", ["source", "claim", "group", "settings", "parent"])
async def test_summary_show_repair_rolls_back_when_provider_snapshot_changes(work_delivery, monkeypatch, change):
    from app.gateway import authz
    from app.telegram_bots.summary_sources import SummaryRead
    from deerflow.persistence.agent_instances.work import AgentWorkRow

    e = work_delivery
    views, clock, parent, _callback, identity = await pending_view(e)
    plans = views.plans
    before = len(e.channel.bot.request.calls)
    clock.advance(2)
    async with e.db.sessions.begin() as writer:
        (await writer.get(AgentWorkRow, e.work["id"])).objective = "Fresh title before repair"
    authorize, resolve = SummaryRead.authorize, authz.resolve_route_permissions
    held, changed = {}, []

    async def guarded(self, **kwargs):
        held["writer"] = kwargs.get("session")
        try:
            return await authorize(self, **kwargs)
        finally:
            held.pop("writer", None)

    async def provider(*args, **kwargs):
        permissions = await resolve(*args, **kwargs)
        writer = held.get("writer")
        if writer is not None and not changed:
            row = await writer.get(TelegramOutboxRow, identity)
            if change == "source":
                (await writer.get(AgentWorkRow, e.work["id"])).objective = "Changed during final admission"
            elif change == "claim":
                row.claim_id = "foreign"
            elif change == "settings":
                (await writer.get(TelegramPersonSettingsRow, e.link.owner_user_id)).locale = "zh-CN"
            elif change == "parent":
                (await writer.get(TelegramOutboxRow, parent)).message_id = 9999
            else:
                control = await writer.scalar(select(TelegramCallbackRow).where(TelegramCallbackRow.group_id == row.parts[-1]["callback_group_id"]))
                control.consumed_at, control.closed_reason = plans.outbox.clock(), "superseded"
            await writer.flush()
            changed.append(True)
        return permissions

    monkeypatch.setattr(SummaryRead, "authorize", guarded)
    monkeypatch.setattr(authz, "resolve_route_permissions", provider)
    assert await e.sender.tick() == 1
    await e.sender.drain()
    assert changed
    assert len(e.channel.bot.request.calls) == before
    async with e.db.sessions() as reader:
        row = await reader.get(TelegramOutboxRow, identity)
        assert row.payload["presentation_version"] == 0


@pytest.mark.asyncio
async def test_summary_show_held_repair_fences_final_failure_disposition(work_delivery, monkeypatch):
    from app.gateway import authz
    from app.telegram_bots.producer import ProductSourceRetired
    from app.telegram_bots.summary_sources import SummaryRead

    e = work_delivery
    views, _clock, _parent, _callback, identity = await pending_view(e)
    async with e.db.sessions.begin() as writer:
        row = await writer.get(TelegramOutboxRow, identity)
        row.state, row.failed_reason = "failed", "summary-view-refresh-required"
    authorize, resolve = SummaryRead.authorize, authz.resolve_route_permissions
    held, changed = {}, []

    async def guarded(self, **kwargs):
        held["writer"] = kwargs.get("session")
        try:
            return await authorize(self, **kwargs)
        finally:
            held.pop("writer", None)

    async def provider(*args, **kwargs):
        permissions = await resolve(*args, **kwargs)
        writer = held.get("writer")
        if writer is not None and not changed:
            row = await writer.get(TelegramOutboxRow, identity)
            if row.payload["presentation_version"] == 1:
                row.failed_reason = "invalid-summary-receipt"
                await writer.flush()
                changed.append(True)
        return permissions

    monkeypatch.setattr(SummaryRead, "authorize", guarded)
    monkeypatch.setattr(authz, "resolve_route_permissions", provider)
    live = await e.live.authority.prepare_live_owner(e.link.owner_user_id)
    guard = e.db.links.publication_guard(e.link, check_live_owner=live, check_attempt_current=lambda: True)
    with pytest.raises(ProductSourceRetired):
        await views.repair(e.link, identity, guard=guard)
    assert changed
    async with e.db.sessions() as reader:
        row = await reader.get(TelegramOutboxRow, identity)
        assert row.payload["presentation_version"] == 0 and row.failed_reason == "summary-view-refresh-required"


@pytest.mark.asyncio
async def test_summary_show_held_repair_cannot_take_an_active_claim(work_delivery):
    e = work_delivery
    recovery, _clock, _parent, _callback, identity = await pending_view(e)
    async with e.db.sessions.begin() as writer:
        row = await writer.get(TelegramOutboxRow, identity)
        row.state, row.failed_reason, row.claim_id = "failed", "summary-view-refresh-required", "foreign"
    live = await e.live.authority.prepare_live_owner(e.link.owner_user_id)
    guard = e.db.links.publication_guard(e.link, check_live_owner=live, check_attempt_current=lambda: True)
    assert await recovery.repair(e.link, identity, guard=guard) is None
    async with e.db.sessions.begin() as writer:
        (await writer.get(TelegramOutboxRow, identity)).claim_id = None
    assert await recovery.repair(e.link, identity, guard=guard) == identity
    async with e.db.sessions() as reader:
        row = await reader.get(TelegramOutboxRow, identity)
        assert row.state == "pending" and row.payload["presentation_version"] == 1


@pytest.mark.asyncio
async def test_summary_show_repeated_refresh_retains_valid_original_history(work_delivery):
    from deerflow.persistence.agent_instances.work import AgentWorkRow

    e = work_delivery
    _views, clock, parent, _callback, identity = await pending_view(e)
    for revision in range(2):
        async with e.db.sessions.begin() as writer:
            (await writer.get(AgentWorkRow, e.work["id"])).objective = f"Current summary title {revision}"
        clock.advance(2)
        assert await e.sender.tick() == 1
        await e.sender.drain()
    async with e.db.sessions() as reader:
        row = await reader.get(TelegramOutboxRow, identity)
        assert row.payload["presentation_version"] == 2, e.dispatch_errors
        assert len(row.parts) == 3 and len(row.payload["summary_view_basis"]["history"]) == 2
    e.channel.bot.request.enqueue("editMessageText", result={"message_id": 7501, "date": 1893456000, "chat": {"id": e.link.chat_id, "type": "private"}, "text": "Current summary"})
    clock.advance(2)
    assert await e.sender.tick() == 1
    await e.sender.drain()
    async with e.db.sessions() as reader:
        row = await reader.get(TelegramOutboxRow, identity)
        original = await reader.get(TelegramOutboxRow, parent)
        assert row.state == "sent", e.dispatch_errors
        controls = list(await reader.scalars(select(TelegramCallbackRow).where(TelegramCallbackRow.group_id == row.parts[-1]["callback_group_id"])))
        assert controls and all(control.created_at == original.created_at and control.message_id == 7501 for control in controls)


@pytest.mark.asyncio
async def test_summary_show_edit_falls_back_from_omitted_work_url(work_delivery):
    from copy import deepcopy
    from datetime import timedelta
    from uuid import uuid4

    from deerflow.persistence.agent_instances.human_input import HumanInputRequestRow
    from deerflow.persistence.agent_instances.work import AgentWorkRow

    e = work_delivery
    e.live.config.model_extra["telegram_bots"] = {"public_base_url": "https://app.example.test"}
    now = e.sender.outbox.clock()
    async with e.db.sessions.begin() as writer:
        work = await writer.get(AgentWorkRow, e.work["id"])
        request = await writer.get(HumanInputRequestRow, e.request["id"])
        for i in range(5):
            work_id, request_id, basis_id = uuid4().hex, uuid4().hex, uuid4().hex
            fields = {col.name: deepcopy(getattr(work, col.name)) for col in work.__table__.columns}
            fields.update(id=work_id, blocker={**fields["blocker"], "id": basis_id}, created_at=now - timedelta(minutes=i + 1))
            writer.add(AgentWorkRow(**fields))
            await writer.flush()
            fields = {col.name: deepcopy(getattr(request, col.name)) for col in request.__table__.columns}
            fields.update(id=request_id, work_id=work_id, basis_id=basis_id, created_at=now - timedelta(minutes=i + 1))
            writer.add(HumanInputRequestRow(**fields))
    _views, clock, _parent, _callback, identity = await pending_view(e)
    async with e.db.sessions() as reader:
        row = await reader.get(TelegramOutboxRow, identity)
        assert any(button.get("url") for line in row.payload["summary_view_basis"]["keyboard"] for button in line)
    e.channel.bot.request.enqueue("editMessageText", status=400, body={"ok": False, "error_code": 400, "description": "Bad Request: button url invalid"})
    clock.advance(2)
    await e.sender.tick()
    await e.sender.drain()
    async with e.db.sessions() as reader:
        row = await reader.get(TelegramOutboxRow, identity)
        assert row.state == "pending" and row.parts[-1].get("url_button_fallback") is True, (row.state, row.parts[-1], e.dispatch_errors)
    e.channel.bot.request.enqueue("editMessageText", result={"message_id": 7501, "date": 1893456000, "chat": {"id": e.link.chat_id, "type": "private"}, "text": "Full summary"})
    clock.advance(2)
    await e.sender.tick()
    await e.sender.drain()
    async with e.db.sessions() as reader:
        assert (await reader.get(TelegramOutboxRow, identity)).state == "sent", e.dispatch_errors
    calls = [call for call in e.channel.bot.request.calls if call.endpoint == "editMessageText"]
    assert "https://app.example.test" in calls[-1].parameters["text"]
